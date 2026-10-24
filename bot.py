# -*- coding: utf-8 -*-
"""
bot.py - Linh Bot chạy trên GitHub Actions (Long Polling).

Khác bản chạy trên laptop:
- Mọi khóa bí mật và nội dung nhân vật đọc từ GitHub Secrets (repo public không lộ gì).
- Bộ nhớ (lịch sử chat + mốc thời gian) lưu vào một repo RIÊNG TƯ khác qua GitHub API,
  vì máy chạy Actions bị xóa sạch sau mỗi lượt.
- Mỗi lượt chạy tối đa RUN_SECONDS (mặc định 5h40m) rồi tự thoát gọn gàng;
  workflow sẽ tự kích hoạt lượt kế tiếp để bot gần như chạy liên tục.
- Log công khai => tuyệt đối không ghi nội dung tin nhắn ra log.
"""

import base64
import json
import logging
import os
import random
import re
import signal
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

import requests


# ============================================================
# CẤU HÌNH - ĐỌC TỪ BIẾN MÔI TRƯỜNG (GitHub Secrets)
# ============================================================
def env(name, default=""):
    """Đọc biến môi trường; rỗng hoặc chưa đặt thì dùng giá trị mặc định."""
    return (os.environ.get(name) or default).strip()


TELEGRAM_BOT_TOKEN = env("TELEGRAM_BOT_TOKEN")
CHAT_ID = env("CHAT_ID")
DEEPSEEK_API_KEY = env("DEEPSEEK_API_KEY")
STATE_REPO = env("STATE_REPO")                  # Dạng "ten-github/linh-bot-data" (repo RIÊNG TƯ)
STATE_REPO_TOKEN = env("STATE_REPO_TOKEN")      # Token quyền Contents: Read and write cho repo đó

RUN_SECONDS = int(env("RUN_SECONDS", "20400"))  # Thời gian chạy mỗi lượt (5h40m < giới hạn 6h)
TZ_OFFSET_HOURS = float(env("TZ_OFFSET_HOURS", "7"))  # Múi giờ để tính giờ yên lặng (VN = +7)

# Nội dung nhân vật nằm trong Secrets. Giá trị mặc định dưới đây cố tình trung tính
# để mã nguồn public không chứa nội dung nhạy cảm.
DEFAULT_SYSTEM_PROMPT = (
    "Bạn là Linh, 25 tuổi, người yêu ảo ngọt ngào, vui tính. "
    "Xưng 'em', gọi người kia là 'anh'. Dùng các cử chỉ đặt trong dấu *. "
    "Trả lời tiếng Việt, 3-5 câu, dùng emoji vừa phải."
)
DEFAULT_PROACTIVE_INSTRUCTION = (
    "[Tình huống: đã khoảng {hours} giờ anh không nhắn tin cho em. "
    "Hãy nhắn trước một tin ngắn, ngọt ngào, nói em nhớ anh.]"
)
SYSTEM_PROMPT = env("SYSTEM_PROMPT", DEFAULT_SYSTEM_PROMPT)
STYLE_REMINDER = env("STYLE_REMINDER", "")      # Lời nhắc gắn vào cuối mỗi tin của chủ nhân (có thể để trống)
PROACTIVE_INSTRUCTION = env("PROACTIVE_INSTRUCTION", DEFAULT_PROACTIVE_INSTRUCTION)

# ============================================================
# HẰNG SỐ HỆ THỐNG
# ============================================================
DEEPSEEK_URL = "https://api.deepseek.com/chat/completions"
DEEPSEEK_MODEL = "deepseek-chat"
GH_API = "https://api.github.com"
STATE_FILE = "state.json"    # Tên file lưu bộ nhớ trong repo riêng tư

MEMORY_WINDOW = 14           # Số tin gần nhất gửi kèm làm ngữ cảnh mỗi lần gọi API
MAX_STORED_MESSAGES = 60     # Chỉ cần lưu hơn 14 tin một chút -> mỗi lần lưu rất nhẹ
POLL_TIMEOUT = 20            # Long Polling: Telegram giữ kết nối tối đa 20s nếu chưa có tin mới
RETRY_DELAY = 5              # Giây chờ khi lỗi mạng / lỗi API trước khi thử lại
MAX_RETRY_PER_UPDATE = 3     # Một tin nhắn lỗi tối đa 3 lần thì bỏ qua

# --- Tính năng Linh chủ động nhắn trước khi chủ nhân im lặng ---
PROACTIVE_ENABLED = True
PROACTIVE_MIN_IDLE = 2 * 3600
PROACTIVE_MAX_IDLE = 3 * 3600
MAX_UNANSWERED_PROACTIVE = 2     # Nhắn trước tối đa 2 tin liên tiếp nếu chưa được trả lời
QUIET_START_HOUR = 0             # Không nhắn trước từ 0h...
QUIET_END_HOUR = 7               # ...đến 7h sáng (theo TZ_OFFSET_HOURS)
PROACTIVE_RETRY_DELAY = 60


# ============================================================
# LOGGING (in ra console của Actions; log của repo public ai cũng xem được)
# ============================================================
class RedactFormatter(logging.Formatter):
    """Che token/API key trong log (kể cả traceback) phòng khi lộ qua thông báo lỗi."""

    def format(self, record):
        text = super().format(record)
        for secret in (TELEGRAM_BOT_TOKEN, DEEPSEEK_API_KEY, STATE_REPO_TOKEN):
            if secret:
                text = text.replace(secret, "***")
        return text


def setup_logging():
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(RedactFormatter("%(asctime)s [%(levelname)s] %(message)s"))
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)


session = requests.Session()  # Dùng lại kết nối -> nhanh và nhẹ


# ============================================================
# BỘ NHỚ: lưu trong repo riêng tư qua GitHub Contents API
# ============================================================
# store = toàn bộ trạng thái cần giữ giữa các lượt chạy
store = {
    "history": [],       # Danh sách {"role": "user"/"assistant", "content": "..."}
    "last_user": 0.0,    # Lần cuối chủ nhân nhắn
    "last_event": 0.0,   # Lần cuối có tương tác (chủ nhân nhắn HOẶC Linh nhắn trước)
    "unanswered": 0,     # Số tin nhắn trước liên tiếp chưa được trả lời
}
remote = {"sha": None, "dirty": False}  # sha của file trên repo; dirty = có thay đổi chưa lưu được


def gh_headers():
    return {
        "Authorization": "Bearer {}".format(STATE_REPO_TOKEN),
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def state_url():
    return "{}/repos/{}/contents/{}".format(GH_API, STATE_REPO, STATE_FILE)


def pull_state():
    """Tải state.json từ repo riêng tư. Trả về dict, hoặc None nếu file chưa tồn tại."""
    resp = session.get(state_url(), headers=gh_headers(), timeout=(5, 20))
    if resp.status_code == 404:
        remote["sha"] = None
        return None
    resp.raise_for_status()
    info = resp.json()
    remote["sha"] = info["sha"]
    return json.loads(base64.b64decode(info["content"]).decode("utf-8"))


def refresh_sha():
    """Lấy lại sha mới nhất của file (dùng khi PUT bị báo lệch phiên bản)."""
    resp = session.get(state_url(), headers=gh_headers(), timeout=(5, 20))
    remote["sha"] = resp.json().get("sha") if resp.status_code == 200 else None


def push_state():
    """
    Ghi store lên repo riêng tư (mỗi lần = 1 commit nhỏ). Trả về True nếu thành công.
    Lỗi chỉ ghi log và đánh dấu dirty để vòng lặp tự thử lưu lại, không làm sập bot.
    """
    text = json.dumps(store, ensure_ascii=False, indent=1)
    body = {
        "message": "state",
        "content": base64.b64encode(text.encode("utf-8")).decode("ascii"),
    }
    for attempt in range(2):
        if remote["sha"]:
            body["sha"] = remote["sha"]
        try:
            resp = session.put(state_url(), headers=gh_headers(), json=body, timeout=(5, 30))
            if resp.status_code in (409, 422) and attempt == 0:
                refresh_sha()   # sha lệch -> lấy sha mới rồi thử lại 1 lần
                continue
            resp.raise_for_status()
            remote["sha"] = resp.json()["content"]["sha"]
            remote["dirty"] = False
            return True
        except Exception as e:
            logging.error("Không lưu được bộ nhớ lên repo: %s", e)
            break
    remote["dirty"] = True
    return False


def load_store():
    """Nạp bộ nhớ từ repo riêng tư vào store (chưa có file thì giữ giá trị mặc định)."""
    data = pull_state()
    if not isinstance(data, dict):
        return
    history = data.get("history", [])
    if isinstance(history, list):
        store["history"] = [
            m for m in history
            if isinstance(m, dict)
            and m.get("role") in ("user", "assistant")
            and isinstance(m.get("content"), str)
        ]
    for key, cast in (("last_user", float), ("last_event", float), ("unanswered", int)):
        try:
            store[key] = cast(data.get(key, store[key]))
        except (TypeError, ValueError):
            pass


def load_history():
    return list(store["history"])


def save_history(history):
    """Cập nhật lịch sử trong store rồi đẩy lên repo riêng tư."""
    store["history"] = history[-MAX_STORED_MESSAGES:]
    push_state()


def startup_sync():
    """Kiểm tra repo dữ liệu + tải bộ nhớ. Lỗi mạng thử tối đa 3 lần; sai cấu hình thì dừng ngay."""
    repo_url = "{}/repos/{}".format(GH_API, STATE_REPO)
    for attempt in range(3):
        try:
            resp = session.get(repo_url, headers=gh_headers(), timeout=(5, 20))
            if resp.status_code in (401, 403, 404):
                logging.error(
                    "Không truy cập được repo dữ liệu (HTTP %s). Kiểm tra secret STATE_REPO và STATE_REPO_TOKEN.",
                    resp.status_code,
                )
                return False
            resp.raise_for_status()
            load_store()
            return True
        except Exception as e:
            logging.error("Lỗi khi tải bộ nhớ (lần %s): %s", attempt + 1, e)
            time.sleep(RETRY_DELAY)
    return False


# ============================================================
# TELEGRAM API
# ============================================================
def tg_call(method, payload=None, timeout=10):
    """Gọi một phương thức Telegram Bot API, ném lỗi nếu HTTP lỗi hoặc ok=false."""
    url = "https://api.telegram.org/bot{}/{}".format(TELEGRAM_BOT_TOKEN, method)
    resp = session.post(url, json=payload or {}, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError("Telegram trả về lỗi: {}".format(data))
    return data["result"]


def get_updates(offset):
    """Long Polling: Telegram giữ kết nối tới 20s, có tin mới là trả về ngay lập tức."""
    payload = {"timeout": POLL_TIMEOUT, "allowed_updates": ["message"]}
    if offset is not None:
        payload["offset"] = offset  # Xác nhận đã nhận các update trước đó
    return tg_call("getUpdates", payload, timeout=(5, POLL_TIMEOUT + 10))


def send_message(chat_id, text):
    # Không dùng parse_mode để dấu * trong cử chỉ không làm lỗi định dạng
    tg_call("sendMessage", {"chat_id": chat_id, "text": text[:4000]})


def send_typing(chat_id):
    """Hiện trạng thái 'đang nhập...'. Lỗi ở đây không quan trọng nên bỏ qua."""
    try:
        tg_call("sendChatAction", {"chat_id": chat_id, "action": "typing"}, timeout=5)
    except Exception:
        pass


# ============================================================
# DEEPSEEK API
# ============================================================
# Các mẫu nhận diện câu trả lời "vô nghĩa" do model sinh lỗi
CJK_PATTERN = re.compile(r"[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff\uac00-\ud7af]")  # chữ Trung/Nhật/Hàn
REPEAT_CHAR_PATTERN = re.compile(r"(.)\1{14,}")   # 1 ký tự lặp liên tiếp từ 15 lần
LEAK_MARKERS = ("[Nhắc Linh", "[Tình huống", "[BỐI CẢNH")      # model lỡ đọc lại chỉ dẫn nội bộ
VI_CHARS = set("àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ")


def find_problem(text):
    """Kiểm tra nhanh một câu trả lời có bị lỗi/vô nghĩa không. Trả về lý do, hoặc None nếu ổn."""
    if len(text) < 15:
        return "quá ngắn/rỗng"
    if CJK_PATTERN.search(text):
        return "lẫn chữ Trung/Nhật/Hàn"
    if any(marker in text for marker in LEAK_MARKERS):
        return "lộ chỉ dẫn nội bộ"
    if REPEAT_CHAR_PATTERN.search(text):
        return "lặp ký tự"
    if len(text) >= 60 and not any(ch in VI_CHARS for ch in text.lower()):
        return "không phải tiếng Việt"

    words = text.lower().split()
    if len(words) >= 30:
        if len(set(words)) / len(words) < 0.35:
            return "lặp từ"
        # Cùng một cụm 4 từ xuất hiện từ 4 lần trở lên = model đang kẹt vòng lặp
        grams = Counter(zip(words, words[1:], words[2:], words[3:]))
        if grams.most_common(1)[0][1] >= 4:
            return "lặp cụm từ"
    return None


def trim_incomplete(text):
    """Nếu câu trả lời bị cắt vì hết token, bỏ phần câu dở dang ở cuối để không đứt giữa chừng."""
    cut = max(text.rfind(ch) for ch in ".!?…")
    if cut > len(text) * 0.4:
        text = text[:cut + 1]
    # Dấu * lẻ nghĩa là cử chỉ bị bỏ dở -> cắt bỏ phần cử chỉ đó
    if text.count("*") % 2 == 1:
        text = text[:text.rfind("*")].rstrip()
    return text


def call_deepseek(messages):
    """
    Gửi messages tới DeepSeek. Câu trả lời lỗi/vô nghĩa bị loại và thử lại 1 lần
    với nhiệt độ thấp hơn. Lỗi HTTP/timeout sẽ ném exception cho vòng lặp chính xử lý.
    """
    last_problem = "không rõ"
    for attempt in range(2):
        payload = {
            "model": DEEPSEEK_MODEL,
            "messages": messages,
            "temperature": 1.0 if attempt == 0 else 0.7,   # Lần thử lại hạ nhiệt độ cho ổn định
            "top_p": 0.9,
            "max_tokens": 1000,
            "stream": False,
        }
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer {}".format(DEEPSEEK_API_KEY),
        }

        # (5s kết nối, 60s chờ phản hồi) - quá hạn sẽ ném lỗi để vòng lặp chính xử lý
        resp = session.post(DEEPSEEK_URL, headers=headers, json=payload, timeout=(5, 60))
        resp.raise_for_status()
        choice = resp.json()["choices"][0]
        content = (choice["message"].get("content") or "").strip()

        # Bị cắt vì hết token -> bỏ câu dở dang ở cuối
        if choice.get("finish_reason") == "length":
            content = trim_incomplete(content)

        problem = find_problem(content)
        if problem is None:
            return content

        # Log công khai: chỉ ghi lý do và độ dài, KHÔNG ghi nội dung
        last_problem = problem
        logging.warning("Loại câu trả lời lỗi (%s), lần %s, dài %s ký tự", problem, attempt + 1, len(content))

    # Lỗi cả 2 lần -> ném exception: KHÔNG gửi, KHÔNG lưu vào bộ nhớ, vòng lặp chính sẽ thử lại
    raise ValueError("DeepSeek trả lời lỗi 2 lần liền: {}".format(last_problem))


def build_context(history):
    """Ghép system prompt + tối đa 14 tin gần nhất làm ngữ cảnh."""
    window = history[-MEMORY_WINDOW:]
    # Bỏ các tin assistant nằm đầu cửa sổ để ngữ cảnh luôn bắt đầu bằng lượt của user
    while window and window[0]["role"] != "user":
        window = window[1:]

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    messages.extend(window)
    return messages


def with_reminder(text):
    """Gắn lời nhắc phong cách (nếu có) vào cuối nội dung gửi API; lịch sử vẫn lưu bản gốc."""
    return (text + "\n\n" + STYLE_REMINDER) if STYLE_REMINDER else text


def ask_deepseek(user_text, history):
    """Trả lời tin nhắn mới của chủ nhân."""
    messages = build_context(history)
    messages.append({"role": "user", "content": with_reminder(user_text)})
    return call_deepseek(messages)


def generate_proactive(history, idle_hours):
    """Tạo tin nhắn Linh tự gửi trước, dựa trên bối cảnh trò chuyện gần đây."""
    messages = build_context(history)
    instruction = PROACTIVE_INSTRUCTION.replace("{hours}", str(idle_hours))
    messages.append({"role": "user", "content": with_reminder(instruction)})
    return call_deepseek(messages)


# ============================================================
# NHẮN TRƯỚC CHỦ ĐỘNG (khi chủ nhân im lặng quá lâu)
# ============================================================
timer = {"next_at": 0.0}   # Thời điểm sớm nhất Linh được phép nhắn trước


def schedule_next(base):
    """Hẹn giờ nhắn trước kế tiếp: ngẫu nhiên 2-3 giờ tính từ mốc base."""
    timer["next_at"] = base + random.uniform(PROACTIVE_MIN_IDLE, PROACTIVE_MAX_IDLE)


def mark_user_activity():
    """Chủ nhân vừa nhắn: đặt lại đồng hồ im lặng và bộ đếm tin chưa được trả lời."""
    now = time.time()
    store["last_user"] = now
    store["last_event"] = now
    store["unanswered"] = 0
    schedule_next(now)


def local_hour():
    """Giờ hiện tại (0-23) theo múi giờ của bạn, vì máy chạy Actions dùng giờ UTC."""
    return datetime.now(timezone(timedelta(hours=TZ_OFFSET_HOURS))).hour


def in_quiet_hours(hour):
    """True nếu giờ nằm trong khung yên lặng (hỗ trợ khung qua nửa đêm, vd 23 -> 7)."""
    if QUIET_START_HOUR == QUIET_END_HOUR:
        return False
    if QUIET_START_HOUR < QUIET_END_HOUR:
        return QUIET_START_HOUR <= hour < QUIET_END_HOUR
    return hour >= QUIET_START_HOUR or hour < QUIET_END_HOUR


def maybe_send_proactive():
    """Gọi sau mỗi lần quét tin. Đến giờ thì Linh tự nhắn trước cho chủ nhân."""
    if not PROACTIVE_ENABLED:
        return

    now = time.time()
    if now < timer["next_at"]:
        return  # Chưa đến giờ

    if in_quiet_hours(local_hour()):
        return  # Không làm phiền ban đêm; hết giờ yên lặng sẽ tự gửi

    # Đã nhắn trước nhiều lần mà chủ nhân chưa trả lời -> dừng hẳn, tránh spam
    # (bộ đếm được lưu giữa các lượt chạy nên khởi động lại không làm nó về 0)
    if store["unanswered"] >= MAX_UNANSWERED_PROACTIVE:
        return

    idle_hours = max(1, round((now - (store["last_user"] or store["last_event"] or now)) / 3600))
    try:
        history = load_history()
        text = generate_proactive(history, idle_hours)
        send_message(CHAT_ID, text)

        # Chỉ lưu tin của Linh (không lưu chỉ dẫn nội bộ)
        history.append({"role": "assistant", "content": text})
        store["unanswered"] += 1
        store["last_event"] = now
        schedule_next(now)
        save_history(history)
        logging.info("Đã nhắn trước (chưa trả lời: %s)", store["unanswered"])
    except Exception as e:
        # Lỗi mạng / API: thử lại sau 1 phút, không làm sập vòng lặp
        logging.error("Lỗi khi nhắn trước: %s", e)
        timer["next_at"] = time.time() + PROACTIVE_RETRY_DELAY


# ============================================================
# XỬ LÝ TỪNG TIN NHẮN
# ============================================================
def handle_update(update):
    """
    Trả về True  nếu đã xử lý xong (hoặc bỏ qua hợp lệ) -> được phép sang tin kế tiếp.
    Trả về False nếu gặp lỗi tạm thời -> vòng lặp chính sẽ đợi 5s rồi thử lại tin này.
    """
    message = update.get("message")
    if not message:
        return True

    chat_id = message.get("chat", {}).get("id")

    # BẢO MẬT: người lạ nhắn vào bot sẽ bị im lặng bỏ qua (không ghi định danh vào log công khai)
    if str(chat_id) != str(CHAT_ID):
        logging.warning("Bỏ qua tin nhắn từ người lạ.")
        return True

    # Chủ nhân vừa nhắn -> đặt lại đồng hồ "im lặng" của tính năng nhắn trước
    mark_user_activity()

    text = message.get("text")
    if not text:
        send_message(chat_id, "Em chỉ đọc được tin nhắn chữ thôi anh ơi 😘")
        return True

    command = text.strip().lower()

    # Lệnh /start: chào mở đầu
    if command.startswith("/start"):
        send_message(chat_id, "Anh yêu~ em chờ anh nãy giờ đó 😘 *nhào vào lòng anh* Hôm nay của anh thế nào?")
        push_state()
        return True

    # Lệnh /reset: xóa toàn bộ bộ nhớ trò chuyện
    if command.startswith("/reset"):
        save_history([])
        send_message(chat_id, "Em quên hết rồi nè, mình bắt đầu lại từ đầu nhé anh 💕")
        return True

    send_typing(chat_id)

    history = load_history()                       # Đọc bộ nhớ
    reply = ask_deepseek(text, history)            # Gọi AI (lỗi sẽ ném exception ra ngoài)
    send_message(chat_id, reply)                   # Gửi trả lời về Telegram

    # Chỉ lưu vào bộ nhớ SAU KHI đã gửi thành công -> không bao giờ lưu nửa vời
    history.append({"role": "user", "content": text})
    history.append({"role": "assistant", "content": reply})
    save_history(history)
    return True


def notify_failure():
    """Báo nhẹ cho chủ nhân khi một tin nhắn lỗi liên tục và bị bỏ qua."""
    try:
        send_message(CHAT_ID, "Em bị chập chờn sóng một chút, anh nhắn lại giúp em nhé 💋")
    except Exception as e:
        logging.error("Không gửi được thông báo lỗi: %s", e)


# ============================================================
# VÒNG LẶP CHÍNH
# ============================================================
def shutdown(offset):
    """Dọn dẹp trước khi thoát: xác nhận tin đã xử lý với Telegram và lưu nốt bộ nhớ."""
    # Gọi getUpdates với offset để Telegram không gửi lại tin cuối cho lượt chạy kế tiếp
    if offset is not None:
        try:
            tg_call("getUpdates", {"offset": offset, "timeout": 0, "limit": 1, "allowed_updates": ["message"]})
        except Exception as e:
            logging.error("Không xác nhận được offset khi thoát: %s", e)
    if remote["dirty"]:
        push_state()


def main():
    missing = [
        name for name, value in (
            ("TELEGRAM_BOT_TOKEN", TELEGRAM_BOT_TOKEN),
            ("CHAT_ID", CHAT_ID),
            ("DEEPSEEK_API_KEY", DEEPSEEK_API_KEY),
            ("STATE_REPO", STATE_REPO),
            ("STATE_REPO_TOKEN", STATE_REPO_TOKEN),
        ) if not value
    ]
    if missing:
        logging.error("Thiếu secret: %s", ", ".join(missing))
        return 1

    deadline = time.time() + RUN_SECONDS
    logging.info("=== Bot khởi động (chạy tối đa %s giây) ===", RUN_SECONDS)

    if not startup_sync():
        return 1

    # getUpdates không dùng chung được với Webhook, nên xóa webhook cũ (nếu có)
    for attempt in range(5):
        try:
            tg_call("deleteWebhook", {"drop_pending_updates": False})
            break
        except Exception as e:
            logging.error("Chưa xóa được webhook (lần %s): %s", attempt + 1, e)
            time.sleep(RETRY_DELAY)
    else:
        return 1

    # Tiếp tục đồng hồ nhắn trước từ mốc tương tác cuối (đã lưu từ lượt chạy trước)
    schedule_next(store["last_event"] or time.time())

    offset = None       # update_id kế tiếp cần lấy
    fail_count = 0      # Số lần liên tiếp tin hiện tại bị lỗi

    try:
        while time.time() < deadline:
            try:
                updates = get_updates(offset)  # Treo tối đa 20s, có tin là trả về ngay

                for update in updates:
                    try:
                        ok = handle_update(update)
                    except Exception as e:
                        logging.error("Lỗi khi xử lý tin nhắn: %s", e)
                        ok = False

                    if ok:
                        offset = update["update_id"] + 1   # Xác nhận đã xử lý xong tin này
                        fail_count = 0
                        continue

                    # Tin bị lỗi: giữ nguyên offset để lần quét sau lấy lại đúng tin này
                    fail_count += 1
                    if fail_count >= MAX_RETRY_PER_UPDATE:
                        offset = update["update_id"] + 1   # Quá số lần thử -> bỏ qua tin này
                        fail_count = 0
                        logging.error("Bỏ qua một tin nhắn sau %s lần lỗi", MAX_RETRY_PER_UPDATE)
                        notify_failure()
                    else:
                        time.sleep(RETRY_DELAY)            # Đợi 5 giây rồi thử lại
                    break

                # Sau mỗi lần quét: kiểm tra đã đến lúc Linh nhắn trước chưa
                maybe_send_proactive()

                # Nếu lần lưu bộ nhớ trước bị lỗi mạng thì thử lưu lại
                if remote["dirty"]:
                    push_state()

            except Exception as e:
                # Mất mạng, Telegram timeout, lỗi bất ngờ... -> đợi 5s rồi quét tiếp, không sập
                logging.error("Lỗi vòng lặp (%s), thử lại sau %ss", e, RETRY_DELAY)
                time.sleep(RETRY_DELAY)
    finally:
        shutdown(offset)

    logging.info("=== Hết thời gian lượt chạy, thoát để workflow nối lượt kế tiếp ===")
    return 0


def on_sigterm(signum, frame):
    """Actions gửi SIGTERM khi hủy job -> đổi thành KeyboardInterrupt để thoát gọn và lưu bộ nhớ."""
    raise KeyboardInterrupt


if __name__ == "__main__":
    setup_logging()
    signal.signal(signal.SIGTERM, on_sigterm)
    try:
        code = main()
    except KeyboardInterrupt:
        logging.info("Bot dừng theo yêu cầu.")
        code = 0
    sys.exit(code)
