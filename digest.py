import os
import requests
from datetime import datetime
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # pip install backports.zoneinfo

TOKEN = os.environ.get("TELEGRAM_TOKEN") or os.environ.get("TG_TOKEN")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID") or os.environ.get("TG_CHAT_ID")

def send_message(text, parse_mode="HTML", disable_preview=True):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_preview
    }
    r = requests.post(url, data=payload, timeout=20)
    r.raise_for_status()
    return r.json()

def build_digest():
    now_msk = datetime.now(ZoneInfo("Europe/Moscow")).strftime("%Y-%m-%d %H:%M")
    lines = [
        f"<b>Дайджест</b> — {now_msk} МСК",
        "• Макро сегодня: пример — CPI/NFP/FOMC (боевой список подключу).",
        "• Биржи: листинги/техработы — пример.",
        "• Деривативы: экспирации, узлы OI — пример.",
        "• Разлоки: порог > $25–30M или >1% цирк. — пример.",
        "• План: риск‑чек; избегать входов за T‑5 к красным событиям."
    ]
    return "\n".join(lines)

if __name__ == "__main__":
    assert TOKEN and CHAT_ID, "Не заданы TELEGRAM_TOKEN/TG_TOKEN и TELEGRAM_CHAT_ID/TG_CHAT_ID"
    msg = build_digest()
    resp = send_message(msg)
    print("OK:", resp.get("ok"), "message_id:", resp.get("result", {}).get("message_id"))
