#!/usr/bin/env python3
import os, sys, json, math, textwrap, time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import requests, feedparser
from dateutil import parser as dtp

CONFIG_PATH = None

def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def now_utc():
    return datetime.now(timezone.utc)

def to_tz(dt, tz):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(ZoneInfo(tz))

def fmt_time(dt, tz):
    return to_tz(dt, tz).strftime("%d.%m %H:%M")

def split_chunks(text, max_len):
    parts = []
    buf = text
    while len(buf) > max_len:
        cut = buf.rfind("\n", 0, max_len)
        if cut == -1:
            cut = max_len
        parts.append(buf[:cut])
        buf = buf[cut:].lstrip("\n")
    if buf:
        parts.append(buf)
    return parts

def tg_send(token, chat_id, text, parse_mode="HTML", disable_preview=True):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": "true" if disable_preview else "false",
    }
    r = requests.post(url, data=data, timeout=20)
    if r.status_code >= 400:
        raise RuntimeError(f"Telegram send error {r.status_code}: {r.text}")
    return r.json()

def within_day(dt, day_start, day_end):
    return day_start <= dt < day_end

def normalize_entry_time(entry):
    # Try entry.published / updated / created
    for k in ("published", "updated", "created"):
        if k in entry and entry[k]:
            try:
                return dtp.parse(entry[k])
            except Exception:
                continue
    # Fallback: now
    return now_utc()

def fetch_rss_items(url, limit=50):
    try:
        feed = feedparser.parse(url)
        items = []
        for e in feed.entries[:limit]:
            t = normalize_entry_time(e)
            title = e.get("title", "").strip()
            link = e.get("link", "").strip()
            summary = (e.get("summary") or "").strip()
            items.append({"title": title, "link": link, "time": t, "summary": summary})
        return items
    except Exception as e:
        return []

def section_listings(cfg, tz):
    if not cfg["sections"]["listings"]["enabled"]:
        return {"today": [], "tomorrow": [], "errors": []}
    srcs = cfg["sources"].get("listings", {})
    items = []
    errors = []
    for name, url in srcs.items():
        it = fetch_rss_items(url, limit=40)
        for e in it:
            title = e["title"]
            if any(k in title.lower() for k in ["will list", "lists", "listing", "launches", "new listing", "listare", "листинг", "список"]):
                items.append({**e, "source": name})
    # Group by local day
    tzinfo = ZoneInfo(tz)
    now_local = now_utc().astimezone(tzinfo)
    today_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1)
    tomorrow_end = today_start + timedelta(days=2)

    today, tomorrow = [], []
    for e in items:
        lt = e["time"].astimezone(tzinfo)
        if within_day(lt, today_start, today_end):
            today.append(e)
        elif within_day(lt, today_end, tomorrow_end):
            tomorrow.append(e)

    # Keep up to N
    N = 5
    return {
        "today": sorted(today, key=lambda x: x["time"])[:N],
        "tomorrow": sorted(tomorrow, key=lambda x: x["time"])[:N],
        "errors": errors
    }

def section_status(cfg, tz):
    srcs = cfg["sources"].get("status_pages", {})
    items = []
    tzinfo = ZoneInfo(tz)
    now_local = now_utc().astimezone(tzinfo)
    today_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1)
    tomorrow_end = today_start + timedelta(days=2)
    today, tomorrow = [], []

    for name, url in srcs.items():
        for e in fetch_rss_items(url, limit=30):
            lt = e["time"].astimezone(tzinfo)
            entry = {**e, "source": name}
            if within_day(lt, today_start, today_end):
                today.append(entry)
            elif within_day(lt, today_end, tomorrow_end):
                tomorrow.append(entry)

    N = 5
    return {
        "today": sorted(today, key=lambda x: x["time"])[:N],
        "tomorrow": sorted(tomorrow, key=lambda x: x["time"])[:N],
        "errors": []
    }

def section_macro_placeholder(cfg):
    # Placeholder until API key is provided; keeps structure consistent.
    return {"today": [], "tomorrow": [], "note": "Подключу TradingEconomics/альтернативу при наличии ключа."}

def section_derivatives_placeholder(cfg):
    return {"today": [], "tomorrow": [], "note": "Deribit/CME подключу (экспирации, фандинг, OI); пороги учтены."}

def section_unlocks_placeholder(cfg):
    return {"today": [], "tomorrow": [], "note": "TokenUnlocks/альтернативы добавлю при ключе."}

def section_risk_placeholder(cfg):
    return {"today": [], "tomorrow": [], "note": "Инциденты/де-пеги добавлю при подключении источников."}

def render_section(title, data, tz):
    lines = []
    if data.get("today") or data.get("tomorrow"):
        lines.append(f"<b>{title}</b>")
        if data.get("today"):
            lines.append("Сегодня:")
            for e in data["today"]:
                t = fmt_time(e["time"], tz)
                lines.append(f"• {t} — {e['title']} ({e.get('source','')})")
                if e.get("link"):
                    lines.append(f"  {e['link']}")
        if data.get("tomorrow"):
            lines.append("Завтра:")
            for e in data["tomorrow"]:
                t = fmt_time(e["time"], tz)
                lines.append(f"• {t} — {e['title']} ({e.get('source','')})")
                if e.get("link"):
                    lines.append(f"  {e['link']}")
    else:
        note = data.get("note", "Нет событий по заданным фильтрам.")
        lines.append(f"<b>{title}</b>\n{note}")
    return "\n".join(lines)

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    cfg = load_config(args.config)
    tz = cfg["timezone"]
    chat_id = cfg["telegram"]["chat_id"]
    parse_mode = cfg["telegram"].get("parse_mode", "HTML")
    max_len = int(cfg["telegram"].get("max_message_length", 3500))
    split_msgs = bool(cfg["telegram"].get("split_long_messages", True))
    token = os.environ.get("TG_TOKEN")
    if not token:
        print("Missing TG_TOKEN env", file=sys.stderr)
        sys.exit(1)

    # DND check (optional override via input)
    dnd_cfg = cfg["schedule"].get("dnd", {"enabled": False})
    override_dnd = (os.environ.get("OVERRIDE_DND", "false").lower() == "true")
    if dnd_cfg.get("enabled") and not override_dnd:
        tzinfo = ZoneInfo(tz)
        now_local = now_utc().astimezone(tzinfo)
        fmt = "%H:%M"
        try:
            s = datetime.strptime(dnd_cfg["start"], "%H:%M").time()
            e = datetime.strptime(dnd_cfg["end"], "%H:%M").time()
            in_dnd = (s <= now_local.time() < e) if s < e else (now_local.time() >= s or now_local.time() < e)
            if in_dnd:
                print("DND window active, skipping send")
                return
        except Exception:
            pass

    today_local = to_tz(now_utc(), tz).strftime("%d.%m.%Y")
    suffix = os.environ.get("MESSAGE_SUFFIX", "")
    header = f"Ежедневный дайджест • {today_local}"
    if suffix:
        header += f" • {suffix}"

    # Collect sections
    macro = section_macro_placeholder(cfg)
    listings = section_listings(cfg, tz)
    derivatives = section_derivatives_placeholder(cfg)
    unlocks = section_unlocks_placeholder(cfg)
    status = section_status(cfg, tz)
    risk = section_risk_placeholder(cfg)

    parts = [
        f"<b>{header}</b>",
        render_section("Макро", macro, tz),
        render_section("Листинги", listings, tz),
        render_section("Деривативы", derivatives, tz),
        render_section("Разлоки", unlocks, tz),
        render_section("Сети/Статусы", status, tz),
        render_section("Риски/Инциденты", risk, tz),
    ]
    message = "\n\n".join([p for p in parts if p.strip()])

    if split_msgs and len(message) > max_len:
        chunks = split_chunks(message, max_len)
        for i, ch in enumerate(chunks, 1):
            suffix = f" ({i}/{len(chunks)})" if len(chunks) > 1 else ""
            tg_send(token, chat_id, ch + suffix, parse_mode=parse_mode)
            time.sleep(0.7)
    else:
        tg_send(token, chat_id, message, parse_mode=parse_mode)

if __name__ == "__main__":
    main()
