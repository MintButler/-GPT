#!/usr/bin/env python3
import os, sys, json, math, textwrap, time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import requests, feedparser
from dateutil import parser as dtp

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
    r = requests.post(url, data=data, timeout=30)
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

def fetch_json(url, params=None, timeout=25):
    try:
        r = requests.get(url, params=params or {}, timeout=timeout)
        if r.status_code == 200:
            return r.json()
        return None
    except Exception:
        return None

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
    except Exception:
        return []

# ----------------- Sections -----------------

def section_listings(cfg, tz):
    if not cfg["sections"]["listings"]["enabled"]:
        return {"today": [], "tomorrow": [], "errors": []}
    srcs = cfg["sources"].get("listings", {})
    items = []
    for name, url in srcs.items():
        it = fetch_rss_items(url, limit=40)
        for e in it:
            title_l = e["title"].lower()
            if any(k in title_l for k in ["will list", "lists", "listing", "launches", "new listing", "листинг", "запуст", "список"]):
                items.append({**e, "source": name})
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
    N = 5
    return {
        "today": sorted(today, key=lambda x: x["time"], reverse=True)[:N],
        "tomorrow": sorted(tomorrow, key=lambda x: x["time"], reverse=True)[:N],
        "errors": []
    }

def section_status(cfg, tz):
    srcs = cfg["sources"].get("status_pages", {})
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
        "today": sorted(today, key=lambda x: x["time"], reverse=True)[:N],
        "tomorrow": sorted(tomorrow, key=lambda x: x["time"], reverse=True)[:N],
        "errors": []
    }

def section_macro_forexfactory(cfg, tz):
    if not cfg["sections"]["macro"]["enabled"]:
        return {"today": [], "tomorrow": [], "note": "Макро выключено."}
    url = cfg["sources"]["macro"]["forex_factory"]
    data = fetch_json(url) or []
    regions = set(cfg["sections"]["macro"].get("regions", []))
    high_only = bool(cfg["sections"]["macro"].get("high_impact_only", False))
    lookahead_h = int(cfg["sections"]["macro"].get("lookahead_hours", 48))
    tzinfo = ZoneInfo(tz)
    now_local = now_utc().astimezone(tzinfo)
    end_local = now_local + timedelta(hours=lookahead_h)
    today_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1)

    # Region mapping
    region_countries = {
        "US": {"USD"},
        "UK": {"GBP"},
        "EU": {"EUR", "DEU", "DE", "FRA", "ITA", "ESP"}
    }
    allowed = set()
    for r in regions:
        allowed |= region_countries.get(r, set())

    def impact_pass(impact):
        imp = (impact or "").lower()
        if high_only:
            return "high" in imp
        return ("high" in imp) or ("medium" in imp)

    today, tomorrow = [], []
    for ev in data:
        try:
            # ForexFactory fields are commonly: "date", "title", "country", "impact", "timestamp"
            ts = ev.get("timestamp")
            if not ts:
                # fallback try parse 'date' if provided
                d = ev.get("date")
                if d:
                    et = dtp.parse(d)
                else:
                    continue
            else:
                et = datetime.fromtimestamp(int(ts), tz=timezone.utc)
            loc = et.astimezone(tzinfo)
            if not (now_local <= loc <= end_local):
                continue
            country = (ev.get("country") or "").upper()
            if allowed and country not in allowed:
                continue
            if not impact_pass(ev.get("impact")):
                continue
            title = (ev.get("title") or "").strip()
            actual = ev.get("actual")
            forecast = ev.get("forecast")
            previous = ev.get("previous")
            details = []
            if actual: details.append(f"факт: {actual}")
            if forecast: details.append(f"прогноз: {forecast}")
            if previous: details.append(f"пред.: {previous}")
            line = {
                "time": et,
                "title": f"{country} — {title}" + (f" ({'; '.join(details)})" if details else "")
            }
            if within_day(loc, today_start, today_end):
                today.append(line)
            else:
                tomorrow.append(line)
        except Exception:
            continue

    today = sorted(today, key=lambda x: x["time"])
    tomorrow = sorted(tomorrow, key=lambda x: x["time"])
    note = "" if (today or tomorrow) else "Нет макро-событий по фильтрам в горизонте 48ч."
    return {"today": today, "tomorrow": tomorrow, "note": note}

def binance_symbol_for(asset):
    return f"{asset.upper()}USDT"

def get_binance_funding(url, symbol):
    try:
        r = requests.get(url, params={"symbol": symbol, "limit": 1}, timeout=15)
        if r.status_code == 200:
            arr = r.json()
            if isinstance(arr, list) and arr:
                fr = float(arr[0].get("fundingRate", 0.0))
                ts = int(arr[0].get("fundingTime", 0)) // 1000
                return fr, datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        pass
    return None, None

def get_bybit_funding(url, symbol):
    try:
        r = requests.get(url, params={"category": "linear", "symbol": symbol, "limit": 1}, timeout=15)
        if r.status_code == 200:
            obj = r.json()
            if obj.get("retCode") == 0:
                lst = obj.get("result", {}).get("list", [])
                if lst:
                    fr = float(lst[0].get("fundingRate", 0.0))
                    ts = int(lst[0].get("fundingTime", 0)) // 1000
                    return fr, datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        pass
    return None, None

def get_binance_oi_change_pct(url, symbol):
    try:
        params = {"symbol": symbol, "period": "1d", "limit": 2}
        r = requests.get(url, params=params, timeout=20)
        if r.status_code == 200:
            arr = r.json()
            if isinstance(arr, list) and len(arr) >= 2:
                prev = float(arr[-2]["sumOpenInterest"])
                cur = float(arr[-1]["sumOpenInterest"])
                if prev > 0:
                    return (cur - prev) / prev * 100.0
    except Exception:
        pass
    return None

def section_derivatives(cfg, tz):
    if not cfg["sections"]["derivatives"]["enabled"]:
        return {"today": [], "tomorrow": [], "note": "Деривативы выключены."}

    tzinfo = ZoneInfo(tz)
    now_local = now_utc().astimezone(tzinfo)
    today_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1)
    tomorrow_end = today_start + timedelta(days=2)

    items_today, items_tomorrow = [], []

    # 1) Deribit expiry notional aggregation (BTC, ETH)
    try:
        base_url = cfg["sources"]["derivatives"]["deribit_book_summary"]
        for curr in ["BTC", "ETH"]:
            params = {"currency": curr, "kind": "option"}
            r = requests.get(base_url, params=params, timeout=25)
            if r.status_code != 200:
                continue
            res = r.json().get("result", [])
            # group by expiration
            buckets = {}
            for it in res:
                # Fields per item commonly: instrument_name, underlying_price, open_interest, expiration_timestamp
                exp_ts = it.get("expiration_timestamp")
                if not exp_ts:
                    # parse from instrument_name: BTC-30AUG24-60000-C
                    name = it.get("instrument_name", "")
                    try:
                        parts = name.split("-")
                        dt_part = parts[1]
                        exp = dtp.parse(dt_part)
                        exp_ts = int(exp.replace(tzinfo=timezone.utc).timestamp() * 1000)
                    except Exception:
                        continue
                exp_dt = datetime.fromtimestamp(int(exp_ts) / 1000, tz=timezone.utc)
                und = float(it.get("underlying_price") or 0.0)
                oi = float(it.get("open_interest") or 0.0)
                notional = oi * und
                buckets.setdefault(exp_dt, 0.0)
                buckets[exp_dt] += notional
            # Evaluate buckets against threshold and day grouping
            thr = float(cfg["sections"]["derivatives"].get("deribit_expiry_notional_min_usd", 2e8))
            for exp_dt, notional in buckets.items():
                if notional >= thr:
                    loc = exp_dt.astimezone(tzinfo)
                    line = {
                        "time": exp_dt,
                        "title": f"Deribit {curr} экспирация: ≈ ${int(notional):,}".replace(",", " ")
                    }
                    if within_day(loc, today_start, today_end):
                        items_today.append(line)
                    elif within_day(loc, today_end, tomorrow_end):
                        items_tomorrow.append(line)
    except Exception:
        pass

    # 2) Funding extremes (Binance/Bybit)
    funding_thr_bps = float(cfg["sections"]["derivatives"].get("funding_extreme_threshold_bps", 10))
    funding_exchanges = cfg["sections"]["derivatives"].get("funding_exchanges", ["binance", "bybit"])
    for asset in cfg["watchlists"]["tickers"]:
        sym_bin = binance_symbol_for(asset)
        # Binance
        if "binance" in funding_exchanges:
            fr, ts = get_binance_funding(cfg["sources"]["derivatives"]["binance_funding"], sym_bin)
            if fr is not None:
                bps = abs(fr) * 10000.0
                if bps >= funding_thr_bps:
                    line = {
                        "time": ts or now_utc(),
                        "title": f"Funding {asset} (Binance): {fr*100:.3f}% (~{bps:.1f} б.п.)"
                    }
                    loc = (ts or now_utc()).astimezone(tzinfo)
                    if within_day(loc, today_start, today_end):
                        items_today.append(line)
                    elif within_day(loc, today_end, tomorrow_end):
                        items_tomorrow.append(line)
        # Bybit
        if "bybit" in funding_exchanges:
            fr, ts = get_bybit_funding(cfg["sources"]["derivatives"]["bybit_funding"], sym_bin)
            if fr is not None:
                bps = abs(fr) * 10000.0
                if bps >= funding_thr_bps:
                    line = {
                        "time": ts or now_utc(),
                        "title": f"Funding {asset} (Bybit): {fr*100:.3f}% (~{bps:.1f} б.п.)"
                    }
                    loc = (ts or now_utc()).astimezone(tzinfo)
                    if within_day(loc, today_start, today_end):
                        items_today.append(line)
                    elif within_day(loc, today_end, tomorrow_end):
                        items_tomorrow.append(line)

    # 3) Binance OI 24h Δ
    oi_thr = float(cfg["sections"]["derivatives"].get("oi_change_threshold_pct", 10))
    for asset in cfg["watchlists"]["tickers"]:
        sym_bin = binance_symbol_for(asset)
        pct = get_binance_oi_change_pct(cfg["sources"]["derivatives"]["binance_oi_hist"], sym_bin)
        if pct is not None and abs(pct) >= oi_thr:
            line = {
                "time": now_utc(),
                "title
to be continued...
