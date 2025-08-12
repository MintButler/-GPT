# notifier.py
import os, time, math, json, datetime, pytz, requests
from dateutil import tz

# ---- Настройки ----
MSK = pytz.timezone("Europe/Moscow")
BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
CHAT_ID = os.getenv("TG_CHAT_ID")

# Параметры дейтрейда (можно менять под себя)
WATCHLIST_TOP_N = int(os.getenv("WATCHLIST_TOP_N", 12))
WATCHLIST_MIN_QUOTE_VOL = float(os.getenv("WATCHLIST_MIN_QUOTE_VOL", 50_000_000))  # по котировочной валюте USDT
INTRADAY_TOP_BY_VOLUME = int(os.getenv("INTRADAY_TOP_BY_VOLUME", 40))  # скольким топ-парам считать 1ч-движение
INTRADAY_TOP_N = int(os.getenv("INTRADAY_TOP_N", 5))  # сколько самых сильных/слабых за 1ч

# ---- Утилиты ----
def get_json(url, params=None, headers=None, timeout=10):
    try:
        r = requests.get(url, params=params or {}, headers=headers or {}, timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None

def tg_send(text):
    if not BOT_TOKEN or not CHAT_ID:
        print("Set TG_BOT_TOKEN and TG_CHAT_ID")
        return
    # Telegram лимит 4096 символов; режем на чанки
    chunk = 3500
    for i in range(0, len(text), chunk):
        try:
            requests.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                params={"chat_id": CHAT_ID, "text": text[i:i+chunk]},
                timeout=10
            )
        except Exception:
            pass

# ---- Источники данных (без ключей) ----
def binance_ticker_24h(symbol):
    url = "https://api.binance.com/api/v3/ticker/24hr"
    j = get_json(url, {"symbol": symbol})
    if not j: return None
    try:
        return {
            "last": float(j["lastPrice"]),
            "ch24": float(j["priceChangePercent"]),
            "quote_vol": float(j.get("quoteVolume", 0))
        }
    except Exception:
        return None

def binance_price(symbol="ETHBTC"):
    url = "https://api.binance.com/api/v3/ticker/price"
    j = get_json(url, {"symbol": symbol})
    if j and "price" in j:
        return float(j["price"])
    return None

def binance_kline_pdhl(symbol="BTCUSDT", interval="1d"):
    # Предыдущий день High/Low (из двух последних дневных свечей)
    url = "https://api.binance.com/api/v3/klines"
    j = get_json(url, {"symbol": symbol, "interval": interval, "limit": 2})
    if not j or len(j) < 2:
        return None, None
    prev = j[-2]
    return float(prev[2]), float(prev[3])

def binance_funding(symbol="BTCUSDT"):
    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    j = get_json(url, {"symbol": symbol, "limit": 1})
    if j and len(j) > 0:
        try:
            return float(j[0].get("fundingRate", 0.0))
        except Exception:
            return None
    return None

def binance_open_interest_trend(symbol="BTCUSDT"):
    # Короткий тренд OI: последние 12 x 5m точек (~1 час)
    url = "https://fapi.binance.com/futures/data/openInterestHist"
    j = get_json(url, {"symbol": symbol, "period": "5m", "limit": 12})
    if not j: return None
    try:
        vals = [float(x["sumOpenInterest"]) for x in j]
        if len(vals) < 2: return None
        delta = vals[-1] - vals[0]
        pct = (delta / vals[0])*100 if vals[0] != 0 else 0.0
        direction = "↑" if delta > 0 else ("↓" if delta < 0 else "→")
        return {"last": vals[-1], "delta": delta, "pct": pct, "dir": direction}
    except Exception:
        return None

def binance_watchlist_usdt(top_n=12, min_quote_vol=50_000_000, exclude_leveraged=True):
    # Топ USDT-пары по ликвидности и 24ч моментуму
    url = "https://api.binance.com/api/v3/ticker/24hr"
    j = get_json(url)
    if not j: return []
    out = []
    for x in j:
        sym = x.get("symbol", "")
        if not sym.endswith("USDT"):
            continue
        if exclude_leveraged and any(t in sym for t in ("UP","DOWN","BULL","BEAR","3L","3S","5L","5S")):
            continue
        try:
            quote_vol = float(x.get("quoteVolume", 0))
            ch24 = float(x.get("priceChangePercent", 0))
            last_price = float(x.get("lastPrice", 0))
        except Exception:
            continue
        if quote_vol >= min_quote_vol and last_price > 0:
            out.append({"symbol": sym, "quote_vol": quote_vol, "ch24": ch24})
    out.sort(key=lambda z: (z["quote_vol"], z["ch24"]), reverse=True)
    return [o["symbol"] for o in out[:top_n]]

def binance_intraday_movers(top_by_volume=40, top_n=5):
    # Находим топ ликвидные USDT-пары и считаем 1ч-изменение (12 свечей по 5м)
    url = "https://api.binance.com/api/v3/ticker/24hr"
    j = get_json(url)
    if not j: return [], []
    rows = []
    for x in j:
        sym = x.get("symbol","")
        if not sym.endswith("USDT"): continue
        if any(t in sym for t in ("UP","DOWN","BULL","BEAR","3L","3S","5L","5S")): continue
        try:
            quote_vol = float(x.get("quoteVolume", 0))
            last_price = float(x.get("lastPrice", 0))
        except Exception:
            continue
        if quote_vol > 0 and last_price > 0:
            rows.append((sym, quote_vol))
    rows.sort(key=lambda z: z[1], reverse=True)
    pool = [r[0] for r in rows[:top_by_volume]]

    movers = []
    kline_url = "https://api.binance.com/api/v3/klines"
    for sym in pool:
        k = get_json(kline_url, {"symbol": sym, "interval": "5m", "limit": 12})
        if not k or len(k) < 2: continue
        try:
            first_open = float(k[0][1])
            last_close = float(k[-1][4])
            ch1h = (last_close/first_open - 1)*100 if first_open > 0 else 0.0
            movers.append((sym, ch1h))
        except Exception:
            continue
    if not movers:
        return [], []
    movers.sort(key=lambda z: z[1], reverse=True)
    top_pos = movers[:top_n]
    top_neg = sorted(movers[-top_n:], key=lambda z: z[1])  # самые слабые
    return top_pos, top_neg

def yfinance_macro():
    # Без ключей: UUP (DXY proxy), ES=F (S&P500 fut), ^VIX
    try:
        import yfinance as yf
    except Exception:
        return []
    syms = ["UUP", "ES=F", "^VIX"]
    out = []
    for s in syms:
        try:
            h = yf.Ticker(s).history(period="2d", interval="1d")
            if h is None or h.empty:
                continue
            closes = h["Close"].tolist()
            if len(closes) == 1:
                price = closes[-1]
                ch = 0.0
            else:
                price = closes[-1]
                prev = closes[-2]
                ch = (price/prev - 1)*100 if prev else 0.0
            out.append(f"{s}: {price:.2f} ({ch:+.2f}%)")
        except Exception:
            continue
    return out

# ---- Формирование сообщения ----
def fmt_usd(x):
    if x >= 1000:
        return f"{x:,.2f}".replace(",", " ")
    return f"{x:.2f}"

def build_message():
    now = datetime.datetime.now(MSK).strftime("%Y-%m-%d %H:%M")
    lines = [f"Крипто-дайджест (дейтрейд) {now} МСК"]

    # Рынок: BTC/ETH (Binance)
    btc = binance_ticker_24h("BTCUSDT")
    eth = binance_ticker_24h("ETHUSDT")
    lines.append("- Рынок:")
    if btc:
        lines.append(f"  • BTC: ${fmt_usd(btc['last'])} ({btc['ch24']:+.2f}% за 24ч)")
    else:
        lines.append("  • BTC: n/a")
    if eth:
        lines.append(f"  • ETH: ${fmt_usd(eth['last'])} ({eth['ch24']:+.2f}% за 24ч)")
    else:
        lines.append("  • ETH: n/a")

    # ETH/BTC (ротация)
    eb = binance_price("ETHBTC")
    if eb is not None:
        lines.append(f"  • ETH/BTC: {eb:.6f}")

    # PDH/PDL (вчера)
    for sym, tag in (("BTCUSDT","BTC"), ("ETHUSDT","ETH")):
        hi, lo = binance_kline_pdhl(sym, "1d")
        if hi is not None:
            lines.append(f"- {tag} уровни (вчера): High {hi:.2f} / Low {lo:.2f}")
        else:
            lines.append(f"- {tag} уровни: n/a")

    # Деривативы
    f_btc = binance_funding("BTCUSDT")
    f_eth = binance_funding("ETHUSDT")
    fb = f"{f_btc*100:.4f}%" if f_btc is not None else "n/a"
    fe = f"{f_eth*100:.4f}%" if f_eth is not None else "n/a"
    lines.append(f"- Фандинг (Binance Perp): BTC {fb} | ETH {fe}")

    oi_btc = binance_open_interest_trend("BTCUSDT")
    oi_eth = binance_open_interest_trend("ETHUSDT")
    if oi_btc:
        lines.append(f"  • OI BTC (час): {oi_btc['dir']} {oi_btc['pct']:+.2f}%")
    if oi_eth:
        lines.append(f"  • OI ETH (час): {oi_eth['dir']} {oi_eth['pct']:+.2f}%")

    # Макро без ключей (опционально)
    macro = yfinance_macro()
    if macro:
        lines.append("- Макро: " + " | ".join(macro))
    else:
        lines.append("- Макро: (yfinance недоступен)")

    # Динамический Watchlist (ликвидные USDT-пары)
    wl = binance_watchlist_usdt(top_n=WATCHLIST_TOP_N, min_quote_vol=WATCHLIST_MIN_QUOTE_VOL)
    if wl:
        lines.append("- Watchlist (ликвидные):")
        lines.append("  " + ", ".join(wl))
    else:
        lines.append("- Watchlist: n/a")

    # Интрадей-движки (последний час) — для дейтрейда
    pos, neg = binance_intraday_movers(top_by_volume=INTRADAY_TOP_BY_VOLUME, top_n=INTRADAY_TOP_N)
    if pos or neg:
        if pos:
            lines.append(f"- 1ч лонг-импульс: " + ", ".join([f"{s} {c:+.2f}%" for s,c in pos]))
        if neg:
            lines.append(f"- 1ч шорт-импульс: " + ", ".join([f"{s} {c:+.2f}%" for s,c in neg]))
    else:
        lines.append("- 1ч импульсы: n/a")

    # События
    lines.append("- События: без API-ключей — проверяйте https://coinmarketcal.com вручную")

    # Тактический чек-лист под дейтрейд
    lines.append("- Тактика:")
    lines.append("  • Следим за реакцией на PDH/PDL; удержание/ретест уровня даёт триггер, ложный пробой — инвалидация.")
    lines.append("  • ETH/BTC ↑ — повышаем фокус на альты; ETH/BTC ↓ — приоритет BTC/мейджоры.")
    lines.append("  • Фандинг и OI: рост OI при падающей цене и высоком позитивном фандинге — риск лонг-сквиза.")
    lines.append("  • Риск-менеджмент: фиксируйте стоп заранее, 1–2% риска на сделку, избегайте усреднений против тренда.")

    return "\n".join(lines)

if __name__ == "__main__":
    # В GitHub Actions используем: python notifier.py once
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "once":
        tg_send(build_message())
    else:
        # Для локальной отладки одноразовый запуск
        tg_send(build_message())
