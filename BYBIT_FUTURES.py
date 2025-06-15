import os
import time
import sqlite3
import requests
import threading
import pandas as pd
import schedule
import logging
from datetime import datetime
from flask import Flask
from ta.trend import EMAIndicator, MACD
from ta.momentum import RSIIndicator, StochRSIIndicator
from ta.volatility import AverageTrueRange, BollingerBands
from pybit.unified_trading import HTTP
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# === KONFIGURASI ===
api_key = os.getenv("BYBIT_API_KEY", "R4Nv7r38iigntfHjYe")
api_secret = os.getenv("BYBIT_API_SECRET", "aeK29VCgC6dKJrUuvouHAlPSKtzAzSqauUPz")
tg_token = os.getenv("TG_TOKEN", "8153894385:AAFa4kbNHTlDkJ0Fq2BWFk-jyZ0k9rxbk5k")
tg_id = os.getenv("TG_CHAT_ID", "7153166439")
category = "linear"
timeframes = ["1m", "5m", "15m", "30m", "1h"]
MAX_SYMBOLS = 100
BATCH_SLEEP = 2
MAX_WORKERS = 10
interval_map = {
    "30s": "0.5", "1m": "1", "5m": "5",
    "15m": "15", "30m": "30", "1h": "60",
    "4h": "240", "6h": "360", "1d": "D"
}

# === INISIALISASI ===
session = HTTP(api_key=api_key, api_secret=api_secret)
app = Flask(__name__)
symbols = []
message_queue = Queue()
MAX_MESSAGES_PER_SECOND = 1

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.WARNING
)

# === TELEGRAM SENDER ===
def telegram_worker():
    while True:
        if not message_queue.empty():
            msg = message_queue.get()
            send_telegram(msg)
            time.sleep(1 / MAX_MESSAGES_PER_SECOND)
        else:
            time.sleep(0.1)

def send_telegram(message):
    url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
    payload = {"chat_id": tg_id, "text": message, "parse_mode": "HTML"}
    try:
        print("[TELEGRAM]", datetime.now(), "->", message.splitlines()[0])
        res = requests.post(url, data=payload, timeout=10)
        if res.status_code == 429:
            retry = res.json().get("parameters", {}).get("retry_after", 5)
            logging.warning(f"[RATE LIMIT] Retry after {retry}s")
            time.sleep(retry + 1)
            message_queue.put(message)
        elif res.status_code != 200:
            logging.error("Telegram error: %s", res.text)
    except Exception as e:
        logging.error("Telegram exception: %s", e)

# === DATABASE ===
def init_db():
    with sqlite3.connect("signals.db") as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                timeframe TEXT,
                price REAL,
                signal TEXT,
                timestamp TEXT
            )
        ''')
        conn.commit()

# === FETCH SYMBOLS ===
def fetch_symbols():
    try:
        resp = session.get_instruments_info(category=category)
        lst = resp.get("result", {}).get("list", []) or []
        return [i["symbol"] for i in lst if "USDT" in i["symbol"]][:MAX_SYMBOLS]
    except Exception as e:
        logging.error("Symbol fetch failed: %s", e)
        return []

# === FETCH OHLCV ===
def fetch_ohlcv(symbol, interval, limit=200):
    for attempt in range(3):
        try:
            res = session.get_kline(category=category, symbol=symbol, interval=interval, limit=limit)
            df = pd.DataFrame(res["result"]["list"], columns=[
                "timestamp", "open", "high", "low", "close", "volume", "turnover"])
            df["timestamp"] = pd.to_datetime(df["timestamp"].astype(int), unit='ms')
            return df.astype(float).sort_values("timestamp")
        except Exception as e:
            logging.warning(f"[{symbol}] OHLCV fetch failed ({attempt+1}/3): {e}")
            time.sleep(1 + attempt)
    return pd.DataFrame()

# === CANDLESTICK PATTERN ===
def detect_candlestick_pattern(df):
    last = df.iloc[-1]
    body = abs(last.close - last.open)
    upper = last.high - max(last.close, last.open)
    lower = min(last.close, last.open) - last.low
    if lower > 2 * body and upper < body: return "🔨 Hammer"
    if upper > 2 * body and lower < body: return "⭐ Shooting Star"
    return "–"

# === STRATEGI ANALISIS ===
def analyze_symbol(symbol, tf):
    df = fetch_ohlcv(symbol, interval_map.get(tf))
    if df.empty or len(df) < 50: return
    try:
        df["ema20"] = EMAIndicator(df.close, 20).ema_indicator()
        df["ema50"] = EMAIndicator(df.close, 50).ema_indicator()
        df["rsi"] = RSIIndicator(df.close).rsi()
        df["macd"] = MACD(df.close).macd_diff()
        df["atr"] = AverageTrueRange(df.high, df.low, df.close).average_true_range()
        df["stochrsi"] = StochRSIIndicator(df.close).stochrsi()
        bb = BollingerBands(df.close)
        df["bb_upper"] = bb.bollinger_hband()
        df["bb_lower"] = bb.bollinger_lband()

        latest = df.iloc[-1]
        candle = detect_candlestick_pattern(df)

        signal = ""
        if latest.ema20 > latest.ema50 and latest.rsi < 70:
            signal = "📈 BUY"
        elif latest.ema20 < latest.ema50 and latest.rsi > 30:
            signal = "📉 SELL"

        if not signal: return

        tahan = "–"
        if latest.ema20 >= latest.ema50:
            if latest.rsi < 50: tahan = "📆 Potensi 1–2 Hari"
            elif latest.rsi < 70: tahan = "📆 HOLD 1–3 Hari"
            else: tahan = "⚠️ Trend lemah"
        else:
            tahan = "⏳ Tidak disarankan ditahan"

        sniper = latest.close
        sdf = fetch_ohlcv(symbol, interval_map["4h"])
        swing = sdf.close.iloc[-1] if not sdf.empty else sniper

        msg = (
            f"📊 **BYBIT FUTURES**\n"
            f"📈 {signal.upper()} Signal\n"
            f"<b>{signal} Signal</b>\n"
            f"Symbol: <code>{symbol}</code>\n"
            f"TF: <code>{tf}</code> | Price: <code>{sniper:.6f}</code>\n"
            f"EMA20: <code>{latest.ema20:.6f}</code> | EMA50: <code>{latest.ema50:.6f}</code>\n"
            f"Bollinger: <code>{latest.bb_lower:.6f} - {latest.bb_upper:.6f}</code>\n"
            f"RSI: <code>{latest.rsi:.2f}</code> | StochRSI: <code>{latest.stochrsi:.2f}</code>\n"
            f"MACD: <code>{latest.macd:.6f}</code> | ATR: <code>{latest.atr:.6f}</code>\n"
            f"Candle: {candle}\n"
            f"🎯 Sniper: <code>{sniper:.6f}</code> | 🔁 Swing: <code>{swing:.6f}</code>\n"
            f"{tahan}\n"
            f"🕒 {latest.timestamp}\n"
            f"✅ Valid by RSIbantaiBot @ {datetime.now():%Y-%m-%d %H:%M:%S}"
        )
        message_queue.put(msg)

        with sqlite3.connect("signals.db") as conn:
            conn.execute(
                "INSERT INTO signals (symbol, timeframe, price, signal, timestamp) VALUES (?, ?, ?, ?, ?)",
                (symbol, tf, sniper, signal, str(latest.timestamp))
            )
            conn.commit()
    except Exception as e:
        logging.error(f"[{symbol} {tf}] Error: {e}")

# === JOB SCHEDULER ===
def job():
    if not symbols: return
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for tf in timeframes:
            for sym in symbols:
                ex.submit(analyze_symbol, sym, tf)
    time.sleep(BATCH_SLEEP)

# === ENTRY POINT ===
def main():
    init_db()
    time.sleep(1)
    global symbols
    symbols = fetch_symbols()
    if symbols:
        logging.warning(f"Loaded {len(symbols)} symbols")
        threading.Thread(target=telegram_worker, daemon=True).start()
        schedule.every(60).seconds.do(job)
        while True:
            schedule.run_pending()
            time.sleep(1)
    else:
        logging.error("No symbols fetched, exiting")

if __name__ == "__main__":
    main()
