# bybit_futures.py — FINAL SYNC VERSION
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
from concurrent.futures import ThreadPoolExecutor
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
MAX_WORKERS = 10
BATCH_SLEEP = 2
interval_map = {"30s": "0.5", "1m": "1", "5m": "5", "15m": "15", "30m": "30", "1h": "60", "4h": "240", "1d": "D"}

# === INISIALISASI ===
session = HTTP(api_key=api_key, api_secret=api_secret)
app = Flask(__name__)
symbols = []
message_queue = Queue()

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.WARNING)

# === TELEGRAM ===
def telegram_worker():
    while True:
        if not message_queue.empty():
            msg = message_queue.get()
            try:
                send_telegram(msg)
            except Exception as e:
                logging.error("[Telegram] Gagal kirim: %s", e)
            time.sleep(2.5)
        else:
            time.sleep(0.1)

def send_telegram(message):
    url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
    payload = {"chat_id": tg_id, "text": message, "parse_mode": "Markdown"}
    res = requests.post(url, data=payload, timeout=10)
    if res.status_code == 429:
        retry = res.json().get("parameters", {}).get("retry_after", 5)
        logging.warning(f"[Telegram RATE LIMIT] retry after {retry}s")
        time.sleep(retry + 1)
        message_queue.put(message)
    elif res.status_code != 200:
        logging.error("[Telegram] Error: %s", res.text)

# === DATABASE ===
def init_db():
    with sqlite3.connect("signals.db") as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            timeframe TEXT,
            price REAL,
            signal TEXT,
            timestamp TEXT
        )''')
        conn.commit()

# === AMBIL SYMBOL ===
def fetch_symbols():
    try:
        resp = session.get_instruments_info(category=category)
        data = resp.get("result", {}).get("list", []) or []
        return [item["symbol"] for item in data if "USDT" in item["symbol"]][:MAX_SYMBOLS]
    except Exception as e:
        logging.error("[Fetch Symbol] Gagal: %s", e)
        return []

# === OHLCV ===
def fetch_ohlcv(symbol, interval, limit=200):
    for i in range(3):
        try:
            res = session.get_kline(category=category, symbol=symbol, interval=interval, limit=limit)
            df = pd.DataFrame(res["result"]["list"], columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"])
            df["timestamp"] = pd.to_datetime(df["timestamp"].astype(int), unit='ms')
            return df.astype(float).sort_values("timestamp")
        except Exception as e:
            logging.warning(f"[{symbol}] OHLCV Error ({i+1}/3): {e}")
            time.sleep(1 + i)
    return pd.DataFrame()

# === POLA CANDLE ===
def detect_candlestick_pattern(df):
    last = df.iloc[-1]
    body = abs(last.close - last.open)
    upper = last.high - max(last.close, last.open)
    lower = min(last.close, last.open) - last.low
    if lower > 2 * body and upper < body: return "🔨 Hammer"
    if upper > 2 * body and lower < body: return "⭐ Shooting Star"
    return "–"

# === ANALISIS ===
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
            elif latest.rsi < 70: tahan = "📆 Disarankan HOLD 1–3 Hari"
            else: tahan = "⚠️ Trend lemah"
        else:
            tahan = "⏳ Tidak disarankan ditahan"

        sniper = latest.close
        sdf = fetch_ohlcv(symbol, interval_map["4h"])
        swing = sdf.close.iloc[-1] if not sdf.empty else sniper

        msg = (
            f"📊 *BYBIT FUTURES*\n"
            f"{signal} Signal\n"
            f"Symbol: `{symbol}`\n"
            f"Timeframe: {tf}\n"
            f"Price: {sniper:.4f}\n"
            f"EMA20: {latest.ema20:.4f}\n"
            f"EMA50: {latest.ema50:.4f}\n"
            f"Bollinger Bands: {latest.bb_lower:.4f} - {latest.bb_upper:.4f}\n"
            f"RSI: {latest.rsi:.2f}\n"
            f"StochRSI: {latest.stochrsi:.2f}\n"
            f"MACD: {latest.macd:.4f}\n"
            f"ATR: {latest.atr:.4f}\n"
            f"Candlestick: {candle}\n"
            f"🎯 Masuk di harga (sniper): {sniper:.4f} [TF {tf}]\n"
            f"🔁 Masuk di harga (swing): {swing:.4f} [TF 4h]\n"
            f"{tahan}\n"
            f"🕒 Time: {latest.timestamp.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"✅ Valid @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        message_queue.put(msg)

        with sqlite3.connect("signals.db") as conn:
            conn.execute("INSERT INTO signals (symbol, timeframe, price, signal, timestamp) VALUES (?, ?, ?, ?, ?)",
                         (symbol, tf, sniper, signal, str(latest.timestamp)))
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

# === MAIN RUN ===
def run_bot():
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
    run_bot()
