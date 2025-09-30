# raw.py
import os
import json
from datetime import date, timedelta
import pandas as pd
import yfinance as yf
from pathlib import Path

# 📁 설정값
CONFIG_PATH = "config/sp500_companies.csv"
RAW_DATA_DIR = Path("data/raw")
LOG_PATH = Path("logs/failed_tickers.json")

START_DATE = "2015-01-01"
END_DATE = (date.today() + timedelta(days=1)).isoformat()

# 폴더 생성
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

# 📄 S&P500 티커 로딩
sp500 = pd.read_csv(CONFIG_PATH)
tickers = sp500['Symbol'].dropna().unique().tolist()

# =============================
# 📥 RAW 데이터 수집 함수
# =============================
def fetch_raw_data(ticker: str):
    path = RAW_DATA_DIR / f"{ticker}.csv"
    if path.exists():
        print(f"[⏩] {ticker}: 이미 수집됨, 건너뜀")
        return True
    try:
        df = yf.download(ticker, start=START_DATE, end=END_DATE, auto_adjust=False, progress=False)
        if df.empty:
            print(f"[❌] {ticker}: 데이터 없음")
            return False
        df = df.reset_index()
        df.rename(columns={'Adj Close': 'Adj_Close'}, inplace=True)
        df['Raw_Close'] = df['Close']
        df = df[['Date','Open','High','Low','Raw_Close','Adj_Close','Volume']]
        df.to_csv(path, index=False)
        print(f"[✅] {ticker}: raw 데이터 수집 완료")
        return True
    except Exception as e:
        print(f"[💥] {ticker}: 수집 실패 → {e}")
        return False

# =============================
# 🔄 전체 티커 수집
# =============================
def run_raw_collection():
    failed = []
    for ticker in tickers:
        if not fetch_raw_data(ticker):
            failed.append(ticker)
    if failed:
        with open(LOG_PATH, "w", encoding="utf-8") as f:
            json.dump(failed, f, indent=2, ensure_ascii=False)
        print(f"[📌] 실패 티커 기록 → {LOG_PATH}")

if __name__ == "__main__":
    run_raw_collection()
