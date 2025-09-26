import pandas as pd
import yfinance as yf
from datetime import date, timedelta
import os
import json

# 📁 설정값
CONFIG_PATH = "config/sp500_companies.csv"
RAW_DATA_DIR = "data/raw"
LOG_PATH = "logs/failed_tickers.json"

# 📌 날짜 설정
START_DATE = "2015-01-01"
END_DATE = (date.today() + timedelta(days=1)).isoformat()

# 📂 저장 경로 생성
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs("logs", exist_ok=True)

# 📄 S&P500 기업 리스트 불러오기
try:
    sp500 = pd.read_csv(CONFIG_PATH)
    tickers = sp500['Symbol'].dropna().unique().tolist()
except Exception as e:
    raise FileNotFoundError(f"[❌] 기업 리스트 로딩 실패: {e}")

# 📦 중복 수집 방지: 이미 저장된 파일 확인
existing_files = set(os.listdir(RAW_DATA_DIR))
existing_tickers = {f.replace(".csv", "") for f in existing_files}

# 📉 실패한 종목 기록용
failed_tickers = []

# 📥 데이터 다운로드
for ticker in tickers:
    if ticker in existing_tickers:
        print(f"[⏩] {ticker}: 이미 수집됨, 건너뜀")
        continue

    try:
        df = yf.download(ticker, start=START_DATE, end=END_DATE, auto_adjust=False, progress=False)
        if df.empty:
            print(f"[❌] {ticker}: 데이터 없음")
            failed_tickers.append({"ticker": ticker, "reason": "empty"})
            continue

        df = df.reset_index()
        df.rename(columns={'Adj Close': 'Adj_Close'}, inplace=True)
        df['Raw_Close'] = df['Close']
        df = df[['Date', 'Open', 'High', 'Low', 'Raw_Close', 'Adj_Close', 'Volume']]

        df.to_csv(f"{RAW_DATA_DIR}/{ticker}.csv", index=False)
        print(f"[✅] {ticker}: 수집 완료")

    except Exception as e:
        print(f"[💥] {ticker}: 수집 실패 → {e}")
        failed_tickers.append({"ticker": ticker, "reason": str(e)})

# 📄 실패 로그 저장
if failed_tickers:
    with open(LOG_PATH, "w", encoding="utf-8") as f:
        json.dump(failed_tickers, f, indent=2, ensure_ascii=False)
    print(f"[📌] 실패한 종목 {len(failed_tickers)}개 기록됨 → {LOG_PATH}")