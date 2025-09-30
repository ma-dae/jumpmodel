# preprocess.py
import pandas as pd
import numpy as np
from pathlib import Path
import os

# 📁 경로 설정
RAW_DATA_DIR = Path("data/raw")
CLEAN_DATA_DIR = Path("data/clean")
CLEAN_DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = Path("logs/failed_preprocess_tickers.json")
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

# =============================
# 🛠️ 티커 전처리 함수
# =============================
def preprocess_ticker(ticker: str):
    raw_path = RAW_DATA_DIR / f"{ticker}.csv"
    clean_path = CLEAN_DATA_DIR / f"{ticker}.csv"
    if not raw_path.exists():
        print(f"[❌] {ticker}: raw 파일 없음")
        return False
    try:
        df = pd.read_csv(raw_path)
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        for col in ["Open","High","Low","Raw_Close","Adj_Close","Volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df[df["Volume"]>0]
        for col in ["Open","High","Low","Raw_Close","Adj_Close"]:
            df = df[df[col]>0]
        df = df.sort_values("Date").drop_duplicates(subset="Date").copy()

        # 수익률, 변동성, RSI 계산
        df["Return"] = df["Adj_Close"].pct_change()
        df["LogReturn"] = np.log(df["Adj_Close"]/df["Adj_Close"].shift(1))
        df["Volatility"] = df["High"] - df["Low"]
        delta = df["Adj_Close"].diff()
        gain = np.where(delta>0, delta, 0)
        loss = np.where(delta<0, -delta, 0)
        avg_gain = pd.Series(gain).rolling(window=14).mean()
        avg_loss = pd.Series(loss).rolling(window=14).mean()
        rs = np.where(avg_loss!=0, avg_gain/avg_loss, 0)
        df["RSI_14"] = 100 - (100 / (1 + rs))

        df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["Return","LogReturn","RSI_14"])
        df.to_csv(clean_path, index=False)
        print(f"[✅] {ticker}: 전처리 완료 → {len(df)}행")
        return True
    except Exception as e:
        print(f"[💥] {ticker}: 전처리 실패 → {e}")
        return False

# =============================
# 🔄 전체 전처리 실행
# =============================
def run_all_preprocess(tickers):
    failed = []
    for ticker in tickers:
        if not preprocess_ticker(ticker):
            failed.append(ticker)
    if failed:
        with open(LOG_PATH, "w", encoding="utf-8") as f:
            json.dump(failed, f, indent=2, ensure_ascii=False)
        print(f"[📌] 전처리 실패 티커 기록 → {LOG_PATH}")

if __name__ == "__main__":
    # 기본적으로 raw에 있는 모든 csv 대상으로 실행
    all_tickers = [f.stem for f in RAW_DATA_DIR.glob("*.csv")]
    run_all_preprocess(all_tickers)
