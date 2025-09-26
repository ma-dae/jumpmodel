import pandas as pd
import numpy as np
from pathlib import Path

# 📁 경로 설정
RAW_DATA_DIR = Path("data/raw")
CLEAN_DATA_DIR = Path("data/clean")
CLEAN_DATA_DIR.mkdir(parents=True, exist_ok=True)

def preprocess_for_gupdeung(df: pd.DataFrame, ticker: str = "") -> pd.DataFrame:
    """
    📌 급등주 탐지 모델용 전처리 함수
    """

    required_cols = ["Date", "Open", "High", "Low", "Raw_Close", "Adj_Close", "Volume"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"[❌] {ticker}: 누락된 컬럼 {missing}")

    # 초기 복사본 생성 (경고 방지)
    df = df.copy()

    # 날짜 정제 + 변환
    df["Date"] = df["Date"].astype(str).str.strip()
    df = df[df["Date"].str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)]
    df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d", errors="coerce")

    # 수치형 변환
    for col in required_cols[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 정렬 및 중복 제거
    df = df.sort_values("Date").drop_duplicates(subset="Date")

    # 거래량 0 제거
    df = df[df["Volume"] > 0]

    # 가격 음수 제거
    for col in ["Open", "High", "Low", "Raw_Close", "Adj_Close"]:
        df = df[df[col] > 0]

    # OHLC 논리 검증
    df = df[
        (df["High"] >= df["Low"]) &
        (df["High"] >= df["Open"]) &
        (df["High"] >= df["Raw_Close"]) &
        (df["Low"] <= df["Open"]) &
        (df["Low"] <= df["Raw_Close"])
    ]

    # 필터링 후 새 복사본 생성 (중요!)
    df = df.copy()

    # 수익률 계산
    df["Return"] = df["Adj_Close"].pct_change()
    df["LogReturn"] = np.log(df["Adj_Close"] / df["Adj_Close"].shift(1))

    # 변동성 지표
    df["Volatility"] = df["High"] - df["Low"]

    # RSI 계산 (14일 기준)
    delta = df["Adj_Close"].diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(window=14).mean()
    avg_loss = pd.Series(loss).rolling(window=14).mean()
    
    # 0으로 나누기 방지
    rs = np.where(avg_loss != 0, avg_gain / avg_loss, 0)
    df["RSI_14"] = 100 - (100 / (1 + rs))

    # 결측치 및 무한값 제거
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["Return", "LogReturn", "RSI_14"])

    return df

def process_ticker(ticker: str):
    """개별 티커 처리"""
    path = RAW_DATA_DIR / f"{ticker}.csv"
    if not path.exists():
        print(f"[❌] {ticker}: 파일 없음")
        return False

    try:
        df_raw = pd.read_csv(path)
        df_clean = preprocess_for_gupdeung(df_raw, ticker)

        if len(df_clean) < 100:
            print(f"[⚠️] {ticker}: 데이터가 너무 적음 ({len(df_clean)}행)")

        df_clean.to_csv(CLEAN_DATA_DIR / f"{ticker}.csv", index=False)
        print(f"[✅] {ticker}: 전처리 완료 → {len(df_clean)}행")
        return True
        
    except Exception as e:
        print(f"[💥] {ticker}: 전처리 실패 → {e}")
        return False

def process_all():
    """모든 파일 처리"""
    files = list(RAW_DATA_DIR.glob("*.csv"))
    success_count = 0
    
    for file in files:
        ticker = file.stem
        if process_ticker(ticker):
            success_count += 1
    
    print(f"\n[📊] 전체 결과: {success_count}/{len(files)} 성공")

if __name__ == "__main__":
    process_all()