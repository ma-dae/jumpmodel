# feature_engineering.py
import pandas as pd
from pathlib import Path
import numpy as np

# 📁 경로 설정
CLEAN_DATA_DIR = Path("data/clean")
MERGE_DIR = Path("data/merged")
FEATURE_DIR = Path("data/features")
FEATURE_DIR.mkdir(parents=True, exist_ok=True)

# =============================
# 🔄 티커별 피처 계산 함수
# =============================
def compute_features(df):
    """
    📌 거래량, 가격, 이동평균, 변동성, 모멘텀 피처 계산
    """
    df = df.copy()
    
    # ---------------------
    # 거래량 / 거래대금
    # ---------------------
    df['volume_today'] = df['Volume']
    df['value_today'] = df['Adj_Close'] * df['Volume']
    df['volume_ratio_3m'] = df['Volume'] / df['Volume'].rolling(63).mean()  # 약 3개월
    df['volume_change_pct'] = df['Volume'].pct_change() * 100

    # ---------------------
    # 가격 / 캔들 구조
    # ---------------------
    df['price_change_pct'] = df['Adj_Close'].pct_change() * 100
    df['intraday_range_pct'] = (df['High'] - df['Low']) / df['Adj_Close'] * 100
    df['gap_open_pct'] = (df['Open'] - df['Adj_Close'].shift(1)) / df['Adj_Close'].shift(1) * 100
    df['candle_type'] = np.where(df['Adj_Close'] > df['Open'], '양봉', '음봉')

    # ---------------------
    # 이동평균 기반
    # ---------------------
    df['ma5'] = df['Adj_Close'].rolling(5).mean()
    df['ma20'] = df['Adj_Close'].rolling(20).mean()
    df['ma60'] = df['Adj_Close'].rolling(60).mean()
    df['ma5_cross_ma20'] = df['ma5'] > df['ma20']
    df['price_above_ma60'] = df['Adj_Close'] > df['ma60']
    df['ma_alignment_score'] = ((df['Adj_Close'] > df['ma5']) & (df['ma5'] > df['ma20']) & (df['ma20'] > df['ma60'])).astype(int)

    # ---------------------
    # 변동성
    # ---------------------
    df['atr_14'] = (df['High'] - df['Low']).rolling(14).mean()
    df['bollinger_band_width'] = (df['Adj_Close'].rolling(20).max() - df['Adj_Close'].rolling(20).min()) / df['Adj_Close'].rolling(20).mean()
    df['volatility_spike'] = df['intraday_range_pct'] > df['intraday_range_pct'].rolling(14).mean() * 1.5

    # ---------------------
    # 모멘텀
    # ---------------------
    delta = df['Adj_Close'].diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    roll_up = up.rolling(14).mean()
    roll_down = down.rolling(14).mean()
    rs = roll_up / roll_down
    df['rsi_14'] = 100 - (100 / (1 + rs))
    df['macd'] = df['Adj_Close'].ewm(span=12).mean() - df['Adj_Close'].ewm(span=26).mean()
    df['macd_signal_diff'] = df['macd'] - df['macd'].ewm(span=9).mean()
    df['stochastic_k'] = ((df['Adj_Close'] - df['Low'].rolling(14).min()) / (df['High'].rolling(14).max() - df['Low'].rolling(14).min())) * 100

    return df

# =============================
# 🔄 티커별 피처셋 생성
# =============================
def run_feature_engineering(tickers, use_news=True):
    for ticker in tickers:
        # merged 데이터가 있으면 뉴스 포함, 없으면 clean만
        merge_path = MERGE_DIR / f"{ticker}_merged.csv"
        clean_path = CLEAN_DATA_DIR / f"{ticker}.csv"

        if merge_path.exists() and use_news:
            print(f"[🔄] {ticker}: MERGED 데이터 로드 중 (뉴스 포함)")
            df = pd.read_csv(merge_path)
        elif clean_path.exists():
            print(f"[🔄] {ticker}: CLEAN 데이터 로드 중 (뉴스 없음)")
            df = pd.read_csv(clean_path)
        else:
            print(f"[❌] {ticker}: 데이터 없음, 건너뜀")
            continue

        # 피처 계산
        df_feat = compute_features(df)

        # 저장
        save_path = FEATURE_DIR / f"{ticker}_features.csv"
        df_feat.to_csv(save_path, index=False)
        print(f"[✅] {ticker}: 피처셋 저장 완료 → {save_path}")

    print("[🎉] 모든 티커 feature셋 생성 완료!")

# =============================
# 🔄 독립 실행
# =============================
if __name__ == "__main__":
    tickers = [f.stem for f in CLEAN_DATA_DIR.glob("*.csv")]
    run_feature_engineering(tickers)
