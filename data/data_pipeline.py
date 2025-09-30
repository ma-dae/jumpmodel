# daily_pipeline.py
from raw import run_raw_collection
from preprocess import run_all_preprocess
from news_sentiment import run_news_collection
from flow import merge_features
from feature_engineering import run_feature_engineering
from pathlib import Path
import datetime

# 📁 경로 설정
CLEAN_DATA_DIR = Path("data/clean")

# =============================
# 🔄 하루 뉴스 티커 슬라이싱
# =============================
def get_ticker_slice(all_tickers, chunk_size=100):
    """
    📌 하루에 100개 티커만 뉴스 수집하도록 슬라이싱
    - 오늘 날짜 기준으로 "n일차"를 계산해서 구간 결정
    """
    today = datetime.date.today()
    day_number = (today - datetime.date(2025, 9, 30)).days + 1  # 기준 날짜
    total_chunks = (len(all_tickers) // chunk_size) + 1
    start = ((day_number - 1) % total_chunks) * chunk_size
    end = min(start + chunk_size, len(all_tickers))
    return all_tickers[start:end]

# =============================
# 🔄 Daily Pipeline
# =============================
if __name__ == "__main__":
    print("\n=== 1️⃣ RAW 데이터 수집 ===")
    run_raw_collection()

    print("\n=== 2️⃣ 전처리 단계 ===")
    all_tickers = [f.stem for f in Path("data/raw").glob("*.csv")]
    run_all_preprocess(all_tickers)

    print("\n=== 3️⃣ 뉴스 + 감성 분석 ===")
    selected_tickers = get_ticker_slice(all_tickers, chunk_size=100)
    print(f"[📰] 오늘 처리할 뉴스 티커 개수: {len(selected_tickers)}")
    print(f"[📰] 오늘 대상 티커: {selected_tickers[:5]} ...")  # 앞부분 샘플 출력
    run_news_collection(selected_tickers)

    print("\n=== 4️⃣ Feature merge ===")
    tickers_clean = [f.stem for f in CLEAN_DATA_DIR.glob("*.csv")]
    merge_features(tickers_clean)

    print("\n=== 5️⃣ Feature셋 생성 ===")
    tickers_merged = [f.stem.replace("_merged","") for f in Path("data/merged").glob("*.csv")]
    run_feature_engineering(tickers_merged, use_news=True)

    print("\n=== ✅ Daily pipeline 완료! ===")
