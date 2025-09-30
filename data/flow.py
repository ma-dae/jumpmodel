# flow.py
import pandas as pd
from pathlib import Path

# 📁 경로 설정
CLEAN_DATA_DIR = Path("data/clean")
NEWS_DATA_DIR = Path("data/news")
MERGE_DIR = Path("data/merged")
MERGE_DIR.mkdir(parents=True, exist_ok=True)

# =============================
# 🔄 티커별 feature merge
# =============================
def merge_features(tickers):
    """
    📌 CLEAN 데이터 + 뉴스 감성 데이터 병합
        - 뉴스 데이터 없으면 CLEAN 데이터만 저장
        - 뉴스 데이터 있으면 Date 기준으로 병합
    """
    news_file = NEWS_DATA_DIR / "news_sentiment_features.csv"
    if news_file.exists():
        print(f"[📊] 뉴스 데이터 로드 중: {news_file}")
        news_df = pd.read_csv(news_file)
    else:
        print(f"[⚠️] 뉴스 데이터 없음 → {news_file}, 뉴스 컬럼 없이 CLEAN 데이터만 병합")
        news_df = None

    for ticker in tickers:
        clean_path = CLEAN_DATA_DIR / f"{ticker}.csv"
        if not clean_path.exists():
            print(f"[❌] {ticker}: 전처리 데이터 없음")
            continue

        print(f"[🔄] {ticker}: 전처리 데이터 로드 중...")
        df_clean = pd.read_csv(clean_path)

        if news_df is not None:
            # 뉴스 데이터 티커별 필터링
            df_news = news_df[news_df['Ticker'] == ticker]
            # merge on Date + Ticker
            df_merge = pd.merge(
                df_clean,
                df_news,
                how="left",
                left_on="Date",
                right_on="providerPublishTime"
            )
            # 필요없는 컬럼 제거
            df_merge.drop(columns=["Ticker", "providerPublishTime"], inplace=True, errors="ignore")
            print(f"[✅] {ticker}: feature merge 완료 → 뉴스 포함")
        else:
            df_merge = df_clean.copy()
            print(f"[✅] {ticker}: feature merge 완료 → 뉴스 없음")

        # 저장
        merge_path = MERGE_DIR / f"{ticker}_merged.csv"
        df_merge.to_csv(merge_path, index=False)

    print("[🎉] 모든 티커 feature merge 완료!")

# =============================
# 🔄 독립 실행
# =============================
if __name__ == "__main__":
    tickers = [f.stem for f in CLEAN_DATA_DIR.glob("*.csv")]
    merge_features(tickers)
