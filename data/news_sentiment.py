# news_sentiment.py
import os
import json
import pandas as pd
import datetime
from pathlib import Path
from newsapi import NewsApiClient  # pip install newsapi-python

# 📁 환경설정
CONFIG_PATH = "config/sp500_companies.csv"
NEWS_DATA_DIR = Path("data/news")
NEWS_DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = NEWS_DATA_DIR / "failed_news_tickers.json"

# 📌 뉴스API 키 (환경변수 또는 직접 입력)
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY") or "524a1766bfd74c48aee3a384c0dea908"
newsapi = NewsApiClient(api_key=NEWSAPI_KEY)

# =============================
# 🔄 뉴스 수집 및 감성 분석
# =============================
def run_news_collection(days_back: int = 30):
    """S&P500 티커별 뉴스 수집 및 FinBERT 감성 분석"""
    
    # 티커 불러오기
    sp500 = pd.read_csv(CONFIG_PATH)
    tickers = sp500['Symbol'].dropna().unique().tolist()
    print(f"[📊] {len(tickers)}개 티커 로드 완료")

    failed_tickers = []
    all_news_df = pd.DataFrame()

    # 날짜 범위
    to_date = datetime.datetime.now()
    from_date = to_date - datetime.timedelta(days=days_back)
    from_str = from_date.strftime("%Y-%m-%d")
    to_str = to_date.strftime("%Y-%m-%d")

    # FinBERT 로딩
    try:
        from transformers import pipeline
        sentiment_pipeline = pipeline(
            "sentiment-analysis",
            model="yiyanghkust/finbert-tone",
            tokenizer="yiyanghkust/finbert-tone"
        )
        print("✅ FinBERT 모델 로드 완료")
    except Exception as e:
        print(f"[❌] FinBERT 로드 실패: {e}")
        return

    for i, ticker in enumerate(tickers, 1):
        print(f"\n[{i}/{len(tickers)}] {ticker}: 뉴스 수집 중...")
        try:
            news_results = newsapi.get_everything(
                q=ticker,
                from_param=from_str,
                to=to_str,
                language='en',
                sort_by='relevancy',
                page_size=100  # 최대 100개
            )

            articles = news_results.get('articles', [])
            if not articles:
                print(f"[⚠️] {ticker}: 뉴스 없음")
                failed_tickers.append({"ticker": ticker, "reason": "empty"})
                continue

            df = pd.DataFrame(articles)
            df['Ticker'] = ticker
            df = df[['Ticker', 'title', 'publishedAt']].rename(columns={'publishedAt': 'Date'})
            df['Date'] = pd.to_datetime(df['Date'])

            # 감성 분석
            sentiments = []
            for text in df['title']:
                result = sentiment_pipeline(text)[0]
                label = result['label'].lower()
                score = result['score']
                if label == "positive":
                    sentiments.append(score)
                elif label == "negative":
                    sentiments.append(-score)
                else:
                    sentiments.append(0.0)
            df['news_sentiment_score'] = sentiments

            # 긍정 키워드 등장 횟수
            df['positive_keywords_count'] = df['title'].str.count(r'\b(earnings|growth|merger|acquisition|contract)\b')

            all_news_df = pd.concat([all_news_df, df], ignore_index=True)
            print(f"[✅] {ticker}: {len(df)}개 뉴스 수집 및 감성 분석 완료")

        except Exception as e:
            print(f"[💥] {ticker}: 뉴스 수집 실패 → {e}")
            failed_tickers.append({"ticker": ticker, "reason": str(e)})

    # CSV 저장
    output_path = NEWS_DATA_DIR / "news_sentiment_features.csv"
    all_news_df.to_csv(output_path, index=False)
    print(f"\n💾 모든 뉴스 저장 완료 → {output_path}")
    print(f"📊 총 뉴스: {len(all_news_df)}개")

    # 실패 티커 기록
    if failed_tickers:
        with open(LOG_PATH, "w", encoding="utf-8") as f:
            json.dump(failed_tickers, f, indent=2, ensure_ascii=False)
        print(f"[📌] 실패 티커 {len(failed_tickers)}개 기록됨 → {LOG_PATH}")

# =============================
# 🔄 독립 실행
# =============================
if __name__ == "__main__":
    run_news_collection(days_back=30)
