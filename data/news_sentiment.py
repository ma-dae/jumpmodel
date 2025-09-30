# news_sentiment.py
import pandas as pd
import requests
from datetime import date, timedelta
from pathlib import Path
import os
import json

# 📁 경로 설정
CONFIG_PATH = "config/sp500_companies.csv"
NEWS_DIR = Path("data/news")
NEWS_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = NEWS_DIR / "failed_tickers.json"

# 📌 날짜 설정
TODAY = date.today()
FROM_DATE = (TODAY - timedelta(days=7)).isoformat()
TO_DATE = TODAY.isoformat()

# 📄 API Key 설정 (NewsAPI)
NEWS_API_KEY = os.getenv("524a1766bfd74c48aee3a384c0dea908")  # 환경변수로 설정 권장
BASE_URL = "https://newsapi.org/v2/everything"

# 📄 S&P500 티커 불러오기
try:
    sp500 = pd.read_csv(CONFIG_PATH)
    tickers = sp500['Symbol'].dropna().tolist()
except Exception as e:
    raise FileNotFoundError(f"[❌] 기업 리스트 로딩 실패: {e}")

# =============================
# 🔄 뉴스 수집 함수 (티커별)
# =============================
def fetch_news_newsapi(ticker):
    print(f"📰 {ticker}: 뉴스 수집 중...")
    params = {
        "q": ticker,
        "from": FROM_DATE,
        "to": TO_DATE,
        "sortBy": "publishedAt",
        "language": "en",
        "apiKey": NEWS_API_KEY,
        "pageSize": 100
    }
    try:
        response = requests.get(BASE_URL, params=params)
        data = response.json()
        if data.get("status") != "ok":
            raise ValueError(data.get("message", "API 오류"))

        articles = data.get("articles", [])
        if not articles:
            print(f"   ⚠️ {ticker}: 뉴스 없음")
            return pd.DataFrame()

        df = pd.DataFrame(articles)
        df['Ticker'] = ticker
        df = df.rename(columns={'publishedAt': 'providerPublishTime'})
        df['providerPublishTime'] = pd.to_datetime(df['providerPublishTime'])
        print(f"   ✅ {ticker}: {len(df)}개 뉴스 수집 완료")
        return df[['Ticker', 'title', 'providerPublishTime']]

    except Exception as e:
        print(f"   ❌ {ticker} 수집 실패 → {e}")
        return pd.DataFrame()

# =============================
# 🔄 하루 100개씩 슬라이싱
# =============================
def get_today_tickers(chunk_size=100):
    chunks = [tickers[i:i + chunk_size] for i in range(0, len(tickers), chunk_size)]
    today_index = TODAY.toordinal() % len(chunks)
    return chunks[today_index]

# =============================
# 🔄 전체 뉴스 수집 함수
# =============================
def run_news_collection(selected_tickers=None):
    """
    📌 뉴스 전체 수집 실행
    - selected_tickers: 리스트로 전달 가능
    """
    failed_tickers = []
    all_news_df = pd.DataFrame()

    if selected_tickers is None:
        tickers_to_process = get_today_tickers()
    else:
        tickers_to_process = selected_tickers

    print(f"[📊] 오늘 처리할 뉴스 티커 수: {len(tickers_to_process)}")
    for i, ticker in enumerate(tickers_to_process, 1):
        print(f"\n[{i}/{len(tickers_to_process)}] {ticker} 처리 중...")
        news_df = fetch_news_newsapi(ticker)
        if not news_df.empty:
            all_news_df = pd.concat([all_news_df, news_df], ignore_index=True)
        else:
            failed_tickers.append({"ticker": ticker, "reason": "empty or error"})

    # 저장
    output_path = NEWS_DIR / f"news_sentiment_{TODAY}.csv"
    all_news_df.to_csv(output_path, index=False)
    print(f"\n💾 뉴스 수집 완료 → {output_path}")
    print(f"📊 총 수집된 뉴스: {len(all_news_df)}개")

    # 실패 로그 저장
    if failed_tickers:
        with open(LOG_PATH, "w", encoding="utf-8") as f:
            json.dump(failed_tickers, f, indent=2, ensure_ascii=False)
        print(f"[📌] 실패한 종목 {len(failed_tickers)}개 기록됨 → {LOG_PATH}")
