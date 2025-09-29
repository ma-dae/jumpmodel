import yfinance as yf
import pandas as pd
import datetime
import os
from pathlib import Path

print("🚀 뉴스 감성 분석 시작...")

# 0. 환경설정 경로
CONFIG_PATH = "config/sp500_companies.csv"

# 1. S&P 500 종목 리스트 불러오기
print("📊 S&P500 데이터 로딩 중...")
if not os.path.exists(CONFIG_PATH):
    raise FileNotFoundError(f"{CONFIG_PATH} 파일이 존재하지 않습니다.")
sp500 = pd.read_csv(CONFIG_PATH)
tickers = sp500['Symbol'].tolist()
print(f"✅ {len(tickers)}개 티커 로드 완료")

# 2. FinBERT 모델 로딩 (진행상황 표시)
print("🤖 FinBERT 모델 로딩 중... (최초 실행시 다운로드로 5-10분 소요)")
print("   - 모델 크기: ~400MB")
print("   - 다운로드 위치: ~/.cache/huggingface/")

try:
    from transformers import pipeline
    sentiment_pipeline = pipeline(
        "sentiment-analysis",
        model="yiyanghkust/finbert-tone",
        tokenizer="yiyanghkust/finbert-tone"
    )
    print("✅ FinBERT 모델 로드 완료!")
    
except Exception as e:
    print(f"❌ FinBERT 로드 실패: {e}")
    print("🔄 기본 감성 모델로 대체 시도...")
    sentiment_pipeline = pipeline("sentiment-analysis")

# 3. 뉴스 데이터 수집
def fetch_news(ticker, period_days=7):
    print(f"📰 {ticker} 뉴스 수집 중...")
    try:
        stock = yf.Ticker(ticker)
        news_items = stock.news
        
        if not news_items:
            print(f"   ⚠️ {ticker}: 뉴스 없음")
            return pd.DataFrame()
            
        df = pd.DataFrame(news_items)
        df['providerPublishTime'] = pd.to_datetime(df['providerPublishTime'], unit='s')
        start_date = datetime.datetime.now() - datetime.timedelta(days=period_days)
        df = df[df['providerPublishTime'] >= start_date]
        df['Ticker'] = ticker
        print(f"   ✅ {ticker}: {len(df)}개 뉴스 수집")
        return df[['Ticker', 'title', 'providerPublishTime']]
        
    except Exception as e:
        print(f"   ❌ {ticker} 수집 실패: {e}")
        return pd.DataFrame()

# 4. 감성 분석
def analyze_sentiment(df):
    if df.empty:
        return df
    
    print(f"   🔍 감성 분석 중... ({len(df)}개 뉴스)")
    sentiments = []
    
    for i, text in enumerate(df['title']):
        if i % 5 == 0:  # 5개마다 진행상황 출력
            print(f"      진행: {i+1}/{len(df)}")
            
        result = sentiment_pipeline(text)[0]
        label = result['label']
        score = result['score']
        
        if label.lower() == "positive":
            sentiments.append(score)
        elif label.lower() == "negative":
            sentiments.append(-score)
        else:
            sentiments.append(0.0)
    
    df['news_sentiment_score'] = sentiments
    df['positive_keywords_count'] = df['title'].str.count(r'\b(실적|신사업|M&A|계약|성장)\b')
    return df

# 5. 전체 실행
print("\n🔄 뉴스 수집 및 분석 시작...")
all_news_df = pd.DataFrame()

test_tickers = tickers[:5]  # 테스트용 5개로 축소
for i, ticker in enumerate(test_tickers, 1):
    print(f"\n[{i}/{len(test_tickers)}] {ticker} 처리 중...")
    
    news_df = fetch_news(ticker)
    if not news_df.empty:
        news_df = analyze_sentiment(news_df)
        all_news_df = pd.concat([all_news_df, news_df], ignore_index=True)

# 6. 결과 저장
print("\n💾 결과 저장 중...")
output_dir = Path("data/news")
output_dir.mkdir(parents=True, exist_ok=True)
output_path = output_dir / "news_sentiment_features.csv"

all_news_df.to_csv(output_path, index=False)
print(f"🎉 완료! 저장 경로: {output_path}")
print(f"📊 총 수집된 뉴스: {len(all_news_df)}개")