import pandas as pd
import yfinance as yf
from datetime import date, timedelta
import os

# S&P500 기업 리스트 불러오기
sp500 = pd.read_csv("C:/Users/kim45/sp500_companies.csv")
tickers = sp500['Symbol'].dropna().unique().tolist()

# 10년치 데이터 (2015-01-01 ~ 오늘)
start_date = "2015-01-01"
# end_date = "2025-09-25"
end_date = (date.today() + timedelta(days=1)).isoformat()

# 저장 경로 생성
os.makedirs("data/raw", exist_ok=True)

# 데이터 다운로드
for ticker in tickers:
    try:
        df = yf.download(ticker, start=start_date, end=end_date, auto_adjust=False, progress=False)
        if df.empty:
            print(f"[❌] {ticker}: 데이터 없음")
            continue

        df = df.reset_index()
        df.rename(columns={'Adj Close': 'Adj_Close'}, inplace=True) # 조정된 close는 백테스팅에 사용
        df['Raw_Close'] = df['Close']  # 비조정값 따로 저장 # 실시간 매매에 사용 

        df = df[['Date', 'Open', 'High', 'Low', 'Raw_Close', 'Adj_Close', 'Volume']]
        df.to_csv(f"data/raw/{ticker}.csv", index=False)
        print(f"[✅] {ticker}: 수집 완료")

    except Exception as e:
        print(f"[💥] {ticker}: 수집 실패 → {e}")

# 예시: 애플 데이터 확인
print(df.head(20))