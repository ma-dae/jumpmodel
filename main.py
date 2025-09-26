import pandas as pd
import numpy as np
from pathlib import Path

# 📁 경로 설정
CLEAN_DATA_DIR = Path("data/clean")

def check_cleaned_data(ticker: str):
    """전처리된 데이터 확인 및 날짜 타입 변환"""
    clean_file_path = CLEAN_DATA_DIR / f"{ticker}.csv"
    
    if not clean_file_path.exists():
        print(f"[❌] {ticker} 전처리된 파일이 없습니다.")
        return
    
    # 전처리된 데이터 로드
    df_clean = pd.read_csv(clean_file_path)

    # ✅ 날짜 컬럼 변환
    if "Date" in df_clean.columns:
        df_clean["Date"] = pd.to_datetime(df_clean["Date"], format="%Y-%m-%d", errors="coerce")
    
    # ✅ 품질 체크
    print(f"[📊] {ticker} 전처리된 데이터 정보:")
    print(df_clean.info())
    print(f"[📈] {ticker} 데이터 샘플:")
    print(df_clean.head())
    

if __name__ == "__main__":
    # 확인할 티커 목록
    tickers_to_check = ["BA", "AAPL", "MSFT"]  # 확인하고 싶은 티커 추가
    for ticker in tickers_to_check:
        check_cleaned_data(ticker)

## ** 데이터를 불러올 때, 날짜 변환도 같이 해야한다. 