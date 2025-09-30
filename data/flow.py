# flow.py
import pandas as pd
from pathlib import Path
import glob
import traceback

# 📁 경로 설정
CLEAN_DATA_DIR = Path("data/clean")
NEWS_DATA_DIR = Path("data/news")
MERGE_DIR = Path("data/merged")
MERGE_DIR.mkdir(parents=True, exist_ok=True)

# =============================
# 🔎 유틸: 최신 뉴스 파일 찾기
# =============================
def find_latest_news_file():
    # 우선 고정 이름 시도
    candidate = NEWS_DATA_DIR / "news_sentiment_features.csv"
    if candidate.exists():
        return candidate

    # 패턴으로 검색 (예: news_sentiment_2025-09-30.csv 등)
    pattern = str(NEWS_DATA_DIR / "news_sentiment*.csv")
    files = glob.glob(pattern)
    if not files:
        return None
    # 최신 파일 반환
    files_sorted = sorted(files, key=lambda p: Path(p).stat().st_mtime, reverse=True)
    return Path(files_sorted[0])

# =============================
# 🔄 티커별 feature merge
# =============================
def merge_features(tickers):
    """
    📌 CLEAN 데이터 + 뉴스 감성 데이터 병합 (방어적 구현)
      - 뉴스 파일 자동 검색 (고정 이름 우선, 없으면 pattern으로 최신 파일 사용)
      - 뉴스에 여러 날짜 칼럼 가능성 대응 (providerPublishTime, publishedAt, datetime 등)
      - 날짜를 date 단위로 통일하여 병합
      - 뉴스가 없거나 해당 티커 뉴스가 없을 경우 CLEAN 데이터만 저장
    """
    news_file = find_latest_news_file()
    if news_file is None:
        print(f"[⚠️] 뉴스 파일을 찾지 못했습니다: {NEWS_DATA_DIR}")
        news_df = None
    else:
        print(f"[📊] 뉴스 파일 로드: {news_file}")
        try:
            news_df = pd.read_csv(news_file)
            # 소문자 컬럼 정규화(선택적) - 보수적으로 유지
            print(f"     뉴스 데이터 로드 완료: {len(news_df)}행")
        except Exception as e:
            print(f"[❌] 뉴스 파일 로드 실패: {e}")
            news_df = None

    # 후보 날짜 칼럼 목록 (우선순위)
    possible_date_cols = ["providerPublishTime", "publishedAt", "datetime", "date", "Date"]

    for i, ticker in enumerate(tickers, 1):
        print(f"\n[{i}/{len(tickers)}] {ticker} 병합 시작...")
        clean_path = CLEAN_DATA_DIR / f"{ticker}.csv"
        if not clean_path.exists():
            print(f"[❌] {ticker}: clean 파일 없음 ({clean_path}) — 건너뜀")
            continue

        try:
            df_clean = pd.read_csv(clean_path)
            # Date 칼럼이 없거나 형식 문제 방지
            if "Date" not in df_clean.columns:
                # 시도: index가 날짜일 수도 있으므로 그냥 에러 메시지 출력
                print(f"[⚠️] {ticker}: clean 파일에 'Date' 컬럼이 없습니다. 파일 확인 필요.")
                # 그래도 저장해서 진행중인 상태 유지
                df_merge = df_clean.copy()
                merge_path = MERGE_DIR / f"{ticker}_merged.csv"
                df_merge.to_csv(merge_path, index=False)
                print(f"[✅] {ticker}: clean 그대로 저장 → {merge_path}")
                continue

            # Date 표준화: pandas datetime -> date (년-월-일)
            df_clean['Date'] = pd.to_datetime(df_clean['Date'], errors='coerce').dt.date
            df_clean['Ticker'] = ticker  # 병합 편의용

            if news_df is None or news_df.empty:
                # 뉴스 데이터 없음: clean만 저장
                df_merge = df_clean.copy()
                merge_path = MERGE_DIR / f"{ticker}_merged.csv"
                df_merge.to_csv(merge_path, index=False)
                print(f"[✅] {ticker}: 뉴스 없음 — clean만 저장 → {merge_path}")
                continue

            # 티커별 뉴스 필터
            df_news_ticker = news_df[news_df.get('Ticker', '') == ticker].copy()
            if df_news_ticker.empty:
                print(f"[⚠️] {ticker}: 뉴스 파일에는 티커별 항목이 없습니다. clean만 저장")
                df_merge = df_clean.copy()
                merge_path = MERGE_DIR / f"{ticker}_merged.csv"
                df_merge.to_csv(merge_path, index=False)
                print(f"[✅] {ticker}: clean 저장 → {merge_path}")
                continue

            # 날짜 컬럼 자동 탐색
            right_date_col = None
            for col in possible_date_cols:
                if col in df_news_ticker.columns:
                    right_date_col = col
                    break

            if right_date_col is None:
                print(f"[⚠️] {ticker}: 뉴스 데이터에 날짜 칼럼이 없습니다. 가능한 칼럼: {possible_date_cols}")
                print("       -> clean만 저장")
                df_merge = df_clean.copy()
                merge_path = MERGE_DIR / f"{ticker}_merged.csv"
                df_merge.to_csv(merge_path, index=False)
                print(f"[✅] {ticker}: clean 저장 → {merge_path}")
                continue

            # 표준화: 뉴스 날짜를 date 단위로 변환
            df_news_ticker[right_date_col] = pd.to_datetime(df_news_ticker[right_date_col], errors='coerce')
            df_news_ticker = df_news_ticker.dropna(subset=[right_date_col])
            df_news_ticker['Date'] = df_news_ticker[right_date_col].dt.date

            # 필요한 뉴스 컬럼 선택(안정성 확보)
            news_cols = [c for c in ['Date','news_sentiment_score','positive_keywords_count','title','url'] if c in df_news_ticker.columns]
            if 'Date' not in news_cols:
                news_cols = ['Date'] + news_cols  # ensure Date included

            df_news_small = df_news_ticker[news_cols].drop_duplicates(subset=['Date'])
            # 병합: Date + Ticker (df_clean already has Ticker)
            df_merge = pd.merge(df_clean, df_news_small, on='Date', how='left')

            # 저장
            merge_path = MERGE_DIR / f"{ticker}_merged.csv"
            df_merge.to_csv(merge_path, index=False)
            print(f"[✅] {ticker}: merge 완료 → {merge_path} (rows: {len(df_merge)})")

        except Exception as e:
            print(f"[💥] {ticker}: 병합 도중 오류 발생 → {e}")
            traceback.print_exc()
            # 실패해도 다음 티커로 진행
            continue

    print("\n[🎉] 전체 병합 작업 완료!")

# =============================
# 🔄 독립 실행
# =============================
if __name__ == "__main__":
    tickers = [f.stem for f in CLEAN_DATA_DIR.glob("*.csv")]
    merge_features(tickers)
