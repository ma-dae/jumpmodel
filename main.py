from utils.symbol_fetcher import get_sp500_symbols

symbols = get_sp500_symbols()
print(symbols[:10])  # 상위 10개만 출력해서 확인
print(f"총 종목 수: {len(symbols)}")