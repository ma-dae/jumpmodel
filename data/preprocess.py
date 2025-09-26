# 전처리 파일

import numpy as np
import pandas as pd

def basic_clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("Date").drop_duplicates(subset="Date")
    df = df[df["Volume"] > 0]
    req = ["Date", "Open", "High", "Low", "Raw_Close", "Adj_Close", "Volume"]
    df = df.dropna(subset=req)
    df["Return"] = df["Adj_Close"].pct_change()
    df["LogReturn"] = np.log(df["Adj_Close"] / df["Adj_Close"].shift(1))
    df = df.dropna(subset=["Return", "LogReturn"])
    return df