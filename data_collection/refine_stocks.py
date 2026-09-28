from utils import save_data
import pandas as pd
from datetime import datetime
from argparse import ArgumentParser
from pathlib import Path

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--data_lake_path", required=True)
    parser.add_argument("--execution_date", required=True)
    args = parser.parse_args()

    stocks_df_list = []
    file_path = Path(args.data_lake_path) / "processed" / "parquet" / "stocks"

    for file in file_path.iterdir():
        df = pd.read_csv(file)
        stocks_df = pd.DataFrame({
            "date": pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d"),
            "open": df["Open"].astype("float64"),
            "high": df["High"].astype("float64"),
            "low": df["Low"].astype("float64"),
            "close": df["Close"].astype("float64"),
            "volume": df["Volume"].astype("int64"),
            "ticker_name": file.name.split("_")[2].split(".")[0],
            "collect_date": datetime.strptime(
                file.name.split("_")[0], "%Y%m%d"
            ).strftime("%Y-%m-%d"),
        })
        stocks_df_list.append(stocks_df)

    df = pd.concat(stocks_df_list, ignore_index=True)
    # print(df)
    save_data(df, "stocks", zone="processed", context="stocks", file_type="parquet")
    ndf = pd.read_parquet(file_path / "stocks.parquet")
    print(ndf)
