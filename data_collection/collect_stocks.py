from yfinance import Ticker
from pprint import pprint
from utils import save_data
import pandas as pd
import datetime
from argparse import ArgumentParser


def collect_stock_data(stock_ticker: str):

    ticker_data = Ticker(stock_ticker)
    df = ticker_data.history(period="1d")
    nrows = df.shape[0]
    if nrows == 1:
        return df
    else:
        raise Exception("resulting DataFrame is empty or contains more than one row")

    return df


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--data_lake_path", required=True)
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--stock_ids", required=True)
    args = parser.parse_args()

    ticker_list = args.stock_ids.split(",")
    print("tikcer_id to be downloaded: ", ticker_list)

    for ticker in ticker_list:
        df = collect_stock_data(ticker).reset_index()
        file_name = f"{datetime.date.today().strftime('%Y%m%d')}_stock_{ticker}"
        save_data(df, file_name, "raw", "stocks", "csv", base_path=args.data_lake_path)
        pprint(df)
