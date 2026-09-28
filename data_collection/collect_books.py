import requests
from pprint import pprint
from datetime import datetime, date
from utils import save_data
from argparse import ArgumentParser
import pandas as pd
from pathlib import Path

BASE_API_URL = "https://openlibrary.org/works/"
FILE_FORMAT = ".json"


def collect_single_book_date(open_lib_id: str, base_data_lake_path):

    file_url = f"{BASE_API_URL}{open_lib_id}{FILE_FORMAT}"
    try:
        response = requests.get(file_url)
        if response.status_code == 200:
            current_date = date.today().strftime("%Y%m%d")
            file_name = f"{current_date}_book_{open_lib_id}"
            save_data(
                response.json(),
                file_name=file_name,
                file_type="json",
                base_path=base_data_lake_path,
            )
        else:
            msg = (
                f"error occured in the request. Response status {response.status_code}"
            )
            raise Exception(msg)
    except Exception as e:
        print(f"Error {e}")
        raise


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--data_lake_path", required=True, help="data lake path")
    parser.add_argument("--open_library_ids", required=True, help="ID of books")
    parser.add_argument(
        "--execution_date", required=True, help="Pipeline execution date"
    )
    args = parser.parse_args()
    ids_list = args.open_library_ids.split(",")
    print("execution date: ", args.execution_date)

    try:
        # check if book already exist in processed parquet file
        # read parquet file
        df = pd.read_parquet(
            # Path(args.data_lake_path)
            Path("/Users/bekzatnurlanbekova/airflow/data_lake")
            / "processed"
            / "parquet"
            / "books"
            / "books.parquet"
        )
        df["collect_date"] = pd.to_datetime(df["collect_date"])
        df["execution_date"] = pd.to_datetime(args.execution_date)
        df["months_diff"] = df["execution_date"].dt.to_period("M").astype(int) - df[
            "collect_date"
        ].dt.to_period("M").astype(int)
        pprint(df.describe())
        blocklist = df.query("months_diff < 12")["id"].to_list()
        print(
            "Following book IDs won't be downloaded, due to time delta 12 month restriction: ",
            blocklist,
        )
        ids_list = list(set(ids_list) - set(blocklist))
    except FileNotFoundError:
        pass
    except Exception as e:
        print(e)
        raise

    for item in ids_list:
        item = item.strip()
        print(f"retrieving for book ID: {item}")
        collect_single_book_date(item, args.data_lake_path)
