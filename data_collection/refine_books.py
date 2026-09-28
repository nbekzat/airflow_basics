from pathlib import Path
import pandas as pd
import json
from datetime import datetime
from utils import save_data
import pprint as pp
from argparse import ArgumentParser


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--data_lake_path", required=True, help="data lake path")
    args = parser.parse_args()

    books_list = []
    RAW_BOOKS_PATH = Path(args.data_lake_path) / "raw" / "json" / "books"
    PROCESSED_BOOKS_PATH = Path(args.data_lake_path) / "processed" / "parquet" / "books"

    try:
        for file in RAW_BOOKS_PATH.iterdir():
            print(f"reading file {file.name}")

            with open(file, "r", encoding="utf-8") as f:
                book = json.load(f)
                data = {}
                data["title"] = book.get("title", None)
                data["subtitle"] = book.get("subtitle", None)
                data["number_of_pages"] = book.get("number_of_pages", None)
                data["publish_date"] = book.get("publish_date", None)
                data["publish_country"] = book.get("publish_country", None)
                data["by_statement"] = book.get("by_statement", None)

                publish_places = book.get("publish_places", None)
                data["publish_places"] = (
                    "|".join(publish_places) if type(publish_places) == list else None
                )

                publishers = book.get("publishers", None)
                data["publishers"] = (
                    "|".join(publishers) if type(publishers) == list else None
                )
                authors = book.get("authors", [])
                # if authors name represented as string not a dictionary
                authors_list = [x for x in authors if type(x) == str]
                data["authors_uri"] = (
                    "|".join(authors_list) if len(authors_list) > 0 else None
                )
                file_name = file.name.split("_")

                data["id"] = file_name[2].split(".")[0]
                data["collect_date"] = datetime.strptime(
                    file_name[0], "%Y%m%d"
                ).strftime("%Y-%m-%d")

                print(data)
                books_list.append(data)
        save_data(
            books_list,
            "books",
            zone="processed",
            context="books",
            file_type="parquet",
            base_path=args.data_lake_path,
        )

        df_parquet = pd.read_parquet(PROCESSED_BOOKS_PATH / "books.parquet")
        pp.pprint(df_parquet)
    except json.JSONDecodeError as e:
        print(f"Error decoding json file {e}")
    except Exception as e:
        print(f"Error: {e}")
