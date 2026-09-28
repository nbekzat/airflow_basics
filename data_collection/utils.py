from typing import List, Dict
from pathlib import Path
import json
import pandas as pd


def save_data(
    file_content: List[Dict] | Dict | List | str | pd.DataFrame,
    file_name: str,
    zone="raw",
    context="books",
    file_type="csv",
    base_path="/Users/bekzatnurlanbekova/airflow/data_lake/",
):
    file_path = (
        Path(base_path) / zone / file_type / context / f"{file_name}.{file_type}"
    )

    try:
        if file_type == "json":
            with open(file_path, "w") as file:
                json.dump(file_content, file, indent=3)

        elif file_type == "csv" and type(file_content) == pd.DataFrame:
            file_content.to_csv(file_path, index=False)

        elif file_type == "parquet":
            # expected data is array of dictionaries
            df = pd.DataFrame(file_content)
            if df.shape[0] <= 1:
                raise Exception("DataFrame has one or less rows")

            cols_names_except_cl = [
                c for c in df.columns.to_list() if c != "collect_date"
            ]
            df = df.sort_values("collect_date", ascending=False).drop_duplicates(
                subset=cols_names_except_cl, keep="first", ignore_index=True
            )
            df.to_parquet(file_path)
            # pprint.pprint(df)
        else:
            raise Exception(f"File type {file_type} is not supported")

        print(f"file saved {file_path}")
    except Exception as e:
        print(f"Error occured when trying to save file with msg: {e}")
        raise
