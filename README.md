# Airflow Books & Stocks ETL

A weekend learning project for building and operating an Apache Airflow DAG locally. The pipeline collects book metadata from the Open Library API and daily stock prices through Yahoo Finance, stores the source data in a simple local data lake, and refines it into Parquet datasets.

## What the pipeline does

The `etl_dag` runs every day at 16:00 and has two branches:

```text
start
 ├─ get book IDs → collect raw book JSON → refine books → end
 └─ collect raw stock CSV → refine stocks ────────────→ end
```

- **Books:** retrieves a list of Open Library IDs, downloads each book record as JSON, then extracts selected fields into `data_lake/processed/parquet/books/books.parquet`.
- **Stocks:** downloads daily OHLCV data for selected tickers as CSV, then combines and types it into `data_lake/processed/parquet/stocks/stocks.parquet`.

Raw data is kept separately from the processed layer:

```text
data_lake/
├── raw/json/books/
├── raw/csv/stocks/
└── processed/parquet/{books,stocks}/
```

## Airflow concepts practiced

- **DAG definition and scheduling:** a DAG with a cron schedule, `start_date`, `catchup=False`, and a human-readable description.
- **Tasks and dependencies:** `EmptyOperator` start/end markers and `>>` dependency chaining to model sequential and parallel work.
- **Operators:** `PythonOperator` to retrieve IDs and `BashOperator` to run the collection and transformation scripts.
- **XCom:** the book-ID Python task returns a list; the collection task pulls that value with `ti.xcom_pull(...)`.
- **Jinja templating:** runtime values such as `logical_date | ds` and XCom output are rendered into task environment variables.
- **Variables:** paths are supplied through Airflow Variables instead of being hard-coded in the DAG configuration.
- **Reliability and observability:** default retries (three attempts with a two-minute delay), task logs, DAG runs, and task status can be inspected in the Airflow UI.

## Run locally

Prerequisites: Python 3.13+, `uv` (recommended), and a local Airflow installation.

```bash
uv sync
export AIRFLOW_HOME="$(pwd)" 
```

Set the paths used by the DAG (replace `"$(pwd)"` with the absolute project path if your shell does not preserve it):

```bash
airflow variables set BASE_PROJECT_DIR "$(pwd)/"
airflow variables set BASE_DATA_COLLECTION_PATH "$(pwd)/data_collection"
airflow variables set BASE_DATA_LAKE_PATH "$(pwd)/data_lake"
```

Start Airflow components in separate terminals:

```bash
airflow api-server --port 8080
airflow scheduler
airflow dag-processor
```

Open `http://localhost:8080` to inspect the `etl_dag`, its graph, task logs, and run history. Trigger it from the UI or the CLI:

```bash
airflow dags trigger etl_dag
```
 

## Project layout

```text
dags/book_stock_dag.py       # Airflow DAG and task orchestration
data_collection/             # API collection and Parquet refinement scripts
data_lake/                   # Local raw and processed data
scripts/                     # Local Airflow setup/service helpers
```

## Ideas to continue learning

- Add validation tasks and data-quality checks before publishing Parquet files.
- Replace local paths with object storage and Airflow Connections.
- Add tests, alerts, and a containerized deployment.
- Pass stock IDs through the existing `get_stock_ids` task and XCom, mirroring the book branch.
