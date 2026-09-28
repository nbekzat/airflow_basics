from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import task
from datetime import datetime, timedelta
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param
from airflow.sdk import Variable
from data_collection.get_ids import retrieve_book_ids, retrieve_stock_ids

BASE_DATA_COLLECTION_PATH = Variable.get("BASE_DATA_COLLECTION_PATH")
# "/Users/bek/airflow/data_collection"
BASE_DATA_LAKE_PATH = Variable.get("BASE_DATA_LAKE_PATH")
# "/Users/bek/airflow/data_lake"
BASE_PROJECT_DIR = Variable.get("BASE_PROJECT_DIR")
# "/Users/bek/airflow/"


with DAG(
    dag_id="etl_dag",
    start_date=datetime.strptime("2026-09-23", "%Y-%m-%d"),
    schedule="00 16 * * *",
    description="This ETL pipeline downloads books and stock info",
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    catchup=False,
    is_paused_upon_creation=False,
    # params={"open_library_ids": Param([], type="array", items={"type": "string"})},
    render_template_as_native_obj=True,
) as dag:
    start_task = EmptyOperator(task_id="start")
    end_task = EmptyOperator(task_id="end")

    # retrieve Book ID from another source, lets say from DB
    get_book_id_task = PythonOperator(
        task_id="get_book_ids", python_callable=retrieve_book_ids
    )

    collect_books_task = BashOperator(
        task_id="collect_raw_books",
        env={
            "DATA_LAKE_PATH": BASE_DATA_LAKE_PATH,
            # "OPEN_LIBRARY_IDS": "'{{params.open_library_ids| join(',') }}'",
            "OPEN_LIBRARY_IDS": "{{ti.xcom_pull(task_ids='get_book_ids') | join(',') }}",
            "EXECUTION_DATE": "{{ logical_date | ds }}",
        },  # unix env variables
        bash_command=f"""python3 \
            {BASE_DATA_COLLECTION_PATH}/collect_books.py \
                --data_lake_path $DATA_LAKE_PATH \
                --open_library_ids "$OPEN_LIBRARY_IDS" \
                --execution_date "$EXECUTION_DATE"
            """,
    )

    process_books_task = BashOperator(
        task_id="process_books_task",
        env={"DATA_LAKE_PATH": BASE_DATA_LAKE_PATH},
        bash_command=f""" 
                python3 {BASE_DATA_COLLECTION_PATH}/refine_books.py --data_lake_path $DATA_LAKE_PATH 
                """,
    )

    # collect stock information
    get_stock_ids_task = PythonOperator(
        task_id="get_stock_ids", python_callable=retrieve_stock_ids
    )

    collect_stocks_task = BashOperator(
        task_id="collect_stock",
        env={
            "DATA_LAKE_PATH": BASE_DATA_LAKE_PATH,
            "EXECUTION_DATE": "{{logical_date | ds}}",
            "STOCK_IDS": '{{ ti.xcom_pull(task_ids="get_stock_ids")|join(",")}}',
        },
        bash_command=f"""python3 {BASE_DATA_COLLECTION_PATH}/collect_stocks.py \
            --data_lake_path "$DATA_LAKE_PATH" \
            --execution_date "$EXECUTION_DATE" \
            --stock_ids "$STOCK_IDS"\
        """,
    )

    process_stocks_task = BashOperator(
        task_id="process_stocks",
        env={
            "DATA_LAKE_PATH": BASE_DATA_LAKE_PATH,
            "EXECUTION_DATE": "{{logical_date | ds}}",
        },
        bash_command=f"""python3 {BASE_DATA_COLLECTION_PATH}/refine_stocks.py \
                --data_lake_path "$DATA_LAKE_PATH" \
                --execution_date "$EXECUTION_DATE" 
            """,
    )

    (
        start_task
        >> get_book_id_task
        >> collect_books_task
        >> process_books_task
        >> end_task
    )
    (
        start_task
        >> get_stock_ids_task
        >> collect_stocks_task
        >> process_stocks_task
        >> end_task
    )
