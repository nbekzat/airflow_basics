FROM apache/airflow:3.3.1

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

# COPY dags /opt/airflow/dags
# COPY data_collection /opt/airflow/data_collection
# COPY data_lake /opt/airflow/data_lake