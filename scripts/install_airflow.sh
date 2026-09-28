#!/bin/bash

export AIRFLOW_HOME=/Users/bekzatnurlanbekova/Documents/python/__Data_engineer/airflow

# Set variables AIRFLOW_VERSION and PYTHON_VERSION
AIRFLOW_VERSION=3.3.1
PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

# Generate URL for downloading a constraint file
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example this would install 2.6.3 with python 3.7: https://raw.githubusercontent.com/apache/airflow/constraints-2.6.3/constraints-3.7.txt

# Install airflow
pip3 install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"