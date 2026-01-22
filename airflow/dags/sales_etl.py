from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def hello_world():
    print("Hello world!")


with DAG(
    dag_id="example_python_operator",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:
    task = PythonOperator(
        task_id="print_hello",
        python_callable=hello_world
    )
