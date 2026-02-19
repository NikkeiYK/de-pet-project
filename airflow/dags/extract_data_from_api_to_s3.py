from airflow import DAG
from airflow.models import Variable
import duckdb
import os
import datetime
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

LAYER = 'raw'
SOURCE = 'data-bucket'
DAG_ID = 'EXTRACT_DATA_FROM_API_TO_S3'


def get_dates(**context):
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    return start_date, end_date


LONG_DESCRIPTION = """
Даг для переноса данных с API фейковых товаров 
"""


def get_and_transfer_api_to_s3(**context):
    start_date, end_date = get_dates()

    con = duckdb.connect()

    con.sql(
        f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET S3_access_key_id = '{os.getenv("minio_access_key")}';
        SET S3_secret_access_key = '{os.getenv("minio_secret_key")}';
        SET S3_use_ssl = 'FALSE';
        
        COPY
        (
            SELECT 
                id::INTEGER AS product_id,
                title::VARCHAR AS product_name,
                price::DECIMAL(10,2) AS price,
                category::VARCHAR AS category,
                CURRENT_TIMESTAMP::TIMESTAMP AS loaded_at
            FROM read_json_auto('https://fakestoreapi.com/products')
        ) TO 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';
        
        """
    )

    con.close()


with DAG(
    dag_id=DAG_ID,
    schedule='@daily',
    start_date=datetime.datetime(2026, 2, 20),
    default_args={"retries": 2},
    tags=["s3", "raw"],
    max_active_tasks=1,
    max_active_runs=1
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id="start")

    get_and_transfer_api_to_s3 = PythonOperator(
        task_id="get_and_transfer_api_to_s3",
        python_callable=get_and_transfer_api_to_s3
    )

    end = EmptyOperator(task_id="end")

    start >> get_and_transfer_api_to_s3 >> end
