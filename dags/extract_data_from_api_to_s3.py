from airflow import DAG
from airflow.models import Variable
import duckdb
import datetime
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
import logging

LAYER = 'raw'
SOURCE = 'data-bucket'
DAG_ID = 'EXTRACT_DATA_FROM_API_TO_S3'
BUCKET_NAME = 'bronze'


def get_dates(**context):
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    return start_date, end_date


LONG_DESCRIPTION = """
Даг для переноса данных с API фейковых товаров 
"""
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")


def get_and_transfer_api_to_s3(**context):
    try:
        print("Функция запущена")
        start_date, end_date = get_dates(**context)
        logging.info(f"Start load for dates: {start_date}")

        con = duckdb.connect()

        con.sql(
            f"""
            SET TIMEZONE='UTC';
            INSTALL httpfs;
            LOAD httpfs;
            
            SET s3_url_style = 'path';
            SET s3_endpoint = 'minio:9000';
            SET S3_access_key_id = '{ACCESS_KEY}';
            SET S3_secret_access_key = '{SECRET_KEY}';
            SET S3_use_ssl = FALSE;
            
            COPY
            (
                SELECT 
                    *
                FROM read_json_auto('https://fakestoreapi.com/products')
            ) TO 's3://{BUCKET_NAME}/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';
            """
        )
        logging.info(
            f"📦 S3 путь: {f's3://{BUCKET_NAME}/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet'}")
        con.close()
        logging.info(f"Download for date success: {start_date}")
    except Exception as e:
        logging.error(f"Ошибка: {e}")
        raise


with DAG(
    dag_id=DAG_ID,
    schedule='@daily',
    start_date=datetime.datetime(2026, 1, 1),
    default_args={"retries": 1},
    tags=["s3", "raw"],
    max_active_tasks=1,
    max_active_runs=1
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id="start")

    api_to_s3 = PythonOperator(
        task_id="get_and_transfer_api_to_s3",
        python_callable=get_and_transfer_api_to_s3
    )

    end = EmptyOperator(task_id="end")

    start >> api_to_s3 >> end
