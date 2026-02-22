import logging
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
import datetime
from airflow.models import Variable
from airflow import DAG
from minio import Minio

import io
import requests
import pandas as pd

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

client = Minio(endpoint="minio:9000", access_key=ACCESS_KEY,
               secret_key=SECRET_KEY, secure=False)


def get_and_transfer_api_to_s3(**context):
    try:

        print("Функция запущена")
        start_date, end_date = get_dates(**context)
        logging.info(f"Start load for dates: {start_date}/{end_date}")

        response = requests.get("https://fakestoreapi.com/products")
        data = response.json()

        buffer = io.BytesIO()

        df = pd.DataFrame(data)
        df.to_parquet(buffer, engine="pyarrow", compression="snappy")

        buffer.seek(0)

        if not client.bucket_exists("bronze"):
            client.make_bucket("bronze")

        client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=f"{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet",
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type="application/parquet"
        )

        logging.info(
            f"📦 S3 путь: {f's3://{BUCKET_NAME}/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet'}")

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


# def get_and_transfer_api_to_s3(**context):
    # start_date = context["data_interval_start"].strftime("%Y-%m-%d")

    # # 1. Читаем API (БЕЗ S3 настроек!)
    # con_api = duckdb.connect()
    # con_api.execute("INSTALL httpfs; LOAD httpfs")

    # df_api = con_api.sql("""
    #     SELECT *, '{{ start_date }}' as load_date
    #     FROM read_json_auto('https://fakestoreapi.com/products')
    # """)

    # # 2. S3 только для записи
    # con_s3 = duckdb.connect()
    # con_s3.execute("""
    #     INSTALL httpfs; LOAD httpfs;
    #     SET s3_endpoint='minio:9000';
    #     SET s3_access_key_id='{{ ACCESS_KEY }}';
    #     SET s3_secret_access_key='{{ SECRET_KEY }}';
    #     SET s3_use_ssl=FALSE
    # """)

    # con_s3.execute(f"""
    #     COPY df_api TO 's3://bronze/raw/data-bucket/{start_date}/{start_date}_00-00-00.parquet'
    # """)

    # logging.info(f"✅ Загружено: {len(df_api)} строк")
