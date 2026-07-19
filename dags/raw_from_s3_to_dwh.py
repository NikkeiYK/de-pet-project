# dags/raw_from_s3_to_dwh.py
import logging
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import clickhouse_connect

LAYER = "raw"
SOURCE = 'products-bucket'
DAG_ID = "raw_from_s3_to_dwh"
BUCKET_NAME = 'products-catalog'
LONG_DESCRIPTION = """
Waiting S3 data to load into clickhouse dwh
"""

SHORT_DESCRIPTION = "Loading products into dwh ods layer"

ACCESS_KEY = Variable.get("products-access-key")
SECRET_KEY = Variable.get("products-secret-key")

PASSWORD = Variable.get("CLICKHOUSE_PASSWORD")
SCHEMA = "ods"
TARGET_TABLE = "products"


def get_date(**context):
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    return start_date, end_date


def raw_from_s3_to_clickhouse(**context):
    start_date, end_date = get_date(**context)
    logging.info(f"Этап переноса данных из s3 начат за даты: {start_date}/{end_date}")
    
    client = clickhouse_connect.get_client(
        host="clickhouse",
        port=8123,
        username="default",
        password=PASSWORD,
        database="ods"
    )
    
    init_schema = f"""
        CREATE DATABASE IF NOT EXISTS {SCHEMA};
    """
    
    client.command(init_schema)

    init_products_table = f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{TARGET_TABLE} (
            id UInt8,
            title String,
            price Decimal32(2),
            description String,
            category String,
            image String,
            rating_rate Decimal32(1),
            rating_count UInt32
        ) ENGINE = MergeTree()
        ORDER BY id
    """
    
    client.command(init_products_table)
    
    s3_path = f"http://minio:9000/{BUCKET_NAME}/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet"
    
    insert_sql = f"""
        INSERT INTO {SCHEMA}.{TARGET_TABLE} (id, title, price, description, category, image, rating_rate, rating_count) 
        SELECT 
            id,
            title,
            price,
            description,
            category,
            image,
            rating.rate as rating_rate,
            rating.count as rating_count
        FROM s3(
            '{s3_path}',
            '{ACCESS_KEY}',
            '{SECRET_KEY}',
            'Parquet'
        )
    """
    client.query(insert_sql)
    client.close()
    print(f"✅ Загружено в Clickhouse: {SCHEMA}.{TARGET_TABLE}")


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2026, 1, 1),
    max_active_runs=1,
    max_active_tasks=1,
    description=SHORT_DESCRIPTION,
    tags=["s3", "pg", "ods"]
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id="start")

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id='wait_load_complete',
        external_task_id="get_and_transfer_api_to_s3",
        external_dag_id="EXTRACT_DATA_FROM_API_TO_S3",
        allowed_states=["success"],
        timeout=600,
        poke_interval=10,
        mode='reschedule',
    )

    get_and_transfer_raw_to_ods_clickhouse = PythonOperator(
        task_id="get_and_transfer_raw_to_ods_clickhouse",
        python_callable=raw_from_s3_to_clickhouse
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> sensor_on_raw_layer >> get_and_transfer_raw_to_ods_clickhouse >> end
