import logging
import duckdb
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

BUCKET_NAME = "bronze"
LAYER = "raw"
SOURCE = "data-bucket"

DAG_ID = "raw_from_s3_to_pg"

LONG_DESCRIPTION = """
Your description
"""

SHORT_DESCRIPTION = ""

ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

PASSWORD = Variable.get("POSTGRES_PASSWORD")
SCHEMA = "ods"
TARGET_TABLE = "products"


def get_date(**context):
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    return start_date, end_date


def raw_from_s3_to_pg(**context):
    start_date, end_date = get_date(**context)
    logging.info(
        f"Этап переноса данных из s3 начат за даты: {start_date}/{end_date}")

    con = duckdb.connect()

    con.sql(
        f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;
        SET s3_url_style = 'path';
        
        CREATE SECRET dwh_postgres (
            TYPE postgres,
            HOST 'postgres-dwh',
            PORT 5432,
            DATABASE postgres,
            USER 'postgres',
            PASSWORD '{PASSWORD}'
        );
        
        
        
        ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);
        
        CREATE SCHEMA IF NOT EXISTS dwh_postgres_db.{SCHEMA};
        
        CREATE TABLE IF NOT EXISTS dwh_postgres_db.{SCHEMA}.{TARGET_TABLE} 
        (
            id INTEGER,
            title VARCHAR,
            price DECIMAL(10,2),
            description VARCHAR,
            category VARCHAR,
            image VARCHAR
        );
        
        INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
        (
            id,
            title,
            price,
            description,
            category,
            image
        )
        
        SELECT 
            id,
            title,
            price,
            description,
            category,
            image
        FROM 's3://{BUCKET_NAME}/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet'
        """,
    )
    con.close()
    print(f"✅ Загружено в PostgreSQL: {SCHEMA}.{TARGET_TABLE}")


with DAG(
    dag_id=DAG_ID,
    schedule="@daily",
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
        poke_interval=60,
        mode='reschedule',
    )

    get_and_transfer_raw_to_ods_pg = PythonOperator(
        task_id="get_and_transfer_raw_to_ods_pg",
        python_callable=raw_from_s3_to_pg
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> sensor_on_raw_layer >> get_and_transfer_raw_to_ods_pg >> end
