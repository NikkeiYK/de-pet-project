from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime

CONNECTION_PG = "postgres_dwh"
TMP_TABLE = "stg_tmp_products_segment_{{ ds_nodash }}"
TARGET_TABLE = "dm_products_segments"


def get_date(**context):
    start_date = context["data_interval_start"].format("YYYY-mm-dd")
    end_date = context["data_interval_start"].format("YYYY-mm-dd")
    return start_date, end_date


with DAG(
    dag_id="fct_products_segment_dm",
    schedule="@daily",
    max_active_runs=1,
    max_active_tasks=1,
    start_date=datetime(2026, 1, 1),
    tags=["dm", "pg"]
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    sensor = ExternalTaskSensor(
        task_id="wait_load_data_to_ods_pg",
        allowed_states=["seccess"],
        external_task_id="get_and_transfer_raw_to_ods_pg",
        external_dag_id="raw_from_s3_to_pg",
        poke_interval=60,
        timeout=1440
    )

    prepare_stg = SQLExecuteQueryOperator(
        task_id="prepare_stg_layer",
        con_id=CONNECTION_PG,
        sql=f'''
        DROP TABLE IF EXISTS {TMP_TABLE};
        
        CREATE TABLE {TMP_TABLE} AS (
            SELECT id, TRIM(title) as name, case when price > 100 then "premium" else "Standard" end as cegmant from ods.products 
            where price is not NULL; 
        )
        '''
    )

    swap_stg_to_dm = SQLExecuteQueryOperator(
        task_id="swap_tmp_layer_to_dm",
        conn_id=CONNECTION_PG,
        sql=f'''
        BEGIN;
            ALTER TABLE IF EXISTS {TMP_TABLE} RENAME TO {TARGET_TABLE};
        
            CREATE SCHEMA IF NOT EXISTS dm;
        
            ALTER TABLE {TMP_TABLE} SET SCHEMA dm;
        COMMIT;
        '''
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> sensor >> prepare_stg >> swap_stg_to_dm >> end
