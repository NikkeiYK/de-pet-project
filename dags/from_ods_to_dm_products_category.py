# dags/from_ods_to_dm_products_category.py
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime
import clickhouse_connect

# OLD_SCHEMA = "ods"
# OLD_TABLE = "products"

# TMP_SCHEMA = "stg"


# TARGET_SCHEMA = "dm"
# TARGET_TABLE = "dm_products_categories"

PASSWORD = Variable.get("CLICKHOUSE_PASSWORD")



client = clickhouse_connect.get_client(
    host="clickhouse",
    port=8123,
    username="default",
    password=PASSWORD,
    database="ods"
)


def get_date(**context):
    start_date = context["data_interval_start"].format("YYYY-mm-dd")
    end_date = context["data_interval_start"].format("YYYY-mm-dd")
    return start_date, end_date

# def prepare_clickhouse_stg(**context):
#     ds_nodash = context["ds_nodash"]
#     TMP_TABLE = f"stg_tmp_products_categories_{ds_nodash}"
    
#     create_stg_schema = f"""
#         CREATE DATABASE IF NOT EXISTS {TMP_SCHEMA};
#     """
    
#     client.command(create_stg_schema)
    
#     drop_table = f"""
#         DROP TABLE IF EXISTS {TMP_SCHEMA}.{TMP_TABLE};
#     """
    
#     client.command(drop_table)
    
#     init_tmp_table = f"""
#         CREATE TABLE {TMP_SCHEMA}.{TMP_TABLE} (
#             category String,
#             avg_price Decimal32(2),
#             avg_rating Decimal32(2),
#             avg_count Decimal32(2)
#         ) Engine = MergeTree()
#         order by category
#     """
#     client.command(init_tmp_table)
    
    
# def hydrate_table_data(**context):
#     ds_nodash = context["ds_nodash"]
#     TMP_TABLE = f"stg_tmp_products_categories_{ds_nodash}"
    
#     create_dm_schema = f"""
#         CREATE DATABASE IF NOT EXISTS {TARGET_SCHEMA};
#     """
    
#     client.command(create_dm_schema)
    
#     insert_data = f"""
#         INSERT INTO {TMP_SCHEMA}.{TMP_TABLE} (category, avg_price, avg_rating, avg_count) 
#         SELECT category, avg(price) as avg_price, avg(rating_rate) as avg_rating, avg(rating_count) as avg_count from {OLD_SCHEMA}.{OLD_TABLE}
#             WHERE rating_count > 100
#             GROUP BY category
#     """
    
#     client.query(insert_data)
    
# def swap_tables(**context):
#     ds_nodash = context["ds_nodash"]
#     TMP_TABLE = f"stg_tmp_products_categories_{ds_nodash}"
    
#     # Проверяем, существует ли целевая таблица
#     exists = client.command(f"EXISTS TABLE {TARGET_SCHEMA}.{TARGET_TABLE}")  # вернёт 0 или 1

#     if exists:
#         # Атомарное переименование: старую -> бэкап, временную -> целевую
#         rename_sql = f"RENAME TABLE {TARGET_SCHEMA}.{TARGET_TABLE} TO {TMP_SCHEMA}.{TARGET_TABLE}_old, {TMP_SCHEMA}.{TMP_TABLE} TO {TARGET_SCHEMA}.{TARGET_TABLE}"
#     else:
#         # Если целевой нет, просто переименовываем временную в целевую
#         rename_sql = f"RENAME TABLE {TMP_SCHEMA}.{TMP_TABLE} TO {TARGET_SCHEMA}.{TARGET_TABLE}"

#     client.command(rename_sql)
    
with DAG(
    dag_id="from_ods_to_dm_products_category",
    schedule="@once",
    max_active_runs=1,
    max_active_tasks=1,
    start_date=datetime(2026, 1, 1),
    tags=["dm", "clickhouse"]
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    sensor = ExternalTaskSensor(
        task_id="wait_load_data_to_ods_clickhouse",
        allowed_states=["success"],
        external_task_id="get_and_transfer_raw_to_ods_clickhouse",
        external_dag_id="raw_from_s3_to_dwh",
        timeout=600,
        poke_interval=10,
        mode='reschedule',
    )
    
    prepare_stg = BashOperator(
        task_id="prepare_stg_layer",
        bash_command="docker exec dbt_runner dbt run --select stg_products"
    )
    
    dm_dbt_products_creation = BashOperator(
        task_id="dbt_dm_building",
        bash_command="docker exec dbt_runner dbt run --select dm_products"
    )

    # prepare_stg = PythonOperator(
    #     task_id="prepare_stg_layer",
    #     python_callable=prepare_clickhouse_stg
    # )
    
    # hydrate_table_data_stg = PythonOperator(
    #     task_id="hydrate_table_data_stg",
    #     python_callable=hydrate_table_data
    # )

    # swap_stg_to_dm = PythonOperator(
    #     task_id="swap_tables",
    #     python_callable=swap_tables
    # )

    end = EmptyOperator(
        task_id="end"
    )

    start >> sensor >> prepare_stg >> dm_dbt_products_creation >> end
