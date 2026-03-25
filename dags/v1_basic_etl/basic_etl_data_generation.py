import os
import random
from datetime import datetime
from io import StringIO

import pandas as pd
import psycopg2
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from dotenv import load_dotenv
from faker import Faker

load_dotenv()

# Настройки
NUM_BATCHES = 500_000
NUM_REACTORS = 50
NUM_SENSOR_READINGS = 10_000_000  # 10 млн показаний датчиков
NUM_QUALITY_TESTS = 200_000
DB_CONFIG = {
    "host": os.getenv("HOST"),
    "port": int(os.getenv("PORT")),
    "dbname": os.getenv("INDUSTRY_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

SCHEMA = os.getenv("SCHEMA")

fake = Faker("ru_RU")

# Справочники
PRODUCTS = [
    "Polypropylene homopolymer",
    "Polypropylene copolymer",
    "Butadiene",
    "Methanol",
]
REACTOR_TYPES = ["Polymerization", "Extrusion", "Distillation", "Hydrogenation"]
QUALITY_PARAMS = [
    "Melt Flow Index",
    "Density",
    "Ash Content",
    "Volatile Matter",
    "Catalyst Residue",
]
DOWNTIME_REASONS = [
    "Coke formation",
    "Blade wear",
    "Catalyst depletion",
    "Scheduled maintenance",
    "Sensor failure",
]


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def create_schemas(cur):
    sql = """
        CREATE SCHEMA IF NOT EXISTS raw;
        
        CREATE SCHEMA IF NOT EXISTS ods;
        
        CREATE SCHEMA IF NOT EXISTS dm;
    """

    cur.execute(sql)


def create_table_if_not_exists(cur, schema, table_name, df):
    type_map = {
        "int64": "INTEGER",
        "int32": "INTEGER",
        "float64": "DOUBLE PRECISION",
        "float32": "REAL",
        "bool": "BOOLEAN",
        "object": "TEXT",
        "datetime64[ns]": "TIMESTAMP",
    }

    columns = []

    for col, dtype in df.dtypes.items():
        pg_type = type_map.get(str(dtype), "TEXT")
        columns.append(f"{col} {pg_type}")

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" (
            {", ".join(columns)}
        )
    """

    cur.execute(create_sql)


def load_to_postgres(df, table_name, schema="raw", **context):
    """Быстрая загрузка через PostgreSQL COPY"""
    print(f"→ COPY-загрузка {len(df):,} строк в {schema}.{table_name}...")

    # 1. Создаём подключение
    conn = get_connection()

    try:
        with conn.cursor() as cur:
            create_schemas(cur)
            create_table_if_not_exists(cur, schema, table_name, df)
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False, header=False, na_rep="\\N")
            csv_buffer.seek(0)

            copy_sql = f"""
            COPY "{schema}"."{table_name}" FROM STDIN WITH (FORMAT CSV, NULL '\\N')
            """
            cur.copy_expert(copy_sql, csv_buffer)
            conn.commit()
            print("✓ Готово")

    except Exception as e:
        print("Error!", e)
    finally:
        conn.close()


def generate_reactors(n, **context):
    """Генерация реакторов с историей изменений (для отработки SCD2)"""
    data = []
    for i in range(1, n + 1):
        # Базовые параметры реактора
        data.append(
            {
                "reactor_id": i,
                "reactor_name": f"{fake.random_element(REACTOR_TYPES)}-{i:03d}",
                "type": fake.random_element(REACTOR_TYPES),
                "max_temperature": round(random.uniform(150, 300), 1),
                "max_pressure": round(random.uniform(10, 50), 1),
                "installation_date": fake.date_between(
                    start_date="-10y", end_date="-1y"
                ),
                "last_maintenance": fake.date_between(
                    start_date="-6m", end_date="today"
                ),
                "status": random.choice(["active", "maintenance", "decommissioned"]),
            }
        )
    df = pd.DataFrame(data)
    load_to_postgres(df, "raw_reactors")


with DAG(
    dag_id="basic_etl_data_generation",
    schedule="@once",
    start_date=datetime(2026, 3, 25),
    catchup=False,
    tags=["data", "basic"],
) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup("generate_all_datasets") as generate_all_datasets:
        gen_start = EmptyOperator(task_id="generation_start")

        reactors = PythonOperator(
            task_id="generate_reactors",
            python_callable=generate_reactors,
            op_args=[NUM_REACTORS],
        )

        gen_end = EmptyOperator(task_id="generation_end")
