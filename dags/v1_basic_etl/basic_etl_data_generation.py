import os
import random
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import psycopg2
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
)
from airflow.models import Variable
from dotenv import load_dotenv
from faker import Faker

load_dotenv()

# Настройки
NUM_BATCHES = 5_000
NUM_REACTORS = 50
NUM_SENSOR_READINGS = 50_000  # 10 млн показаний датчиков
NUM_QUALITY_TESTS = 20_000

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

    create_sql = f'''
        CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" (
            {", ".join(columns)}
        )
    '''  # noqa: Q001

    cur.execute(create_sql)


def load_to_postgres(df, table_name, schema="raw", **context):
    print(f"→ COPY-загрузка {len(df):,} строк в {schema}.{table_name}...")

    # 1. Создаём подключение
    conn = get_connection()

    try:
        with conn.cursor() as cur:
            create_table_if_not_exists(cur, schema, table_name, df)
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False, header=False, na_rep="\\N")
            csv_buffer.seek(0)

            copy_sql = f'''
                COPY "{schema}"."{table_name}" FROM STDIN WITH (FORMAT CSV, NULL '\\N');
            '''  # noqa: Q001
            cur.copy_expert(copy_sql, csv_buffer)
            conn.commit()
            print("✓ Готово")

    except Exception as e:
        print("Error!", e)
    finally:
        conn.close()


def get_ids(table: str, column: str, schema="raw") -> list:

    conn = get_connection()

    try:
        with conn.cursor() as cur:
            sql = f'''
                select {column} from "{schema}"."{table}";
            '''  # noqa: Q001

            cur.execute(sql)
            result = [i[0] for i in cur.fetchall()]
            return result

    except Exception as e:
        print("Error", e)
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


def generate_batches(n, product_list, **context):
    """Генерация производственных партий"""
    data = []
    start_date = datetime.now() - timedelta(days=730)  # 2 года данных

    for i in range(1, n + 1):
        batch_start = fake.date_time_between(start_date=start_date, end_date="now")
        duration_hours = random.uniform(4, 24)

        reactor_ids = get_ids("raw_reactors", "reactor_id")

        data.append(
            {
                "batch_id": i,
                "reactor_id": random.choice(reactor_ids),
                "product_name": fake.random_element(product_list),
                "target_quantity_tons": round(random.uniform(10, 100), 2),
                # Может быть меньше из-за потерь
                "actual_quantity_tons": round(random.uniform(8, 102), 2),
                "process_temp_avg": round(random.uniform(180, 280), 1),
                "process_pressure_avg": round(random.uniform(15, 45), 1),
                "catalyst_id": random.randint(1, 20),  # Ссылка на катализатор
                "batch_start": batch_start,
                "batch_end": batch_start + timedelta(hours=duration_hours),
                "status": random.choices(
                    ["completed", "rejected", "reworked"], weights=[0.92, 0.05, 0.03]
                )[0],
            }
        )
    df = pd.DataFrame(data)
    load_to_postgres(df, "raw_batches")


def generate_sensor_telemetry(n, **context):
    """Генерация временных рядов с датчиков (основной объём данных)"""
    # Генерируем пачками для экономии памяти
    chunk_size = 100_000

    reactor_ids = get_ids("raw_reactors", "reactor_id")
    batch_ids = get_ids("raw_batches", "batch_id")

    for start in range(0, n, chunk_size):
        end = min(start + chunk_size, n)
        chunk = []
        for _ in range(end - start):
            timestamp = fake.date_time_between(start_date="-2y", end_date="now")
            chunk.append(
                {
                    "reading_id": start + len(chunk) + 1,
                    "reactor_id": random.choice(reactor_ids),
                    # 10% - холостой ход
                    "batch_id": random.choice(batch_ids)
                    if random.random() > 0.1
                    else None,
                    "sensor_type": random.choice(
                        ["temperature", "pressure", "flow_rate", "vibration"]
                    ),
                    "value": round(random.uniform(0, 100), 3),
                    "unit": random.choice(["°C", "bar", "m³/h", "mm/s"]),
                    "timestamp": timestamp,
                    "is_anomaly": random.random() < 0.005,  # 0.5% аномалий для тестов
                }
            )
        load_to_postgres(pd.DataFrame(chunk), "raw_sensor_telemetry")


def generate_quality_tests(n, **context):
    """Лабораторные тесты качества"""
    data = []
    batch_ids = get_ids("raw_batches", "batch_id")
    for i in range(1, n + 1):
        data.append(
            {
                "test_id": i,
                "batch_id": random.choice(batch_ids),
                "parameter": fake.random_element(QUALITY_PARAMS),
                "measured_value": round(random.uniform(0.1, 100), 4),
                "spec_min": round(random.uniform(0, 50), 2),
                "spec_max": round(random.uniform(50, 150), 2),
                "test_method": random.choice(["ASTM D1238", "ISO 1133", "GOST 11645"]),
                "lab_date": fake.date_between(start_date="-2y", end_date="today"),
                "passed": None,  # Заполним позже на основе spec
            }
        )
    df = pd.DataFrame(data)
    # Логика: тест пройден, если значение в допуске
    df["passed"] = (df["measured_value"] >= df["spec_min"]) & (
        df["measured_value"] <= df["spec_max"]
    )
    load_to_postgres(df, table_name="raw_quality_tests")


def generate_downtime_events(n, **context):
    """Журнал простоев — критично для расчёта потерь"""
    data = []
    reactor_ids = get_ids("raw_reactors", "reactor_id")
    for i in range(1, n + 1):
        start_time = fake.date_time_between(start_date="-2y", end_date="now")
        duration_hours = random.choice(
            [0.5, 1, 2, 4, 8, 24, 72, 168]
        )  # от 30 мин до недели

        data.append(
            {
                "event_id": i,
                "reactor_id": random.choice(reactor_ids),
                "reason": fake.random_element(DOWNTIME_REASONS),
                "start_time": start_time,
                "end_time": start_time + timedelta(hours=duration_hours),
                "duration_hours": duration_hours,
                "lost_production_tons": round(
                    duration_hours * random.uniform(2, 10), 1
                ),
                "estimated_loss_rub": round(
                    duration_hours * random.uniform(50000, 500000), 2
                ),
            }
        )

    df = pd.DataFrame(data)
    load_to_postgres(df, table_name="raw_downtime_events")


def check_all_tables(**context):
    conn = get_connection()
    tasks_to_run = []

    try:
        with conn.cursor() as cur:
            # Словарь: задача → таблица
            checks = {
                "generate_reactors": "raw_reactors",
                "generate_batches": "raw_batches",
                "generate_quality_tests": "raw_quality_tests",
                "generate_downtime_events": "raw_downtime_events",
                "generate_sensor_telemetry":"raw_sensor_telemetry",
                "generate_catalysts": "raw_catalysts"
            }

            for task_name, table_name in checks.items():
                cur.execute(
                    '''
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = 'raw' AND table_name = %s
                ''',
                    (table_name,),
                )

                table_exists = cur.fetchone()[0] > 0

                needs_generation = False

                if not table_exists:
                    print(f"{table_name}: не существует → генерируем")
                    needs_generation = True
                else:
                    # Таблица есть, проверяем строки
                    cur.execute(f'SELECT COUNT(*) FROM "raw"."{table_name}"')
                    row_count = cur.fetchone()[0]

                    if row_count == 0:
                        print(f"📋 {table_name}: пуста → генерируем")
                        needs_generation = True
                    else:
                        print(f"✅ {table_name}: {row_count} строк → пропускаем")

                if needs_generation:
                    tasks_to_run.append(task_name)

            if not tasks_to_run:
                print("✅ Все таблицы заполнены → пропускаем генерацию")
                return "skip_all"

            print(f"Задачи для запуска: {tasks_to_run}")
            return "generate_reactors"

    except Exception as e:
        print(f"Критическая ошибка проверки: {e}")
        return "skip_generation"
    finally:
        conn.close()


def init_schemas(**context):
    conn = get_connection()

    try:
        with conn.cursor() as cur:
            sql = f'''
            create schema if not exists raw;
            create schema if not exists ods;
            create schema if not exists dwh;
            create schema if not exists dm;
            '''  

            cur.execute(sql)
            conn.commit()
    except Exception as e:
        print("Error", e)
    finally:
        conn.close()
        
def generate_catalysts(n=50, **context):
    """Генерация справочника катализаторов"""
    print(f"🔄 Generating {n} catalysts...")
    
    CATALYST_TYPES = [
        "Ziegler-Natta ZN-1",
        "Ziegler-Natta ZN-2",
        "Metallocene MC-1",
        "Metallocene MC-2",
        "Chromium Cr-1",
        "Vanadium V-1",
    ]
    
    SUPPLIERS = ["SIBUR Catalysts", "BASF", "Dow Chemical", "Mitsui Chemicals", "INEOS"]
    
    data = []
    for i in range(1, n + 1):
        activation_date = fake.date_between(start_date="-3y", end_date="-6m")
        data.append({
            "catalyst_id": i,
            "catalyst_name": f"{fake.random_element(CATALYST_TYPES)}-{i:03d}",
            "type": fake.random_element(CATALYST_TYPES),
            "supplier": fake.random_element(SUPPLIERS),
            "cost_per_kg": round(random.uniform(5000, 50000), 2),
            "activation_date": activation_date,
            "expected_lifetime_hours": random.choice([500, 1000, 1500, 2000]),
            "current_status": random.choice(["active", "depleted", "reserved"]),
            "metal_content_pct": round(random.uniform(1, 10), 2),
        })
    
    df = pd.DataFrame(data)
    load_to_postgres(df, "raw_catalysts", SCHEMA)
    print("✅ Catalysts done")


with DAG(
    dag_id="basic_etl_data_generation",
    schedule="5 * * * *",
    start_date=datetime(2026, 3, 25),
    catchup=False,
    tags=["data", "basic"],
) as dag:
    start = EmptyOperator(task_id="start")

    init = PythonOperator(task_id="init_schemas", python_callable=init_schemas)

    check_data = BranchPythonOperator(
        task_id="check_all_tables", python_callable=check_all_tables
    )

    # Инициализация реакторов
    reactors = PythonOperator(
        task_id="generate_reactors",
        python_callable=generate_reactors,
        op_args=[NUM_REACTORS],
    )
    # Инициализация партий
    batches = PythonOperator(
        task_id="generate_batches",
        python_callable=generate_batches,
        op_args=[NUM_BATCHES, PRODUCTS],
    )
    # Инициализация тестов
    quality_tests = PythonOperator(
        task_id="generate_quality_tests",
        python_callable=generate_quality_tests,
        op_args=[NUM_QUALITY_TESTS],
    )
    # Инициализация ивентов
    downtime_events = PythonOperator(
        task_id="generate_downtime_events",
        python_callable=generate_downtime_events,
        op_args=[2000],
    )
    # Инициализация телеметрии
    sensor_telemetry = PythonOperator(
        task_id="generate_sensor_telemetry",
        python_callable=generate_sensor_telemetry,
        op_args=[NUM_SENSOR_READINGS],
    )
    
    catalyst = PythonOperator(
        task_id="generate_catalysts",
        python_callable=generate_catalysts
    )
    
    # Пропуск всех стадий
    skip_tasks = EmptyOperator(task_id="skip_generation")

    end = EmptyOperator(task_id="end")

    start >> init >> check_data
    check_data >> reactors
    check_data >> skip_tasks
    reactors >> batches
    batches >> [quality_tests, downtime_events, sensor_telemetry, catalyst]
    [quality_tests, downtime_events, sensor_telemetry, catalyst] >> end
