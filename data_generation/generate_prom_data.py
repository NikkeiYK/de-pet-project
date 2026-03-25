# data_generator_sibur.py
import os
import random
from datetime import datetime, timedelta
from io import StringIO

import numpy as np
import pandas as pd
import psycopg2
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

SCHEMA = os.getenv("SCHEMA", "ods")

fake = Faker("ru_RU")
Faker.seed(42)
np.random.seed(42)

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


def generate_reactors(n):
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
    return pd.DataFrame(data)


def generate_batches(n, reactor_ids, product_list):
    """Генерация производственных партий"""
    data = []
    start_date = datetime.now() - timedelta(days=730)  # 2 года данных

    for i in range(1, n + 1):
        batch_start = fake.date_time_between(start_date=start_date, end_date="now")
        duration_hours = random.uniform(4, 24)

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
    return pd.DataFrame(data)


def generate_sensor_telemetry(n, reactor_ids, batch_ids):
    """Генерация временных рядов с датчиков (основной объём данных)"""
    # Генерируем пачками для экономии памяти
    chunk_size = 100_000
    chunks = []

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
        chunks.append(pd.DataFrame(chunk))

    return pd.concat(chunks, ignore_index=True)


def generate_quality_tests(n, batch_ids):
    """Лабораторные тесты качества"""
    data = []
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
    return df


def generate_downtime_events(n, reactor_ids):
    """Журнал простоев — критично для расчёта потерь"""
    data = []
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
    return pd.DataFrame(data)


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

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" (
            {", ".join(columns)}
        )
    """

    cur.execute(create_sql)


def load_to_postgres(df, table_name, schema="ods"):
    """Быстрая загрузка через PostgreSQL COPY"""
    print(f"→ COPY-загрузка {len(df):,} строк в {schema}.{table_name}...")

    # 1. Создаём подключение
    conn = get_connection()

    try:
        with conn.cursor() as cur:
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


if __name__ == "__main__":
    print("🏭 Генерация данных для SIBUR DWH Pet Project")
    print("=" * 50)

    # Инициализация подключения
    conn_str = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}?options=-csearch_path%3D{SCHEMA}"

    # Генерация справочников
    df_reactors = generate_reactors(NUM_REACTORS)
    reactor_ids = df_reactors["reactor_id"].tolist()

    # Генерация основных данных
    df_batches = generate_batches(NUM_BATCHES, reactor_ids, PRODUCTS)
    batch_ids = df_batches["batch_id"].tolist()

    print("Генерация телеметрии (10 млн строк) — это займёт ~2-3 минуты...")
    df_telemetry = generate_sensor_telemetry(
        NUM_SENSOR_READINGS, reactor_ids, batch_ids
    )

    df_quality = generate_quality_tests(NUM_QUALITY_TESTS, batch_ids)
    df_downtime = generate_downtime_events(2000, reactor_ids)

    # Загрузка в БД
    print("\n Загрузка в PostgreSQL...")
    load_to_postgres(df_reactors, "raw_reactors")
    load_to_postgres(df_batches, "raw_batches")
    load_to_postgres(df_telemetry, "raw_sensor_telemetry")
    load_to_postgres(df_quality, "raw_quality_tests")
    load_to_postgres(df_downtime, "raw_downtime_events")

    print("\n✅ Все данные загружены! Можно приступать к проектированию DWH.")
