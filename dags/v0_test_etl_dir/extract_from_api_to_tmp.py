# Динамические распараллеленные  такски
import json
import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

DATA_DIR = Variable.get("DATA_DIR")
CHUNK_SIZE = 60
DAG_ID = "extract_from_api_to_tmp"
conn = BaseHook.get_connection("api_jsonplaceholder")
BASE_URL = f"{conn.host}/posts"


@task
def get_data(url: str):
    try:
        res = requests.get(url)
        res.raise_for_status()
        data = res.json()
        return data
    except requests.RequestException as e:
        print(f"Запрос упал с ошибкой, ошибка: {e}")
        return []


@task
def chunking_data(data, output_dir: str, chunk_size: int):
    if not data:
        print("Поступил пустой массив данных")
        return

    total_chunks = (len(data) + chunk_size - 1) // chunk_size
    print(f"Разбиваем данные длиной {len(data)} на {total_chunks} чанков.")

    os.makedirs(DATA_DIR, exist_ok=True)

    filenames = []

    for idx, i in enumerate(range(0, len(data), chunk_size)):
        chunk = data[i : i + chunk_size]
        filename = f"{output_dir}/chunk_{idx + 1}.txt"
        with open(filename, "w", encoding="utf-8") as file:
            for el in chunk:
                file.write(json.dumps(el) + "\n")
            filenames.append(filename)
    return filenames


@task(sla=timedelta(minutes=2))
def analyze_txt_chunk(filename: str):
    """Анализирует один .txt файл - возвращает размер и строки"""
    size_bytes = os.path.getsize(filename)

    # Считаем строки (каждая = 1 JSON объект)
    line_count = 0
    with open(filename, "r", encoding="utf-8") as f:
        line_count = sum(1 for _ in f)

    print(
        f"Файл: {os.path.basename(filename)} | Размер: {size_bytes / 1024 / 1024:.2f} MB | Строк: {line_count}"
    )

    return {
        "filename": filename,
        "size_mb": round(size_bytes / (1024 * 1024), 2),
        "lines": line_count,
    }


@task
def collect_result(data: list):
    for i in data:
        print(f"Filename: {i['filename']}, size: {i['size_mb']}, lines: {i['lines']}")


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    catchup=False,
    start_date=datetime(2026, 3, 17),
    tags=["example", "taskflow"],
) as dag:
    dag.doc_md = __doc__

    start = EmptyOperator(task_id="Start")

    data = get_data(BASE_URL)

    filenames = chunking_data(data, DATA_DIR, CHUNK_SIZE)

    with TaskGroup("analyze_chunks") as analyze_group:
        analyse_start = EmptyOperator(task_id="Analyse_start")

        result = analyze_txt_chunk.expand(filename=filenames)

        collection = collect_result(result)

        analyse_end = EmptyOperator(task_id="Analyse_end")

        analyse_start >> result >> collection >> analyse_end

    end = EmptyOperator(task_id="End")

    start >> data >> filenames >> analyze_group >> end
