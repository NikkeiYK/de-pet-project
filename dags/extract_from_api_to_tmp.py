from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
import requests
import pandas as pd
import os
import json
from datetime import datetime
from airflow.utils.task_group import TaskGroup

URL_API = "https://jsonplaceholder.typicode.com/posts"
DATA_DIR = "/tmp/files"
CHUNK_SIZE = 60
DAG_ID = "extract_from_api_to_tmp"

os.makedirs(DATA_DIR, exist_ok=True)


def get_data(url: str):
    try:
        res = requests.get(url)
        res.raise_for_status()
        data = res.json()
        return data
    except requests.RequestException as e:
        print(f"Запрос упал с ошибкой, ошибка: {e}")
        return []


def chunking_data(data, output_dir: str, chunk_size: int):
    if not data:
        print("Поступил пустой массив данных")
        return

    total_chunks = (len(data) + chunk_size - 1) // chunk_size
    print(f"Разбиваем данные длиной {len(data)} на {total_chunks} чанков.")

    for i in range(0, len(data), chunk_size):
        chunk = data[i:i + chunk_size]
        filename = f"{output_dir}/chunk_{1 if i == 0 else i // chunk_size}.txt"
        with open(filename, 'w', encoding="utf-8") as file:
            for el in chunk:
                file.write(json.dumps(el) + '\n')


def extract_pipeline(**context):
    data = get_data(URL_API)

    chunking_data(data, DATA_DIR, CHUNK_SIZE)


def get_txt_files_list(**context):  # ✅ Новая функция для динамического списка
    return [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR)
            if f.endswith('.txt')]


def analyze_txt_chunk(filename: str):
    """Анализирует один .txt файл - возвращает размер и строки"""
    size_bytes = os.path.getsize(filename)

    # Считаем строки (каждая = 1 JSON объект)
    line_count = 0
    with open(filename, 'r', encoding='utf-8') as f:
        line_count = sum(1 for _ in f)

    print(
        f"Файл: {os.path.basename(filename)} | Размер: {size_bytes/1024/1024:.2f} MB | Строк: {line_count}")

    return {
        'filename': filename,
        'size_mb': round(size_bytes / (1024*1024), 2),
        'lines': line_count
    }


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    catchup=False,
    start_date=datetime(2026, 3, 17)
) as dag:
    start = EmptyOperator("Start")

    task_extract = PythonOperator(
        task_id="extract_pipeline",
        python_callable=extract_pipeline,
    )

    list_files_task = PythonOperator(
        task_id="get_txt_files_list",
        python_callable=get_txt_files_list
    )

    with TaskGroup("analyze_chunks") as analyze_group:

        analyse_start = EmptyOperator(task_id="Analyse_start")

        analyze_tasks = PythonOperator.partial(
            task_id="analyze_txt_chunk",
            python_callable=analyze_txt_chunk,
        ).expand(
            # Ключ 'filename' должен совпадать с аргументом в функции analyze_txt_chunk
            # Значение - это ссылка на выход (output/XCom) предыдущей задачи
            filename=list_files_task.output
        )

    end = EmptyOperator(task_id="End")

    start >> task_extract >> list_files_task >> analyze_group >> end
