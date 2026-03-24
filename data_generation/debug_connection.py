# debug_connection.py
import psycopg2
from dotenv import load_dotenv
import os
import sys

load_dotenv()

print("🔍 Диагностика подключения к PostgreSQL...")
print(f"HOST: {os.getenv('HOST')}")
print(f"PORT: {os.getenv('PORT')}")
print(f"DB: {os.getenv('INDUSTRY_DB')}")
print(f"USER: {os.getenv('POSTGRES_USER')}")
print(f"SCHEMA: {os.getenv('SCHEMA', 'public')}")
print()

# 🔧 Пробуем подключиться БЕЗ search_path для чистоты теста
try:
    conn = psycopg2.connect(
        host=os.getenv('HOST', 'localhost'),
        port=int(os.getenv('PORT', 5432)),
        dbname=os.getenv('INDUSTRY_DB', 'postgres'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'postgres'),
        connect_timeout=5
    )
    print("✅ Подключение успешно!")

    cur = conn.cursor()

    # Проверка версии
    cur.execute("SELECT version();")
    print(f"📦 PostgreSQL: {cur.fetchone()[0][:60]}...")

    # Проверка схемы
    schema = os.getenv('SCHEMA', 'public')
    cur.execute(
        "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
        (schema,)
    )
    if cur.fetchone():
        print(f"✅ Схема '{schema}' существует")
    else:
        print(f"⚠️ Схема '{schema}' НЕ найдена!")
        print(f"💡 Создайте её: CREATE SCHEMA IF NOT EXISTS {schema};")

    cur.close()
    conn.close()

except psycopg2.Error as e:
    print(f"❌ Ошибка psycopg2: {e}")
    print(f"   Код ошибки: {e.pgcode}")
    print()
    print("🔧 Частые решения:")
    if "Connection refused" in str(e):
        print("   • PostgreSQL не запущен → запустите службу или Docker-контейнер")
        print("   • Неправильный HOST → для Docker используйте 'host.docker.internal'")
    elif "password authentication failed" in str(e):
        print("   • Неверный пароль → проверьте POSTGRES_PASSWORD в .env")
    elif "database does not exist" in str(e):
        print(
            "   • База не создана → создайте: CREATE DATABASE {INDUSTRY_DB};")
    elif "role does not exist" in str(e):
        print("   • Пользователь не найден → проверьте POSTGRES_USER")
    sys.exit(1)
except Exception as e:
    print(f"❌ Неожиданная ошибка: {type(e).__name__}: {e}")
    sys.exit(1)
