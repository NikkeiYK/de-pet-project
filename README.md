# DE Pet Project — Modern Data Stack Sandbox

![Docker](https://img.shields.io/badge/-Docker-2496ED?logo=docker&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/-Airflow-017CEE?logo=apacheairflow&logoColor=white)
![ClickHouse](https://img.shields.io/badge/-ClickHouse-FCC624?logo=clickhouse&logoColor=black)
![dbt](https://img.shields.io/badge/-dbt-FF694B?logo=dbt&logoColor=white)
![Minio](https://img.shields.io/badge/-Minio-1A74EE?logo=minio&logoColor=white)
![Apache Superset](https://img.shields.io/badge/-Superset-20A0FF?logo=apache&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/-PostgreSQL-4169E1?logo=postgresql&logoColor=white)
![Redis](https://img.shields.io/badge/-Redis-DC382D?logo=redis&logoColor=white)
![Python](https://img.shields.io/badge/-Python-3776AB?logo=python&logoColor=white)

A **Data Engineering pet project** implementing medallion architecture (Raw → ODS → DM) with a fully containerized modern open-source data stack. Built for hands-on practice with industry-standard data pipeline patterns, orchestration, storage, transformation, and BI visualization.

---

## Architecture

```
  ┌───────────────────┐
  │  FakeStore API    │
  │  /products        │
  └────────┬──────────┘
           │  HTTP GET
           ▼
  ┌───────────────────┐     ┌───────────────────┐     ┌───────────────────┐
  │     Minio (S3)    │     │   ClickHouse ODS  │     │   ClickHouse DM   │
  │  Parquet (Snappy) │────▶│   ods.products    │────▶│  dm_products     │  
  │  products-catalog │     │   MergeTree       │     │  (by category)    │
  └───────────────────┘     └───────────────────┘     └───────────────────┘
           │                          ▲                         ▲
           │                          │                         │
           │              ┌───────────┴─────────────┐           │
           │              │  Apache Airflow 3.1.6   │           │
           │              │  CeleryExecutor         │           │
           │              │  Redis + PostgreSQL     │           │
           │              └─────────────────────────┘           │
           │                          │                         │
           │              ┌───────────┘                         │
           │              ▼                                     │
           │     ┌───────────────────┐                          │
           └────▶   dbt-runner      │───────────────────────────
                 │  stg_products     │
                 │  dm_products      │
                 └───────────────────┘

  ┌───────────────────┐
  │   Apache Superset │
  │   BI Dashboards   │
  └───────────────────┘
```

### Data Flow

1. **Extract** — Airflow DAG pulls products from [FakeStoreAPI](https://fakestoreapi.com), converts to Parquet (Snappy compressed), uploads to Minio S3
2. **Load (Raw → ODS)** — Airflow DAG reads Parquet via ClickHouse `s3()` table function, inserts into `ods.products` (MergeTree engine)
3. **Transform (ODS → DM)** — Airflow triggers dbt-runner which:
   - `stg_products` — view filtering products with >= 100 ratings
   - `dm_products` — materialized table with category-level aggregates (avg price, rating, count)
4. **Visualize** — Superset connects to ClickHouse DM for dashboards

---

## Tool Selection Rationale

| Tool | Why |
|------|-----|
| **Apache Airflow 3.1.6** | Industry-standard workflow orchestrator; rich UI, dependency management, scheduling. CeleryExecutor enables distributed task execution. |
| **ClickHouse 24.8** | Columnar OLAP database optimized for real-time analytical queries. Superior aggregation performance over traditional row-store DBs. |
| **Minio** | Lightweight S3-compatible object storage. Enables local development without cloud costs while maintaining S3 API compatibility for future cloud migration. |
| **dbt** | Declarative SQL-first transformations with built-in testing, documentation generation, and version control. Separates transformation logic from orchestration. |
| **Apache Superset 4.0** | Open-source BI platform with rich visualization library, SQL Lab, and no-code chart builder. Native ClickHouse support via SQLAlchemy. |
| **Redis 7.2** | High-performance in-memory cache and message broker. Used as Celery backend for task queue management. |
| **PostgreSQL 16** | Reliable relational database for Airflow metadata storage and Celery result backend. Battle-tested in production. |

---

## Project Structure

```
├── .env                          # Environment variables (UID, ClickHouse creds)
├── .env.example                  # Template for .env
├── docker-compose.yaml           # Main orchestration — all services
├── Dockerfile.dbt                # Custom dbt image with clickhouse adapter
├── profiles.yml                  # dbt ClickHouse connection profile
│
├── config/
│   └── airflow.cfg               # Airflow configuration (1460+ lines)
│
├── dags/                         # Airflow DAGs (Python)
│   ├── extract_data_from_api_to_s3.py          # API → Minio S3
│   ├── raw_from_s3_to_dwh.py                  # Minio S3 → ClickHouse ODS
│   └── from_ods_to_dm_products_category.py    # ClickHouse ODS → DM via dbt
│
├── dbt/                          # dbt project — transformation layer
│   ├── dbt_project.yml           # dbt project configuration
│   ├── models/
│   │   ├── sources.yaml          # Source definitions (api_ods, api_dm)
│   │   ├── staging/
│   │   │   └── stg_products.sql  # Staging view — filter & clean
│   │   └── marts/
│   │       └── dm_products.sql   # Data mart — category aggregates
│   ├── tests/                    # dbt tests (placeholder)
│   ├── macros/                   # Custom dbt macros (placeholder)
│   ├── seeds/                    # Static seed data (placeholder)
│   ├── snapshots/                # Slowly changing dimensions (placeholder)
│   └── target/                   # Compiled dbt artifacts
│
├── data/                         # Minio S3 persistent storage
│   └── products-catalog/         # Parquet files by date
│       └── raw/products-bucket/
│           └── YYYY-MM-DD/       # Daily snapshots
│
├── superset/
│   └── init.sh/                  # Superset init scripts (placeholder)
│
├── logs/                         # Airflow task logs
│
├── plugins/                      # Airflow custom plugins (placeholder)
│
└── venv/                         # Local Python virtual env (for dev only)
```

---

## Services & Ports

| Service | Image | Port(s) | URL | Default Credentials |
|---------|-------|---------|-----|-------------------|
| **Minio API** | `minio/minio:RELEASE.2025-02-18` | `9002:9000` | — | `minioadmin` / `minioadmin` |
| **Minio Console** | (same container) | `9003:9003` | http://localhost:9003 | `minioadmin` / `minioadmin` |
| **PostgreSQL (Airflow)** | `postgres:16` | — (internal) | — | `airflow` / `airflow` |
| **Redis** | `redis:7.2-bookworm` | — (internal 6379) | — | — |
| **Airflow Web UI** | `apache/airflow:3.1.6` | `8080:8080` | http://localhost:8080 | `admin` / `admin` |
| **Airflow API** | (same container) | `8080` | http://localhost:8080/api/v1 | `admin` / `admin` |
| **ClickHouse HTTP** | `clickhouse/clickhouse-server:24.8` | `8123:8123` | — | `default` / `clickhouse` |
| **ClickHouse Native** | (same container) | `9000:9000` | — | `default` / `clickhouse` |
| **ClickHouse UI** | `ghcr.io/caioricciuti/ch-ui:latest` | `8081:5522` | http://localhost:8081 | `default` / `clickhouse` |
| **Apache Superset** | `apache/superset:4.0.0` | `8088:8088` | http://localhost:8088 | `admin` / `admin` |
| **PostgreSQL (Superset)** | `postgres:16` | — (internal) | — | `superset` / `superset` |
| **dbt-runner** | `python:3.10-slim` + dbt | — (CLI) | — | — |
| **Flower** 🔸 | `apache/airflow:3.1.6` | `5555:5555` | http://localhost:5555 | — |

🔸 — requires `--profile flower` to start.

---

## How to Run

### Prerequisites

- **Docker** & **Docker Compose** (v2+)
- **RAM**: 20+ GB recommended (all services combined)
- **Disk**: 10+ GB free for images + data volumes
- **OS**: Linux, macOS, or Windows (WSL2 recommended)

### Quick Start

```bash
# 1. Clone and enter project
git clone <repo-url>
cd de-pet-project

# 2. Create environment file
cp .env.example .env

# 3. Start all services
docker compose up -d

# 4. Wait for initialization (2-3 minutes)
#    The airflow-init container runs DB migrations and user creation.
docker compose logs -f airflow-init
# Wait until you see "Initialization complete"

# 5. Verify services
docker compose ps
```

### Run DAGs

Open **Airflow UI** at http://localhost:8080 (`admin` / `admin`).

Trigger DAGs **in order** (each waits for the previous via `ExternalTaskSensor`):

1. **`EXTRACT_DATA_FROM_API_TO_S3`** — pulls products from FakeStoreAPI → Minio S3
2. **`raw_from_s3_to_dwh`** — loads Parquet from S3 → ClickHouse `ods.products`
3. **`from_ods_to_dm_products_category`** — runs dbt transformations → ClickHouse DM

You can also trigger all three at once — the sensors will wait automatically.

### Verify Data

- **ClickHouse UI**: http://localhost:8081 — explore `ods.products` and `dm.products`
- **Direct SQL**:
  ```bash
  docker exec -it $(docker ps -q -f name=clickhouse) clickhouse-client
  SELECT * FROM ods.products;
  SELECT * FROM dm.products;
  ```

### Run dbt Manually

```bash
# Run staging model
docker exec dbt_runner dbt run --select stg_products

# Run data mart model
docker exec dbt_runner dbt run --select dm_products

# Run all models
docker exec dbt_runner dbt run

# Generate dbt docs
docker exec dbt_runner dbt docs generate
```

### Access Superset

http://localhost:8088 (`admin` / `admin`)

Add ClickHouse database connection:
- **Host**: `clickhouse`
- **Port**: `8123`
- **Database**: `dm`
- **Username**: `default`
- **Password**: `clickhouse`

---

## Resource Usage

### RAM Consumption (~20 GB total)

| Service | Typical RAM | Notes |
|---------|-----------|-------|
| Airflow services (x5 containers) | ~8–10 GB | Each container ~1.5–2 GB (Celery worker is heaviest) |
| ClickHouse | ~2–3 GB | Caches, query processing, MergeTree parts |
| Superset | ~1–2 GB | Python app + metadata queries |
| Minio | ~0.5–1 GB | Mostly idle for small datasets |
| PostgreSQL (x2) | ~1–2 GB | Shared buffers, connections |
| Redis | ~0.3 GB | In-memory queue |
| dbt-runner | ~0.3 GB | Only active during transformation |
| Flower | ~0.3 GB | Optional, Celery monitoring |
| **Total** | **~16–20 GB** | Varies by activity level |

### Disk Usage

| Item | Size |
|------|------|
| Docker images (pulled) | ~3.5 GB |
| ClickHouse data | grows with data volume |
| Minio /data | grows with Parquet files |
| Airflow logs | grows with DAG runs |
| dbt compiled artifacts | negligible |

### Optimization Tips

#### 1. Use Docker Compose Profiles

Start only what you need:

```bash
# Minimal stack (orchestration + storage + DWH)
docker compose --profile min up -d

# Full stack without optional monitoring
docker compose up -d
docker compose stop flower

# Only dbt + ClickHouse (for local dbt development)
docker compose up -d clickhouse dbt-runner
```

Available profiles are defined in `docker-compose.yaml`.

#### 2. Set Per-Container Memory Limits

Create `docker-compose.override.yml`:

```yaml
services:
  clickhouse:
    deploy:
      resources:
        limits:
          memory: 2G
  airflow-worker:
    deploy:
      resources:
        limits:
          memory: 2G
```

Apply with: `docker compose -f docker-compose.yaml -f docker-compose.override.yml up -d`

#### 3. Reduce Airflow Parallelism

In `docker-compose.yaml` environment variables or `config/airflow.cfg`:

```ini
parallelism = 8
dag_concurrency = 4
max_active_tasks_per_dag = 4
```

#### 4. Disable Unused Services

The `postgres-dwh` service (port 5433) is not currently used by any DAG — can be safely removed from `docker-compose.yaml`.

#### 5. Clean Up Old Data

```bash
# Remove unused Docker resources
docker system prune -a --volumes
```

---

## Roadmap / Future Plans

### Apache Spark
Distributed data processing for large-scale transformations. Integration via ClickHouse Spark connector or Spark-S3 direct read/write. Enables complex ETL beyond SQL capabilities.

### Apache Kafka
Real-time data streaming with event-driven architecture. Replace batch API extraction with CDC or event streams. Topics: `raw_products`, `enriched_products`.

### Alerts & Monitoring
- **DAG failure alerts** — Slack / email notifications via Airflow `SlackWebhookHook`
- **Infrastructure monitoring** — Prometheus metrics export + Grafana dashboards
- **Performance tracking** — DAG duration, task latency, resource trends

### Airflow Tests
- **Unit tests** — test DAG structure, task dependencies, schedule intervals
- **Validation tests** — `dag-bag` integrity check, import errors
- **Integration tests** — test DAGs against local containers (testinfra / pytest)

### dbt Tests
- **Generic tests** — `unique`, `not_null`, `accepted_values`, `relationships` on staging models
- **Custom tests** — business logic validation (price > 0, valid categories)
- **Data freshness** — `dbt source freshness` to monitor data latency

### Performance Optimization
- **ClickHouse partitioning** — by date or category for faster queries
- **Skipping indexes** — bloom filter / minmax indexes on frequently filtered columns
- **Materialized views** — pre-computed aggregates for dashboard queries
- **dbt incremental models** — replace full refresh with incremental loads

### Advanced Dashboards
- **Sales analytics** — revenue trends, top categories, price distribution
- **Product catalog KPIs** — rating analysis, category performance
- **Pipeline monitoring** — DAG health, data volume tracking over time

---

## Troubleshooting

### "airflow-init" container hangs or fails

```
docker compose logs airflow-init
```

**Solution**: Ensure `.env` contains `AIRFLOW_UID=4000`. On Linux, the UID must match the host user's UID for volume permissions.

### ClickHouse connection refused

```
clickhouse_connect.common.ClickhouseError: ... Connection refused
```

**Solution**: ClickHouse takes 10–20s to initialize. Wait and retry. Verify with:

```bash
docker compose logs clickhouse
# Look for: "Ready for connections"
```

### Minio bucket not found

```
S3Error: The specified bucket does not exist
```

**Solution**: The bucket `products-catalog` is auto-created by the DAG. Either:
- Trigger the DAG (it creates the bucket on first run)
- Create manually via Minio Console at http://localhost:9003

### dbt-runner command fails

```
Runtime Error: Database error: Code: 516. DB::NetException: Connection refused
```

**Solution**: Ensure ClickHouse is running and reachable. The `dbt-runner` container connects via internal Docker network to hostname `clickhouse`.

### Out of memory / Docker exits with 137

```
Container exited with code 137
```

**Solution**: Reduce parallelism (see [Optimization](#optimization-tips)), or add `memory_limit` in `docker-compose.override.yml`. On Docker Desktop, increase allocated memory in Settings → Resources.

### Port conflicts on localhost

```
Error response from daemon: Ports are not available
```

**Solution**: Change port mappings in `docker-compose.yaml`. Example:

```yaml
services:
  airflow-apiserver:
    ports:
      - "8081:8080"  # changed from 8080
```

### Airflow user "admin" already exists

```
airflow users create ... User with username admin already exists.
```

**Solution**: This is non-fatal — the `airflow-init` script handles duplicates with `|| true`. If stuck, reset with:

```bash
docker compose down -v   # WARNING: removes volumes (DB data)
docker compose up -d
```

### dbt logs not visible

```
dbt: command not found
```

**Solution**: The `dbt-runner` container uses a custom entrypoint. Use the full command:

```bash
docker exec dbt_runner dbt run
```

If the container is not running:

```bash
docker compose up -d dbt-runner
```

### Windows — volume mount permission issues

```
PermissionError: [Errno 13] Permission denied: '/opt/airflow/logs'
```

**Solution**: Use WSL2 backend for Docker Desktop. Ensure `.env` has `AIRFLOW_UID=4000`. On Windows, shared drives must be enabled in Docker Desktop Settings.

---

## Requirements

- **Docker** 24+ with Docker Compose v2
- **RAM**: 20+ GB (16 GB minimum with tuning)
- **Disk**: 10+ GB free
- **OS**: Linux, macOS, Windows (WSL2 recommended)

---

## License

MIT
