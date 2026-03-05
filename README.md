# 📈 Real-Time Stock Market Data Pipeline

> An end-to-end streaming data pipeline that ingests live stock quotes from Finnhub, processes them through a Medallion Architecture (Bronze → Silver → Gold), and serves a real-time Power BI dashboard — all orchestrated locally with Docker.

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Kafka](https://img.shields.io/badge/Apache_Kafka-2.13-black?logo=apachekafka)
![Airflow](https://img.shields.io/badge/Apache_Airflow-2.9.3-017CEE?logo=apacheairflow)
![dbt](https://img.shields.io/badge/dbt-1.x-FF694B?logo=dbt)
![Snowflake](https://img.shields.io/badge/Snowflake-OLAP-29B5E8?logo=snowflake)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![MinIO](https://img.shields.io/badge/MinIO-S3_Compatible-C72E49?logo=minio)
![Power BI](https://img.shields.io/badge/Power_BI-DirectQuery-F2C811?logo=powerbi)

---

## Architecture Overview

```
Finnhub API
     │  (REST, 6s polling)
     ▼
┌─────────────┐
│  Producer   │  ── streams enriched JSON payloads ──►  Kafka Topic: stock-quotes
│  (Python)   │                                              │  (3 partitions)
└─────────────┘                                              │
                                                             ▼
                                                    ┌─────────────────┐
                                                    │    Consumer     │  ── sinks JSON ──►  MinIO (Bronze Layer)
                                                    │    (Python)     │                     s3://bronze-stocks-quotes/
                                                    └─────────────────┘                          {SYMBOL}/{timestamp}.json
                                                                                                 │
                                                                                    ┌────────────┘
                                                                                    │  Airflow DAG (every 1 min)
                                                                                    │  MinIO → Snowflake COPY INTO
                                                                                    ▼
                                                                          Snowflake: BRONZE_STOCK_QUOTES_RAW
                                                                                    │  (VARIANT column)
                                                                                    │
                                                                          ┌─────────┴──────────────────────────┐
                                                                          │           dbt Transformations      │
                                                                          │  Bronze → Silver → Gold (Views)    │
                                                                          └─────────┬──────────────────────────┘
                                                                                    │
                                                                                    ▼
                                                                             Power BI Dashboard
                                                                             (DirectQuery mode)
```

**Data flow summary:**
- **Bronze**: Raw JSON, immutable, stored as Snowflake `VARIANT` column
- **Silver**: JSON flattened, null-filtered, numerics rounded — `stg_stock_quotes` → `silver_clean_stock_quotes`
- **Gold**: Business-ready views for each chart type — `gold_kpi`, `gold_candlestick`, `gold_treechart`

---

## Tech Stack

| Layer | Tool | Why |
|---|---|---|
| Data Source | Finnhub API | Free real-time stock quotes with generous rate limits |
| Message Broker | Apache Kafka (Docker) | Decouples ingestion from storage; enables replay via offset management |
| Object Storage | MinIO | S3-compatible local storage; forces understanding of path-style addressing |
| Orchestration | Apache Airflow 2.9.3 | Minute-level scheduling; TaskFlow API for clean DAG definition |
| Data Warehouse | Snowflake | Native JSON support via VARIANT; serverless compute model |
| Transformation | dbt | SQL-based lineage, testability, and layer separation |
| Visualization | Power BI (DirectQuery) | Live data refresh without caching layer |
| Containerization | Docker Compose | Reproducible local environment across all services |

---

## Project Structure

```
.
├── producer.py                  # Polls Finnhub API → publishes to Kafka topic
├── consumer.py                  # Reads Kafka topic → sinks JSON files to MinIO
├── docker-compose.yml           # Spins up Zookeeper, Kafka, Kafdrop, MinIO, Airflow, Postgres
├── .env.example                 # Environment variable template (copy to .env)
├── dags/
│   └── transfer_to_snowflake.py # Airflow DAG: extracts MinIO files → COPY INTO Snowflake
└── stock_market/                # dbt project root
    ├── dbt_project.yml
    ├── models/
    │   ├── sources.yml           # Declares Snowflake source table
    │   ├── bronze/
    │   │   └── stg_stock_quotes.sql       # Flattens VARIANT JSON into typed columns
    │   ├── silver/
    │   │   └── silver_clean_stock_quotes.sql  # Null filtering, numeric rounding
    │   └── gold/
    │       ├── gold_kpi.sql               # Latest price per symbol (KPI cards)
    │       ├── gold_candlestick.sql       # OHLC aggregation per symbol per day
    │       └── gold_treechart.sql         # Avg price + volatility for treemap
    └── README.md
```

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (v4.x+)
- Python 3.11+
- [Finnhub API Key](https://finnhub.io/) (free tier, ~60 calls/min)
- [Snowflake Account](https://signup.snowflake.com/) (free trial available)
- [dbt CLI](https://docs.getdbt.com/docs/core/installation) (`pip install dbt-snowflake`)
- Power BI Desktop (Windows) — optional, for dashboard

---

## Quick Start

### 1. Clone & Configure Environment

```bash
git clone https://github.com/<your-username>/stock-market-pipeline.git
cd stock-market-pipeline
cp .env.example .env
```

Edit `.env` with your credentials. **Important: do not use quotes around values in `.env` when used with Docker Compose — Docker reads them as literal characters.**

```env
# Finnhub
API_KEY=your_finnhub_key

# MinIO (Docker internal hostname — not localhost)
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=password123
BUCKET=bronze-stocks-quotes
LOCAL_DIR=/tmp/airflow_minio_downloads

# Snowflake
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_id
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DB=STOCKS
SNOWFLAKE_SCHEMA=COMMON
```

### 2. Start All Services

```bash
docker compose up -d
```

Verify services are healthy:

| Service | URL |
|---|---|
| Kafdrop (Kafka UI) | http://localhost:9000 |
| MinIO Console | http://localhost:9001 (admin / password123) |
| Airflow UI | http://localhost:8080 (admin / admin) |

### 3. Create Kafka Topic

Open Kafdrop at `http://localhost:9000` → **+ New** → create topic `stock-quotes` with **3 partitions**, replication factor **1**.

### 4. Provision Snowflake

Run the following in Snowflake's SQL editor:

```sql
USE ROLE ACCOUNTADMIN;
CREATE DATABASE stocks;
CREATE SCHEMA stocks.common;
CREATE TABLE stocks.common.bronze_stock_quotes_raw (v VARIANT);
```

### 5. Start the Streaming Pipeline

Open two terminal windows:

```bash
# Terminal 1 — Producer: polls Finnhub every 6 seconds
python producer.py

# Terminal 2 — Consumer: reads Kafka, sinks to MinIO
python consumer.py
```

### 6. Trigger Airflow DAG

Navigate to `http://localhost:8080`, enable the `minio_to_snowflake_taskflow` DAG. It runs every minute, extracting files from MinIO and loading them into Snowflake via `COPY INTO`.

### 7. Run dbt Transformations

```bash
cd stock_market
dbt run        # builds all Bronze / Silver / Gold views
dbt test       # validates source freshness and column constraints
```

### 8. Connect Power BI

Open Power BI Desktop → **Get Data → Snowflake** → enter your account and warehouse details → select **DirectQuery** → load the `GOLD_*` views.

---

## Key Engineering Decisions & Lessons Learned

These are the non-trivial problems encountered during development — the kind that managed cloud services quietly abstract away.

### 1. Kafka's Two-Listener Network Model

Kafka requires two distinct listener configurations: one for intra-Docker communication (`PLAINTEXT://kafka:9092`) and one for external host access (`PLAINTEXT_HOST://localhost:29092`). When a client first connects, Kafka returns an `ADVERTISED_LISTENER` address — the actual address it will use for all subsequent data transmission. Misconfiguring this causes the broker to return an internal hostname (`kafka:9092`) to your host machine, which DNS cannot resolve.

**Takeaway:** Understanding this mechanism explains why managed services like AWS MSK or Confluent Cloud feel frictionless — they handle this routing layer transparently. Building it locally forced a genuine understanding of how Kafka's distributed networking actually works.

### 2. MinIO Requires Path-Style S3 Addressing

By default, Boto3 uses virtual-hosted style addressing (`http://{bucket}.localhost:9002`), which fails DNS resolution on local MinIO. Forcing path-style (`Config(s3={'addressing_style': 'path'})`) resolves this by keeping the bucket name in the URL path (`http://localhost:9002/{bucket}/...`).

### 3. Docker Container Isolation & Environment Variable Injection

Environment variables present on the host machine are **not** inherited by Docker containers. Variables must be explicitly injected via `env_file` in `docker-compose.yml`. Additionally, the MinIO endpoint must use Docker's internal service name (`http://minio:9000`) rather than `http://localhost:9002` — because from inside a container, `localhost` resolves to the container itself, not the host.

### 4. Kafka Consumer Offset Management

`auto_offset_reset='earliest'` only applies when a Consumer Group has **no committed offset**. Once a group has read and committed progress, this setting is ignored. To force re-read from the beginning, either reset the group offset via CLI (`kafka-consumer-groups --reset-offsets --to-earliest`) or call `consumer.poll()` followed by `consumer.seek_to_beginning()` in code.

---

## Known Limitations & Production Gaps

This project is intentionally a learning-focused implementation. The following gaps would need to be addressed before production deployment:

| Issue | Current State | Production Solution |
|---|---|---|
| **Small File Problem** | Every Kafka message creates one JSON file in MinIO | Use Kafka Connect S3 Sink to batch into Parquet files (e.g., every 10MB or 5 min) |
| **dbt Materialization** | All models are `view` (full recompute on every query) | Switch Gold models to `incremental` with `unique_key` on `(symbol, fetched_at)` |
| **Credential Management** | `.env` file injected into Docker | Use Airflow Connections & Variables or a secrets backend (Vault, AWS SSM) |
| **Single-threaded Producer** | Sequential API calls across 5 symbols | Refactor to `asyncio` for concurrent I/O; add producer instance per symbol group |
| **No Schema Validation** | Raw JSON written without contract enforcement | Add JSON Schema validation at consumer layer or use a Schema Registry |
| **Finnhub Rate Limit** | ~60 calls/min (free tier) | Upgrade tier or distribute symbols across multiple API keys |

---

## Dashboard Preview

<img width="621" height="338" alt="截圖 2026-03-05 上午11 11 47" src="https://github.com/user-attachments/assets/cc7850b6-6557-4eb7-a170-595c3b2acc36" />


The Power BI dashboard surfaces three views built from the Gold layer:
- **KPI Cards** (`gold_kpi`): Latest price, change amount, and change percent per symbol
- **Candlestick Chart** (`gold_candlestick`): OHLC aggregation per symbol per trading day, last 12 periods
- **Treechart** (`gold_treechart`): Symbol comparison by average price and relative volatility

---

## Acknowledgments

* **Data with Jay** for providing this amazing project [tutorial](https://www.youtube.com/watch?v=JCDrvXwh4BQ) | [Linkedin](https://www.linkedin.com/in/jayachandrakadiveti/)

---


## License

MIT
