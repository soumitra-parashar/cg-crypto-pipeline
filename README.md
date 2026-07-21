# Crypto Market Data ETL Pipeline

## Project Overview

This project is an end-to-end ETL (Extract, Transform, Load) pipeline that ingests cryptocurrency market data from the [CoinGecko API](https://www.coingecko.com/en/api) and processes it into analysis-ready datasets. Raw data is extracted via Python, containerized with Docker, staged in AWS S3, processed at scale using Databricks (PySpark), modeled with dbt, and orchestrated end-to-end with Apache Airflow. Development takes place in GitHub Codespaces.

This project is part of my data engineering portfolio, built to demonstrate hands-on, production-style pipeline experience for data engineering roles.

## Project Goal

The goal of this project is to build a reliable, scalable, and reproducible data pipeline that:

- Extracts real-time cryptocurrency market data (prices, market cap, volume, etc.) from the CoinGecko API
- Stores raw data in a structured, versioned format in AWS S3 (data lake layer)
- Transforms and cleans data at scale using PySpark on Databricks
- Applies modular, tested data models with dbt to produce trusted, analytics-ready tables
- Automates the entire workflow using Airflow, with scheduled runs and failure handling
- Demonstrates a production-style data engineering workflow, from ingestion to consumption, using industry-standard tools

## Tech Stack

| Layer | Tool |
|---|---|
| Language | Python 3.12.1 |
| Extraction | `requests`, CoinGecko API |
| AWS SDK | `boto3`, `python-dotenv` |
| Containerization | Docker |
| Storage | AWS S3 |
| Processing | Databricks (PySpark), Unity Catalog |
| Transformation | dbt |
| Orchestration | Apache Airflow |
| Database | SQL |
| Dev Environment | GitHub Codespaces (Linux) |

## Data Architecture

The pipeline follows the **medallion architecture** (Bronze → Silver → Gold), a standard lakehouse pattern that separates raw, cleaned, and business-ready data into distinct layers.

**Catalog:** `cg_crypto_data`

| Schema | Table | Purpose |
|---|---|---|
| `bronze` | `market_data` | Raw ingested data, unmodified from source |
| `silver` | `market_data` | Cleaned, typed, deduplicated data |
| `gold` | *(pending)* | Business-level analytical marts |

```
CoinGecko API
      │
      ▼
 extract.py (Dockerized)
      │
      ▼
 upload_to_aws.py (boto3)
      │
      ▼
   AWS S3 (raw JSON landing zone)
      │
      ▼
Databricks (PySpark) ── via Unity Catalog External Location
      │
      ▼
 cg_crypto_data.bronze.market_data
      │
      ▼
 cg_crypto_data.silver.market_data
      │
      ▼
   [dbt modeling - in progress]
```

## Progress Log

### Phase 0: Extraction & Upload —  Complete

- Set up project environments and tooling.
- Wrote `extract.py` using `requests` to pull live market data from the CoinGecko API.
- Wrote `upload_to_aws.py` using `boto3` and `python-dotenv` to securely upload the extracted raw JSON to AWS S3, keeping credentials out of source code via environment variables.
- Wrapped `extract.py` in a Docker image (`python:3.12.1-slim` base) for consistent, portable execution.

### Phase 1: Cloud Infrastructure & Security Setup —  Complete

Instead of hardcoding access keys — a security risk — a secure, role-based trust relationship was configured between Databricks and AWS using Unity Catalog.

**1. AWS S3 Landing Zone**
Created a dedicated S3 bucket (`s3://coingecko-crypto-data-lake-06072026/raw/`) to act as the landing zone for raw JSON pulled from the API.

**2. Databricks Unity Catalog Configuration**
Created a dedicated logical container for the project to isolate tables and maintain strict permission scoping:
- Catalog: `cg_crypto_data`
- Schemas: `bronze`, `silver` (following medallion architecture naming)

**3. The Secure Cross-Account Handshake (IAM & External Locations)**
Established a secure connection between AWS and Databricks without exposing long-lived IAM keys:
- Deployed an AWS CloudFormation stack to generate a dedicated IAM Role
- Created a Storage Credential in Databricks using the AWS Role ARN
- Updated the AWS IAM Trust Policy with the Databricks-generated External ID, including a self-assuming trust statement
- Applied an inline Permissions Policy strictly limiting Databricks' access to only the project's S3 bucket
- Created an External Location in Databricks to bridge the S3 bucket to Unity Catalog
- Verified the connection — all permission checks passed (read, write, list, delete, path exists, assume role, external ID condition)

### Phase 2: Bronze Layer Ingestion (Raw Data) —  Complete

Successfully moved data from cloud storage into the distributed compute engine, handling format inconsistencies along the way.

**1. PySpark JSON Ingestion**
```python
spark.sql("USE CATALOG cg_crypto_data")
spark.sql("USE SCHEMA bronze")

raw_df = spark.read \
    .option("multiline", "true") \
    .json("s3://coingecko-crypto-data-lake-06072026/raw/*.json")
```
- Wrote a PySpark script to dynamically read raw JSON files from the S3 External Location.
- **Overcame parsing errors:** identified and resolved Spark's default single-line JSON parsing behavior. The API returns nested, pretty-printed JSON, which initially caused a `_corrupt_record` schema failure.
- **The fix:** applied `.option("multiline", "true")` to force Spark to parse the payload holistically instead of line-by-line.

**2. Delta Table Creation (Nuke & Pave)**
```python
raw_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("market_data")
```
- Wrote the resulting DataFrame into `cg_crypto_data.bronze.market_data` as a Delta table.
- Used `.mode("overwrite")` with `.option("overwriteSchema", "true")` so the table schema cleanly replaced the corrupted state with the correct inferred schema from the fixed read.

**3. Sanity Check / Validation**
Queried the new Bronze Delta table using the PySpark DataFrame API, checking schema, row counts, nulls, duplicates, and value ranges.

### Data Validation —  Complete

Validated the Bronze layer (`cg_crypto_data.bronze.market_data`, 50 rows) across five standard data quality dimensions — completeness, uniqueness, validity, consistency, and accuracy — using **both SQL and PySpark DataFrame API independently**, to cross-confirm findings and demonstrate equivalent fluency in both approaches.

- Completeness, uniqueness, and validity checks: all passed, zero issues.
- Consistency check (`current_price` within 24h high/low range): 2 of 50 rows flagged — `tether-gold` and `pax-gold`, where `current_price` sat marginally below `low_24h` (~0.04% deviation). Root cause: API snapshot timing — `current_price` is captured live, while `high_24h`/`low_24h` reflect a rolling window recalculated on a separate cycle. Logged as an accepted tolerance, not a pipeline defect.
- Both SQL and PySpark validations independently flagged the same two rows, confirming the anomaly is real and not an artifact of either query approach.

Validation logic: `validation.sql`, `sparkvalidation.py`

### Phase 3: Silver Layer — Cleaning & Standardization —  Complete

Transformed the Bronze table into a cleaned, explicitly typed Silver table using PySpark.

**Key steps:**
- Cast `ath_date`, `atl_date`, and `last_updated` from string to timestamp for proper date arithmetic downstream
- Dropped `image` (logo URL — not analytically useful)
- Enforced an explicit schema via `.cast()` on every column, rather than relying on Spark's inferred types, to lock in an intentional contract for the table
- Added a `price_outside_24h_range` boolean flag column to make the known API timing artifact (see Data Validation above) queryable downstream without re-deriving it — documented, not silently corrected
- Ran a defensive duplicate check on `id` before writing
- Wrote the result to `cg_crypto_data.silver.market_data`

**Schema correction:** during this phase, discovered that the `symbol` and `total_volume` fields — present in the raw CoinGecko API response — were missing from an earlier working column list. Re-verified the true Bronze schema directly via `printSchema()`, then re-ran the Silver transformation to include both fields (`symbol` as `StringType`, `total_volume` as `DoubleType`).

**Naming cleanup:** renamed the raw schema from `raw_crypto` to `bronze`, and the Silver table from `staging` to `market_data`, aligning both layers to a consistent `catalog.schema.market_data` convention where the schema name alone indicates the medallion layer.

Transformation logic: `transform_silver.py`

### Phase 4: dbt Project Setup —  Complete

Initialized a dbt project to move from ad-hoc PySpark cleaning into version-controlled, testable, dependency-aware transformations — connected to Databricks via Unity Catalog.

**1. Project Initialization**
- Ran `dbt init`, selected the `databricks` adapter and Unity Catalog authentication
- Catalog: `cg_crypto_data` (shared with the rest of the pipeline)
- Schema: `dbt_dev` — kept intentionally separate from `bronze`/`silver` to distinguish hand-built PySpark tables from dbt-managed models
- Connected using a Databricks personal access token and the project's Serverless SQL Warehouse HTTP path
- Removed dbt's auto-generated example models and cleaned up the resulting unused config reference in `dbt_project.yml`

**2. Source Definition**
Declared `cg_crypto_data.silver.market_data` as a formal dbt **source** (`models/staging/sources.yml`), rather than hardcoding the table path in every model — keeps a single point of change if the Silver table ever moves.

**3. Staging Model**
Built `stg_crypto.sql` — a thin, 1:1 pass-through of the Silver table with explicit column selection (deliberately avoiding `select *`, so any future upstream schema drift surfaces as a visible error rather than silently changing downstream models).

**4. Automated Testing**
Added `schema.yml` with tests that formalize the manual validation checks performed earlier by hand:
- `unique` + `not_null` on `id`
- `not_null` on `current_price`, `market_cap`

```bash
dbt run    # builds stg_crypto as a view
dbt test   # 4/4 tests passing
```

dbt project logic: `coingecko_dbt/models/staging/`

### Phase 5: Gold Layer — Analytical Marts —  Complete

Built business-facing analytical tables on top of `stg_crypto`, each answering a distinct question rather than just re-exposing cleaned data.

| Mart | Answers | Key logic |
|---|---|---|
| `top_gainers_losers` | Which coins moved most in the last 24h? | `movement_direction` label derived from `market_cap_change_percentage_24h` |
| `market_cap_leaderboard` | Who are the biggest coins by market dominance? | `pct_supply_circulating`, guarded against divide-by-zero for uncapped coins via `nullif` |
| `ath_atl_summary` | How far is each coin from its historical peak/trough? | `days_since_ath`/`days_since_atl` via `datediff` — made possible by the timestamp casting done in Silver |

All three are materialized as **tables** (`{{ config(materialized='table') }}`) rather than views, since Gold output is meant to be pre-computed for repeated downstream querying, unlike staging's lighter view materialization. Each model references `stg_crypto` via `{{ ref(...) }}` rather than a hardcoded table path, so dbt can track dependencies and build order automatically.

**Known limitation:** the current dataset is a single point-in-time snapshot, so these marts reflect a single moment rather than trends over time. Time-series analysis (e.g. rank changes over 30 days) would require the pipeline to run repeatedly and accumulate history — which is what Phase 6 (Airflow) enables. A fourth mart (`volume_to_marketcap_ratio`, a liquidity signal) was considered and deliberately deferred until after orchestration is in place, to decide with better information on what's actually worth building on top of accumulated data.

Gold layer logic: `coingecko_dbt/models/marts/`

### Phase 6: Orchestration (Apache Airflow) —  Complete

Setting up automated, unattended orchestration of the pipeline using Apache Airflow, run via Docker Compose.

**1. Airflow Infrastructure**
- Deployed via Docker Compose rather than standalone mode, to match production-style deployment and reuse containerization skills from Phase 0
- Modified the official Airflow Compose file: switched from `CeleryExecutor` to `LocalExecutor`, removing Redis, `airflow-worker`, and Flower — unnecessary distributed-execution components for a single-developer setup. Kept Postgres as the metadata database.
- Generated a `FERNET_KEY` for encrypting stored connection secrets
- Extended the base Airflow image with a custom `Dockerfile` to install `apache-airflow-providers-databricks`, since Databricks operators aren't part of core Airflow

**2. Databricks Job Wrapping**
- Wrapped the Bronze ingestion and Silver transformation notebooks as Databricks Jobs (Workflows → Create Job), giving each a stable Job ID that Airflow can trigger via the Databricks REST API
- Configured an Airflow Connection (`databricks_default`) authenticating via a scoped personal access token (`jobs` scope only, 30-day lifetime, auto-scoping disabled to keep permissions stable)

**3. DAG: Bronze → Silver Orchestration**
```python
ingest_bronze = DatabricksRunNowOperator(
    task_id="ingest_bronze",
    databricks_conn_id="databricks_default",
    job_id="<bronze_job_id>",
)

transform_silver = DatabricksRunNowOperator(
    task_id="transform_silver",
    databricks_conn_id="databricks_default",
    job_id="<silver_job_id>",
)

ingest_bronze >> transform_silver
```
The `>>` dependency ensures Silver never runs on top of a missing or failed Bronze run — verified in practice when a connection misconfiguration caused `ingest_bronze` to fail, and `transform_silver` correctly refused to start rather than running on stale data.

**Issues resolved along the way:**
- A stray, unrelated import line broke DAG parsing entirely — traced and removed
- Airflow connection was initially saved with a reversed ID (`default_databricks` instead of `databricks_default`), causing a "connection not defined" error at runtime
- Initial access token was scoped too narrowly, causing a `403: missing required scopes: jobs` error — resolved by generating a token with explicit `jobs` scope
- First successful run took ~11 minutes due to Serverless compute cold start on a free-tier workspace; confirmed actual progress via Databricks' own Job Runs tab rather than relying on Airflow's UI alone

**Result:** first full DAG run succeeded end-to-end — Bronze ingestion and Silver transformation both triggered, sequenced, and completed by Airflow with no manual notebook execution.

**4. Extending the DAG: Adding dbt**

Extended the pipeline with two additional tasks running dbt directly inside the Airflow container via `BashOperator`:

```python
dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="cd /opt/airflow/coingecko_dbt && DBT_PROFILES_DIR=/opt/airflow/.dbt dbt run",
)

dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="cd /opt/airflow/coingecko_dbt && DBT_PROFILES_DIR=/opt/airflow/.dbt dbt test",
)

ingest_bronze >> transform_silver >> dbt_run >> dbt_test
```

Unlike Bronze/Silver (which require Databricks compute and are triggered via API), dbt only makes lightweight calls to the SQL Warehouse — so `dbt run`/`dbt test` run directly inside the Airflow container itself. This required mounting the `coingecko_dbt/` project and a container-local copy of `profiles.yml` into the Airflow containers, and extending the Airflow image with `dbt-databricks`.

**Issue hit:** installing `apache-airflow-providers-databricks` and `dbt-databricks` in the same `pip install` command caused pip's dependency resolver to select an incompatible provider version, breaking DAG import with `ImportError: cannot import name 'clear_task_instances'`. Fixed by pinning the Databricks provider version explicitly and splitting the installs into separate Dockerfile `RUN` layers, so each resolves independently rather than jointly.

**5. Final Validation & Scheduling**

- Ran the complete 4-task DAG (`ingest_bronze → transform_silver → dbt_run → dbt_test`) multiple times to confirm reliability, not just a single successful run — execution time dropped from ~11 minutes (cold start) to ~4 minutes on warm runs
- Set the DAG schedule to `@daily` (`0 0 * * *`), completing the transition from manually-triggered to fully orchestrated

**Note on scheduling:** the DAG is configured to run daily, but Airflow only executes on schedule while its containers are actively running. Since this project runs in a local Docker Compose setup rather than a persistently deployed environment, the daily schedule is included to demonstrate orchestration capability rather than to run continuously unattended. In a production deployment, this same DAG would run automatically with no additional changes.

---

## Project Status: Complete 

All six phases are built, tested, and orchestrated end-to-end:

**Extract (Python) → S3 (raw landing zone) → Bronze (PySpark/Delta) → Silver (PySpark, cleaned & typed) → dbt (staging + Gold marts, tested) → Airflow (orchestration, scheduled)**

**Possible future extensions** (not required for the current scope, noted for context):
- A fourth Gold mart (`volume_to_marketcap_ratio`) — deferred pending accumulated historical data from repeated pipeline runs
- Deploying Airflow to a persistent environment so the daily schedule runs unattended rather than on-demand

---

*Built as a data engineering portfolio project. Full development history, debugging notes, and decision rationale are documented in `docs/dev_log.md`.*