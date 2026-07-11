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

### Phase 0: Extraction & Upload — ✅ Complete

- Set up project environments and tooling.
- Wrote `extract.py` using `requests` to pull live market data from the CoinGecko API.
- Wrote `upload_to_aws.py` using `boto3` and `python-dotenv` to securely upload the extracted raw JSON to AWS S3, keeping credentials out of source code via environment variables.
- Wrapped `extract.py` in a Docker image (`python:3.12.1-slim` base) for consistent, portable execution.

### Phase 1: Cloud Infrastructure & Security Setup — ✅ Complete

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

### Phase 2: Bronze Layer Ingestion (Raw Data) — ✅ Complete

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

### Data Validation — ✅ Complete

Validated the Bronze layer (`cg_crypto_data.bronze.market_data`, 50 rows) across five standard data quality dimensions — completeness, uniqueness, validity, consistency, and accuracy — using **both SQL and PySpark DataFrame API independently**, to cross-confirm findings and demonstrate equivalent fluency in both approaches.

- Completeness, uniqueness, and validity checks: all passed, zero issues.
- Consistency check (`current_price` within 24h high/low range): 2 of 50 rows flagged — `tether-gold` and `pax-gold`, where `current_price` sat marginally below `low_24h` (~0.04% deviation). Root cause: API snapshot timing — `current_price` is captured live, while `high_24h`/`low_24h` reflect a rolling window recalculated on a separate cycle. Logged as an accepted tolerance, not a pipeline defect.
- Both SQL and PySpark validations independently flagged the same two rows, confirming the anomaly is real and not an artifact of either query approach.

Validation logic: `validation.sql`, `sparkvalidation.py`

### Phase 3: Silver Layer — Cleaning & Standardization — ✅ Complete

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

## Next Steps

- **Phase 4: dbt Project Setup** — configure the `dbt-databricks` adapter, build staging models on top of Silver, and add automated schema/data tests (`not_null`, `unique`, `accepted_range`)
- **Phase 5: Gold Layer** — build analytical marts (e.g. top gainers/losers, market cap leaderboard, ATH/ATL summary)
- **Phase 6: Orchestration** — automate the full pipeline end-to-end with Apache Airflow

---

*This README will be updated as each phase of the project progresses.*