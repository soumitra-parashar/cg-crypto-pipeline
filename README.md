# Crypto Market Data ETL Pipeline

## Project Overview

This project is an end-to-end ETL ( Extract, Transform, Load) pipeline that ingests cryptocurrency market data from the [CoinGecko API](https://www.coingecko.com/en/api) and processes it into analyisis-ready datasets.

Raw data is extracted via `Python`, containerized with `Docker`, staged in `AWS S3`, processed at scale using `Databricks (PySpark)`, modeled with `dbt`, and orchestrated with `Airflow`. Development takes place in `Github Codespaces`.

The project is part of my data engineering portfolio, built to demonstrate hands-on, production style pipeline for data engineering roles.

## Project Goal

The goal of this project is to build a reliable, scalable, reproductible data pipeline that:
- Extracts real-time cryptocurrency market data (prices, market cap, volume, etc) from the CoinGecko API.
- Stores raw data in a structured,versioned format in AWS S3 (data lake layer).
- Transforms and cleans data at scale using PySpark on Databricks.
- Applies modular, tested data models with dbt to produce trusted, analytics-ready tables.
- Automates the entire workflow using Airflow, with scheduled runs and failure handling.
- Demonstrates a production style data engineering workflow, from ingestion to consumption, using industry-standard tools.

## Tech Stack

| Layer | Tool |
|---|---|
| Language | Python 3.12.1 |
| Extraction | `requests`, CoinGecko API |
| AWS SDK | `boto3`, `python-dotenv` |
| Cointainerization | Docker |
| Storage | AWS S3 |
| Processing | Databricks (PySpark), Unity Catalog |
| Transformation | dbt |
| Orchestration | Apache Airflow |
| Database | SQL |
| Development Environment | Github Codespace (Linux) |

## Data Architecture

The pipeline follows the **medallion architecture** (Bronze &rarr; Silver &rarr; Gold), a standard lakehouse pattern that seperates raw,cleaned, and business-ready data into distinct layers.

**Databricks Catalog:** `cg_crypto_data`

| Schema | Tables | Description |
|---|---|---|
|`bronze` | `market_data` | Raw ingested data, unmodified from source |
|`silver` | `market_data` | Cleaned, typed data (PySpark) |
|`staging`| `stg_crypto`  | Thin, 1:1 dbt layer on top of silver layer, dbt's convention for a consistent model starting point, no additional cleaning done in this.
|`gold` | `top_gainers_losers`, `market_cap_leaderboard`,`ath_atl_summary`| Business-level analytical marts (dbt)|

...

CoinGecko API
    ↓
extract.py (Dockerized)
    ↓
upload_to_aws.py (boto3)
    ↓
AWS S3 (raw JSON landing zone)
    ↓
Databricks (PySpark, via Unity Catalogue External Location)
    ↓
cg_crypto_data.bronze.market_data
    ↓
cg_crypto_data.silver.market_data
    ↓
dbt (staging model &rarr; Gold marts, tested)
    ↓
Apache Airflow (orchestrates all steps)


## Progress Log

### Phase 0: Extraction and Upload - Complete

- Set up environments and tools.
- Wrote `extract.py` using `requests` to pull live market data from the CoinGecko API.
- Wrote `upload_to_aws` using `boto3` and `python-dotenv` to securely upload the extracted raw JSON to AWS S3, keeping credentials out of source code via environment variables.
- Wrapped `extract.py` in a Docker image (`python:3.12.1-slim` base) for consistent, portable execution.


### Phase 1: Cloud Infrastructure and Security Setup - Complete

Instead of hardcoding access keys, a secure, role-based trust relationship was configured between Databricks and AWS using Unity Catalog.

**1. AWS S3 Landing Zone**

Created a dedicated S3 bucket (`s3://coingecko-crypto-data-lake-06072026/raw/`) to act as the landing zone for raw JSON pulled from the API.

**2. Databricks Unity Catalog Configuration**
Created a dedicated logical container for the project to isolate table and maintain strict permission scoping:
- Catalog: `cg_crypto_data`
- Schemas: `bronze`, `silver`,`gold`. 

**3. The Secure Cross-Account Connection (IAM and External Locations)**
Established a secure connection between AWS and Databricks without exposing long-lived IAM keys:
- Deployed an AWS CloudFormation stack to generate a dedicated IAM Role
- Created a Storage Credential in Databricks using the AWS Role ARN
- Updated the AWS IAM Trust Policy with the Databricks-generated External ID, including a self-assuming trust statement
- Apllied an inline Permission Policy strictly limiting Databricks' access to only the project's S3 bucket
- Created an External Location in Databricks to bridge the S3 bucket to Unity Catalog
- Verified the connection, all pernission checks passed (read,write, list, delete, path exists, assume role, external ID condition)

### Phase 2: Bronze Layer Ingestion (Raw Data) - Complete

Successfully moved data from cloud storage into the distributed compute engine, handling the format inconsistencies along the way.

**1. PySpark JSON  Ingestion** 
```python
spark.sql("USE CATALOG cg_crypto_data")
spark.sql("USE SCHEMA bronze")

raw_df = spark.read \
    .option("multiline", "true") \
    .json("s3://coingecko-crypto-data-lake-06072026/raw/*.json")
```


- Wrote a PySpark script to dynamically read raw JSON files from the S3 External Location.
- **Overcame parsing errors:** Identified and resolved Spark's default single-line JSON parsing behaviour. Initially, the API returned nested and caused a `corrupt_record` schema failure.
- **The fix:** Applied `.option("multiline", "true")` to force Spark to parse the payload holistically instead of line-by-line.

**2. Delta Table Creation (Nuke and Pave)**
```python
raw_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("market_data")
```
- Wrote the resulting DataFrame into 'cg_crypto_data.bronze.market_data` as a Delta Table.
- Used `.mode("overwrite")` with `.option("overwriteSchema", "true")` so the tabl schema cleanly replaced the corrupted state with the correct inferred schema from the fixed read.

**3. Sanity Checks / Validation**

Quried the new Bronze Delta Table using the PySpark DataFrame API, checking schema, row counts, nulls, duplicates, and value ranges.

### Data Validation - Complete 

Validated the Bronze layer (`cg_crypto_data.bronze.market_data`, 50 rows) across five standard data quality dimensions- completeness, uniqueness, consistency and accuracy using **both SQL and PySpark DataFrame API independently**, to cross-confirm findings and demonstrate equivalent fluency in both approaches.

- Completeness, uniqueness, and validity checks: all passed, zero issues.
- Consistency check (`current_price` within 24h high/low range): 2 of 50 rows flagged - `tether-gold` and `pax-gold`, where `current price` was marginally below `low_24h`. Root Cause: API snapshot timing- `current_price` is captured live, while `high_24h`/`low_24h` reflects a rolling window recalculated on a seperate cycle. Logged as an accepted tolerance, not a pipeline defect.
- Both SQL and PySpark validations independently flagged the same two rows, confirming the anamoly is real and not an artifact of either query aprroach.

Validation logic: `validation.sql`, `sparkvalidation.py`

### Phase 3: Silver Layer - Cleaning and Standardization - Complete
Transformed the Bronze table into a cleaned, explicitly typed Silver Table using Pyspark.

**Key Steps**
- Cast `ath_date`, `atl_date`, and `last_updated` from string to timestamp for proper date arthimetic downstream
- Dropped `image` (logo URL because its analytically useful)
- Enforced an explicit schema via `.cast()` on every column, rather than relying on Spark's inferred types.
- Added a `price_outside_24h_range` boolean flag column to make the known API timing artifact (mentioned above in Data Validation Section) queryable downstream without re-deriving it.
- Ran a duplicate check on `id` before writing,

**Schema Correction**: during this phase, discovered that the `symbol` and `total_volume` fields present in the raw CoinGecko API response were missing from an earlier working column list. Re-verified the true Bronze schema directly via `printSchema()`, then re-ran the Silver transformation to include both fields (`symbol` as `StringType` and `total_volume` as `DoubleType`).

**Naming Cleanup:** renamed the raw schema from `raw_crypto` to `bronze`, and the Silver table from `staging` to `market_data`, aligning both layers to a consistent `catalog.schema.market_data` convention where the schema name alone indicates the medallion layer.

Transformation logic: `transform_silver.py`

### Phase 4: dbt Project Setup - Complete

Initialized a dbt project to move from ad-hoc PySpark cleaning into a version-controlled, dependency aware, testable transformations, connected to Databricks via Unity Catalog.

**1.Project Initialization**
- Ran `dbt-init`, selected the `databricks` adapter and Unity Catalog authentication
- Catalog: `cg_crypto_data` (shared with the rest of the pipeline)
- Initial target schema: `dbt_dev`(this is dbt's default project schema, I later corrected it in **Schema Consolidation** step below)
- Connected using a databricks personal access token and the projects's Serverless SQL Warehouse in `dbt_project.yml`
- Removed dbt's auto-generated example models and cleaned up the resulting unused config references in `dbt_project.yml`

**2. Source Definition**
Declared `cg_crypto_data.silver.market_data` as a formal dbt **source** (`models/staging/sources.yml`), rather than hardcoding the table path in every model.

**3. Staging Model**
Built `stg_crypto.sql` - a thin 1:1 pass through of the silver table with explicit column selection (deliberately avoiding `select*`, so any future upstream schema drift surfaces as a visible error rather than silently changing downstream models).

**4. Automated Testing**

Added `schema.yml` with tests that formalize the manual validation checks performed earlier by hand:
- `unique` + `not_null` on `id`
- `not_null` on `current_price`, `market_cap`

```bash
dbt run # builds stg_crypto as a view
dbt test # 4/4 tests passed
```

dbt project logic: `coingecko_dbt/models/staging/`

### Phase5 5: Gold Layer - Analytical Marts

Built business-focused analytical tables on top of `stg_crypto`, each answering a distinct question.

| Mart | Answers | Key Logic |
|---|---|---|
| `top_gainers_losers` | Which coins moved the most in the last 24h? | `movement_direction` label derived from `market_cap_change_percentage_24h` |
| `market_cap_leaderboard` | Who are the biggest coins by market domination? | `pct_supply_circulating`, guarded against divide by zero for uncapped coins via `nullif` |
| `ath_atl_summary` | How far is each coin from its historical peak/trough? | `days_since_ath`/`days_since_atl` via `datediff` (datediff was made possible by timestamp casting dne in the Silver layer)

All three are materialized as **tables** (`{{config(materialized='table')}}`) rather than views, since Gold output is meant to be pre-computed for repeated downstream querying, unlike staging's lighter view materialization. Each model refernces `stg_crypto` via `{{ref(...)}}` rather than a hardcoded table path, so dbt can track dependencies and build order automatically.

**Known Limitation:** the current database is a single point-in-point snapshot, so these marts reflect a single moment rather than trends over time. Time-series asalysis (For example- rank changes over 30 days) would require the pipeline to run repeatedlly and accumulate history which is what Phase 6 (Airflow) enables.

Gold layer logic: `coingecko_dbt/models/marts/`

### Phase 6: Orchestration (Apache Airflow)

Setting up automated, unattended orchestration of the pipeline using Apache Airflow, run via Docker Compose.

**1. Airflow Infrastructure**

- Deployed via Docker Compose rather than standalone mode to match production style deployment and resuse containerization from Phase 0.
- Modified the official Airflow Compose file, switched from `CeleryExecutor` to `LocalExecutor`, removed Redis, airflow-worker and Flower, they seemed unnessary components for a project of this scale, kept Postgres as the metadata base.
- Generated a `FERNET_KEY` for encrypting stored connection secrets.
- Extented the base airflow image with a custom `DockerFile` to install `apache-airflow-providers-databricks`, since Databricks operators are not a part of core airflow.

**2. Databricks Job Wrapping**
- Wrapped the Bronze ingestion and Silver transformation as Databricks Jobs (Workflows &rarr; Create Job), giving each a stable Job ID that Airflow can trigger via the Databricks REST API
- Configured an Airflow Connection (`databricks_default`) authenticating via a scoped personal access token (`jobs` scope only, 90 day lifetime, auto-scoping disabled to keep permissions stable)

**3. DAG: Bronze &rarr; Silver Orchestration**
```python
ingest_bronze = DatabricksRunNowOperator(
        task_id="ingest_bronze",
        databricks_conn_id="databricks_default",
        job_id= "<job_id>"
)
transform_silver = DatabricksRunNowOperator(
        task_id = "transform_silver",
        databricks_conn_id="databricks_default",
        job_id="<job_id>"
    )
ingest_bronze >> transform_silver
```
The `>>` dependecy ensures Silver never runs on top of a missingg or failed Bronze run.

**Issues resolved along the way:**
- A stray, unrelated import line broke DAG parsing entirely, traced  and remmoved it.
- Airflow connection was initially saved with a reversed ID (`default_databricks` instead of `databricks_default`), causing a "Connection not defined" error at runtime.
- Initial access token was scoped too narrowly, causing a `403:missing required scopes: job` error, resolved it by generating a token with explict `job` scope
**Result:** first full DAG run succeeded end-to-end Bronze ingestion and Silver transformation both triggered, sequenced, and completed by Airflow.

**4. Extending the DAG: Addint dbt**

Extended the pipeline with two additional tasks running dbt directly inside the Airflow via `BashOperator`:

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

Unlike Bronze/Silver which require Databricks compute and are triggered via API, dbt only makes lightweight calls to the SQL warehouses, so `dbt run and dbt test` run directly inside the Airflow container itself. This required mounting the `coingecko_dbt/` project and a container-local copy if `profiles.yml` into the Airflow containers and extending the Airflow image with `dbt-databricks`.

**5.Final Validation and Scheduling**

- Ran the complete 4-task DAG (`ingest_bronze &rarr; transform_silver &rarr; dbt_run &rarr; dbt_test`) multiple times to confirm reliability.

- Set the DAG schedule to `@daily` (`0 0 ***`),completing the transistion from manually triggered to fully orchestrated.

- The DAG is configured to run daily, but Airflow only executes on schedule while its containers are actively running.
Since this project runs in a local Docker Compose setuo rather than a persistently deployed environment, the daily schedule is included to demonstrate orchestration capability rather than to run continously unattended.

## Project Status:Complete

All six phases are built, tested and orchestrated end-to-end:

**Extracted (Python) &rarr; S3(raw landing zone) &rarr; Bronze (PySpark/Delta) &rarr; Silver (PySpark, cleaned and typed) &rarr; dbt (staging + Gold Marts, tested) &rarr; Airflow (Orchestration, scheduled)**

*Built as a data engineering portfolio project.*