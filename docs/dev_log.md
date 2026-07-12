## 2026-07-11

- Discovered `symbol` and `total_volume` were missing from Bronze schema 
  documentation but present in actual API data. Re-verified with 
  `printSchema()`, re-ran Silver transform to include both fields.
- Renamed `raw_crypto` schema to `bronze`, `staging` table to `market_data`, 
  to align with medallion architecture convention.
- Hit NameError on `raw_df` — turned out to be stale notebook session state, 
  not a real bug. Fixed by Run All.

  ## 2026-07-12

### dbt Project Initialization
- Ran `dbt init coingecko_dbt`, selected the `databricks` adapter and 
  Unity Catalog (option 1), using catalog `cg_crypto_data` and a new 
  dedicated schema `dbt_dev` — kept separate from `bronze`/`silver` to 
  distinguish hand-built PySpark tables from dbt-managed models.
- Found the SQL Warehouse's HTTP Path under Databricks → SQL Warehouses → 
  Connection Details.

**Issues hit:**
- `dbt debug` initially failed with `dbt_project.yml not found` — caused 
  by running the command from the parent folder instead of the 
  `coingecko_dbt/` subfolder `dbt init` created. Fixed by `cd coingecko_dbt` 
  before running dbt commands.
- Connection failed with a 401 `Credential was not sent or was of an 
  unsupported type` error, using a week-old access token from a prior 
  project. Generated a fresh token (Databricks → User Settings → Developer 
  → Access Tokens), named clearly (`dbt-coingecko`), and manually edited 
  `~/.dbt/profiles.yml` (outside the project folder, at 
  `/home/codespace/.dbt/`) to replace it.
- Learned `profiles.yml` is intentionally stored outside the project 
  directory and never committed to git, since it holds credentials.

**Result:** `dbt debug` passes all checks — dbt is fully connected to 
Databricks via Unity Catalog. Ready to build first staging model.


### Staging Model + Tests
- Created `models/staging/sources.yml` defining `silver.market_data` as 
  a dbt source.
- Built `stg_crypto.sql` — thin staging model with explicit column 
  selection (no `select *`) on top of Silver, following dbt best practice 
  to avoid silent schema drift.
- Added `schema.yml` with tests (`unique`+`not_null` on `id`, `not_null` 
  on `current_price`/`market_cap`), formalizing the manual validation 
  checks from earlier into automated, re-runnable dbt tests.
- Removed dbt's auto-generated example models (`my_first_dbt_model`, 
  `my_second_dbt_model`) and cleaned up the leftover `example:` config 
  block in `dbt_project.yml`.
- `dbt run` and `dbt test` both pass clean — 1 model, 4/4 tests passing.


### Gold Layer Marts
- Built `models/marts/` with three analytical models on top of `stg_crypto`:
  - `top_gainers_losers` — coins ranked by 24h % market cap change, with a 
    `movement_direction` label (gainer/loser/unchanged)
  - `market_cap_leaderboard` — ranked view by market cap, including 
    `pct_supply_circulating` (guarded against divide-by-zero for uncapped 
    coins using `nullif`)
  - `ath_atl_summary` — distance from all-time high/low, including 
    `days_since_ath`/`days_since_atl` via `datediff` (made possible by 
    casting date fields properly back in the Silver phase)
- All three materialized as tables (`{{ config(materialized='table') }}`), 
  since Gold output is meant to be pre-computed for repeated querying, 
  unlike staging's default view materialization.

**Issue hit:** `ath_atl_summary` failed with an UNRESOLVED_COLUMN error on 
`atl`. Traced upstream — the actual typo was in `stg_crypto.sql`, which 
had never selected `atl` correctly, but hadn't surfaced yet since no 
existing test queried that column. Fixed at the source (staging model), 
not just the symptom (the mart referencing it).

**Deliberately deferred:** considered a 4th mart 
(`volume_to_marketcap_ratio`, a liquidity signal) but decided to hold off 
until after building Airflow orchestration — current data is a single 
point-in-time snapshot, and seeing how orchestration accumulates data over 
time may change what additional marts are actually worth building.

### Airflow Setup (Docker Compose, LocalExecutor)
- Set up Airflow via Docker Compose rather than standalone mode, to match 
  production-style deployment and reuse Docker skills from Phase 0.
- Modified the official Airflow docker-compose.yaml: switched 
  AIRFLOW__CORE__EXECUTOR from CeleryExecutor to LocalExecutor, removed 
  Redis, airflow-worker, and Flower services (unnecessary for a 
  single-developer setup, since they exist for distributed/multi-worker 
  execution). Kept Postgres as the metadata database.
- Generated a FERNET_KEY for encrypting connection secrets, added to .env 
  alongside AIRFLOW_UID.
- Ran `docker compose up airflow-init` (one-time DB init + admin user 
  creation), then `docker compose up -d` to start all services.
- Confirmed all 5 services (postgres, apiserver, scheduler, dag-processor, 
  triggerer) running and healthy via `docker compose ps`. Logged into 
  Airflow UI successfully.


  Attempted to install apache-airflow-providers-databricks locally for 
Pylance support, but it conflicts with dbt-databricks's 
databricks-sql-connector version requirement. Left the import 
unresolved locally (Pylance warning only, cosmetic) — the package is 
correctly installed inside the Airflow Docker container via the custom 
Dockerfile, which is the only environment where this code actually runs.



### Airflow DAG: Bronze → Silver Orchestration
- Wrapped `ingest_bronze` and `transform_silver` Databricks notebooks as 
  Databricks Jobs, giving each a stable Job ID that Airflow can trigger 
  via API.
- Extended the Airflow Docker image with a custom Dockerfile 
  (`FROM apache/airflow:3.2.2` + `apache-airflow-providers-databricks`), 
  since the Databricks operators aren't part of core Airflow.
- Wrote `coingecko_etl_pipeline.py`, using `DatabricksRunNowOperator` for 
  both tasks, with `ingest_bronze >> transform_silver` enforcing that 
  Silver never runs on top of a failed or missing Bronze run.

**Issues hit:**
- DAG failed to load: `ModuleNotFoundError: No module named 'transformation'` 
  — a stray import line that didn't belong in the file, unrelated to any 
  actual dependency. Removed it.
- `AirflowNotFoundException: conn_id 'databricks_default' isn't defined` — 
  connection had been saved as `default_databricks` (words reversed). 
  Recreated with the correct ID.
- `403: Provided access token does not have required scopes: jobs` — 
  original token was scoped too narrowly. Generated a new token with 
  explicit `jobs` scope (auto-scoping turned off, so scope won't silently 
  narrow again), 30-day lifetime.
- First full run: `ingest_bronze` took ~11 minutes due to Serverless 
  compute cold start on a free trial workspace — Airflow's progress 
  indicator looked stuck but was correctly polling; confirmed actual 
  progress directly in Databricks' own Job Runs tab rather than relying 
  on Airflow's UI alone.

**Result:** full DAG run succeeded — `ingest_bronze` → `transform_silver`, 
triggered and sequenced entirely by Airflow, no manual notebook execution.