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