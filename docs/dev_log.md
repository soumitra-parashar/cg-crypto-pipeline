## 2026-07-11

- Discovered `symbol` and `total_volume` were missing from Bronze schema 
  documentation but present in actual API data. Re-verified with 
  `printSchema()`, re-ran Silver transform to include both fields.
- Renamed `raw_crypto` schema to `bronze`, `staging` table to `market_data`, 
  to align with medallion architecture convention.
- Hit NameError on `raw_df` — turned out to be stale notebook session state, 
  not a real bug. Fixed by Run All.