from pyspark.sql.functions import col, count, to_timestamp

# Load the Bronze table
df = spark.table("cg_crypto_data.bronze.market_data")

# ──────────────────────────────────────────────────────────────────────────────────────────
# 0. Row count sanity check
# ──────────────────────────────────────────────────────────────────────────────────────────
print("Row count:", df.count())  # expect 50


# ──────────────────────────────────────────────────────────────────────────────────────────
# 1. Completeness 
#──────────────────────────────────────────────────────────────────────────────────────────
df.select(
    (df.count() - count(col("id"))).alias("null_id"),
    (df.count() - count(col("current_price"))).alias("null_price"),
    (df.count() - count(col("market_cap"))).alias("null_market_cap"),
    (df.count() - count(col("market_cap_rank"))).alias("null_rank")
).show()

# max_supply is allowed to be null (some coins have no cap) — just report it
print("Null max_supply count:", df.filter(col("max_supply").isNull()).count())


# ──────────────────────────────────────────────────────────────────────────────────────────
# 2. Uniqueness — id and market_cap_rank should each appear once
# ──────────────────────────────────────────────────────────────────────────────────────────
df.groupBy("id").count().filter(col("count") > 1).show()
df.groupBy("market_cap_rank").count().filter(col("count") > 1).show()


# ──────────────────────────────────────────────────────────────────────────────────────────
# 3. Validity — no negative prices/market caps/supply; dates must parse
# ──────────────────────────────────────────────────────────────────────────────────────────
df.filter(
    (col("current_price") < 0) |
    (col("market_cap") < 0) |
    (col("circulating_supply") < 0)
).select("id", "current_price", "market_cap", "circulating_supply").show()

df.filter(
    to_timestamp(col("ath_date")).isNull() |
    to_timestamp(col("atl_date")).isNull() |
    to_timestamp(col("last_updated")).isNull()
).select("id", "ath_date", "atl_date", "last_updated").show()


# ──────────────────────────────────────────────────────────────────────────────────────────
# 4. Consistency — related fields should logically agree
# Why: catches real bugs, not just missing data
# ──────────────────────────────────────────────────────────────────────────────────────────

# current_price should sit within the 24h range
df.filter(
    (col("current_price") > col("high_24h")) |
    (col("current_price") < col("low_24h"))
).select("id", "current_price", "high_24h", "low_24h").show()

# ATH must be >= current price
df.filter(col("current_price") > col("ath")) \
  .select("id", "current_price", "ath").show()

# ATL must be <= current price
df.filter(col("current_price") < col("atl")) \
  .select("id", "current_price", "atl").show()

# circulating supply shouldn't exceed max supply, when max_supply exists
df.filter(
    col("max_supply").isNotNull() & (col("circulating_supply") > col("max_supply"))
).select("id", "circulating_supply", "max_supply").show()


# ──────────────────────────────────────────────────────────────────────────────────────────
# 5. Accuracy of derived fields — percentage sign logic
# ──────────────────────────────────────────────────────────────────────────────────────────
df.filter(
    (col("ath_change_percentage") > 0) |
    (col("atl_change_percentage") < 0)
).select("id", "ath_change_percentage", "atl_change_percentage").show()