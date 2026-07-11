
spark.sql("USE CATALOG cg_crypto_data")
spark.sql("USE SCHEMA bronze")


raw_df = spark.read \
    .option("multiline", "true") \
    .json("s3://coingecko-crypto-data-lake-06072026/raw/*.json")

raw_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("market_data")

print("RAW Table completely overwritten with the correct schema!")


raw_df.select('id', 'symbol', 'current_price', 'market_cap', 'total_volume').show(10)

print(raw_df.columns)