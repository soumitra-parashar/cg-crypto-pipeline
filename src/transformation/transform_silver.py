from pyspark.sql.functions import col, to_timestamp

bronze_df = spark.table("cg_crypto_data.bronze.market_data")

silver_df = bronze_df \
    .withColumn("ath_date", to_timestamp(col("ath_date"))) \
    .withColumn("atl_date", to_timestamp(col("atl_date"))) \
    .withColumn("last_updated", to_timestamp(col("last_updated")))
    
silver_df = silver_df.drop("image")

from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, LongType, StringType, TimestampType

silver_df = silver_df \
    .withColumn("id", col("id").cast(StringType())) \
    .withColumn("name", col("name").cast (StringType())) \
    .withColumn("current_price", col("current_price").cast(DoubleType())) \
    .withColumn("high_24h", col("high_24h").cast(DoubleType())) \
    .withColumn("low_24h", col("low_24h").cast(DoubleType())) \
    .withColumn("market_cap", col("market_cap").cast(LongType())) \
    .withColumn("market_cap_rank", col("market_cap_rank").cast(LongType())) \
    .withColumn("market_cap_change_24h", col("market_cap_change_24h").cast(DoubleType())) \
    .withColumn("fully_diluted_valuation", col("fully_diluted_valuation").cast(LongType())) \
    .withColumn("circulating_supply", col("circulating_supply").cast(DoubleType())) \
    .withColumn("max_supply", col("max_supply").cast(DoubleType())) \
    .withColumn("ath", col("ath").cast(DoubleType())) \
    .withColumn("ath_change_percentage", col ("ath_change_percentage").cast(DoubleType())) \
    .withColumn("atl", col("atl").cast(DoubleType())) \
    .withColumn("atl_change_percentage", col("atl_change_percentage").cast(DoubleType())) \

        
