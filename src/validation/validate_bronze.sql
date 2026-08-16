-- 1. ROW count check 

      SELECT COUNT(*) AS row_count
      FROM cg_crypto_data.bronze.market_data;
      -- 50 rows. So correct.


-- 2. Completeness

    SELECT
      COUNT(*) AS total_rows,
      COUNT(*) - COUNT(id) AS null_id,
      COUNT(*) - COUNT(current_price) AS null_price,
      COUNT(*) - COUNT(market_cap) AS null_market_cap,
      COUNT(*) - COUNT(market_cap_rank) AS null_rank
    FROM cg_crypto_data.bronze.market_data;

    -- Result - no nulls in all 4 columns.

SELECT
COUNT(*) - COUNT(max_supply) AS null_max_supply
FROM cg_crypto_data.bronze.market_data;

--max_supply has 26 nulls. But they can legitimately be nulls because some coins have no supply cap, didnt flag them as errors.

-- 3. Uniqueness


      SELECT id, COUNT(*) AS occurrences
      FROM cg_crypto_data.bronze.market_data
      GROUP BY id
      HAVING COUNT(*) > 1;


      SELECT market_cap_rank, COUNT(*) AS occurences
      FROM cg_crypto_data.bronze.market_data
      GROUP BY market_cap_rank
      HAVING COUNT(*) > 1;


-- Result- No rows returned so no dupes.

-- 4. Validity


      SELECT id, current_price, market_cap, circulating_supply, total_supply, max_supply
      FROM cg_crypto_data.bronze.market_data
      WHERE current_price < 0 OR market_cap < 0 OR circulating_supply < 0 OR total_supply < 0 OR max_supply < 0 ;


      SELECT id, ath_date, atl_date, last_updated
      FROM cg_crypto_data.bronze.market_data
      WHERE TRY_CAST(ath_date AS TIMESTAMP) IS NULL
      OR TRY_CAST(atl_date AS TIMESTAMP) IS NULL
      OR TRY_CAST(last_updated AS TIMESTAMP) IS NULL;

-- Result = No rows returned.

-- 5. Consistency

-- current_price should fall within the 24h high/low range
      SELECT id, current_price, low_24h, high_24h
      FROM cg_crypto_data.bronze.market_data
      WHERE current_price > high_24h OR current_price < low_24h;

--2 rows returned : 
|id|current_price|low_24h|high_24h|
|---|---|---|---|
|tether-gold|4148.52|4150.06|4182.61|
|pax-gold|4151.58|4152.72|4188.82|

-- The real reason is almost certainly timing/snapshot lag: current_price is captured at the exact moment the API call runs, while high_24h/low_24h are computed by CoinGecko over a rolling 24-hour window that gets recalculated on its own cycle.

-- ATH should be >= current price (you can't be above your own all-time high)

      SELECT id, current_price, ath
      FROM cg_crypto_data.bronze.market_data
      WHERE current_price > ath;

--No rows returned.

-- ATL should be <= current price

      SELECT id, current_price, atl
      FROM cg_crypto_data.bronze.market_data
      WHERE current_price < atl;

--No rows returned

-- circulating supply shouldn't exceed max supply (when max_supply exists)

      SELECT id, circulating_supply, max_supply
      FROM cg_crypto_data.bronze.market_data
      WHERE max_supply IS NOT NULL AND circulating_supply > max_supply;

-- No rows returned

-- 6. Accuracy of derived fields 
-- ath_change_percentage should almost always be ≤ 0 
-- and atl_change_percentage should almost always be ≥ 0. 

      SELECT id, ath_change_percentage, atl_change_percentage
      FROM cg_crypto_data.bronze.market_data
      WHERE ath_change_percentage > 0 OR atl_change_percentage < 0;

-- No rows returned.

