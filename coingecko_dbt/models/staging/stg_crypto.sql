SELECT
    id,
    symbol,
    name,
    current_price,
    high_24h,
    low_24h,
    total_volume,
    market_cap,
    market_cap_rank,
    market_cap_change_24h,
    market_cap_change_percentage_24h,
    fully_diluted_valuation,
    circulating_supply,
    max_supply,
    ath,
    ath_change_percentage,
    ath_date,
    last_updated,
    price_outside_24h_range

from {{ source('silver', 'market_data') }}