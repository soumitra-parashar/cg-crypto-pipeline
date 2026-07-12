{{ config(materialized='table') }}

select
    market_cap_rank,
    id,
    symbol,
    name,
    market_cap,
    current_price,
    circulating_supply,
    max_supply,
    round(circulating_supply / nullif(max_supply, 0) * 100, 2) as pct_supply_circulating

from {{ ref('stg_crypto') }}
order by market_cap_rank asc