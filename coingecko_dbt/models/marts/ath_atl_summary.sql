{{ config(materialized='table') }}

select
    id,
    symbol,
    name,
    current_price,
    ath,
    ath_change_percentage,
    ath_date,
    atl,
    atl_change_percentage,
    atl_date,
    datediff(current_date(), ath_date) as days_since_ath,
    datediff(current_date(), atl_date) as days_since_atl

from {{ ref('stg_crypto') }}
order by ath_change_percentage asc