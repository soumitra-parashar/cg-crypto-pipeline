{{ config(materialized='table') }}

select
    id,
    symbol,
    name,
    current_price,
    market_cap_change_percentage_24h,
    case
        when market_cap_change_percentage_24h > 0 then 'gainer'
        when market_cap_change_percentage_24h < 0 then 'loser'
        else 'unchanged'
    end as movement_direction


from {{ ref('stg_crypto') }}
order by market_cap_change_percentage_24h desc