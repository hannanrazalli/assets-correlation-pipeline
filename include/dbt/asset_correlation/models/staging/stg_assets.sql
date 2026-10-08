{{ config(materialized='view') }}

with raw_stocks as (
    select * from {{ source('assets_sources', 'raw_stocks') }}
),

raw_bitcoin as (
    select * from {{ source('assets_sources', 'raw_bitcoin') }}
),

raw_gold as (
    select * from {{ source('assets_sources', 'raw_gold') }}
),

renamed_and_casted as (
    select
        cast(date as date) as price_date,
        '^GSPC' as ticker_symbol,
        cast(gspc as double) as close_price
    from raw_stocks
    where gspc is not null

    union all

    select
        cast(date as date) as price_date,
        '^KLSE' as ticker_symbol,
        cast(klse as double) as close_price
    from raw_stocks
    where klse is not null

    union all

    select
        cast(date as date) as price_date,
        'BTC_USD' as ticker_symbol,
        cast("btc-usd" as double) as close_price
    from raw_bitcoin
    where "btc-usd" is not null

    union all

    select
        cast(date as date) as price_date,
        'GLD' as ticker_symbol,
        cast(gld as double) as close_price
    from raw_gold
    where gld is not null
),

final_stg as (
    select
        *,
        {{ audit_columns('staging') }}
    from renamed_and_casted
)

select *
from final_stg