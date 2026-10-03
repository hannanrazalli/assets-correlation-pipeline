{{ config(materialized='view') }}

with raw_stocks as (
    select * from {{ source('assets_sources', 'raw_stocks') }}
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
),

final_stg as (
    select
        *,
        {{ audit_columns('staging') }}
    from renamed_and_casted
)

select *
from final_stg