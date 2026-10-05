{{ config(materialized='view') }}

with stg_assets as (
    select * from {{ ref('stg_assets') }}
),

deduplicate as (
    select
        *,
        row_number() over(
            partition by price_date, ticker_symbol
            order by _staged_at desc
        ) as rn
    from stg_assets
),

clean_assets as (
    select
        {{ dbt_utils.generate_surrogate_key(['price_date', 'ticker_symbol']) }} as asset_key,

        price_date,
        ticker_symbol,
        close_price,

        {{ audit_columns('intermediate') }}
    from deduplicate
    where rn = 1
)

select *
from clean_assets