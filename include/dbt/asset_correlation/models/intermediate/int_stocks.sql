{{ config(materialized='view') }}

with stg_stocks as (
    select * from {{ ref('stg_stocks') }}
),

deduplicate as (
    select
        *,
        row_number() over(
            partition by price_date, ticker_symbol
            order by _staged_at
        ) as rn
    from stg_stocks
),

clean_stocks as (
    select
        {{ dbt_utils.generate_surrogate_key(['price_date', 'ticker_symbol']) }} as stock_key,

        price_date,
        ticker_symbol,
        close_price,

        {{ audit_columns('intermediate') }}
    from deduplicate
    where rn = 1
)

select *
from clean_stocks