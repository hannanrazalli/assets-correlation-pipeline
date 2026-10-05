{{ config(materialized='view') }}

with fct_assets as (
    select * from {{ ref('fct_assets') }}
),

prev_price as (
    select
        *,
        lag(close_price_myr) over(
            partition by ticker_symbol
            order by price_date
        ) as prev_close_price
    from fct_assets
),

daily_returns as (
    select
        price_date,
        ticker_symbol,
        close_price_myr,
        case
            when prev_close_price = 0 or prev_close_price is null then null
            else round(100.0 * (close_price_myr - prev_close_price) / prev_close_price, 2)
        end as daily_returns_pct
    from prev_price
)

select *
from daily_returns