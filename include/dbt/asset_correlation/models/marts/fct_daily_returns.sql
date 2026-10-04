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
        ) as prev_close_price_myr
    from fct_assets
),

daily_returns as (
    select
        price_date,
        ticker_symbol,
        close_price_myr,
        case
            when prev_close_price_myr is null or prev_close_price_myr = 0 then null
            else round(100.0 * (close_price_myr - prev_close_price_myr) / prev_close_price_myr, 4)
        end as daily_return
    from prev_price
)

select *
from daily_returns