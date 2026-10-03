{{ config(materialized='view') }}

with fct_assets as (select * from {{ ref('fct_assets') }}),

dim_dates as (select * from {{ ref('dim_dates') }}),

fact_report as (
    select
        d.price_date,
        d.year,
        d.month_name,
        d.day_name,
        d.is_weekend,

        max(case when a.ticker_symbol = '^GSPC' then a.close_price_myr end) as sp500,
        max(case when a.ticker_symbol = '^KLSE' then a.close_price_myr end) as klci
    from dim_dates d
    left join fct_assets a
        on d.price_date = a.price_date
    where d.price_date <= current_date
    group by 
        d.price_date,
        d.year,
        d.month_name,
        d.day_name,
        d.is_weekend
)

select *
from fact_report