{{ config(materialized='view') }}

with fct_assets as (select * from {{ ref('fct_assets') }}),

fct_correlation as (select * from {{ ref('fct_asset_correlation') }}),

dim_dates as (select * from {{ ref('dim_dates') }}),

price_pivoted as (
    select
        price_date,
        max(case when ticker_symbol = '^GSPC' then close_price_myr end) as sp500_close,
        max(case when ticker_symbol = '^KLSE' then close_price_myr end) as klci_close,
        max(case when ticker_symbol = '^GSPC' then daily_returns_pct end) as sp500_return,
        max(case when ticker_symbol = '^KLSE' then daily_returns_pct end) as klci_return
    from {{ ref('fct_daily_returns') }}
    group by price_date
),

fact_report as (
    select
        d.price_date,
        d.year,
        d.month_name,
        d.day_name,
        d.is_weekend,

        p.sp500_close,
        p.klci_close,
        p.sp500_return,
        p.klci_return,

        c.correlation_30d,
        c.correlation_90d,
        c.correlation_365d

    from dim_dates d
    left join price_pivoted p
        on d.price_date = p.price_date
    left join fct_correlation c
        on d.price_date = c.price_date
    where d.price_date <= current_date
)

select *
from fact_report