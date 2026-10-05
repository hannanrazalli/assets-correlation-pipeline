{{ config(materialized='view') }}

with fct_daily_returns as (
    select * from {{ ref('fct_daily_returns') }}
),

returns_pivoted as (
    select
        price_date,
        max(case when ticker_symbol = '^GSPC' then daily_returns_pct end) as sp500_return,
        max(case when ticker_symbol = '^KLSE' then daily_returns_pct end) as klci_return,
        row_number() over(order by price_date) as day_number
    from fct_daily_returns
    group by price_date
),

correlation as (
    select
        price_date,
        day_number,
        round(corr(klci_return, sp500_return) over(
            order by price_date
            rows between 29 preceding and current row
        ), 2) as correlation_30d,
        round(corr(klci_return, sp500_return) over(
            order by price_date
            rows between 90 preceding and current row
        ), 2) as correlation_90d,
        round(corr(klci_return, sp500_return) over(
            order by price_date
            rows between 365 preceding and current row
        ), 2) as correlation_365d
    from returns_pivoted
)

select *
from correlation
where day_number >= 30