{{ config(materialized='view') }}

with daily_returns as (
    select * from {{ ref('fct_daily_returns') }}
),

returns_pivoted as (
    select
        price_date,
        max(case when ticker_symbol = '^GSPC' then daily_return end) as sp500_return,
        max(case when ticker_symbol = '^KLSE' then daily_return end) as klci_return,
        row_number() over (order by price_date) as day_number
    from daily_returns
    group by price_date
),

rolling_correlation as (
    select
        price_date,
        day_number,
        corr(klci_return, sp500_return) over (
            order by price_date
            rows between 29 preceding and current row
        ) as corr_klci_sp500_30d
    from returns_pivoted
)

select *
from rolling_correlation
where day_number >= 30