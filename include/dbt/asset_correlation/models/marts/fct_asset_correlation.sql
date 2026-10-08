{{ config(materialized='view') }}

with fct_daily_returns as (
    select * from {{ ref('fct_daily_returns') }}
),

returns_pivoted as (
    select
        price_date,
        max(case when ticker_symbol = '^GSPC' then daily_returns_pct end) as sp500_return,
        max(case when ticker_symbol = '^KLSE' then daily_returns_pct end) as klci_return,
        max(case when ticker_symbol = 'BTC_USD' then daily_returns_pct end) as btc_return,
        max(case when ticker_symbol = 'GLD' then daily_returns_pct end) as gold_return,
        row_number() over(order by price_date) as day_number
    from fct_daily_returns
    group by price_date
    having max(case when ticker_symbol = '^GSPC' then daily_returns_pct end) is not null
       and max(case when ticker_symbol = '^KLSE' then daily_returns_pct end) is not null
       and max(case when ticker_symbol = 'BTC_USD' then daily_returns_pct end) is not null
       and max(case when ticker_symbol = 'GLD' then daily_returns_pct end) is not null
),

correlation as (
    select
        price_date,
        day_number,
        -- 1. KLCI vs S&P 500
        ROUND(CORR(klci_return, sp500_return) over(
            ORDER BY price_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 2) as corr_sp500_klse_30d,

        -- 2. S&P 500 vs BTC
        ROUND(CORR(btc_return, sp500_return) OVER (
            ORDER BY price_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 2) AS corr_sp500_btc_30d,

        -- 3. S&P 500 vs Gold
        ROUND(CORR(gold_return, sp500_return) OVER (
            ORDER BY price_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 2) AS corr_sp500_gold_30d,

        -- 4. KLCI vs BTC
        ROUND(CORR(btc_return, klci_return) OVER (
            ORDER BY price_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 2) AS corr_klse_btc_30d,

        -- 5. KLCI vs Gold
        ROUND(CORR(gold_return, klci_return) OVER (
            ORDER BY price_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 2) AS corr_klse_gold_30d,

        -- 6. BTC vs Gold
        ROUND(CORR(gold_return, btc_return) OVER (
            ORDER BY price_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 2) AS corr_btc_gold_30d
    from returns_pivoted
)

select *
from correlation
where day_number >= 30