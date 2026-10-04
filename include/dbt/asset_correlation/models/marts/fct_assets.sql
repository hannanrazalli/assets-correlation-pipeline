{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='stock_key',
    table_type='iceberg',
    on_schema_change='append_new_column',
) }}

with int_forex as (
    select * from {{ ref('int_forex') }}
),

int_stocks as (
    select * from {{ ref('int_assets') }}
),

joined as (
    select
        s.stock_key,
        s.price_date,
        s.ticker_symbol,
        s.close_price,

        f.exchange_rate,

        case
            when s.ticker_symbol = '^KLSE' then s.close_price
            else round(s.close_price * coalesce(f.exchange_rate, 1.0), 2)
        end as close_price_myr,
    from int_stocks s
    left join int_forex f
        on s.price_date = f.forex_date
),

final_fact as (
    select
        *,
        {{ audit_columns('marts') }}
    from joined

    {% if is_incremental() %}
        where price_date >= (
            select coalesce(max(price_date), cast('1900-01-01' as date)) - interval '3' day
            from {{ this }}
        )
    {% endif %}
)

select *
from final_fact