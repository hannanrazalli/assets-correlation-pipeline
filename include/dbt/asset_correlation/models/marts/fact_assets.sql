{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='date',
    table_type='iceberg',
    on_schema_change='append_new_columns'
) }}

with int_stocks as (
    select * from {{ ref('int_stocks') }}
),

int_forex as (
    select * from {{ ref('int_forex') }}
),

joined as (
    select
        s.date,
        s.gspc_close,
        s.klse_close,

        coalesce(f.exchange_rate, 1.0) as exchange_rate,
        round(s.gspc_close * coalesce(f.exchange_rate, 1.0), 2) as gspc_close_myr
    from int_stocks s
    left join int_forex f
        on s.date = f.date
),

final_fact as (
    select
        date,
        gspc_close_myr,
        klse_close,
        
        {{ audit_columns('marts') }}
    from joined

    {% if is_incremental() %}
        date >= (
            select coalesce(max(date), cast('1900-01-01' as date)) - interval "3" day
            from {{ this }}
        )
    {% endif %}
)

select *
from final_fact