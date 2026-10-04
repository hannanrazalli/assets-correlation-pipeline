{{ config(materialized='view') }}

with stg_forex as (
    select * from {{ ref('stg_forex') }}
),

deduplicate as (
    select
        *,
        row_number() over(
            partition by forex_date, base_currency, target_currency
            order by _staged_at desc
        ) as rn
    from stg_forex
),

clean_forex as (
    select
        {{ dbt_utils.generate_surrogate_key(['forex_date', 'base_currency', 'target_currency']) }} as forex_key,

        forex_date,
        base_currency,
        target_currency,
        exchange_rate,

        {{ audit_columns('intermediate') }}
    from deduplicate
    where rn = 1
)

select *
from clean_forex