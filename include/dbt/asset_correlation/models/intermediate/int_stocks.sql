{{ config(materialized='view') }}

with stg_stocks as (
    select * from {{ ref('stg_stocks') }}
),

deduplicate as (
    select
        *,
        row_number() over(
            partition by date, gspc_close, klse_close
            order by _staged_at
        ) as rn
    from stg_stocks
),

clean_stocks as (
    select
        {{ dbt_utils.generate_surrogate_key(['date', 'gspc_close', 'klse_close']) }} as stock_key,

        date,
        gspc_close,
        klse_close,

        {{ audit_columns('intermediate') }}
    from deduplicate
    where rn = 1
)

select *
from clean_stocks