{{ config(materialized='view') }}

WITH raw_forex AS (
    SELECT * FROM {{ source('assets_sources', 'raw_forex') }}
),

renamed_and_casted AS (
    SELECT
        cast(date as date) as forex_date,
        cast(base as varchar) as base_currency,
        cast(quote as varchar) as target_currency,
        cast(rate as double) as exchange_rate,

        {{ audit_columns('staging') }}
    FROM raw_forex
)

SELECT *
FROM renamed_and_casted