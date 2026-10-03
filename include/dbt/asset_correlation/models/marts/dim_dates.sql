{{ config(materialized='table') }}

with date_range as (
    select sequence(date '2020-01-01', date '2030-12-31', interval '1' day) as date_array
),

flattened_dates as (
    select date_val
    from date_range
    cross join unnest(date_array) as t(date_val)
)

select
    cast(date_format(date_val, '%Y%m%d') as integer) as date_id,
    
    cast(date_val as date) as price_date,
    
    year(date_val) as year,
    quarter(date_val) as quarter,
    month(date_val) as month,
    date_format(date_val, '%M') as month_name,
    day(date_val) as day,
    day_of_week(date_val) as day_of_week,
    date_format(date_val, '%W') as day_name,
    case when day_of_week(date_val) in (6, 7) then true else false end as is_weekend
from flattened_dates