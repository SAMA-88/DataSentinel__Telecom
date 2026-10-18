-- DIMENSION: one row per calendar day. It needs no input table: dbt builds the days itself.
-- The first and last day come from dbt_project.yml (date_start, date_end).

with days as (
    select explode(sequence(to_date('{{ var("date_start") }}'), to_date('{{ var("date_end") }}'), interval 1 day)) as full_date
)

select
    cast(date_format(full_date, 'yyyyMMdd') as int) as date_key,     -- 20261007 for 7 October 2026
    full_date,
    year(full_date) as year,
    quarter(full_date) as quarter,
    month(full_date) as month,
    date_format(full_date, 'MMMM') as month_name,
    date_format(full_date, 'yyyy-MM') as year_month,
    day(full_date) as day,
    dayofweek(full_date) as day_of_week,
    date_format(full_date, 'EEEE') as day_name,
    dayofweek(full_date) in (1, 7) as is_weekend
from days
