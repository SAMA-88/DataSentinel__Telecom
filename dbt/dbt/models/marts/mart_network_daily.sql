-- Dashboard 4: network load per day and record type.
select
    usage_date, usage_date_key, event_type,
    count(*) as records,
    count(distinct msisdn_hash) as active_numbers,
    sum(duration_sec) / 60.0 as minutes,
    sum(data_mb) as data_mb,
    sum(case when call_result = 'DROPPED' then 1 else 0 end) as dropped,
    sum(case when call_result = 'DROPPED' then 1 else 0 end) / nullif(count(*), 0) as drop_rate
from {{ ref('fact_usage') }}
group by usage_date, usage_date_key, event_type
