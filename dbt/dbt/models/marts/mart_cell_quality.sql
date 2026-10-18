-- Dashboard 4: voice quality per cell (which towers drop calls).
select
    cell_id,
    count(*) as voice_calls,
    sum(case when call_result = 'DROPPED' then 1 else 0 end) as dropped_calls,
    sum(case when call_result = 'DROPPED' then 1 else 0 end) / nullif(count(*), 0) as drop_rate,
    sum(duration_sec) / 60.0 as minutes
from {{ ref('fact_usage') }}
where event_type = 'VOICE' and cell_id is not null
group by cell_id
