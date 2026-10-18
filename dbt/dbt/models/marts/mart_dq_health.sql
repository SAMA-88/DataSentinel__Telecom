-- Dashboard 1: the latest result of every gate rule and monitor (one row per dataset / check / rule / scope).
with ranked as (
    select
        *, row_number() over (partition by dataset, check_name, rule_id, scope order by run_ts desc) as _rn
    from {{ source('monitoring', 'dq_results') }}
)
select
    dataset, check_name, rule_id, dimension, scope, rows_checked, rows_failed, pass_rate, status, severity, detail,
    contract_version, run_id, run_ts
from ranked
where _rn = 1
