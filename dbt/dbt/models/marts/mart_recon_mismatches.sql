-- Dashboard 2: the individual mismatches of the latest run (what the billing team investigates).
with last_run as (
    select run_id from {{ source('monitoring', 'recon_results') }} order by run_ts desc limit 1
)
select m.check_id, m.entity_type, m.entity_key, m.value_a, m.value_b, m.diff, m.amount_at_risk_eur, m.reason, m.run_id
from {{ source('monitoring', 'recon_mismatches') }} m
join last_run l on m.run_id = l.run_id
