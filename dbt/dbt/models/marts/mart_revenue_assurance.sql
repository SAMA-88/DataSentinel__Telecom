-- Dashboard 2: the latest reconciliation run, one row per check, with the EUR at risk.
with last_run as (
    select run_id from {{ source('monitoring', 'recon_results') }} order by run_ts desc limit 1
)
select
    r.check_id, r.check_name, r.source_a, r.source_b, r.rows_checked, r.rows_mismatched, r.amount_at_risk_eur,
    r.status, r.severity, r.detail, r.run_id, r.run_ts,
    case r.check_id
        when 'R1' then 'Unbilled customer'
        when 'R2' then 'Billing without customer'
        when 'R3' then 'Invoice arithmetic'
        when 'R4' then 'Payment mismatch'
        when 'R5' then 'Wrong plan price'
        when 'R6' then 'File vs database'
        when 'R7' then 'Unmatched payment'
        when 'R8' then 'Unregistered network usage'
        when 'R9' then 'Unregistered partner usage'
        when 'R10' then 'Unpaid invoices'
    end as leak_type,
    r.check_id <> 'R10' as is_data_defect
from {{ source('monitoring', 'recon_results') }} r
join last_run l on r.run_id = l.run_id
