-- Dashboard 2: EUR at risk per check and run, to see if leakage grows or shrinks.
select run_id, run_ts, check_id, check_name, rows_mismatched, amount_at_risk_eur, status
from {{ source('monitoring', 'recon_results') }}
