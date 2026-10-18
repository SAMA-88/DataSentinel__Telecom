-- Dashboard 1: rows in quarantine by dataset and status (REJECTED, PENDING_FIX, FIXED_PENDING_REVALIDATION, ...).
with q as (
    {{ quarantine_union() }}
)
select dataset, quarantine_status, count(*) as rows, min(quarantined_at) as oldest_quarantined_at
from q
group by dataset, quarantine_status
