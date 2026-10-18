-- DIMENSION: one row per plan VERSION (a plan keeps one row each time its price or limits change).
-- Input: silver.plan_history

-- Part 1: all real plan versions
select
    xxhash64(plan_id, valid_from) as plan_key,     -- a unique number made from plan_id + start date
    plan_id,
    version,
    valid_from,
    valid_to,
    is_current,
    plan_name,
    monthly_fee,
    data_gb,
    voice_minutes
from {{ source('silver', 'plan_history') }}

union all

-- Part 2: one special row, key -1, for "plan unknown".
-- A row that has no matching plan points here, so we never lose a row.
select
    cast(-1 as bigint),
    cast(-1 as int),
    cast(1 as int),
    timestamp'1970-01-01 00:00:00',
    timestamp'9999-12-31 00:00:00',
    true,
    'UNKNOWN',
    cast(null as decimal(8,2)),
    cast(null as int),
    cast(null as int)
