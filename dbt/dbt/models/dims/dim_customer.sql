-- DIMENSION: one row per customer VERSION (a customer gets a new row when plan, city, status... change).
-- Inputs: silver.customer_history (the customers) and dim_plan (the plan prices).
-- Only masked or hashed personal data is here. Raw phone, email and name never reach Gold.

-- Part 1: all real customer versions
select
    xxhash64(c.customer_id, c.valid_from) as customer_key,   -- a unique number made from customer_id + start date
    c.customer_id,
    c.version,
    c.valid_from,
    c.valid_to,
    c.is_current,
    c.name_masked,
    c.email_masked,
    c.email_hash,
    c.msisdn_masked,
    c.msisdn_hash,
    c.city,
    c.country,
    c.status,
    c.plan_id,
    -- plan_key: first choice = the plan version that was valid when this customer version started,
    --           second choice = the current plan, last choice = -1 (unknown)
    coalesce(p.plan_key, cp.plan_key, -1) as plan_key,
    c.created_at,
    c._was_repaired
from {{ source('silver', 'customer_history') }} c

-- join 1: the plan version valid on the day this customer version started
left join {{ ref('dim_plan') }} p
    on  c.plan_id = p.plan_id
    and c.valid_from >= p.valid_from
    and c.valid_from < p.valid_to
    and p.plan_key <> -1

-- join 2: the current version of the plan (used only if join 1 found nothing)
left join (
    select plan_id, plan_key
    from {{ ref('dim_plan') }}
    where is_current = true and plan_key <> -1
) cp
    on c.plan_id = cp.plan_id

union all

-- Part 2: one special row, key -1, for "customer unknown"
select
    cast(-1 as bigint),
    cast(-1 as bigint),
    cast(1 as int),
    timestamp'1970-01-01 00:00:00',
    timestamp'9999-12-31 00:00:00',
    true,
    'UNKNOWN',
    'UNKNOWN',
    cast(null as string),
    'UNKNOWN',
    cast(null as string),
    cast(null as string),
    cast(null as string),
    cast(null as string),
    cast(null as int),
    cast(-1 as bigint),
    cast(null as timestamp),
    false
