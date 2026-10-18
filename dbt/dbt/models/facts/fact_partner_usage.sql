-- FACT: one row per partner usage record (roaming or international).
-- The partner file covers a whole month, so we look up the customer at the start of the NEXT month.
-- If no customer is found, customer_key = -1.

select
    p.partner_record_id,
    p.msisdn_hash,
    p.service,
    p.amount,
    p.currency,
    p.period_start,
    coalesce(c.customer_key, -1) as customer_key,
    c.customer_id,
    cast(date_format(p.period_start, 'yyyyMMdd') as int) as period_start_key,
    c.customer_key is not null as customer_known
from {{ source('silver', 'partner_usage') }} p
left join {{ ref('int_customer_by_phone') }} c
    on  p.msisdn_hash = c.msisdn_hash
    and cast(add_months(p.period_start, 1) as timestamp) >= c.valid_from
    and cast(add_months(p.period_start, 1) as timestamp) < c.valid_to
