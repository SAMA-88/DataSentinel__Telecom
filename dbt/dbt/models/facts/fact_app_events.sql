-- FACT: one row per mobile app event.
-- The app sends the customer id directly, so we look up the customer VERSION valid at event time.
-- If no customer is found (or customer_id is empty), customer_key = -1.

select
    e.event_id,
    e.customer_id,
    e.event_type,
    e.app_version,
    e.event_ts,
    coalesce(c.customer_key, -1) as customer_key,
    to_date(e.event_ts) as event_date,
    cast(date_format(e.event_ts, 'yyyyMMdd') as int) as event_date_key,
    c.customer_key is not null as customer_known
from {{ source('silver', 'app_event') }} e
left join {{ ref('dim_customer') }} c
    on  c.customer_key <> -1
    and e.customer_id = c.customer_id
    and e.event_ts >= c.valid_from
    and e.event_ts < c.valid_to
