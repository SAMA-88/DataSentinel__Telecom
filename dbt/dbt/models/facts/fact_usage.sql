-- FACT: one row per network record (a call, an SMS or a data session).
-- Inputs: silver.network_cdr, silver.customer_xref, and the customer lookup by phone number.
-- Each record gets the customer VERSION that was valid at the moment of the event.
-- If no customer is found, customer_key = -1 (unknown) so the record is still kept.

select
    u.cdr_id,
    u.msisdn_hash,
    u.event_type,
    u.event_ts,
    u.duration_sec,
    u.data_mb,
    u.cell_id,
    u.call_result,
    coalesce(c.customer_key, -1) as customer_key,
    c.customer_id,
    x.account_id,
    to_date(u.event_ts) as usage_date,
    cast(date_format(u.event_ts, 'yyyyMMdd') as int) as usage_date_key,
    c.customer_key is not null as customer_known
from {{ source('silver', 'network_cdr') }} u

-- find the customer who owned this phone number at the time of the event
left join {{ ref('int_customer_by_phone') }} c
    on  u.msisdn_hash = c.msisdn_hash
    and u.event_ts >= c.valid_from
    and u.event_ts < c.valid_to

-- find the billing account of this phone number (only when CRM and billing match)
left join (
    select msisdn_hash, account_id
    from {{ source('silver', 'customer_xref') }}
    where match_status = 'MATCHED'
) x
    on u.msisdn_hash = x.msisdn_hash
