-- FACT: one row per invoice and source (billing_db or billing_file).
-- Each invoice gets the customer VERSION that was valid when the invoice was issued.
-- If no customer is found, customer_key = -1 (unknown) so the invoice is still kept.

select
    i.source,
    i.invoice_id,
    i.account_id,
    i.account_ref,
    i.msisdn_hash,
    i.period_start,
    i.period_end,
    i.plan_fee,
    i.overage,
    i.total_amount,
    i.status,
    i.issued_at,
    i.lines_total,
    i.lines_count,
    coalesce(c.customer_key, -1) as customer_key,
    c.customer_id,
    cast(date_format(i.period_start, 'yyyyMMdd') as int) as period_start_key,
    c.customer_key is not null as customer_known
from {{ ref('stg_invoices') }} i

-- invoice time = when it was issued; if that is empty, the last day of the billing period
left join {{ ref('int_customer_by_phone') }} c
    on  i.msisdn_hash = c.msisdn_hash
    and coalesce(i.issued_at, cast(i.period_end as timestamp)) >= c.valid_from
    and coalesce(i.issued_at, cast(i.period_end as timestamp)) < c.valid_to
