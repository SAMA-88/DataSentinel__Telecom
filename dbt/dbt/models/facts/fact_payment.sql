-- FACT: one row per payment and source.
-- The customer comes from the invoice that was paid.
-- invoice_known = false means the payment points to an invoice that does not exist.
-- We keep those rows on purpose: the reconciliation reports them.

select
    p.source,
    p.payment_id,
    p.invoice_id,
    p.amount,
    p.method,
    p.status,
    p.paid_at,
    coalesce(i.customer_key, -1) as customer_key,
    i.customer_id,
    i.account_id,
    i.invoice_id is not null as invoice_known,
    cast(date_format(p.paid_at, 'yyyyMMdd') as int) as paid_date_key
from {{ ref('stg_payments') }} p
left join {{ ref('fact_invoice') }} i
    on  p.source = i.source
    and p.invoice_id = i.invoice_id
