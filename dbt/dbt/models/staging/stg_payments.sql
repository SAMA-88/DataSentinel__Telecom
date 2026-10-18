-- STAGING: payments from the billing database AND from the monthly billing file, in one list.
-- The column "source" says where each row came from.

select
    'billing_db' as source,
    payment_id, invoice_id, amount, method, status, paid_at
from {{ source('silver', 'payment') }}

union all

select
    'billing_file' as source,
    payment_id, invoice_id, amount, method, status, paid_at
from {{ source('silver', 'payment_file') }}
