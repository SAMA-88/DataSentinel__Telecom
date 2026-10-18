-- HELPER: find the customer behind a phone number.
-- Network usage, invoices and partner usage only know the (hashed) phone number, not the customer id.
-- If two customers share one phone number (a duplicate in the CRM), we pick the one with the lowest customer_id,
-- so every phone number points to exactly ONE customer and the facts never get duplicate rows.
-- The FIRST version of each customer is valid from 1900-01-01 here (not from created_at): invoices, payments and
-- partner usage can be older than the CRM record (e.g. July invoices for a customer created in September), and
-- they must still find their customer. Later versions keep their real dates, so history lookups are unchanged.

select
    c.customer_key,
    c.customer_id,
    c.msisdn_hash,
    case when c.valid_from = min(c.valid_from) over (partition by c.customer_id)
         then cast('1900-01-01' as timestamp) else c.valid_from end as valid_from,
    c.valid_to
from {{ ref('dim_customer') }} c
join (
    select msisdn_hash, min(customer_id) as customer_id
    from {{ ref('dim_customer') }}
    where customer_key <> -1
    group by msisdn_hash
) chosen
    on  c.msisdn_hash = chosen.msisdn_hash
    and c.customer_id = chosen.customer_id
where c.customer_key <> -1
