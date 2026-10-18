-- exactly one current version per customer
select customer_id, count(*) as n
from {{ ref('dim_customer') }}
where is_current and customer_key <> -1
group by customer_id
having count(*) <> 1
