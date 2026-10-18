-- the versions of a customer never overlap in time
select a.customer_id, a.version as version_a, b.version as version_b
from {{ ref('dim_customer') }} a
join {{ ref('dim_customer') }} b
  on a.customer_id = b.customer_id and a.version < b.version and a.valid_to > b.valid_from
where a.customer_key <> -1
