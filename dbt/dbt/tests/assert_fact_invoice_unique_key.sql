-- an invoice appears once per source
select source, invoice_id, count(*) as n
from {{ ref('fact_invoice') }}
group by source, invoice_id
having count(*) > 1
