-- Dashboard 4: partner (roaming and international) cost per month and service, and how much belongs to unknown numbers.
select
    period_start, service,
    count(*) as records,
    sum(amount) as amount_eur,
    sum(case when customer_known then 0 else amount end) as amount_unknown_customer_eur
from {{ ref('fact_partner_usage') }}
group by period_start, service
