-- Dashboard 3: one row per current customer with plan, billing, payments, network usage, app activity and partner usage.
-- Phone, email and name are masked; nothing raw reaches Gold.
with cust as (
    select * from {{ ref('dim_customer') }} where is_current and customer_key <> -1
),
plan as (
    select plan_id, plan_name, monthly_fee, data_gb, voice_minutes from {{ ref('dim_plan') }} where is_current and plan_key <> -1
),
inv as (
    select customer_id, count(*) as invoices, sum(total_amount) as billed_eur
    from {{ ref('fact_invoice') }} where source = 'billing_db' and customer_key <> -1 group by customer_id
),
pay as (
    select customer_id,
           sum(case when status = 'PAID' then amount else 0 end) as paid_eur,
           max(case when status = 'PAID' then paid_at end) as last_payment_at
    from {{ ref('fact_payment') }} where source = 'billing_db' and customer_key <> -1 group by customer_id
),
usage as (
    select customer_id,
           sum(case when event_type = 'VOICE' then duration_sec else 0 end) / 60.0 as voice_minutes_used,
           sum(case when event_type = 'SMS' then 1 else 0 end) as sms_count,
           sum(case when event_type = 'DATA' then data_mb else 0 end) as data_mb_used,
           sum(case when event_type = 'VOICE' and call_result = 'DROPPED' then 1 else 0 end) as dropped_calls,
           max(event_ts) as last_network_activity_at
    from {{ ref('fact_usage') }} where customer_key <> -1 group by customer_id
),
app as (
    select customer_id,
           count(*) as app_events,
           sum(case when event_type = 'crash' then 1 else 0 end) as app_crashes,
           sum(case when event_type = 'plan_change' then 1 else 0 end) as plan_changes,
           max(case when event_type = 'login' then event_ts end) as last_login_at
    from {{ ref('fact_app_events') }} where customer_key <> -1 group by customer_id
),
partner as (
    select customer_id, sum(amount) as partner_amount_eur
    from {{ ref('fact_partner_usage') }} where customer_key <> -1 group by customer_id
)
select
    c.customer_id, c.name_masked, c.email_masked, c.msisdn_masked, c.city, c.country, c.status, c.created_at,
    p.plan_name, p.monthly_fee, p.data_gb as plan_data_gb, p.voice_minutes as plan_voice_minutes,
    coalesce(i.invoices, 0) as invoices,
    coalesce(i.billed_eur, 0) as billed_eur,
    coalesce(y.paid_eur, 0) as paid_eur,
    coalesce(i.billed_eur, 0) - coalesce(y.paid_eur, 0) as unpaid_eur,
    y.last_payment_at,
    coalesce(u.voice_minutes_used, 0) as voice_minutes_used,
    coalesce(u.sms_count, 0) as sms_count,
    coalesce(u.data_mb_used, 0) as data_mb_used,
    coalesce(u.dropped_calls, 0) as dropped_calls,
    u.last_network_activity_at,
    coalesce(a.app_events, 0) as app_events,
    coalesce(a.app_crashes, 0) as app_crashes,
    coalesce(a.plan_changes, 0) as plan_changes,
    a.last_login_at,
    coalesce(t.partner_amount_eur, 0) as partner_amount_eur
from cust c
left join plan p on c.plan_id = p.plan_id
left join inv i on c.customer_id = i.customer_id
left join pay y on c.customer_id = y.customer_id
left join usage u on c.customer_id = u.customer_id
left join app a on c.customer_id = a.customer_id
left join partner t on c.customer_id = t.customer_id
