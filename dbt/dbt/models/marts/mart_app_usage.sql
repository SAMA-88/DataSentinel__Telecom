-- Dashboard 4: app events per day, app version and event type (crash share per version).
select
    event_date, event_date_key, app_version, event_type,
    count(*) as events,
    count(distinct customer_id) as customers
from {{ ref('fact_app_events') }}
group by event_date, event_date_key, app_version, event_type
