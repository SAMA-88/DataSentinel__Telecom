-- Dashboard 1: incidents with owner, priority, deadline, acknowledgement and SLA status.
-- Needs the incident phase (notebooks 70 and 71) to have run once, so monitoring.incident_ops exists.
select
    i.incident_id, i.dataset, i.check_name, i.pillar, i.severity, i.scope, i.description, i.impacted, i.status,
    i.opened_at, i.closed_at,
    o.priority, o.team, o.owner, o.due_at, o.acknowledged_at, o.acknowledged_by,
    coalesce(o.escalation_level, 0) as escalation_level,
    round((unix_timestamp(coalesce(i.closed_at, current_timestamp())) - unix_timestamp(i.opened_at)) / 3600.0, 2) as open_hours,
    round((unix_timestamp(o.acknowledged_at) - unix_timestamp(i.opened_at)) / 60.0, 1) as minutes_to_ack,
    case
        when i.status = 'RESOLVED' and i.closed_at <= o.due_at then 'RESOLVED_IN_SLA'
        when i.status = 'RESOLVED' then 'RESOLVED_LATE'
        when o.due_at is null then 'UNASSIGNED'
        when current_timestamp() > o.due_at then 'BREACHED'
        else 'ON_TRACK'
    end as sla_status
from {{ source('monitoring', 'incidents') }} i
left join {{ source('monitoring', 'incident_ops') }} o on i.incident_id = o.incident_id
