# Databricks notebook source
# MAGIC %run ./11_dq_lib

# COMMAND ----------

# Views for dashboard 1 (incident panel) and for the on-call person.
M = f"{CATALOG}.monitoring"
T_OPS, T_EVENTS, T_ALERTS = f"{M}.incident_ops", f"{M}.incident_events", f"{M}.alerts"

spark.sql(f"""
CREATE OR REPLACE VIEW {M}.v_incident_board AS
SELECT i.incident_id, i.status, o.priority, i.severity, i.dataset, i.check_name, i.pillar, i.scope, i.description,
       i.impacted, o.team, o.owner, i.opened_at, o.due_at, o.acknowledged_at, o.acknowledged_by, o.escalation_level,
       i.closed_at,
       round((unix_timestamp(coalesce(i.closed_at, current_timestamp())) - unix_timestamp(i.opened_at)) / 60.0, 1) AS age_minutes,
       round((unix_timestamp(o.acknowledged_at) - unix_timestamp(i.opened_at)) / 60.0, 1) AS minutes_to_ack,
       CASE WHEN i.closed_at IS NOT NULL THEN round((unix_timestamp(i.closed_at) - unix_timestamp(i.opened_at)) / 60.0, 1) END AS minutes_to_resolve,
       CASE
         WHEN i.status = 'RESOLVED' AND i.closed_at <= o.due_at THEN 'RESOLVED_IN_SLA'
         WHEN i.status = 'RESOLVED' THEN 'RESOLVED_LATE'
         WHEN current_timestamp() > o.due_at THEN 'BREACHED'
         WHEN unix_timestamp(o.due_at) - unix_timestamp(current_timestamp()) < 0.25 * o.sla_seconds THEN 'AT_RISK'
         ELSE 'ON_TRACK' END AS sla_status,
       CASE WHEN o.acknowledged_at IS NOT NULL THEN 'ACKNOWLEDGED' WHEN i.status = 'OPEN' THEN 'WAITING' ELSE 'AUTO_CLOSED' END AS ack_status,
       o.runbook
FROM {T_INCIDENTS} i JOIN {T_OPS} o ON i.incident_id = o.incident_id""")

spark.sql(f"""
CREATE OR REPLACE VIEW {M}.v_incident_kpis AS
SELECT
  sum(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) AS open_incidents,
  sum(CASE WHEN status = 'OPEN' AND priority = 'P1' THEN 1 ELSE 0 END) AS open_p1,
  sum(CASE WHEN status = 'OPEN' AND priority = 'P2' THEN 1 ELSE 0 END) AS open_p2,
  sum(CASE WHEN sla_status = 'BREACHED' THEN 1 ELSE 0 END) AS open_sla_breached,
  sum(CASE WHEN status = 'OPEN' AND ack_status = 'WAITING' THEN 1 ELSE 0 END) AS waiting_for_ack,
  round(avg(minutes_to_ack), 1) AS mtta_minutes,
  round(avg(minutes_to_resolve), 1) AS mttr_minutes,
  round(100.0 * sum(CASE WHEN sla_status = 'RESOLVED_IN_SLA' THEN 1 ELSE 0 END)
        / nullif(sum(CASE WHEN status = 'RESOLVED' THEN 1 ELSE 0 END), 0), 1) AS resolved_in_sla_pct,
  sum(CASE WHEN status = 'RESOLVED' THEN 1 ELSE 0 END) AS resolved_incidents
FROM {M}.v_incident_board""")

spark.sql(f"""
CREATE OR REPLACE VIEW {M}.v_incident_by_dataset AS
SELECT dataset, count(*) AS incidents,
       sum(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) AS open_incidents,
       sum(CASE WHEN status = 'RESOLVED' THEN 1 ELSE 0 END) AS resolved_incidents,
       min(priority) AS worst_priority, max(opened_at) AS last_opened
FROM {M}.v_incident_board GROUP BY dataset""")

spark.sql(f"""
CREATE OR REPLACE VIEW {M}.v_incident_timeline AS
SELECT e.incident_id, e.event_ts, e.event_type, e.actor, e.detail, b.dataset, b.check_name, b.priority
FROM {T_EVENTS} e LEFT JOIN {M}.v_incident_board b ON e.incident_id = b.incident_id""")

spark.sql(f"""
CREATE OR REPLACE VIEW {M}.v_alert_log AS
SELECT a.sent_at, a.kind, a.priority, a.channel, a.status, a.recipient, a.body, a.incident_id
FROM {T_ALERTS} a WHERE a.sent_at >= current_timestamp() - INTERVAL 7 DAYS""")

print("incident views are ready")
display(spark.table(f"{M}.v_incident_kpis"))
