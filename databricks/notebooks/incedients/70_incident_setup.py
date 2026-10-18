# Databricks notebook source
# MAGIC %run ./11_dq_lib

# COMMAND ----------

# Incident and alert management: tables and configuration.
# The quality gate, the monitors and the reconciliation already OPEN and RESOLVE incidents (sync_incidents).
# This phase adds what a real team needs on top: who owns it, how fast, who was told, and what happened.
#   incident_routing  dataset (+ pillar) -> team, owner, who to escalate to        (edit this table)
#   priority_policy   severity -> priority, SLA, escalation time, Slack or log     (edit this table)
#   runbooks          pillar -> first steps to investigate                          (edit this table)
#   incident_ops      one row per incident: priority, owner, due, acknowledged, escalation level
#   incident_events   timeline: OPENED, ASSIGNED, ALERTED, ACKED, ESCALATED, COMMENT, RESOLVED
#   alerts            every notification sent (or logged)
# The existing monitoring.incidents table is not changed.

# COMMAND ----------

M = f"{CATALOG}.monitoring"
T_ROUTING, T_POLICY, T_RUNBOOKS = f"{M}.incident_routing", f"{M}.priority_policy", f"{M}.runbooks"
T_OPS, T_EVENTS, T_ALERTS = f"{M}.incident_ops", f"{M}.incident_events", f"{M}.alerts"

spark.sql(f"""CREATE TABLE IF NOT EXISTS {T_ROUTING} (
  dataset STRING, pillar STRING, team STRING, owner STRING, escalation_to STRING)""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {T_POLICY} (
  severity STRING, priority STRING, sla_minutes INT, escalate_after_minutes INT, notify_slack BOOLEAN, escalate BOOLEAN)""")
spark.sql(f"CREATE TABLE IF NOT EXISTS {T_RUNBOOKS} (pillar STRING, steps STRING)")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {T_OPS} (
  incident_id STRING, priority STRING, team STRING, owner STRING, runbook STRING,
  sla_seconds INT, escalate_after_seconds INT, due_at TIMESTAMP, acknowledged_at TIMESTAMP,
  acknowledged_by STRING, escalation_level INT, assigned_at TIMESTAMP)""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {T_EVENTS} (
  incident_id STRING, event_ts TIMESTAMP, event_type STRING, actor STRING, detail STRING)""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {T_ALERTS} (
  alert_id STRING, incident_id STRING, kind STRING, sent_at TIMESTAMP, channel STRING, recipient STRING,
  priority STRING, subject STRING, body STRING, status STRING)""")

# COMMAND ----------

# Who owns what. Replace the example names and addresses with real ones. '*' means any.
CRM, BILL, NET, APP, PART, PLAT = "CRM team", "Billing team", "Network team", "Mobile app team", "Partner team", "Data platform"
LEAD = "data-platform-lead"
routing = [
    ("crm_customers", "*", CRM, "crm-owner@telco.example", LEAD),
    ("crm_plans", "*", CRM, "crm-owner@telco.example", LEAD),
    ("billing_accounts", "*", BILL, "billing-owner@telco.example", LEAD),
    ("billing_invoices", "*", BILL, "billing-owner@telco.example", LEAD),
    ("billing_invoice_lines", "*", BILL, "billing-owner@telco.example", LEAD),
    ("billing_payments", "*", BILL, "billing-owner@telco.example", LEAD),
    ("invoices_file", "*", BILL, "billing-owner@telco.example", LEAD),
    ("payments_file", "*", BILL, "billing-owner@telco.example", LEAD),
    ("network_cdr", "*", NET, "network-owner@telco.example", LEAD),
    ("app_events", "*", APP, "app-owner@telco.example", LEAD),
    ("partner_usage", "*", PART, "partner-owner@telco.example", LEAD),
    ("recon", "*", PLAT, "data-platform@telco.example", LEAD),
    ("*", "*", PLAT, "data-platform@telco.example", LEAD),
]
spark.createDataFrame(routing, "dataset string, pillar string, team string, owner string, escalation_to string") \
    .write.mode("overwrite").saveAsTable(T_ROUTING)

# severity -> priority and time limits (minutes). P1 and P2 go to Slack and escalate; P3 and P4 are logged only.
policy = [
    ("critical", "P1", 60, 30, True, True),
    ("high", "P2", 240, 120, True, True),
    ("medium", "P3", 1440, 720, False, False),
    ("low", "P4", 4320, 2160, False, False),
]
spark.createDataFrame(policy, "severity string, priority string, sla_minutes int, escalate_after_minutes int, "
                              "notify_slack boolean, escalate boolean").write.mode("overwrite").saveAsTable(T_POLICY)

runbooks = [
    ("freshness", "1) Is the file or topic late at the source? 2) Check the producer job. 3) Ask the source owner for a re-delivery. 4) Replay when it arrives."),
    ("volume", "1) Compare with the same weekday last week. 2) Check for a partial file or a stopped producer. 3) Ask for a full re-delivery."),
    ("distribution", "1) Look at the values that moved (dq_results detail). 2) Real business event or a unit/scale bug? 3) If a bug, fix upstream and replay."),
    ("schema", "1) Compare the file header with the contract. 2) Missing column: block and ask for a corrected file. 3) New column: add a contract version, then replay."),
    ("lineage", "1) Open the lineage view. 2) List the downstream tables that read the failing dataset. 3) Tell their owners."),
    ("validity", "1) Open the quarantine table for the dataset. 2) Group by _failure_reasons. 3) Auto-fix what is deterministic, send the rest to the source owner."),
    ("consistency", "1) Open recon_mismatches for the check. 2) Sort by EUR at risk. 3) Confirm with the source owners which system is right."),
    ("accuracy", "1) Open recon_mismatches for the check. 2) Compare both sides for the top rows. 3) Correct the source, then rerun recon."),
    ("completeness", "1) Find which column is empty and since when. 2) Check the producer for a recent change. 3) Flag or reject per the contract."),
    ("uniqueness", "1) Check if the producer retries. 2) Deduplicate on the key. 3) Ask the source owner to stop repeats."),
    ("timeliness", "1) Check the clocks and time zones at the source. 2) Reject future timestamps. 3) Ask the source owner to fix."),
    ("domain", "1) List the unexpected values. 2) Is it a new valid value? If yes, update the contract. If not, reject and report."),
    ("*", "1) Open the incident details. 2) Check the quarantine table and dq_results for the dataset. 3) Contact the owner."),
]
spark.createDataFrame(runbooks, "pillar string, steps string").write.mode("overwrite").saveAsTable(T_RUNBOOKS)

print("incident tables and configuration are ready")
display(spark.table(T_POLICY))
