# Databricks notebook source
# MAGIC %run ./11_dq_lib

# COMMAND ----------

# Run this after every quality gate, monitor and recon run (it is also a task in the Databricks Job).
# 1. Assign every new incident: priority, owner, due time, runbook.
# 2. Alert on new P1/P2 incidents (one Slack message per run, not one per incident) and log all of them.
# 3. Escalate P1/P2 incidents nobody acknowledged in time.
# 4. Record RESOLVED for incidents that the pipeline closed, and tell people who had been alerted.
# Safe to run again: nothing is assigned, alerted or closed twice.

# COMMAND ----------

dbutils.widgets.text("sla_scale", "1", "SLA scale (1 = real times; 0.01 = demo, 100x faster)")
SCALE = float(dbutils.widgets.get("sla_scale") or 1)

M = f"{CATALOG}.monitoring"
T_ROUTING, T_POLICY, T_RUNBOOKS = f"{M}.incident_routing", f"{M}.priority_policy", f"{M}.runbooks"
T_OPS, T_EVENTS, T_ALERTS = f"{M}.incident_ops", f"{M}.incident_events", f"{M}.alerts"
EV_SCHEMA = "incident_id string, event_ts timestamp, event_type string, actor string, detail string"
AL_SCHEMA = ("alert_id string, incident_id string, kind string, sent_at timestamp, channel string, recipient string, "
             "priority string, subject string, body string, status string")


def add_events(rows):
    if rows:
        spark.createDataFrame(rows, EV_SCHEMA).write.mode("append").saveAsTable(T_EVENTS)


def deliver(subject, body):
    """Send one Slack message when the webhook secret exists; otherwise only log. Returns (channel, status)."""
    try:
        url = dbutils.secrets.get("sentinel", "slack_webhook")
    except Exception:
        return "log", "LOGGED"
    if not url:
        return "log", "LOGGED"
    try:
        import requests
        r = requests.post(url, json={"text": f"*{subject}*\n{body}"}, timeout=10)
        return "slack", ("SENT" if r.ok else "FAILED")
    except Exception as e:
        print("slack delivery failed:", e)
        return "slack", "FAILED"


def send_digest(kind, items, title):
    """items: list of dict(incident_id, priority, line, recipient, slack). One message for the slack ones,
    one alert row per incident for everything."""
    if not items:
        return 0
    slack_items = [i for i in items if i["slack"]]
    channel, status = "log", "LOGGED"
    if slack_items:
        subject = f"{title}: {len(slack_items)} incident(s)"
        channel, status = deliver(subject, "\n".join(i["line"] for i in slack_items))
    now = datetime.utcnow()
    rows = []
    for i in items:
        ch, st = (channel, status) if i["slack"] else ("log", "LOGGED")
        rows.append((uuid.uuid4().hex[:12], i["incident_id"], kind, now, ch, i["recipient"], i["priority"],
                     f"{title}: {i['incident_id']}", i["line"], st))
    spark.createDataFrame(rows, AL_SCHEMA).write.mode("append").saveAsTable(T_ALERTS)
    add_events([(i["incident_id"], now, "ALERTED" if kind != "ESCALATION" else "ESCALATION_ALERT", "system",
                 f"{kind} alert, channel {channel if i['slack'] else 'log'}") for i in items])
    return len(items)

# COMMAND ----------

# 1. Assign new incidents
routing = {(r["dataset"], r["pillar"]): r for r in spark.table(T_ROUTING).collect()}
policy = {r["severity"]: r for r in spark.table(T_POLICY).collect()}
runbooks = {r["pillar"]: r["steps"] for r in spark.table(T_RUNBOOKS).collect()}


def route(dataset, pillar):
    for k in ((dataset, pillar), (dataset, "*"), ("*", pillar), ("*", "*")):
        if k in routing:
            return routing[k]
    raise RuntimeError("no routing rule, add ('*','*') to incident_routing")


new = spark.sql(f"SELECT i.* FROM {T_INCIDENTS} i LEFT ANTI JOIN {T_OPS} o ON i.incident_id = o.incident_id").collect()
ops_rows, ev_rows = [], []
for r in new:
    p = policy.get(r["severity"]) or policy["medium"]
    rt = route(r["dataset"], r["pillar"])
    ops_rows.append((r["incident_id"], p["priority"], rt["team"], rt["owner"],
                     runbooks.get(r["pillar"], runbooks["*"]),
                     max(1, int(p["sla_minutes"] * 60 * SCALE)), max(1, int(p["escalate_after_minutes"] * 60 * SCALE)),
                     None, None, None, 0, datetime.utcnow()))
    ev_rows.append((r["incident_id"], r["opened_at"], "OPENED", "system",
                    f"{r['dataset']} / {r['check_name']}: {r['description']}"))
    ev_rows.append((r["incident_id"], datetime.utcnow(), "ASSIGNED", "system",
                    f"{p['priority']}, owner {rt['owner']} ({rt['team']})"))
if ops_rows:
    spark.createDataFrame(ops_rows, "incident_id string, priority string, team string, owner string, runbook string, "
                                    "sla_seconds int, escalate_after_seconds int, due_at timestamp, "
                                    "acknowledged_at timestamp, acknowledged_by string, escalation_level int, "
                                    "assigned_at timestamp").write.mode("append").saveAsTable(T_OPS)
    add_events(ev_rows)
spark.sql(f"""MERGE INTO {T_OPS} o USING {T_INCIDENTS} i ON o.incident_id = i.incident_id
WHEN MATCHED AND o.due_at IS NULL THEN UPDATE SET due_at = timestampadd(SECOND, o.sla_seconds, i.opened_at)""")
print(f"assigned {len(ops_rows)} new incident(s)")

# COMMAND ----------

# 2. Alert on new incidents (open ones that never had a NEW alert)
new_alerts = spark.sql(f"""
SELECT i.incident_id, i.dataset, i.check_name, i.pillar, i.scope, i.description, i.impacted,
       o.priority, o.owner, o.team, o.due_at, o.runbook, p.notify_slack
FROM {T_INCIDENTS} i
JOIN {T_OPS} o ON i.incident_id = o.incident_id
JOIN {T_POLICY} p ON p.priority = o.priority
LEFT ANTI JOIN {T_ALERTS} a ON a.incident_id = i.incident_id AND a.kind = 'NEW'
WHERE i.status = 'OPEN'""").collect()


def fmt(r, extra=""):
    due = r["due_at"].strftime("%Y-%m-%d %H:%M") if r["due_at"] else "?"
    imp = f" | impacts: {r['impacted']}" if r["impacted"] else ""
    return (f"[{r['priority']}] {r['dataset']} / {r['check_name']} ({r['pillar']}, {r['scope']}): "
            f"{r['description']}{imp} | owner {r['owner']} | due {due}{extra}")


items = [dict(incident_id=r["incident_id"], priority=r["priority"], line=fmt(r), recipient=r["owner"],
              slack=bool(r["notify_slack"])) for r in new_alerts]
n_new = send_digest("NEW", items, "New data quality incident")
print(f"new alerts: {n_new}")

# COMMAND ----------

# 3. Escalate P1/P2 incidents that nobody acknowledged in time (level 1 after one interval, level 2 after two)
esc = spark.sql(f"""
SELECT i.incident_id, i.dataset, i.check_name, i.pillar, i.scope, i.description, i.impacted,
       o.priority, o.owner, o.team, o.due_at, o.escalation_level, rt.escalation_to
FROM {T_INCIDENTS} i
JOIN {T_OPS} o ON i.incident_id = o.incident_id
JOIN {T_POLICY} p ON p.priority = o.priority AND p.escalate = true
LEFT JOIN {T_ROUTING} rt ON rt.dataset = i.dataset AND rt.pillar = '*'
WHERE i.status = 'OPEN' AND o.acknowledged_at IS NULL AND o.escalation_level < 2
  AND current_timestamp() >= timestampadd(SECOND, (o.escalation_level + 1) * o.escalate_after_seconds, i.opened_at)
""").collect()
if esc:
    ids = ",".join("'" + r["incident_id"] + "'" for r in esc)
    spark.sql(f"UPDATE {T_OPS} SET escalation_level = escalation_level + 1 WHERE incident_id IN ({ids})")
    now = datetime.utcnow()
    add_events([(r["incident_id"], now, "ESCALATED", "system",
                 f"level {r['escalation_level'] + 1}: not acknowledged, escalated to {r['escalation_to'] or 'lead'}")
                for r in esc])
    send_digest("ESCALATION",
                [dict(incident_id=r["incident_id"], priority=r["priority"],
                      line=fmt(r, f" | NOT ACKNOWLEDGED, escalated to {r['escalation_to'] or 'lead'}"),
                      recipient=r["escalation_to"] or "lead", slack=True) for r in esc],
                "Escalation")
print(f"escalated: {len(esc)}")

# COMMAND ----------

# 4. Resolved: the pipeline closed these incidents (the problem no longer shows up). Record it and tell people.
res = spark.sql(f"""
SELECT i.incident_id, i.dataset, i.check_name, i.pillar, i.scope, i.description, i.impacted, i.opened_at, i.closed_at,
       o.priority, o.owner, o.team, o.due_at, o.acknowledged_at, p.notify_slack,
       coalesce(w.was_alerted, false) AS was_alerted
FROM {T_INCIDENTS} i
JOIN {T_OPS} o ON i.incident_id = o.incident_id
JOIN {T_POLICY} p ON p.priority = o.priority
LEFT JOIN (SELECT DISTINCT incident_id, true AS was_alerted FROM {T_ALERTS} WHERE kind = 'NEW') w
  ON w.incident_id = i.incident_id
LEFT ANTI JOIN {T_EVENTS} e ON e.incident_id = i.incident_id AND e.event_type = 'RESOLVED'
WHERE i.status = 'RESOLVED'""").collect()
add_events([(r["incident_id"], r["closed_at"], "RESOLVED", "system",
             f"closed by the pipeline after {int((r['closed_at'] - r['opened_at']).total_seconds() // 60)} minutes"
             + ("" if r["acknowledged_at"] else ", never acknowledged")) for r in res])
send_digest("RESOLVED",
            [dict(incident_id=r["incident_id"], priority=r["priority"], line="RESOLVED " + fmt(r),
                  recipient=r["owner"], slack=bool(r["notify_slack"])) for r in res if r["was_alerted"]],
            "Incident resolved")
print(f"resolved recorded: {len(res)}")

# COMMAND ----------

display(spark.sql(f"""
SELECT o.priority, i.dataset, i.check_name, i.pillar, i.severity, o.team, o.owner, i.opened_at, o.due_at,
       o.acknowledged_at, o.escalation_level, i.description
FROM {T_INCIDENTS} i JOIN {T_OPS} o ON i.incident_id = o.incident_id
WHERE i.status = 'OPEN' ORDER BY o.priority, i.opened_at"""))
