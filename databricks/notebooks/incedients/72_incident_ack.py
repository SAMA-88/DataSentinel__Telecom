# Databricks notebook source
# MAGIC %run ./11_dq_lib

# COMMAND ----------

# How a person works an incident. Acknowledging stops the escalation and starts the time-to-acknowledge clock.
# Use the functions below from a notebook cell, or call them from a small app later.
# Resolving is NOT manual: the incident closes by itself when the next run no longer finds the problem
# (the fix is proven by the data, not by a click).

# COMMAND ----------

M = f"{CATALOG}.monitoring"
T_OPS, T_EVENTS = f"{M}.incident_ops", f"{M}.incident_events"
EV_SCHEMA = "incident_id string, event_ts timestamp, event_type string, actor string, detail string"


def _exists(incident_id):
    return spark.table(f"{M}.incident_ops").filter(F.col("incident_id") == incident_id).count() == 1


def ack_incident(incident_id, who, note=""):
    if not _exists(incident_id):
        raise ValueError(f"unknown incident {incident_id} (run 71_incident_manage first)")
    who_q, note_q = who.replace("'", "''"), note.replace("'", "''")
    spark.sql(f"""UPDATE {T_OPS} SET acknowledged_at = current_timestamp(), acknowledged_by = '{who_q}'
                  WHERE incident_id = '{incident_id}' AND acknowledged_at IS NULL""")
    spark.createDataFrame([(incident_id, datetime.utcnow(), "ACKED", who, note or "acknowledged")], EV_SCHEMA) \
        .write.mode("append").saveAsTable(T_EVENTS)
    print(f"{incident_id} acknowledged by {who}")


def comment_incident(incident_id, who, text):
    if not _exists(incident_id):
        raise ValueError(f"unknown incident {incident_id}")
    spark.createDataFrame([(incident_id, datetime.utcnow(), "COMMENT", who, text)], EV_SCHEMA) \
        .write.mode("append").saveAsTable(T_EVENTS)


# COMMAND ----------

# Demo: acknowledge the oldest open P2 incident, and comment on it. Set the widget to "yes" to run it.
dbutils.widgets.dropdown("demo", "no", ["no", "yes"], "Run the acknowledge demo")
if dbutils.widgets.get("demo") == "yes":
    row = spark.sql(f"""SELECT i.incident_id FROM {T_INCIDENTS} i JOIN {T_OPS} o USING (incident_id)
                        WHERE i.status = 'OPEN' AND o.priority = 'P2' AND o.acknowledged_at IS NULL
                        ORDER BY i.opened_at LIMIT 1""").collect()
    if row:
        ack_incident(row[0]["incident_id"], "demo.user", "looking at it")
        comment_incident(row[0]["incident_id"], "demo.user", "asked the source owner for a corrected file")
    else:
        print("no unacknowledged open P2 incident")
