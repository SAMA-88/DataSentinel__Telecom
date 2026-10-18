# Databricks notebook source
# INCIDENT ALERTS (incident management): tell a person about every new OPEN incident, once.
# Sends one digest to a webhook (Slack or Teams) when a secret is configured; otherwise it only logs.
# Every incident is recorded in monitoring.alert_log, so nothing is announced twice and failed sends are retried.
# Free Edition may block outbound webhooks. In that case the status stays LOGGED and the alerts are readable in the table.

# COMMAND ----------

# MAGIC %run ./11_dq_lib

# COMMAND ----------

import requests

dbutils.widgets.text("secret_scope", "data_sentinel")
dbutils.widgets.text("secret_key", "alert_webhook")
T_ALERTS = f"{CATALOG}.monitoring.alert_log"
spark.sql(f"""CREATE TABLE IF NOT EXISTS {T_ALERTS}
              (incident_id STRING, notified_at TIMESTAMP, channel STRING, status STRING, message STRING)""")

done = spark.table(T_ALERTS).filter("status IN ('SENT', 'LOGGED')").select("incident_id").distinct()
new = (spark.table(T_INCIDENTS).filter("status = 'OPEN'").join(done, "incident_id", "left_anti")
       .withColumn("rank", F.when(F.col("severity") == "critical", 0).when(F.col("severity") == "high", 1)
                            .when(F.col("severity") == "medium", 2).otherwise(3))
       .orderBy("rank", "opened_at").collect())

if not new:
    print("no new incidents")
else:
    lines = [f"[{r['severity'].upper()}] {r['dataset']} / {r['check_name']} ({r['scope']}): {r['description']}"
             + (f"  Impacted: {r['impacted']}" if r["impacted"] else "") for r in new]
    digest = f"{len(new)} new data quality incident(s)\n" + "\n".join(lines)

    webhook = None
    try:
        webhook = dbutils.secrets.get(dbutils.widgets.get("secret_scope"), dbutils.widgets.get("secret_key"))
    except Exception:
        pass

    channel, status = "log", "LOGGED"
    if webhook:
        try:
            requests.post(webhook, json={"text": digest}, timeout=10).raise_for_status()
            channel, status = "webhook", "SENT"
        except Exception as e:
            channel, status = "webhook", "FAILED"          # retried on the next run
            print("webhook failed:", e)

    spark.createDataFrame([(r["incident_id"], datetime.utcnow(), channel, status, line) for r, line in zip(new, lines)],
                          "incident_id string, notified_at timestamp, channel string, status string, message string") \
         .write.mode("append").saveAsTable(T_ALERTS)
    print(digest)
    print(f"\nstatus: {status} via {channel}")
