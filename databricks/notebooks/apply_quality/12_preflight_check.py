# Databricks notebook source
# PREFLIGHT: run once before the first gate run, and again whenever a Bronze loader changes.
# Checks that every contract's Bronze source has the technical columns the quality code reads,
# that the monitoring tables exist, and that Kafka topics actually contain events.
# It fails the notebook (and the job) if anything is MISSING, so problems show up here and not as a Spark error inside the gate.

# COMMAND ----------

# MAGIC %run ./11_dq_lib

# COMMAND ----------

NEED = {
    "csv": ["_source_file", "_row_hash"],
    "json": ["value", "topic", "partition", "offset", "kafka_timestamp_ms"],
    "cdc": ["value", "topic", "partition", "offset", "kafka_timestamp_ms"],
}
AUDIT_NEED = ["source", "file_name", "file_date", "row_count", "columns"]
rows = []                                   # (check, object, status, detail)

# 1) monitoring tables and file audit
for t in (T_RESULTS, T_INCIDENTS, T_RULES, T_LINEAGE):
    ok = spark.catalog.tableExists(t)
    rows.append(("monitoring table", t, "OK" if ok else "MISSING", "" if ok else "run 10_quality_setup"))

if spark.catalog.tableExists(T_AUDIT):
    miss = [c for c in AUDIT_NEED if c not in spark.table(T_AUDIT).columns]
    rows.append(("file audit", T_AUDIT, "OK" if not miss else "MISSING", "" if not miss else f"missing columns: {miss}"))
else:
    rows.append(("file audit", T_AUDIT, "MISSING", "the CSV Bronze loader must write one row per file"))

# 2) one check per contract
contracts = load_contracts()
for name, c in contracts.items():
    s, kind = c["source"], c["source"]["kind"]
    if not spark.catalog.tableExists(s["table"]):
        rows.append((name, s["table"], "MISSING", "Bronze table does not exist"))
        continue
    df = spark.table(s["table"])
    cols = set(df.columns)
    miss = [x for x in NEED[kind] if x not in cols]
    if miss:
        rows.append((name, s["table"], "MISSING", f"technical columns not found: {miss}"))
        continue
    detail = "technical columns present"
    status = "OK"
    if kind == "csv":
        absent = [x["name"] for x in c["columns"] if x["name"] not in cols]
        if absent:
            status, detail = "WARN", f"contract columns not in table (circuit breaker will block those files): {absent}"
    else:
        q = df.filter(F.col("topic") == s["topic"]) if kind == "cdc" else df
        if q.limit(1).count() == 0:
            status, detail = "WARN", "no events found yet" + (f" for topic {s['topic']}" if kind == "cdc" else "")
    rows.append((name, s["table"], status, detail))

result = spark.createDataFrame(rows, "check string, object string, status string, detail string")
display(result.orderBy(F.when(F.col("status") == "MISSING", 0).when(F.col("status") == "WARN", 1).otherwise(2), "check"))

n_missing = sum(1 for r in rows if r[2] == "MISSING")
if n_missing:
    raise Exception(f"PREFLIGHT FAILED: {n_missing} item(s) MISSING, see the table above")
print("preflight passed")
