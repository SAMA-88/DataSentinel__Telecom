# Databricks notebook source
# Creates the schemas, the contracts volume and the monitoring tables for the quality phase.
# Run it once, upload the contracts (tools/upload_contracts.py), then run it again to load the rule catalogue.

# MAGIC %run ./11_dq_lib

# COMMAND ----------

for s in ["silver", "quarantine", "monitoring"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.monitoring.contracts")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_RESULTS} (
  run_id STRING, run_ts TIMESTAMP, dataset STRING, check_name STRING, rule_id STRING, dimension STRING,
  scope STRING, rows_checked BIGINT, rows_failed BIGINT, pass_rate DOUBLE, status STRING, severity STRING,
  detail STRING, contract_version STRING)""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_INCIDENTS} (
  incident_id STRING, incident_key STRING, opened_at TIMESTAMP, closed_at TIMESTAMP, status STRING,
  dataset STRING, check_name STRING, pillar STRING, severity STRING, scope STRING, description STRING,
  impacted STRING, run_id STRING, resolved_run_id STRING)""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_RULES} (
  dataset STRING, rule_id STRING, rule_type STRING, column_name STRING, dimension STRING, severity STRING,
  action STRING, description STRING, contract_version STRING)""")

spark.sql(f"CREATE TABLE IF NOT EXISTS {T_LINEAGE} (source_table STRING, target_table STRING)")
print("schemas, volume and monitoring tables are ready")

# COMMAND ----------

# Load the contracts from the volume, validate them, and fill the rule catalogue (dim_dq_rule) and lineage edges.
if not os.path.isdir(CONTRACT_DIR) or not any(f.endswith(".yaml") for f in os.listdir(CONTRACT_DIR)):
    print(f"No contracts found in {CONTRACT_DIR}.")
    print("Upload them with:  python tools/upload_contracts.py   then run this notebook again.")
else:
    contracts = load_contracts()
    problems = {n: validate_contract(c) for n, c in contracts.items()}
    bad = {n: p for n, p in problems.items() if p}
    if bad:
        for n, p in bad.items():
            print(n, p)
        raise Exception("Fix the contract errors above first")

    rule_rows = []
    for n, c in contracts.items():
        for r in expand_rules(c):
            rule_rows.append((n, r["id"], r["type"], r.get("column") or ",".join(r.get("keys", [])), r["dimension"],
                              r["severity"], r["action"], r["description"], c["version"]))
        if c["source"]["kind"] == "csv":
            rule_rows.append((n, "FILE_SCHEMA_BLOCK", "file_block", "", "schema", "critical", "reject",
                              "File is missing a required column (circuit breaker)", c["version"]))
    spark.createDataFrame(rule_rows, "dataset string, rule_id string, rule_type string, column_name string, "
                                     "dimension string, severity string, action string, description string, "
                                     "contract_version string").write.mode("overwrite").saveAsTable(T_RULES)

    edges = edges_from_contracts(contracts)
    spark.createDataFrame(edges, "source_table string, target_table string").write.mode("overwrite").saveAsTable(T_LINEAGE)
    print(f"{len(contracts)} contracts valid, {len(rule_rows)} rules, {len(edges)} lineage edges")
    display(spark.table(T_RULES).groupBy("dataset").count().orderBy("dataset"))
