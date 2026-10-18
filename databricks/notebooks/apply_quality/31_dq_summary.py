# Databricks notebook source
# Views for dashboard 1 (Data quality and contract health) and a quick health check.

# MAGIC %run ./11_dq_lib

# COMMAND ----------

M = f"{CATALOG}.monitoring"
contracts = load_contracts()

spark.sql(f"""
CREATE OR REPLACE VIEW {M}.v_dq_latest AS
SELECT * FROM {T_RESULTS}
QUALIFY row_number() OVER (PARTITION BY dataset, check_name, rule_id, scope ORDER BY run_ts DESC) = 1""")

spark.sql(f"""
CREATE OR REPLACE VIEW {M}.v_pass_rate_by_dataset AS
SELECT dataset, rows_checked AS rows, rows_failed AS not_passed_first_time, pass_rate, status, detail, run_ts
FROM {M}.v_dq_latest WHERE rule_id = '__DATASET__'""")

spark.sql(f"""
CREATE OR REPLACE VIEW {M}.v_pass_rate_after_fix AS
SELECT dataset, pass_rate AS pass_rate_after_fix, status, detail, run_ts
FROM {M}.v_dq_latest WHERE rule_id = '__AFTER_FIX__'""")

spark.sql(f"""
CREATE OR REPLACE VIEW {M}.v_rule_failures AS
SELECT dataset, rule_id, dimension, severity, rows_checked, rows_failed, pass_rate, status, detail
FROM {M}.v_dq_latest WHERE check_name = 'gate_rule' AND rows_failed > 0""")

spark.sql(f"""
CREATE OR REPLACE VIEW {M}.v_pillar_scores AS
SELECT dimension AS pillar, count(*) AS checks, sum(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed,
       round(avg(CASE WHEN status = 'PASS' THEN 1.0 ELSE 0.0 END), 4) AS score
FROM {M}.v_dq_latest
WHERE dimension IN ('freshness', 'volume', 'distribution', 'schema', 'lineage')
GROUP BY dimension""")

spark.sql(f"""
CREATE OR REPLACE VIEW {M}.v_open_incidents AS
SELECT incident_id, opened_at, dataset, check_name, pillar, severity, scope, description, impacted,
       datediff(current_timestamp(), opened_at) AS age_days
FROM {T_INCIDENTS} WHERE status = 'OPEN'""")

parts = []
for name, c in contracts.items():
    qt = quarantine_table(c)
    if spark.catalog.tableExists(qt):
        parts.append(f"SELECT '{name}' AS dataset, _quarantine_status AS status, rule AS failed_rule, count(*) AS rows "
                     f"FROM {qt} LATERAL VIEW OUTER explode(_failed_rules) r AS rule GROUP BY 1, 2, 3")
if parts:
    spark.sql(f"CREATE OR REPLACE VIEW {M}.v_quarantine_summary AS " + " UNION ALL ".join(parts))
print("views created")

# COMMAND ----------

print("Pass rate per dataset (first pass, then after auto-fix)")
display(spark.sql(f"""
SELECT p.dataset, p.rows, p.pass_rate, a.pass_rate_after_fix, p.status
FROM {M}.v_pass_rate_by_dataset p LEFT JOIN {M}.v_pass_rate_after_fix a USING (dataset) ORDER BY p.dataset"""))

print("Pillar scores")
display(spark.table(f"{M}.v_pillar_scores"))

print("Open incidents")
display(spark.table(f"{M}.v_open_incidents").orderBy("severity", "opened_at"))

if parts:
    print("Quarantine by status")
    display(spark.sql(f"SELECT dataset, status, sum(rows) AS rows FROM {M}.v_quarantine_summary GROUP BY 1, 2 ORDER BY 1, 2"))
