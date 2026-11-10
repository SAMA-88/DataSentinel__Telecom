# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./11_dq_lib

# COMMAND ----------

# QUALITY GATE: test every Bronze row against its data contract.
#   PASS    -> silver.<dataset>_validated
#   REJECT  -> quarantine.<dataset>  (status REJECTED)
#   AUTOFIX -> quarantine.<dataset>  (status PENDING_FIX, repaired by 21 and rechecked by 22)
# Flag-only failures stay in validated with the rule names in _flags.
#
# Each contract has two limits in its slo section:
#   min_pass_rate                  warning: below it a 'pass_rate_slo' incident (high) is opened, the run goes on
#   circuit_breaker_min_pass_rate  stop:    below it the circuit breaker trips (see below)
# A contract without circuit_breaker_min_pass_rate uses the widget max_reject_rate (default 0.5 = trip when
# 50% or more of the rows fail). Rows sent to autofix count as failed: the breaker judges the source data.
#
# Circuit breaker, file level: a CSV file missing a required column is rejected as a whole (FILE_SCHEMA_BLOCK).
# Circuit breaker, table level: the table keeps its old validated data, a critical 'circuit_breaker' incident
# is opened, and after all tables are checked the notebook raises, so the Airflow tasks after the gate
# (Silver, recon, Gold) do not run. The incident resolves itself when a later run is back above the limit.

# COMMAND ----------

dbutils.widgets.text("datasets", "all")
dbutils.widgets.text("max_reject_rate", "0.5")      # fallback for contracts without circuit_breaker_min_pass_rate
dbutils.widgets.text("stop_on_breaker", "true")     # false = record the incident but do not stop the pipeline
MAX_REJECT = float(dbutils.widgets.get("max_reject_rate"))
STOP_ON_BREAKER = dbutils.widgets.get("stop_on_breaker").strip().lower() == "true"
tripped_tables = []
selected = dbutils.widgets.get("datasets").strip()
wanted = None if selected in ("", "all") else {x.strip() for x in selected.split(",")}

contracts = load_contracts()
run_id = new_run_id()
summary, results, failing, checked = [], [], [], set()

for name, c in contracts.items():
    if wanted and name not in wanted:
        continue
    print(f"--- {name} ---")
    src_tbl = c["source"].get("table")
    if src_tbl and not spark.catalog.tableExists(src_tbl):
        print(f"  SKIPPED: Bronze table {src_tbl} not found (load Bronze first, or check the catalog name)")
        continue
    blocked = blocked_files(c)
    df = add_typed(load_source(c), c)
    df, rules = apply_rules(df, c, blocked)

    counts = rule_counts(df, rules)
    total, passed = counts["__total"], counts["__pass"]
    pass_rate = (passed / total) if total else 1.0
    slo = c.get("slo", {})
    min_rate = slo.get("min_pass_rate", 0.95)
    reject_rate = (total - passed) / total if total else 0.0
    breaker = slo.get("circuit_breaker_min_pass_rate")
    if breaker is not None:                              # the contract sets the limit for this dataset
        tripped = total > 0 and pass_rate < breaker
        limit_txt = f"pass rate {pass_rate:.1%} below the contract limit {breaker:.0%}"
    else:                                                # no limit in the contract: pipeline default
        tripped = total > 0 and reject_rate >= MAX_REJECT
        limit_txt = f"{reject_rate:.1%} of rows failed (default limit {MAX_REJECT:.0%})"

    # outputs
    if tripped:
        tripped_tables.append(f"{name} ({limit_txt}, {total:,} rows)")
        print(f"  CIRCUIT BREAKER: {limit_txt}, validated table not updated")
    else:
        (build_validated(df.filter(F.col("_dq_status") == "PASS"), c)
            .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .saveAsTable(validated_table(c)))
    upsert_quarantine(build_quarantine(df.filter(F.col("_dq_status") != "PASS"), c, run_id), quarantine_table(c))

    # results: one row per rule, one row for the dataset
    for r in rules:
        n_failed = counts[f"c_{r['id']}"]
        st = "PASS" if n_failed == 0 else ("WARN" if r["action"] == "flag" else "FAIL")
        results.append(result_row(run_id, name, "gate_rule", r["id"], r["dimension"], "all", total, n_failed, st,
                                  r["severity"] if n_failed else "info", f"action={r['action']}; {r['description']}",
                                  c["version"]))
    st = "PASS" if pass_rate >= min_rate else "FAIL"
    results.append(result_row(run_id, name, "gate_pass_rate", "__DATASET__", "validity", "all", total, total - passed,
                              st, "high" if st == "FAIL" else "info",
                              f"pass {passed:,} of {total:,}; reject {counts['__reject']:,}; autofix {counts['__autofix']:,}; SLO {min_rate:.0%}",
                              c["version"]))

    # incidents: pass rate below SLO, circuit breaker, and blocked files
    impacted = ", ".join(impacted_tables(validated_table(c), edges_from_contracts(contracts)))
    checked.add((name, "pass_rate_slo"))
    if st == "FAIL":
        failing.append(dict(dataset=name, check="pass_rate_slo", scope="all", pillar="validity", severity="high",
                            description=f"Pass rate {pass_rate:.1%} is below the SLO of {min_rate:.0%}",
                            impacted=impacted))
    checked.add((name, "circuit_breaker"))
    if tripped:
        failing.append(dict(dataset=name, check="circuit_breaker", scope="all", pillar="validity", severity="critical",
                            description=f"Circuit breaker: {limit_txt} ({total:,} rows); "
                                        f"validated table kept, pipeline stopped",
                            impacted=impacted))
    checked.add((name, "file_schema_block"))
    for fn, missing in blocked.items():
        failing.append(dict(dataset=name, check="file_schema_block", scope=fn, pillar="schema", severity="critical",
                            description=f"File blocked, missing required column(s): {', '.join(missing)}",
                            impacted=", ".join(impacted_tables(validated_table(c), edges_from_contracts(contracts)))))

    summary.append((name, total, passed, counts["__reject"], counts["__autofix"], round(pass_rate, 4), len(blocked)))

save_results(results)
opened, resolved = sync_incidents(run_id, failing, checked)
print(f"\nrun {run_id}: incidents opened {opened}, resolved {resolved}")
display(spark.createDataFrame(summary, "dataset string, rows long, passed long, rejected long, autofix long, "
                                       "pass_rate double, blocked_files int").orderBy("dataset"))

# COMMAND ----------

# Pipeline circuit breaker: raise after results and incidents are saved, so the run stops here.
if tripped_tables and STOP_ON_BREAKER:
    raise Exception("CIRCUIT BREAKER OPEN in: " + "; ".join(tripped_tables))
print("circuit breaker: " + ("tripped, but stop_on_breaker=false" if tripped_tables else "closed, all tables below the limit"))