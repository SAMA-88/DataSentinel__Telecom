# Databricks notebook source
# REVALIDATE: run the same rules again on the repaired rows.
#   rows that now pass -> inserted into silver.<dataset>_validated (_dq_fixed = true), status REVALIDATED_OK
#   rows that still fail -> status REJECTED, with the new failed rules
# It is safe to run repeatedly: rows already revalidated are re-inserted if the gate rebuilt validated.
# Uniqueness rules are skipped here (they were tested on the original values by the gate).

# MAGIC %run ./11_dq_lib

# COMMAND ----------

contracts = load_contracts()
run_id = new_run_id()
results, summary = [], []

for name, c in contracts.items():
    qt, vt = quarantine_table(c), validated_table(c)
    if not (spark.catalog.tableExists(qt) and spark.catalog.tableExists(vt)):
        continue
    cand = spark.table(qt).filter("_quarantine_status IN ('FIXED_PENDING_REVALIDATION', 'REVALIDATED_OK')")
    n = cand.count()
    if n == 0:
        continue

    cols = [x["name"] for x in c["columns"]]
    raw = cand.select(*[F.col("_fixed").getItem(col).alias(col) for col in cols], *[F.col(m) for m in META_COLS])
    df, rules = apply_rules(add_typed(raw, c), c, blocked=blocked_files(c), include_unique=False)
    ok = df.filter(F.col("_dq_status") == "PASS")
    bad = df.filter(F.col("_dq_status") != "PASS")

    # 1) fixed rows that pass go to validated
    (DeltaTable.forName(spark, vt).alias("t")
        .merge(build_validated(ok, c, fixed=True).alias("s"), "t._row_id = s._row_id")
        .whenNotMatchedInsertAll().execute())

    # 2) update the quarantine status
    q = DeltaTable.forName(spark, qt).alias("t")
    q.merge(ok.select("_row_id").alias("s"), "t._row_id = s._row_id") \
     .whenMatchedUpdate(set={"_quarantine_status": "'REVALIDATED_OK'"}).execute()
    q = DeltaTable.forName(spark, qt).alias("t")
    q.merge(bad.select("_row_id", "_failed_rules").alias("s"), "t._row_id = s._row_id") \
     .whenMatchedUpdate(set={"_quarantine_status": "'REJECTED'", "_failed_rules": "s._failed_rules",
                             "_failure_reasons": "concat('autofix_failed:', concat_ws(',', s._failed_rules))"}).execute()

    n_ok = spark.table(qt).filter("_quarantine_status = 'REVALIDATED_OK'").count()
    n_rej = spark.table(qt).filter("_quarantine_status = 'REJECTED'").count()
    n_pend = spark.table(qt).filter("_quarantine_status IN ('PENDING_FIX', 'FIXED_PENDING_REVALIDATION')").count()
    n_val = spark.table(vt).count()
    effective = n_val / (n_val + n_rej + n_pend) if (n_val + n_rej + n_pend) else 1.0
    results.append(result_row(run_id, name, "revalidate", "__REVALIDATE__", "validity", "all", n, n - n_ok,
                              "PASS" if n_ok == n else "WARN", "info" if n_ok == n else "medium",
                              f"{n_ok:,} of {n:,} repaired rows now pass", c["version"]))
    results.append(result_row(run_id, name, "after_fix_pass_rate", "__AFTER_FIX__", "validity", "all",
                              n_val + n_rej + n_pend, n_rej + n_pend, "PASS" if effective >= c.get("slo", {}).get("min_pass_rate", 0.95) else "FAIL",
                              "info", f"validated {n_val:,}; rejected {n_rej:,}; pending {n_pend:,}", c["version"]))
    summary.append((name, n, n_ok, n - n_ok, round(effective, 4)))
    print(f"{name}: {n_ok:,} of {n:,} repaired rows pass revalidation")

save_results(results)
if summary:
    display(spark.createDataFrame(summary, "dataset string, repaired long, now_pass long, still_failing long, "
                                           "pass_rate_after_fix double").orderBy("dataset"))
