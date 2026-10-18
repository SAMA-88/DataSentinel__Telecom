# Databricks notebook source
# QUARANTINE: AUTO-FIX. Repairs the rows the gate marked PENDING_FIX, using the 'fixes' section of the contract.
#   fixes defined      -> PENDING_FIX becomes FIXED_PENDING_REVALIDATION (fixed values are stored in _fixed)
#   no fixes defined   -> PENDING_FIX becomes REJECTED
# Run after 20_quality_gate and before 22_revalidate.

# MAGIC %run ./11_dq_lib

# COMMAND ----------

contracts = load_contracts()
run_id = new_run_id()
results = []

for name, c in contracts.items():
    qt = quarantine_table(c)
    if not spark.catalog.tableExists(qt):
        continue
    pending = spark.table(qt).filter("_quarantine_status = 'PENDING_FIX'")
    n = pending.count()
    if n == 0:
        print(f"{name}: nothing to fix")
        continue

    target = DeltaTable.forName(spark, qt).alias("t")
    if not c.get("fixes"):
        target.merge(pending.select("_row_id").alias("s"), "t._row_id = s._row_id AND t._quarantine_status = 'PENDING_FIX'") \
              .whenMatchedUpdate(set={"_quarantine_status": "'REJECTED'"}).execute()
        print(f"{name}: no fixes defined, {n:,} rows rejected")
        results.append(result_row(run_id, name, "autofix", "__AUTOFIX__", "validity", "all", n, n, "FAIL", "medium",
                                  "no fixes defined in the contract", c["version"]))
        continue

    cols = [x["name"] for x in c["columns"]]
    pairs = []
    for col in cols:
        pairs += [F.lit(col), fix_expression(col, c)]
    upd = pending.select("_row_id", F.create_map(*pairs).alias("_fixed"))
    (target.merge(upd.alias("s"), "t._row_id = s._row_id AND t._quarantine_status = 'PENDING_FIX'")
           .whenMatchedUpdate(set={"_fixed": "s._fixed",
                                   "_quarantine_status": "'FIXED_PENDING_REVALIDATION'",
                                   "_attempts": "t._attempts + 1"}).execute())
    print(f"{name}: fixes applied to {n:,} rows")
    results.append(result_row(run_id, name, "autofix", "__AUTOFIX__", "validity", "all", n, 0, "PASS", "info",
                              "fixes applied, waiting for revalidation", c["version"]))

save_results(results)
