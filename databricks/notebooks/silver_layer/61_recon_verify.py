# Databricks notebook source
# MAGIC %run ./49_silver_lib

# COMMAND ----------

# VERIFY the reconciliation against the defects injected in Postgres and in the monthly files. Nothing here changes data.
# Each expected count comes from defect_manifest.csv. Outside the range = WARN, zero found = FAIL.
T_RECON = f"{CATALOG}.monitoring.recon_results"
T_RECON_ROWS = f"{CATALOG}.monitoring.recon_mismatches"

checks = []
def check(area, name, ok, detail="", warn=False):
    checks.append((area, name, "PASS" if ok else ("WARN" if warn else "FAIL"), str(detail)))

last = spark.table(T_RECON).orderBy(F.col("run_ts").desc()).first()["run_id"]
res = {r["check_id"]: r for r in spark.table(T_RECON).filter(F.col("run_id") == last).collect()}
check("run", f"10 recon checks in the latest run ({last})", len(res) == 10, len(res))

# check id: (minimum, low, high, injected defect)
expected = {
    "R1": (1, 15, 40, "D09: 15 CRM customers without a billing account"),
    "R2": (1, 10, 25, "D10: 10 billing accounts without a CRM customer"),
    "R3": (1, 25, 35, "D11: 30 invoice totals that differ from their lines"),
    "R4": (1, 20, 30, "D12: 25 payment amounts that differ from the invoice"),
    "R5": (1, 18, 30, "D13: 20 plan fees that differ from the CRM price"),
    "R6": (1, 15, 25, "B01: 20 invoice totals that differ between the file and Postgres"),
    "R7": (1, 10, 20, "B03: 15 payments for an unknown invoice"),
    "R8": (1, 1, 10_000, "N09: network usage from phone numbers not in the CRM"),
    "R9": (1, 6, 14, "P02: 8 partner records for phone numbers not in the CRM"),
}
for cid, (mn, lo, hi, what) in expected.items():
    r = res.get(cid)
    n = r["rows_mismatched"] if r else 0
    check("defects", f"{cid} found something ({what})", n >= mn, f"{n} found")
    check("defects", f"{cid} count in the expected range {lo}-{hi}", lo <= n <= hi, f"{n} found", warn=True)

# COMMAND ----------
# every mismatch row belongs to a check, and the EUR at risk adds up
rows = spark.table(T_RECON_ROWS).filter(F.col("run_id") == last)
by = {r["check_id"]: r["n"] for r in rows.groupBy("check_id").agg(F.count("*").alias("n")).collect()}
check("detail", "mismatch rows match the counts in recon_results",
      all(by.get(k, 0) == v["rows_mismatched"] for k, v in res.items()), {k: by.get(k, 0) for k in res})
eur_rows = rows.filter("check_id <> 'R10'").agg(F.sum("amount_at_risk_eur")).first()[0] or 0.0
eur_res = sum(v["amount_at_risk_eur"] for k, v in res.items() if k != "R10")
check("detail", "EUR at risk in the detail rows equals the summary", abs(eur_rows - eur_res) < 0.05, f"{eur_rows:,.2f} vs {eur_res:,.2f}")
check("detail", "no phone number, email or name in the mismatch rows (keys are ids or hashes)",
      rows.filter("entity_key LIKE '+31%' OR entity_key LIKE '%@%'").count() == 0)

# COMMAND ----------
# incidents
inc = spark.table(T_INCIDENTS).filter("dataset = 'recon' AND status = 'OPEN'")
failing_ids = [k for k, v in res.items() if v["status"] == "FAIL"]
open_checks = {r[0] for r in inc.select("check_name").collect()}
check("incidents", "every failing check has an open incident", all(f"recon_{k}" in open_checks for k in failing_ids),
      f"{len(failing_ids)} failing; open: {sorted(open_checks)}")
check("incidents", "the unpaid-invoice exposure (R10) did not open an incident", "recon_R10" not in open_checks)
check("incidents", "no duplicate open incident", inc.groupBy("incident_key").count().filter("count > 1").count() == 0)

out = spark.createDataFrame(checks, "area string, `check` string, status string, detail string")
display(out.orderBy(F.when(F.col("status") == "FAIL", 0).when(F.col("status") == "WARN", 1).otherwise(2), "area"))
n_fail = sum(1 for c in checks if c[2] == "FAIL"); n_warn = sum(1 for c in checks if c[2] == "WARN")
print(f"\n{len(checks)} checks: {len(checks) - n_fail - n_warn} PASS, {n_warn} WARN, {n_fail} FAIL")
print("RECON OK" if n_fail == 0 else "RECON HAS FAILURES - fix the FAIL rows above")
display(spark.table(T_RECON).filter(F.col("run_id") == last)
        .select("check_id", "check_name", "rows_checked", "rows_mismatched", "amount_at_risk_eur", "status")
        .orderBy(F.length("check_id"), "check_id"))
