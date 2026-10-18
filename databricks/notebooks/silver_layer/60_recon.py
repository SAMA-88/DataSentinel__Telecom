# Databricks notebook source
# MAGIC %run ./49_silver_lib

# COMMAND ----------

# CROSS-SYSTEM RECONCILIATION on the Silver tables (history, clean tables and customer_xref; no facts needed) (CRM vs billing vs billing file vs partner vs network).
# One row per check in monitoring.recon_results, one row per mismatch in monitoring.recon_mismatches (with the EUR at risk).
# A check that finds mismatches opens an incident (dataset 'recon'); the incident closes by itself when a later run is clean.
# Run after 50, 51 and 52 (Silver). Its output feeds Gold: dbt reads recon_results / recon_mismatches for the dashboards.   Checks:
#   R1 CRM customer without billing account          R6 invoice total: billing file vs billing database
#   R2 billing account without CRM customer          R7 payment (file) for an unknown invoice
#   R3 invoice total vs the sum of its lines         R8 network usage from a phone number not in CRM
#   R4 payment amount vs invoice total               R9 partner usage for a phone number not in CRM
#   R5 billed plan fee vs CRM plan price             R10 unpaid invoices (business exposure, no incident)

T_RECON = f"{CATALOG}.monitoring.recon_results"
T_RECON_ROWS = f"{CATALOG}.monitoring.recon_mismatches"
TOL = 0.01
OPS = "_op IN ('r', 'c', 'u')"
RUN_ID = new_run_id()
RUN_TS = F.current_timestamp()

spark.sql(f"""CREATE TABLE IF NOT EXISTS {T_RECON} (run_id STRING, run_ts TIMESTAMP, check_id STRING, check_name STRING,
  source_a STRING, source_b STRING, rows_checked BIGINT, rows_mismatched BIGINT, amount_at_risk_eur DOUBLE,
  status STRING, severity STRING, detail STRING)""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {T_RECON_ROWS} (run_id STRING, run_ts TIMESTAMP, check_id STRING,
  entity_type STRING, entity_key STRING, value_a DOUBLE, value_b DOUBLE, diff DOUBLE, amount_at_risk_eur DOUBLE,
  reason STRING)""")

results, failing, checked_pairs = [], [], set()


def std(df, key, a=None, b=None, risk=None, reason=""):
    """Standard shape of a mismatch row."""
    va = F.col(a).cast("double") if a else F.lit(None).cast("double")
    vb = F.col(b).cast("double") if b else F.lit(None).cast("double")
    diff = (va - vb) if (a and b) else F.lit(None).cast("double")
    r = F.lit(0.0) if risk is None else (risk if not isinstance(risk, str) else F.col(risk)).cast("double")
    return df.select(F.col(key).cast("string").alias("entity_key"), va.alias("value_a"), vb.alias("value_b"),
                     diff.alias("diff"), F.abs(F.coalesce(r, F.lit(0.0))).alias("amount_at_risk_eur"),
                     F.lit(reason).alias("reason"))


def register(cid, name, src_a, src_b, severity, entity_type, n_checked, mism, impacted, kind="consistency"):
    agg = mism.agg(F.count("*").alias("n"), F.coalesce(F.sum("amount_at_risk_eur"), F.lit(0.0)).alias("eur")).first()
    n, eur = int(agg["n"]), float(agg["eur"])
    (mism.select(F.lit(RUN_ID).alias("run_id"), RUN_TS.alias("run_ts"), F.lit(cid).alias("check_id"),
                 F.lit(entity_type).alias("entity_type"), "entity_key", "value_a", "value_b", "diff",
                 "amount_at_risk_eur", "reason")
         .write.mode("append").saveAsTable(T_RECON_ROWS))
    status = "INFO" if kind == "business" else ("PASS" if n == 0 else "FAIL")
    detail = f"{n:,} of {n_checked:,} compared; EUR {eur:,.2f} at risk"
    results.append((RUN_ID, cid, name, src_a, src_b, int(n_checked), n, eur, status,
                    "info" if status in ("PASS", "INFO") else severity, detail))
    print(f"{cid} {name}: {detail} [{status}]")
    if kind == "consistency":
        checked_pairs.add(("recon", f"recon_{cid}"))
        if n > 0:
            failing.append(dict(dataset="recon", check=f"recon_{cid}", scope="all", pillar="consistency",
                                severity=severity, description=f"{name}: {n:,} mismatches, EUR {eur:,.2f} at risk",
                                impacted=impacted))


# ---------- shared inputs (Silver tables only) ----------
T_QX = T_XREF
inv_db = spark.table(T_INVOICE)
inv_lines = spark.table(T_INVOICE_LINE)
lagg = inv_lines.groupBy("invoice_id").agg(F.sum("amount").alias("lines_total"), F.count("*").alias("lines_count"))
cust_cur = (spark.table(T_CUSTOMER_HISTORY).filter("is_current")
            .select("customer_id", F.col("status").alias("crm_status"), "plan_id"))
plan_cur = (spark.table(T_PLAN_HISTORY).filter("is_current")
            .select("plan_id", F.col("monthly_fee").alias("crm_fee")))
known_hash = spark.table(T_CUSTOMER_HISTORY).select("msisdn_hash").distinct()
ql_t = quarantine_table(CONTRACTS["billing_invoice_lines"])
if spark.catalog.tableExists(ql_t):
    q_lines = (spark.table(ql_t).withColumn("invoice_id", F.expr("try_cast(invoice_id AS bigint)"))
                    .filter("invoice_id IS NOT NULL").groupBy("invoice_id").agg(F.count("*").alias("_q_lines")))
else:
    q_lines = spark.createDataFrame([], "invoice_id bigint, _q_lines long")

# COMMAND ----------
# R1: CRM customer without billing account. EUR at risk = one monthly plan fee of an ACTIVE customer.
cust_n = spark.table(T_QX).filter("customer_id IS NOT NULL").count()
co = (spark.table(T_QX).filter("match_status = 'CRM_ONLY'").select("customer_id")
      .join(cust_cur, "customer_id").join(plan_cur, "plan_id", "left"))
m1 = std(co, "customer_id", risk=F.when(F.col("crm_status") == "ACTIVE", F.coalesce(F.col("crm_fee"), F.lit(0.0))).otherwise(0.0),
         reason="customer has no billing account")
register("R1", "CRM customer without billing account", "crm.customers", "billing.accounts", "medium", "customer",
         cust_n, m1, "silver.customer_xref, gold.dim_customer")

# R2: billing account without CRM customer. EUR at risk = everything billed to that account.
acc_n = spark.table(T_QX).filter("account_id IS NOT NULL").count()
risk2 = inv_db.groupBy("account_id").agg(F.sum("total_amount").alias("_billed"))
m2 = std(spark.table(T_QX).filter("match_status = 'BILLING_ONLY'").select("account_id")
         .join(risk2, "account_id", "left"), "account_id", risk="_billed", reason="account has no CRM customer")
register("R2", "Billing account without CRM customer", "billing.accounts", "crm.customers", "medium", "account",
         acc_n, m2, "silver.customer_xref, gold.fact_invoice")

# COMMAND ----------
# R3: invoice total vs sum of its lines. Invoices that have a line in quarantine are not compared (the lines are incomplete).
j3 = inv_db.join(lagg, "invoice_id", "left").join(q_lines, "invoice_id", "left").filter(F.col("_q_lines").isNull())
j3 = j3.withColumn("_lines", F.coalesce(F.col("lines_total").cast("double"), F.lit(0.0)))
bad3 = j3.filter(F.abs(F.col("total_amount").cast("double") - F.col("_lines")) > TOL)
m3 = std(bad3, "invoice_id", a="total_amount", b="_lines", risk=F.col("total_amount").cast("double") - F.col("_lines"),
         reason="invoice total differs from the sum of its lines")
register("R3", "Invoice total vs sum of lines", "billing.invoices", "billing.invoice_lines", "high", "invoice",
         j3.count(), m3, "gold.fact_invoice")

# R4: payment vs invoice total. Only invoices whose total matches their lines are used, so one error is not reported twice.
ok_inv = j3.filter(F.abs(F.col("total_amount").cast("double") - F.col("_lines")) <= TOL).select("invoice_id", "total_amount")
j4 = spark.table(T_PAYMENT).filter("status = 'PAID'").join(ok_inv, "invoice_id")
bad4 = j4.filter(F.abs(F.col("amount").cast("double") - F.col("total_amount").cast("double")) > TOL)
m4 = std(bad4, "payment_id", a="amount", b="total_amount",
         risk=F.col("amount").cast("double") - F.col("total_amount").cast("double"),
         reason="payment amount differs from the invoice total")
register("R4", "Payment vs invoice total", "billing.payments", "billing.invoices", "high", "payment",
         j4.count(), m4, "gold.fact_payment, gold.fact_invoice")

# COMMAND ----------
# R5: billed plan fee vs the price of the customer's current plan in the CRM
# (plan fee line -> invoice -> account -> customer through the xref -> current plan)
pl = (inv_lines.filter("line_type = 'PLAN_FEE'").select("line_id", "invoice_id", F.col("amount").alias("billed_fee")))
j5 = (pl.join(inv_db.select("invoice_id", "account_id"), "invoice_id")
        .join(spark.table(T_QX).filter("match_status = 'MATCHED'").select("account_id", "customer_id"), "account_id")
        .join(cust_cur.select("customer_id", "plan_id"), "customer_id").join(plan_cur, "plan_id"))
bad5 = j5.filter(F.abs(F.col("billed_fee").cast("double") - F.col("crm_fee").cast("double")) > TOL)
m5 = std(bad5, "line_id", a="billed_fee", b="crm_fee",
         risk=F.col("billed_fee").cast("double") - F.col("crm_fee").cast("double"),
         reason="billed plan fee differs from the CRM plan price")
register("R5", "Billed plan fee vs CRM plan price", "billing.invoice_lines", "crm.plans", "high", "invoice_line",
         j5.count(), m5, "gold.fact_invoice, gold.dim_plan")

# COMMAND ----------
# R6: invoice total, monthly billing file vs billing database (same invoice id)
f_db = inv_db.select("invoice_id", F.col("total_amount").alias("db_total"))
f_fl = spark.table(T_INVOICE_FILE).select("invoice_id", F.col("total_amount").alias("file_total"))
j6 = f_fl.join(f_db, "invoice_id")
bad6 = j6.filter(F.abs(F.col("file_total").cast("double") - F.col("db_total").cast("double")) > TOL)
m6 = std(bad6, "invoice_id", a="file_total", b="db_total",
         risk=F.col("file_total").cast("double") - F.col("db_total").cast("double"),
         reason="invoice total differs between the billing file and the billing database")
register("R6", "Invoice total: billing file vs database", "billing file", "billing.invoices", "high", "invoice",
         j6.count(), m6, "gold.fact_invoice")

# R7: payment from the billing file whose invoice exists nowhere
pf = spark.table(T_PAYMENT_FILE)
all_inv_ids = inv_db.select("invoice_id").union(spark.table(T_INVOICE_FILE).select("invoice_id")).distinct()
m7 = std(pf.join(all_inv_ids, "invoice_id", "left_anti"), "payment_id", a="amount",
         risk="amount", reason="payment refers to an invoice that does not exist")
register("R7", "Payment for an unknown invoice", "billing file (payments)", "billing.invoices + billing file", "medium",
         "payment", pf.count(), m7, "gold.fact_payment")

# COMMAND ----------
# R8: network usage from a phone number that is not a CRM customer (grouped by hashed phone number)
cdr = spark.table(T_NETWORK_CDR)
u8 = (cdr.join(known_hash, "msisdn_hash", "left_anti").groupBy("msisdn_hash")
         .agg(F.count("*").cast("double").alias("_events")))
m8 = std(u8, "msisdn_hash", a="_events", reason="usage from a phone number that is not in the CRM")
register("R8", "Network usage from a phone number not in CRM", "network cdr", "crm.customers", "medium", "msisdn_hash",
         cdr.select("msisdn_hash").distinct().count(), m8, "gold.fact_usage")

# R9: partner usage for a phone number that is not a CRM customer. EUR at risk = the partner amount.
pu = spark.table(T_PARTNER_USAGE)
m9 = std(pu.join(known_hash, "msisdn_hash", "left_anti"), "partner_record_id", a="amount", risk="amount",
         reason="partner usage for a phone number that is not in the CRM")
register("R9", "Partner usage for a phone number not in CRM", "partner file", "crm.customers", "medium", "partner_record",
         pu.count(), m9, "gold.fact_partner_usage")

# COMMAND ----------
# R10: unpaid invoices (business exposure for revenue assurance, not a data defect, so no incident)
paid = spark.table(T_PAYMENT).filter("status = 'PAID'").groupBy("invoice_id").agg(F.sum("amount").alias("_paid"))
j10 = (inv_db.filter("status <> 'CANCELLED'").join(paid, "invoice_id", "left")
       .withColumn("_unpaid", F.col("total_amount").cast("double") - F.coalesce(F.col("_paid").cast("double"), F.lit(0.0))))
m10 = std(j10.filter(F.col("_unpaid") > TOL), "invoice_id", a="total_amount", b="_paid", risk="_unpaid",
          reason="invoice not (fully) paid")
register("R10", "Unpaid invoices", "billing.invoices", "billing.payments", "low", "invoice", j10.count(), m10,
         "gold.fact_invoice", kind="business")

# COMMAND ----------
spark.createDataFrame(
    [(r[0], None, r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10]) for r in results],
    "run_id string, run_ts timestamp, check_id string, check_name string, source_a string, source_b string, "
    "rows_checked bigint, rows_mismatched bigint, amount_at_risk_eur double, status string, severity string, detail string"
).withColumn("run_ts", F.current_timestamp()).write.mode("append").saveAsTable(T_RECON)

opened, resolved = sync_incidents(RUN_ID, failing, checked_pairs)
print(f"\nrun {RUN_ID}: incidents opened {opened}, resolved {resolved}")
display(spark.table(T_RECON).filter(F.col("run_id") == RUN_ID)
        .select("check_id", "check_name", "rows_checked", "rows_mismatched", "amount_at_risk_eur", "status", "severity")
        .orderBy(F.length("check_id"), "check_id"))
