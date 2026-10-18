# Databricks notebook source
# Checks the Gold galaxy schema that `dbt build` created: 5 facts sharing dim_customer and dim_date,
# dim_plan reached through dim_customer.plan_key. Run it after a successful `dbt build`.
# It is a plain notebook (no %run), so it can sit anywhere in the workspace.

# COMMAND ----------

CATALOG = "data_sentinel"
G, S = f"{CATALOG}.gold", f"{CATALOG}.silver"
checks = []


def check(name, ok, detail=""):
    checks.append((name, bool(ok), detail))
    print(("PASS  " if ok else "FAIL  ") + name + (f"   [{detail}]" if detail else ""))


def n(sql):
    return spark.sql(sql).collect()[0][0]


def cnt(t):
    return n(f"SELECT count(*) FROM {t}")


# COMMAND ----------

# 1. Every table exists and has rows
DIMS = ["dim_date", "dim_plan", "dim_customer"]
FACTS = ["fact_usage", "fact_invoice", "fact_payment", "fact_app_events", "fact_partner_usage"]
MARTS = ["mart_dq_health", "mart_dq_trend", "mart_quarantine", "mart_incidents", "mart_revenue_assurance",
         "mart_revenue_assurance_trend", "mart_recon_mismatches", "mart_customer_360", "mart_network_daily",
         "mart_cell_quality", "mart_app_usage", "mart_partner_usage"]
rows = {}
for t in DIMS + FACTS + MARTS:
    try:
        rows[t] = cnt(f"{G}.{t}")
    except Exception as e:
        rows[t] = None
        check(f"{t} exists", False, str(e)[:100])
for t in DIMS + FACTS:
    if rows.get(t) is not None:
        check(f"{t} has rows", rows[t] > 0, f"{rows[t]:,}")
empty = [t for t in MARTS if rows.get(t) == 0]
missing = [t for t in MARTS if rows.get(t) is None]
check("all 12 marts exist", not missing, ", ".join(missing))
print("marts with 0 rows (can be normal for trend/mismatch marts):", empty or "none")

# COMMAND ----------

# 2. No row lost between Silver and the facts
expect = {
    "fact_usage": f"SELECT count(*) FROM {S}.network_cdr",
    "fact_invoice": f"SELECT (SELECT count(*) FROM {S}.invoice) + (SELECT count(*) FROM {S}.invoice_file)",
    "fact_payment": f"SELECT (SELECT count(*) FROM {S}.payment) + (SELECT count(*) FROM {S}.payment_file)",
    "fact_app_events": f"SELECT count(*) FROM {S}.app_event",
    "fact_partner_usage": f"SELECT count(*) FROM {S}.partner_usage",
}
for f, sql in expect.items():
    if rows.get(f) is not None:
        e = n(sql)
        check(f"{f}: same rows as Silver (no row lost, none doubled)", rows[f] == e, f"gold {rows[f]:,} vs silver {e:,}")
if rows.get("dim_customer") is not None:
    e = cnt(f"{S}.customer_history") + 1
    check("dim_customer = customer versions + unknown member", rows["dim_customer"] == e, f"{rows['dim_customer']:,} vs {e:,}")
if rows.get("dim_plan") is not None:
    e = cnt(f"{S}.plan_history") + 1
    check("dim_plan = plan versions + unknown member", rows["dim_plan"] == e, f"{rows['dim_plan']:,} vs {e:,}")

# COMMAND ----------

# 3. Keys: unique surrogate keys, unknown member exists
check("dim_customer.customer_key is unique", n(f"SELECT count(*) - count(DISTINCT customer_key) FROM {G}.dim_customer") == 0)
check("dim_plan.plan_key is unique", n(f"SELECT count(*) - count(DISTINCT plan_key) FROM {G}.dim_plan") == 0)
check("dim_date.date_key is unique", n(f"SELECT count(*) - count(DISTINCT date_key) FROM {G}.dim_date") == 0)
check("unknown customer member (-1) exists", n(f"SELECT count(*) FROM {G}.dim_customer WHERE customer_key = -1") == 1)
check("unknown plan member (-1) exists", n(f"SELECT count(*) FROM {G}.dim_plan WHERE plan_key = -1") == 1)

# COMMAND ----------

# 4. The galaxy: every fact points at the SAME dim_customer and dim_date, and no key is orphaned
DATE_KEYS = {"fact_usage": "usage_date_key", "fact_invoice": "period_start_key", "fact_payment": "paid_date_key",
             "fact_app_events": "event_date_key", "fact_partner_usage": "period_start_key"}
for f in FACTS:
    if rows.get(f) is None:
        continue
    orphan_c = n(f"SELECT count(*) FROM {G}.{f} x LEFT ANTI JOIN {G}.dim_customer d ON x.customer_key = d.customer_key")
    check(f"{f}: every customer_key exists in dim_customer", orphan_c == 0, f"{orphan_c} orphans")
    dk = DATE_KEYS[f]
    orphan_d = n(f"SELECT count(*) FROM {G}.{f} x LEFT ANTI JOIN {G}.dim_date d ON x.{dk} = d.date_key WHERE x.{dk} IS NOT NULL")
    check(f"{f}: every {dk} exists in dim_date", orphan_d == 0, f"{orphan_d} orphans (widen date_start/date_end in dbt_project.yml)")
check("dim_customer.plan_key points at dim_plan (the plan branch of the galaxy)",
      n(f"SELECT count(*) FROM {G}.dim_customer c LEFT ANTI JOIN {G}.dim_plan p ON c.plan_key = p.plan_key") == 0)

# COMMAND ----------

# 5. Information: how many fact rows have no known customer (key -1)? Explained by recon R8, R9 and rejected customers.
print("\nfact rows with unknown customer (customer_key = -1):")
out = []
for f in FACTS:
    if rows.get(f):
        u = n(f"SELECT count(*) FROM {G}.{f} WHERE customer_key = -1")
        out.append((f, rows[f], u, round(100.0 * u / rows[f], 2)))
display(spark.createDataFrame(out, "fact string, rows long, unknown_customer long, pct double"))

fails = [c for c in checks if not c[1]]
print(f"\n{len(checks)} checks, {len(fails)} failed")
print("GOLD OK" if not fails else "GOLD HAS FAILURES")
