# Databricks notebook source
# OBSERVABILITY MONITORS: the five pillars of data observability, driven by the 'monitors' section of each contract.
#   freshness    did the data arrive on time, and is a file missing?
#   volume       is the row count in the normal range?
#   distribution are the values in the normal range?
#   schema       did columns appear, disappear or change?
#   lineage      which tables are downstream, so an incident shows what it affects
# Results go to monitoring.dq_results. Failures open incidents; incidents resolve themselves when the check passes again.

# MAGIC %run ./11_dq_lib

# COMMAND ----------

dbutils.widgets.text("as_of", str(date.today()))
AS_OF = date.fromisoformat(dbutils.widgets.get("as_of"))

contracts = load_contracts()
edges = edges_from_contracts(contracts)
run_id = new_run_id()
rows, failing, checked = [], [], set()
audit_rows = spark.table(T_AUDIT).collect() if spark.catalog.tableExists(T_AUDIT) else []


def record(c, check, rule_id, pillar, scope, n_checked, n_failed, severity, detail):
    ok = n_failed == 0
    rows.append(result_row(run_id, c["dataset"], check, rule_id, pillar, scope, n_checked, n_failed,
                           "PASS" if ok else "FAIL", "info" if ok else severity, detail, c["version"]))
    checked.add((c["dataset"], check))
    if not ok:
        failing.append(dict(dataset=c["dataset"], check=check, scope=scope, pillar=pillar, severity=severity,
                            description=detail,
                            impacted=", ".join(impacted_tables(validated_table(c), edges))))


def month_range(a, b):
    out, y, m = [], a.year, a.month
    while (y, m) <= (b.year, b.month):
        out.append(date(y, m, 1))
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out


def month_end(d):
    return date(d.year + (d.month == 12), d.month % 12 + 1, 1) - timedelta(days=1)


# COMMAND ----------

for name, c in contracts.items():
    mon, kind, src = c.get("monitors", {}), c["source"]["kind"], c["source"]
    names = [x["name"] for x in c["columns"]]
    files = [r for r in audit_rows if kind == "csv" and r["source"] == src["file_source"]]

    # ---------------- FRESHNESS ----------------
    fr = mon.get("freshness")
    if fr and kind == "csv":
        dates = sorted({r["file_date"] for r in files if r["file_date"]})
        if not dates:
            record(c, "freshness_missing_file", "FRESHNESS_MISSING_FILE", "freshness", "all", 0, 1, "high", "no files loaded yet")
        else:
            if fr["frequency"] == "daily":
                expected = [dates[0] + timedelta(days=i) for i in range((dates[-1] - dates[0]).days + 1)]
                latest_end = dates[-1]
            else:
                expected = month_range(dates[0], dates[-1])
                latest_end = month_end(dates[-1])
            missing = [d for d in expected if d not in set(dates)]
            for d in missing:
                record(c, "freshness_missing_file", "FRESHNESS_MISSING_FILE", "freshness", str(d), len(expected), 1, "high",
                       f"no {fr['frequency']} file for {d}")
            if not missing:
                record(c, "freshness_missing_file", "FRESHNESS_MISSING_FILE", "freshness", "all", len(expected), 0, "high",
                       f"all {len(expected)} expected files present")
            lag = (AS_OF - latest_end).days
            record(c, "freshness_lag", "FRESHNESS_LAG", "freshness", "latest", 1, int(lag > fr["max_delay_days"]), "high",
                   f"latest file covers {latest_end}, {lag} days before {AS_OF} (SLA {fr['max_delay_days']} days)")
    elif fr and fr.get("type") == "stream":
        q = spark.table(src["table"])
        if kind == "cdc":
            q = q.filter(F.col("topic") == src["topic"])
        mx = q.agg(F.max("kafka_timestamp_ms")).first()[0]
        if mx is None:
            record(c, "freshness_lag", "FRESHNESS_LAG", "freshness", "latest", 1, 1, "high", "no events loaded")
        else:
            lag_min = (datetime.utcnow().timestamp() * 1000 - mx) / 60000
            record(c, "freshness_lag", "FRESHNESS_LAG", "freshness", "latest", 1, int(lag_min > fr["max_delay_minutes"]), "medium",
                   f"newest event is {lag_min:,.0f} minutes old (SLA {fr['max_delay_minutes']} minutes)")

    # ---------------- VOLUME ----------------
    vol = mon.get("volume")
    if vol and kind == "csv" and files:
        counts = {r["file_name"]: r["row_count"] for r in files}
        med = statistics.median(counts.values())
        for fn, n in sorted(counts.items()):
            problems = []
            if "min_rows" in vol and n < vol["min_rows"]:
                problems.append(f"below minimum {vol['min_rows']:,}")
            if "max_rows" in vol and n > vol["max_rows"]:
                problems.append(f"above maximum {vol['max_rows']:,}")
            if "ratio_to_median" in vol and med:
                lo, hi = vol["ratio_to_median"]
                if not (lo <= n / med <= hi):
                    problems.append(f"{n / med:.2f}x the median, allowed {lo} to {hi}")
            record(c, "volume", "VOLUME_ROWS", "volume", fn, n, int(bool(problems)), "high",
                   f"{n:,} rows (median {med:,.0f})" + (": " + "; ".join(problems) if problems else ""))
    elif vol and kind in ("json", "cdc"):
        q = spark.table(src["table"])
        if kind == "cdc":
            q = q.filter(F.col("topic") == src["topic"])
        n = q.count()
        record(c, "volume", "VOLUME_ROWS", "volume", "all", n, int(n < vol.get("min_rows", 1)), "high", f"{n:,} events loaded")

    # ---------------- DISTRIBUTION ----------------
    for m in mon.get("distribution", []):
        if kind != "csv":
            continue
        where = f"WHERE {m['where']} " if m.get("where") else ""
        q = (f"SELECT regexp_extract(_source_file, '[^/]+$', 0) AS file_name, {m['expr']} AS v "
             f"FROM {src['table']} {where}GROUP BY 1")
        vals = {r["file_name"]: r["v"] for r in spark.sql(q).collect() if r["v"] is not None}
        if len(vals) < 2:
            continue
        med = statistics.median(vals.values())
        for fn, v in sorted(vals.items()):
            if "ratio_to_median" in m and med:
                lo, hi = m["ratio_to_median"]
                bad = not (lo <= v / med <= hi)
                detail = f"{m['name']} = {v:,.3f} ({v / med:.2f}x the median {med:,.3f}, allowed {lo} to {hi})"
            else:
                tol = m["abs_diff_to_median"]
                bad = abs(v - med) > tol
                detail = f"{m['name']} = {v:,.3f} (median {med:,.3f}, allowed difference {tol})"
            record(c, f"distribution_{m['name']}", f"DIST_{m['name'].upper()}", "distribution", fn, 1, int(bad), "medium", detail)

    # ---------------- SCHEMA ----------------
    if kind == "csv":
        required = [x["name"] for x in c["columns"] if x.get("required")]
        for r in files:
            have = [x for x in (r["columns"] or "").split(",") if x]
            missing = [x for x in required if x not in have]
            extra = [x for x in have if x not in names]
            record(c, "schema_missing_required", "SCHEMA_MISSING_COLUMN", "schema", r["file_name"], len(required), len(missing),
                   "critical", f"missing required: {', '.join(missing)}" if missing else "all required columns present")
            record(c, "schema_extra_columns", "SCHEMA_EXTRA_COLUMN", "schema", r["file_name"], len(have), len(extra), "medium",
                   f"new columns not in the contract: {', '.join(extra)}" if extra else "no new columns")
    elif kind in ("json", "cdc"):
        q = spark.table(src["table"])
        path = "value" if kind == "json" else "get_json_object(value, '$.after')"
        if kind == "cdc":
            q = q.filter(F.col("topic") == src["topic"])
        q.createOrReplaceTempView("_schema_probe")
        keys = {r["k"] for r in spark.sql(f"SELECT DISTINCT explode(json_object_keys({path})) AS k FROM _schema_probe WHERE {path} IS NOT NULL").collect()}
        extra = sorted(keys - set(names))
        missing = sorted(set(x["name"] for x in c["columns"] if x.get("required")) - keys)
        record(c, "schema_extra_columns", "SCHEMA_EXTRA_COLUMN", "schema", "all", len(keys), len(extra), "medium",
               f"new fields not in the contract: {', '.join(extra)}" if extra else "no new fields")
        record(c, "schema_missing_required", "SCHEMA_MISSING_COLUMN", "schema", "all", len(names), len(missing), "critical",
               f"required fields never seen: {', '.join(missing)}" if missing else "all required fields seen")

    # ---------------- LINEAGE ----------------
    has_lineage = bool(c.get("lineage", {}).get("upstream")) and bool(c.get("lineage", {}).get("downstream"))
    record(c, "lineage_declared", "LINEAGE_DECLARED", "lineage", "all", 1, int(not has_lineage), "low",
           "upstream and downstream tables declared in the contract" if has_lineage else "no lineage declared")

# COMMAND ----------

save_results(rows)
opened, resolved = sync_incidents(run_id, failing, checked)
print(f"run {run_id}: {len(rows)} checks, {len(failing)} failing, incidents opened {opened}, resolved {resolved}")
display(spark.createDataFrame([(r[2], r[3], r[6], r[10], r[11], r[12]) for r in rows if r[10] == "FAIL"],
                              "dataset string, check_name string, scope string, status string, severity string, detail string")
        .orderBy("dataset", "check_name", "scope"))
