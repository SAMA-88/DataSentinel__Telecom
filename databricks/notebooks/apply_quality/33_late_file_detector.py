# Databricks notebook source
# LATE FILE DETECTOR: list the files expected each day or month and compare them with the files that arrived.
#   Expected  every period from monitors.freshness.expected_from up to today, one file per period
#             (daily or monthly, read from the file name with source.file_pattern / file_date_format)
#   Deadline  end of the period + max_delay_days, at the deadline time (default 06:00 Europe/Amsterdam)
#   Arrived   the file's modificationTime in the landing Volume, the first time this notebook saw it.
#             Kept in monitoring.file_arrivals, so a file uploaded again later keeps its first arrival time.
#   Status    ON_TIME, LATE (arrived after the deadline), MISSING (deadline passed, no file), PENDING (not due yet)
# A MISSING file opens a 'missing_file' incident (high), closed by itself when the file arrives.
# A LATE file opens a 'late_file' incident (high) that stays open for late_window_days after the file arrived.
# Alerts: 71_incident_manage sends Slack for new high (P2) incidents.
# Stream (CDC) sources have no files; their freshness stays in 30_observability_monitors.

# COMMAND ----------

# MAGIC %run ./11_dq_lib

# COMMAND ----------

dbutils.widgets.text("late_window_days", "7")       # how long a 'late_file' incident stays open after the file came
LATE_WINDOW = timedelta(days=int(dbutils.widgets.get("late_window_days")))

spark.conf.set("spark.sql.session.timeZone", "UTC")  # timestamps in and out of the tables are UTC
BATCH_DIR = f"/Volumes/{CATALOG}/bronze/raw/batch"   # <file_source>/<file>, same folders as 02.1_bronze_batch_files
T_ARRIVALS = f"{CATALOG}.monitoring.file_arrivals"
ARRIVAL_SCHEMA = ("dataset string, file_source string, frequency string, period_start date, file_name string, "
                  "expected_by timestamp, arrived_at timestamp, status string, late_minutes bigint, "
                  "checked_at timestamp, run_id string")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {T_ARRIVALS} (
  dataset STRING, file_source STRING, frequency STRING, period_start DATE, file_name STRING,
  expected_by TIMESTAMP, arrived_at TIMESTAMP, status STRING, late_minutes BIGINT,
  checked_at TIMESTAMP, run_id STRING)""")


def utc(ts):
    """Spark gives naive datetimes in the session time zone (UTC); make them aware."""
    return ts.replace(tzinfo=timezone.utc) if ts is not None and ts.tzinfo is None else ts


def naive_utc(ts):
    return ts.astimezone(timezone.utc).replace(tzinfo=None) if ts is not None else None


def local_txt(ts, fr):
    return ts.astimezone(ZoneInfo(str(fr.get("timezone", FILE_TIMEZONE)))).strftime("%Y-%m-%d %H:%M %Z")

# COMMAND ----------

NOW = datetime.now(timezone.utc)
contracts = load_contracts()
edges = edges_from_contracts(contracts)
run_id = new_run_id()

first_seen = {}
for r in spark.table(T_ARRIVALS).filter("arrived_at IS NOT NULL").select("dataset", "file_name", "arrived_at").collect():
    first_seen.setdefault(r["dataset"], {})[r["file_name"]] = utc(r["arrived_at"])

out_rows, results, failing, checked, bad_contracts = [], [], [], set(), {}
for name, c in contracts.items():
    fr, src = c.get("monitors", {}).get("freshness"), c["source"]
    if src["kind"] != "csv" or not fr or fr.get("type") == "stream":
        continue
    problems = validate_contract(c)
    if problems:                                         # e.g. an old contract without the file schedule
        bad_contracts[name] = problems
        print(f"[{name}] SKIPPED, contract problems: {'; '.join(problems)}")
        continue
    folder = f"{BATCH_DIR}/{src['file_source']}"
    try:
        listing = {f.name: datetime.fromtimestamp(f.modificationTime / 1000, timezone.utc)
                   for f in dbutils.fs.ls(folder) if f.name.lower().endswith(".csv")}
    except Exception as e:                               # no folder yet = no file arrived yet
        print(f"[{name}] cannot list {folder} ({type(e).__name__}); counting it as empty")
        listing = {}

    arrivals = file_arrivals(c, listing, NOW, first_seen.get(name, {}))
    impacted = ", ".join(impacted_tables(validated_table(c), edges))
    due = [a for a in arrivals if a["status"] != "PENDING"]
    missing = [a for a in arrivals if a["status"] == "MISSING"]
    late = [a for a in arrivals if a["status"] == "LATE"]
    late_recent = [a for a in late if NOW - a["arrived_at"] <= LATE_WINDOW]

    for a in arrivals:
        out_rows.append((name, src["file_source"], fr["frequency"], a["period"], a["file_name"],
                         naive_utc(a["expected_by"]), naive_utc(a["arrived_at"]), a["status"], a["late_minutes"],
                         naive_utc(NOW), run_id))

    checked.add((name, "missing_file"))
    for a in missing:
        failing.append(dict(dataset=name, check="missing_file", scope=str(a["period"]), pillar="freshness",
                            severity="high", impacted=impacted,
                            description=f"No {fr['frequency']} file for {a['period']} in {src['file_source']}/; "
                                        f"it was due by {local_txt(a['expected_by'], fr)}"))
    checked.add((name, "late_file"))
    for a in late_recent:
        h = a["late_minutes"] / 60
        failing.append(dict(dataset=name, check="late_file", scope=str(a["period"]), pillar="freshness",
                            severity="high", impacted=impacted,
                            description=f"{a['file_name']} arrived {local_txt(a['arrived_at'], fr)}, "
                                        f"{h:,.1f} hours after its deadline {local_txt(a['expected_by'], fr)}"))

    results.append(result_row(run_id, name, "missing_file", "FILE_MISSING", "freshness", "all", len(due), len(missing),
                              "FAIL" if missing else "PASS", "high" if missing else "info",
                              f"{len(due)} files due, {len(missing)} missing"
                              + (": " + ", ".join(str(a["period"]) for a in missing[:10]) if missing else "")
                              + (" ..." if len(missing) > 10 else ""), c["version"]))
    results.append(result_row(run_id, name, "late_file", "FILE_LATE", "freshness", "all", len(due), len(late),
                              "FAIL" if late_recent else "PASS", "high" if late_recent else "info",
                              f"{len(late)} of {len(due)} due files arrived late "
                              f"({len(late_recent)} in the last {LATE_WINDOW.days} days)", c["version"]))
    print(f"[{name}] {len(arrivals)} expected: " + ", ".join(
        f"{s} {sum(1 for a in arrivals if a['status'] == s)}" for s in FILE_STATUSES))

# COMMAND ----------

# Rewrite the arrivals table: every expected file with its current status. arrived_at never moves later,
# because the first time a file was seen is read back above and wins over the folder listing.
if out_rows:   # a dataset that was skipped keeps its old rows: only the datasets checked now are replaced
    done = list({r[0] for r in out_rows})
    (spark.createDataFrame(out_rows, ARRIVAL_SCHEMA).write.mode("overwrite")
        .option("replaceWhere", "dataset IN (" + ", ".join(f"'{d}'" for d in done) + ")").saveAsTable(T_ARRIVALS))
save_results(results)
opened, resolved = sync_incidents(run_id, failing, checked)
print(f"\nrun {run_id}: incidents opened {opened}, resolved {resolved}")
display(spark.table(T_ARRIVALS)
        .withColumn("_order", F.when(F.col("status") == "MISSING", 0).when(F.col("status") == "LATE", 1).otherwise(2))
        .orderBy("_order", "dataset", "period_start").drop("_order"))

# Fail the task when a contract could not be checked, so a missing schedule is never silent.
if bad_contracts:
    raise Exception("Late file detector could not check: " + "; ".join(f"{n} ({p[0]})" for n, p in bad_contracts.items())
                    + ". Upload the current contracts:  python scripts/upload_contracts.py")
