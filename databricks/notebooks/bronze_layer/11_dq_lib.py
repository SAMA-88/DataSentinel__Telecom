# Databricks notebook source
# Shared helpers for the quality phase. Other notebooks load it with:   %run ./11_dq_lib
#
# Concepts (Data Quality Fundamentals): data contracts and SLAs, testing data (rules),
# circuit breakers, cleaning and resolution (quarantine, auto-fix, revalidate),
# the five pillars of observability (freshness, volume, distribution, schema, lineage),
# incident management.

# COMMAND ----------

import os
import re
import statistics
import uuid
from datetime import date, datetime, timedelta
from functools import reduce

import yaml
from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField, StructType, TimestampType)

CATALOG = "data_sentinel"
CONTRACT_DIR = f"/Volumes/{CATALOG}/monitoring/contracts"

T_RESULTS = f"{CATALOG}.monitoring.dq_results"
T_INCIDENTS = f"{CATALOG}.monitoring.incidents"
T_RULES = f"{CATALOG}.monitoring.dq_rules"
T_LINEAGE = f"{CATALOG}.monitoring.lineage_edges"
T_AUDIT = f"{CATALOG}.bronze.file_audit"

META_COLS = ["_row_id", "_file_name", "_offset", "_ts_ms", "_op", "_is_latest"]

SIMPLE_TYPES = {"string", "int", "bigint", "timestamp", "date", "date_days", "double", "boolean"}
RULE_TYPES = {"not_null", "regex", "domain", "range", "not_future", "compare", "unique", "expr"}
ACTIONS = {"reject", "autofix", "flag"}
DIMENSIONS = {"completeness", "validity", "uniqueness", "domain", "timeliness", "consistency", "accuracy",
              "distribution", "schema"}
SEVERITIES = {"low", "medium", "high", "critical"}
FIX_OPS = {"trim", "lower", "upper", "regex_replace"}
COMPARE_OPS = {"<", "<=", ">", ">=", "=", "!="}


# ======================================================================
# 1. Contracts (pure Python, no Spark)
# ======================================================================

def load_contracts(directory=CONTRACT_DIR):
    contracts = {}
    for fn in sorted(os.listdir(directory)):
        if fn.endswith((".yaml", ".yml")) and not fn.startswith("_"):
            with open(os.path.join(directory, fn), encoding="utf-8") as f:
                c = yaml.safe_load(f)
            contracts[c["dataset"]] = c
    return contracts


def is_valid_type(t):
    return t in SIMPLE_TYPES or re.fullmatch(r"decimal\(\d+,\d+\)", str(t)) is not None


def expand_rules(c, include_unique=True):
    """Contract rules plus one automatic TYPE rule for every non-string column."""
    rules = []
    for col in c["columns"]:
        if col["type"] != "string":
            rules.append(dict(id=f"TYPE_{col['name'].upper()}", type="type", column=col["name"],
                              dimension="validity", severity="high", action=c.get("type_action", "reject"),
                              description=f"{col['name']} must be a valid {col['type']}"))
    for r in c.get("rules", []):
        rr = dict(r)
        rr.setdefault("severity", "medium")
        rr.setdefault("action", "reject")
        rr.setdefault("description", f"{r['type']} on {r.get('column') or r.get('keys') or r.get('left')}")
        rules.append(rr)
    if not include_unique:
        rules = [r for r in rules if r["type"] != "unique"]
    return rules


def apply_fix_steps_py(value, steps):
    """Python twin of the Spark fix steps. Used by the local validator to test the fixes."""
    if value is None:
        return None
    for s in steps:
        op = s["op"]
        if op == "trim":
            value = value.strip()
        elif op == "lower":
            value = value.lower()
        elif op == "upper":
            value = value.upper()
        elif op == "regex_replace":
            value = re.sub(s["pattern"], re.sub(r"\$(\d)", r"\\\1", s["replacement"]), value)
    return value


def validate_contract(c):
    """Return a list of error strings (empty list = contract is valid)."""
    errs = []
    for k in ("dataset", "version", "owner", "source", "columns"):
        if k not in c:
            errs.append(f"missing key '{k}'")
    if errs:
        return errs
    names = [x["name"] for x in c["columns"]]
    if len(names) != len(set(names)):
        errs.append("duplicate column names")
    for x in c["columns"]:
        if not is_valid_type(x.get("type")):
            errs.append(f"column {x['name']}: unknown type {x.get('type')}")
    s = c["source"]
    kind = s.get("kind")
    need = {"csv": ["table", "file_source"], "json": ["table"], "cdc": ["table", "topic", "key"]}
    if kind not in need:
        errs.append(f"source.kind must be csv, json or cdc (got {kind})")
    else:
        errs += [f"source.{k} is required for kind {kind}" for k in need[kind] if k not in s]
    if kind == "cdc":
        errs += [f"source.key column {k} not in columns" for k in s.get("key", []) if k not in names]
    ids = set()
    for r in expand_rules(c):
        rid = r.get("id")
        if not rid or rid in ids:
            errs.append(f"rule id missing or duplicated: {rid}")
        ids.add(rid)
        if r["type"] not in RULE_TYPES | {"type"}:
            errs.append(f"{rid}: unknown rule type {r['type']}")
        if r["action"] not in ACTIONS:
            errs.append(f"{rid}: action must be one of {sorted(ACTIONS)}")
        if r["dimension"] not in DIMENSIONS:
            errs.append(f"{rid}: unknown dimension {r['dimension']}")
        if r["severity"] not in SEVERITIES:
            errs.append(f"{rid}: unknown severity {r['severity']}")
        t = r["type"]
        if t in ("not_null", "regex", "domain", "range", "not_future") and r.get("column") not in names:
            errs.append(f"{rid}: column {r.get('column')} not in contract columns")
        if t == "regex":
            try:
                re.compile(r["pattern"])
            except Exception as e:
                errs.append(f"{rid}: bad regex ({e})")
        if t == "domain" and not r.get("values"):
            errs.append(f"{rid}: domain needs 'values'")
        if t == "range" and "min" not in r and "max" not in r:
            errs.append(f"{rid}: range needs min and/or max")
        if t == "compare":
            if r.get("left") not in names or r.get("right") not in names:
                errs.append(f"{rid}: compare columns must be contract columns")
            if r.get("op") not in COMPARE_OPS:
                errs.append(f"{rid}: compare op must be one of {sorted(COMPARE_OPS)}")
        if t == "unique" and not r.get("keys"):
            errs.append(f"{rid}: unique needs 'keys'")
        if t == "expr" and not r.get("valid_when"):
            errs.append(f"{rid}: expr needs 'valid_when'")
    for fx in c.get("fixes", []):
        if fx.get("column") not in names:
            errs.append(f"fix column {fx.get('column')} not in contract columns")
        for st in fx.get("steps", []):
            if st.get("op") not in FIX_OPS:
                errs.append(f"fix on {fx.get('column')}: unknown op {st.get('op')}")
            if st.get("op") == "regex_replace":
                try:
                    re.compile(st["pattern"])
                except Exception as e:
                    errs.append(f"fix on {fx.get('column')}: bad regex ({e})")
    m = c.get("monitors", {})
    fr = m.get("freshness")
    if fr and fr.get("type") != "stream" and fr.get("frequency") not in ("daily", "monthly"):
        errs.append("monitors.freshness needs type: stream or frequency: daily|monthly")
    return errs


def edges_from_contracts(contracts):
    """Lineage edges (source, target) declared in the contracts."""
    edges = set()
    for c in contracts.values():
        vt = validated_table(c)
        for u in c.get("lineage", {}).get("upstream", []):
            edges.add((u, vt))
        for d in c.get("lineage", {}).get("downstream", []):
            edges.add((vt, d))
    return sorted(edges)


def impacted_tables(table, edges):
    """Every table that is downstream of the given table."""
    seen, queue = set(), [table]
    while queue:
        cur = queue.pop()
        for a, b in edges:
            if a == cur and b not in seen:
                seen.add(b)
                queue.append(b)
    return sorted(seen)


def validated_table(c):
    return f"{CATALOG}.silver.{c['dataset']}_validated"


def quarantine_table(c):
    return f"{CATALOG}.quarantine.{c['dataset']}"


def new_run_id():
    return datetime.utcnow().strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6]


# ======================================================================
# 2. Reading the source into one standard shape
# ======================================================================

def _ensure_meta(df):
    defaults = {"_file_name": F.lit(None).cast("string"), "_offset": F.lit(None).cast("long"),
                "_ts_ms": F.lit(None).cast("long"), "_op": F.lit(None).cast("string"),
                "_is_latest": F.lit(True)}
    for k, v in defaults.items():
        if k not in df.columns:
            df = df.withColumn(k, v)
    return df


def load_source(c):
    """Return the raw data as strings: contract columns + META_COLS."""
    s, kind = c["source"], c["source"]["kind"]
    cols = [x["name"] for x in c["columns"]]
    df = spark.table(s["table"])
    file_name = F.regexp_extract(F.col("_source_file"), r"[^/]+$", 0)

    if kind == "csv":
        for col in cols:
            if col not in df.columns:                       # column missing in every file
                df = df.withColumn(col, F.lit(None).cast("string"))
        w = Window.partitionBy("_source_file", "_row_hash").orderBy(F.lit(1))
        df = (df.withColumn("_file_name", file_name)
                .withColumn("_dup_idx", F.row_number().over(w))
                .withColumn("_row_id", F.sha2(F.concat_ws("|", F.col("_source_file"), F.col("_row_hash"),
                                                           F.col("_dup_idx").cast("string")), 256)))
    elif kind == "json":
        for col in cols:
            df = df.withColumn(col, F.get_json_object("value", f"$.{col}"))
        df = (df.withColumn("_file_name", file_name)
                .withColumn("_offset", F.col("offset"))
                .withColumn("_row_id", F.concat_ws("|", F.col("topic"), F.col("partition").cast("string"),
                                                   F.col("offset").cast("string"))))
    elif kind == "cdc":
        op = F.get_json_object("value", "$.op")
        df = df.filter(F.col("topic") == s["topic"]).filter(op.isin("r", "c", "u"))
        for col in cols:
            df = df.withColumn(col, F.get_json_object("value", f"$.after.{col}"))
        df = (df.withColumn("_op", op)
                .withColumn("_ts_ms", F.get_json_object("value", "$.ts_ms").cast("long"))
                .withColumn("_offset", F.col("offset"))
                .withColumn("_file_name", file_name)
                .withColumn("_row_id", F.concat_ws("|", F.col("topic"), F.col("partition").cast("string"),
                                                   F.col("offset").cast("string"))))
        w = Window.partitionBy(*[F.col(k) for k in s["key"]]).orderBy(F.col("_ts_ms").desc(), F.col("offset").desc())
        df = df.withColumn("_is_latest", F.row_number().over(w) == 1)
    df = _ensure_meta(df)
    return df.select(*[F.col(x) for x in cols], *[F.col(m) for m in META_COLS])


def blocked_files(c):
    """Circuit breaker: files that are missing a required column are blocked as a whole."""
    if c["source"]["kind"] != "csv":
        return {}
    required = [x["name"] for x in c["columns"] if x.get("required")]
    out = {}
    if not spark.catalog.tableExists(T_AUDIT):
        return out
    for r in spark.table(T_AUDIT).filter(F.col("source") == c["source"]["file_source"]).collect():
        have = set((r["columns"] or "").split(","))
        missing = [x for x in required if x not in have]
        if missing:
            out[r["file_name"]] = missing
    return out


# ======================================================================
# 3. Typing and rules
# ======================================================================

def typed_expr(col):
    name, typ = col["name"], col["type"]
    if col.get("sql"):
        return F.expr(col["sql"])
    if typ == "string":
        return F.trim(F.col(name))
    if typ == "date_days":                                   # Debezium DATE = days since 1970-01-01
        return F.expr(f"date_add(DATE'1970-01-01', try_cast(trim(`{name}`) AS int))")
    return F.expr(f"try_cast(trim(`{name}`) AS {typ})")


def add_typed(df, c):
    for col in c["columns"]:
        df = df.withColumn(f"t_{col['name']}", typed_expr(col))
    return df


def _blank(name):
    return F.col(name).isNull() | (F.trim(F.col(name)) == "")


def rule_fail(r, names, blocked):
    """Boolean Column that is true when the row FAILS the rule."""
    t, col = r["type"], r.get("column")
    if t == "not_null":
        return _blank(col)
    if t == "type":
        return ~_blank(col) & F.col(f"t_{col}").isNull()
    if t == "regex":
        return ~_blank(col) & ~F.col(col).rlike(r["pattern"])
    if t == "domain":
        return ~_blank(col) & ~F.trim(F.col(col)).isin(*r["values"])
    if t == "range":
        tc = F.col(f"t_{col}")
        conds = []
        if "min" in r:
            conds.append(tc < F.lit(r["min"]))
        if "max" in r:
            conds.append(tc > F.lit(r["max"]))
        return tc.isNotNull() & reduce(lambda a, b: a | b, conds)
    if t == "not_future":
        tc = F.col(f"t_{col}")
        tol = int(r.get("tolerance_days", 1))
        return tc.isNotNull() & (tc > F.current_timestamp() + F.expr(f"INTERVAL {tol} DAYS"))
    if t == "compare":
        lc, rc = F.col(f"t_{r['left']}"), F.col(f"t_{r['right']}")
        ok = F.expr(f"t_{r['left']} {r['op']} t_{r['right']}")
        return lc.isNotNull() & rc.isNotNull() & ~ok
    if t == "expr":
        return F.coalesce(F.expr(r["valid_when"]), F.lit(True)) == F.lit(False)
    if t == "unique":
        keys = r["keys"]
        present = reduce(lambda a, b: a & b, [~_blank(k) for k in keys])
        scope = present
        if r.get("when"):
            scope = scope & F.coalesce(F.expr(r["when"]), F.lit(False))
        order = [F.col(f"t_{o}") if o in names else F.col(o) for o in r.get("order_by", keys)]
        w = Window.partitionBy(*[F.col(k) for k in keys], scope).orderBy(*order)
        return scope & (F.row_number().over(w) > 1)
    if t == "file_block":
        return F.col("_file_name").isin(list(blocked)) if blocked else F.lit(False)
    raise ValueError(f"unknown rule type {t}")


def apply_rules(df, c, blocked=None, include_unique=True):
    """Add one failure column per rule, plus _dq_status, _failed_rules and _flags."""
    names = [x["name"] for x in c["columns"]]
    rules = expand_rules(c, include_unique)
    if c["source"]["kind"] == "csv":
        rules.append(dict(id="FILE_SCHEMA_BLOCK", type="file_block", dimension="schema", severity="critical",
                          action="reject", description="File is missing a required column (circuit breaker)"))
    for r in rules:
        df = df.withColumn(f"__f_{r['id']}", F.coalesce(rule_fail(r, names, blocked or {}), F.lit(False)))

    def any_of(ids):
        return reduce(lambda a, b: a | b, [F.col(f"__f_{i}") for i in ids], F.lit(False))

    def ids_of(*acts):
        return [r["id"] for r in rules if r["action"] in acts]

    def names_array(ids):
        arr = F.array(*[F.when(F.col(f"__f_{i}"), F.lit(i)) for i in ids]).cast("array<string>")
        return F.filter(arr, lambda x: x.isNotNull())

    df = (df.withColumn("_dq_status", F.when(any_of(ids_of("reject")), "REJECT")
                                       .when(any_of(ids_of("autofix")), "AUTOFIX").otherwise("PASS"))
            .withColumn("_failed_rules", names_array(ids_of("reject", "autofix")))
            .withColumn("_flags", names_array(ids_of("flag"))))
    return df, rules


def rule_counts(df, rules):
    aggs = [F.count(F.lit(1)).alias("__total"),
            F.sum(F.when(F.col("_dq_status") == "PASS", 1).otherwise(0)).alias("__pass"),
            F.sum(F.when(F.col("_dq_status") == "REJECT", 1).otherwise(0)).alias("__reject"),
            F.sum(F.when(F.col("_dq_status") == "AUTOFIX", 1).otherwise(0)).alias("__autofix")]
    aggs += [F.sum(F.col(f"__f_{r['id']}").cast("int")).alias(f"c_{r['id']}") for r in rules]
    row = df.agg(*aggs).first().asDict()
    return {k: (v or 0) for k, v in row.items()}


# ======================================================================
# 4. Output shapes: validated rows and quarantine rows
# ======================================================================

def build_validated(df, c, fixed=False):
    cols = [x["name"] for x in c["columns"]]
    return df.select(*[F.col(f"t_{n}").alias(n) for n in cols], *[F.col(m) for m in META_COLS],
                     F.col("_flags"), F.lit(fixed).alias("_dq_fixed"),
                     F.lit(c["version"]).alias("_contract_version"), F.current_timestamp().alias("_validated_at"))


def build_quarantine(df, c, run_id):
    cols = [x["name"] for x in c["columns"]]
    return df.select(*[F.col(n) for n in cols], *[F.col(m) for m in META_COLS],
                     F.col("_failed_rules"),
                     F.concat_ws(",", F.col("_failed_rules")).alias("_failure_reasons"),
                     F.when(F.col("_dq_status") == "REJECT", "REJECTED").otherwise("PENDING_FIX").alias("_quarantine_status"),
                     F.lit(None).cast("map<string,string>").alias("_fixed"),
                     F.lit(0).alias("_attempts"),
                     F.current_timestamp().alias("_quarantined_at"),
                     F.lit(c["version"]).alias("_contract_version"),
                     F.lit(run_id).alias("_run_id"))


def upsert_quarantine(bad_df, table):
    """Insert new bad rows, keep the status of rows already there, drop rows that are no longer bad."""
    if not spark.catalog.tableExists(table):
        bad_df.write.format("delta").saveAsTable(table)
        return
    (DeltaTable.forName(spark, table).alias("t")
        .merge(bad_df.alias("s"), "t._row_id = s._row_id")
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute())


def fix_expression(name, c):
    """Column expression for the fixed (still raw/string) value of one column."""
    steps = []
    for fx in c.get("fixes", []):
        if fx["column"] == name:
            steps = fx["steps"]
    e = F.col(name)
    for s in steps:
        op = s["op"]
        if op == "trim":
            e = F.trim(e)
        elif op == "lower":
            e = F.lower(e)
        elif op == "upper":
            e = F.upper(e)
        elif op == "regex_replace":
            e = F.regexp_replace(e, s["pattern"], s["replacement"])
    return e


# ======================================================================
# 5. Results and incidents
# ======================================================================

RESULT_SCHEMA = StructType([
    StructField("run_id", StringType()), StructField("run_ts", TimestampType()), StructField("dataset", StringType()),
    StructField("check_name", StringType()), StructField("rule_id", StringType()), StructField("dimension", StringType()),
    StructField("scope", StringType()), StructField("rows_checked", LongType()), StructField("rows_failed", LongType()),
    StructField("pass_rate", DoubleType()), StructField("status", StringType()), StructField("severity", StringType()),
    StructField("detail", StringType()), StructField("contract_version", StringType()),
])

INCIDENT_SCHEMA = StructType([
    StructField("incident_id", StringType()), StructField("incident_key", StringType()),
    StructField("opened_at", TimestampType()), StructField("closed_at", TimestampType()),
    StructField("status", StringType()), StructField("dataset", StringType()), StructField("check_name", StringType()),
    StructField("pillar", StringType()), StructField("severity", StringType()), StructField("scope", StringType()),
    StructField("description", StringType()), StructField("impacted", StringType()),
    StructField("run_id", StringType()), StructField("resolved_run_id", StringType()),
])


def result_row(run_id, dataset, check, rule_id, dimension, scope, checked, failed, status, severity, detail, version):
    rate = 1.0 if not checked else max(0.0, 1.0 - float(failed) / float(checked))
    return (run_id, datetime.utcnow(), dataset, check, rule_id, dimension, scope, int(checked), int(failed),
            float(rate), status, severity, detail, version)


def save_results(rows):
    if rows:
        spark.createDataFrame(rows, RESULT_SCHEMA).write.mode("append").saveAsTable(T_RESULTS)


def sync_incidents(run_id, failing, checked):
    """failing: list of dicts(dataset, check, scope, pillar, severity, description, impacted).
    checked: set of (dataset, check) pairs that were evaluated in this run.
    Opens an incident for each new failure and resolves open incidents that no longer fail."""
    def key(d):
        return f"{d['dataset']}|{d['check']}|{d['scope']}"

    open_keys = {r["incident_key"] for r in spark.table(T_INCIDENTS).filter("status = 'OPEN'")
                 .select("incident_key").collect()}
    fail_keys = {key(d) for d in failing}
    seen, new_rows = set(), []
    for d in failing:
        k = key(d)
        if k in open_keys or k in seen:
            continue
        seen.add(k)
        new_rows.append((uuid.uuid4().hex[:12], k, datetime.utcnow(), None, "OPEN", d["dataset"], d["check"],
                         d["pillar"], d["severity"], d["scope"], d["description"], d.get("impacted", ""),
                         run_id, None))
    if new_rows:
        spark.createDataFrame(new_rows, INCIDENT_SCHEMA).write.mode("append").saveAsTable(T_INCIDENTS)
    to_resolve = [k for k in open_keys
                  if tuple(k.split("|", 2)[:2]) in checked and k not in fail_keys]
    if to_resolve:
        quoted = ",".join("'" + k.replace("'", "''") + "'" for k in to_resolve)
        spark.sql(f"UPDATE {T_INCIDENTS} SET status = 'RESOLVED', closed_at = current_timestamp(), "
                  f"resolved_run_id = '{run_id}' WHERE incident_key IN ({quoted})")
    return len(new_rows), len(to_resolve)
