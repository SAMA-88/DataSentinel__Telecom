# Databricks notebook source
# RULE TESTS: tests the Spark side of 11_dq_lib on small hand-made data, on the real Spark runtime.
#   1) rule_fail / apply_rules: one row per case, the expected _dq_status, _failed_rules and _flags
#   2) fix parity: fix_expression (Spark) gives the same value as apply_fix_steps_py (Python), for every
#      fix in the contracts, on the contract examples plus tricky whitespace values
# The pure-Python parts are tested locally by tests/unit/test_rules.py (pytest, no Spark).
# Run it after every change to 11_dq_lib or to a contract's fixes. It writes nothing; it fails if a test fails.

# COMMAND ----------

# MAGIC %run ./11_dq_lib

# COMMAND ----------

from datetime import datetime, timedelta

C = {
    "dataset": "rule_tests", "version": "test", "owner": "tests",
    "source": {"kind": "json", "table": "unused"},
    "columns": [
        {"name": "id", "type": "bigint", "required": True},
        {"name": "code", "type": "string"},
        {"name": "status", "type": "string"},
        {"name": "amount", "type": "decimal(10,2)"},
        {"name": "period_start", "type": "date"},
        {"name": "period_end", "type": "date"},
        {"name": "event_ts", "type": "timestamp"},
        {"name": "email", "type": "string"},
    ],
    "rules": [
        {"id": "ID_NOT_NULL", "type": "not_null", "column": "id", "dimension": "completeness", "severity": "critical", "action": "reject"},
        {"id": "CODE_FORMAT", "type": "regex", "column": "code", "pattern": "^[A-Z]{3}$", "dimension": "validity", "action": "autofix"},
        {"id": "STATUS_DOMAIN", "type": "domain", "column": "status", "values": ["A", "B"], "dimension": "domain", "action": "reject"},
        {"id": "AMOUNT_NON_NEG", "type": "range", "column": "amount", "min": 0, "dimension": "validity", "action": "flag"},
        {"id": "AMOUNT_EXPR", "type": "expr", "valid_when": "t_amount < 1000", "dimension": "accuracy", "action": "flag"},
        {"id": "PERIOD_ORDER", "type": "compare", "left": "period_start", "op": "<=", "right": "period_end", "dimension": "validity", "action": "reject"},
        {"id": "TS_NOT_FUTURE", "type": "not_future", "column": "event_ts", "dimension": "timeliness", "action": "reject"},
        {"id": "EMAIL_UNIQUE", "type": "unique", "keys": ["email"], "order_by": ["id"], "dimension": "uniqueness", "action": "reject"},
    ],
}
assert validate_contract(C) == [], validate_contract(C)

SOON = (datetime.utcnow() + timedelta(hours=6)).strftime("%Y-%m-%d %H:%M:%S")   # inside the 1-day tolerance
BASE = {"id": None, "code": "ABC", "status": "A", "amount": "10.00", "period_start": "2026-01-01",
        "period_end": "2026-01-31", "event_ts": "2026-01-01 10:00:00", "email": None}

# case: (values that differ from BASE, expected _dq_status, expected _failed_rules, expected _flags)
CASES = {
    "all_good":             ({}, "PASS", set(), set()),
    "id_blank":             ({"id": "  "}, "REJECT", {"ID_NOT_NULL"}, set()),       # counted once: not also TYPE_ID
    "id_not_a_number":      ({"id": "abc"}, "REJECT", {"TYPE_ID"}, set()),
    "code_lowercase":       ({"code": "abc"}, "AUTOFIX", {"CODE_FORMAT"}, set()),
    "code_blank_ignored":   ({"code": ""}, "PASS", set(), set()),                   # blanks are only not_null's job
    "status_outside":       ({"status": "Z"}, "REJECT", {"STATUS_DOMAIN"}, set()),
    "status_padded_ok":     ({"status": " A "}, "PASS", set(), set()),               # domain compares trimmed values
    "amount_negative":      ({"amount": "-5"}, "PASS", set(), {"AMOUNT_NON_NEG"}),   # flag only: row stays valid
    "amount_too_big":       ({"amount": "5000"}, "PASS", set(), {"AMOUNT_EXPR"}),
    "amount_not_a_number":  ({"amount": "12,50"}, "REJECT", {"TYPE_AMOUNT"}, set()),  # no range/expr on a null value
    "period_reversed":      ({"period_start": "2026-02-01", "period_end": "2026-01-01"}, "REJECT", {"PERIOD_ORDER"}, set()),
    "period_end_missing":   ({"period_end": None}, "PASS", set(), set()),            # compare needs both sides
    "ts_far_future":        ({"event_ts": "2999-01-01 00:00:00"}, "REJECT", {"TS_NOT_FUTURE"}, set()),
    "ts_within_tolerance":  ({"event_ts": SOON}, "PASS", set(), set()),
    "dup_first_kept":       ({"id": "9", "email": "dup@x.nl"}, "PASS", set(), set()),
    "dup_second_rejected":  ({"id": "10", "email": "dup@x.nl"}, "REJECT", {"EMAIL_UNIQUE"}, set()),  # 9 < 10 as numbers
    "null_email_a":         ({"email": None}, "PASS", set(), set()),                 # missing keys are never duplicates
    "null_email_b":         ({"email": None}, "PASS", set(), set()),
    "reject_beats_autofix": ({"id": "", "code": "abc"}, "REJECT", {"ID_NOT_NULL", "CODE_FORMAT"}, set()),
}

cols = [x["name"] for x in C["columns"]]
rows = []
for i, (case, (over, *_)) in enumerate(CASES.items(), start=1):
    v = {**BASE, "id": str(100 + i), **over}
    rows.append((case, *[v[c] for c in cols]))
df = spark.createDataFrame(rows, "case string, " + ", ".join(f"`{c}` string" for c in cols))
df = _ensure_meta(df).withColumn("_row_id", F.col("case"))

out, rules = apply_rules(add_typed(df, C), C)
got = {r["case"]: r for r in out.select("case", "_dq_status", "_failed_rules", "_flags").collect()}

report = []
for case, (_, status, failed, flags) in CASES.items():
    r = got[case]
    actual = (r["_dq_status"], set(r["_failed_rules"]), set(r["_flags"]))
    ok = actual == (status, failed, flags)
    report.append(("rules", case, f"{status} {sorted(failed)} {sorted(flags)}",
                   f"{actual[0]} {sorted(actual[1])} {sorted(actual[2])}", "OK" if ok else "FAIL"))

counts = rule_counts(out, rules)
for key, expected in [("__total", len(CASES)), ("c_EMAIL_UNIQUE", 1), ("c_ID_NOT_NULL", 2),
                      ("__autofix", sum(1 for x in CASES.values() if x[1] == "AUTOFIX"))]:
    report.append(("rule_counts", key, str(expected), str(counts[key]), "OK" if counts[key] == expected else "FAIL"))

# COMMAND ----------

# 2) fix parity: Spark fix_expression vs Python apply_fix_steps_py
TRICKY = ["\t0612345678\n", " 06-12345678 ", "  X  ", "\r\nabc\x0b\x0c", "a\tb", " abc", "", None]

contracts = load_contracts() if os.path.isdir(CONTRACT_DIR) else {}
fix_cases = {f"{n}.{fx['column']}": (c, fx) for n, c in contracts.items() for fx in c.get("fixes", [])}
builtin = {"column": "v", "steps": [{"op": "trim"}, {"op": "upper"},
                                    {"op": "regex_replace", "pattern": "^0(6[0-9]{8})$", "replacement": "+31$1"}]}
fix_cases["builtin.trim_upper_replace"] = ({"fixes": [builtin]}, builtin)   # runs even without the contracts volume

for label, (c, fx) in fix_cases.items():
    col = fx["column"]
    values = [g for g, _ in fx.get("examples", [])] + list(fx.get("unfixable", [])) + TRICKY
    vdf = spark.createDataFrame([(v,) for v in values], f"`{col}` string")
    for r in vdf.select(F.col(col).alias("given"), fix_expression(col, c).alias("spark")).collect():
        py = apply_fix_steps_py(r["given"], fx["steps"])
        report.append(("fix_parity", f"{label} {r['given']!r}", repr(py), repr(r["spark"]),
                       "OK" if py == r["spark"] else "FAIL"))

# COMMAND ----------

result = spark.createDataFrame(report, "test string, case string, expected string, actual string, status string")
display(result.orderBy(F.when(F.col("status") == "FAIL", 0).otherwise(1), "test", "case"))

n_fail = sum(1 for r in report if r[4] == "FAIL")
if n_fail:
    raise Exception(f"RULE TESTS FAILED: {n_fail} of {len(report)} checks, see the table above")
print(f"all {len(report)} rule tests passed")
