"""Check the data contracts locally, before they are uploaded to Databricks. No Spark or Databricks needed.

  python scripts\\validate_contracts.py
"""
import glob
import os
import re
import sys
import types
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)


def find_file(name):
    hits = [p for p in glob.glob(os.path.join(ROOT, "**", name), recursive=True)
            if ".venv" not in p and os.path.isfile(p)]
    if not hits:
        sys.exit(f"NOT FOUND under {ROOT}: {name}\n"
                 f"Copy the quality files into the repo first (see the folder list below).")
    return hits[0]


LIB = find_file("11_dq_lib.py")
CONTRACTS = os.path.dirname(find_file("crm_customers.yaml"))
print("lib      :", LIB)
print("contracts:", CONTRACTS, "\n")

# stand-ins so the library can be loaded without Spark
M = mock.MagicMock()
for name, attrs in {"delta": {}, "delta.tables": {"DeltaTable": M}, "pyspark": {},
                    "pyspark.sql": {"Window": M, "functions": M},
                    "pyspark.sql.types": {k: M for k in ("DoubleType", "LongType", "StringType", "StructField", "StructType", "TimestampType")}}.items():
    mod = types.ModuleType(name)
    mod.__dict__.update(attrs)
    sys.modules[name] = mod

ns = {"spark": M, "__name__": "dq_lib"}
exec(compile(open(LIB, encoding="utf-8").read(), LIB, "exec"), ns)
contracts = ns["load_contracts"](CONTRACTS)

failures = 0
print(f"{'dataset':22s} {'kind':5s} {'cols':>4s} {'rules':>5s} {'reject':>6s} {'autofix':>7s} {'flag':>4s}  status")
for name, c in contracts.items():
    errs = ns["validate_contract"](c)
    rules = ns["expand_rules"](c)
    by = lambda a: sum(1 for r in rules if r["action"] == a)

    for fx in c.get("fixes", []):
        col = fx["column"]
        rx = [r for r in c.get("rules", []) if r.get("type") == "regex" and r.get("column") == col]
        for given, expected in fx.get("examples", []):
            got = ns["apply_fix_steps_py"](given, fx["steps"])
            if got != expected:
                errs.append(f"fix {col}: {given!r} -> {got!r}, expected {expected!r}")
            for r in rx:
                if not re.search(r["pattern"], got or ""):
                    errs.append(f"fix {col}: fixed value {got!r} still fails {r['id']}")
        for bad in fx.get("unfixable", []):
            got = ns["apply_fix_steps_py"](bad, fx["steps"])
            for r in rx:
                if re.search(r["pattern"], got or ""):
                    errs.append(f"fix {col}: {bad!r} should stay invalid but became {got!r}")

    status = "OK" if not errs else "ERRORS"
    print(f"{name:22s} {c['source']['kind']:5s} {len(c['columns']):4d} {len(rules):5d} {by('reject'):6d} {by('autofix'):7d} {by('flag'):4d}  {status}")
    for e in errs:
        print("    -", e)
        failures += 1

edges = ns["edges_from_contracts"](contracts)
vt = ns["validated_table"](contracts["network_cdr"])
impact = ns["impacted_tables"](vt, edges)
print(f"\nlineage: {len(edges)} edges; network_cdr validated feeds {impact}")
if "data_sentinel.silver.fact_usage" not in impact:
    print("    - lineage impact check failed")
    failures += 1

print("\nALL CONTRACTS VALID" if not failures else f"\n{failures} PROBLEM(S) FOUND")
sys.exit(1 if failures else 0)