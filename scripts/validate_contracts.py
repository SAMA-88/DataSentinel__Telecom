"""Check the data contracts locally, before they are uploaded to Databricks. No Spark or Databricks needed.

  python scripts\\validate_contracts.py

It runs the real quality library (databricks/notebooks/apply_quality/11_dq_lib.py) through
src/sentinel_dq/quality/contract_loader.py, so the checks are the ones the notebooks use.
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from sentinel_dq.quality.contract_loader import CONTRACT_DIR, LIB_PATH, load_dq_lib  # noqa: E402

lib = load_dq_lib()
print("lib      :", LIB_PATH)
print("contracts:", CONTRACT_DIR, "\n")
contracts = lib.load_contracts(str(CONTRACT_DIR))


def fix_errors(c):
    """Every fix example gives the expected value and passes the column's regex rules;
    every 'unfixable' value still fails them."""
    errs = []
    for fx in c.get("fixes", []):
        col = fx["column"]
        rx = [r for r in c.get("rules", []) if r.get("type") == "regex" and r.get("column") == col]
        for given, expected in fx.get("examples", []):
            got = lib.apply_fix_steps_py(given, fx["steps"])
            if got != expected:
                errs.append(f"fix {col}: {given!r} -> {got!r}, expected {expected!r}")
            for r in rx:
                if not re.search(r["pattern"], got or ""):
                    errs.append(f"fix {col}: fixed value {got!r} still fails {r['id']}")
        for bad in fx.get("unfixable", []):
            got = lib.apply_fix_steps_py(bad, fx["steps"])
            for r in rx:
                if re.search(r["pattern"], got or ""):
                    errs.append(f"fix {col}: {bad!r} should stay invalid but became {got!r}")
    return errs


if __name__ == "__main__":
    failures = 0
    print(f"{'dataset':22s} {'kind':5s} {'cols':>4s} {'rules':>5s} {'reject':>6s} {'autofix':>7s} {'flag':>4s}  status")
    for name, c in contracts.items():
        errs = lib.validate_contract(c) + fix_errors(c)
        rules = lib.expand_rules(c)
        by = lambda a: sum(1 for r in rules if r["action"] == a)  # noqa: E731
        status = "OK" if not errs else "ERRORS"
        print(f"{name:22s} {c['source']['kind']:5s} {len(c['columns']):4d} {len(rules):5d} {by('reject'):6d} "
              f"{by('autofix'):7d} {by('flag'):4d}  {status}")
        for e in errs:
            print("    -", e)
            failures += 1

    edges = lib.edges_from_contracts(contracts)
    impact = lib.impacted_tables(lib.validated_table(contracts["network_cdr"]), edges)
    print(f"\nlineage: {len(edges)} edges; network_cdr validated feeds {impact}")
    if "data_sentinel.silver.fact_usage" not in impact:
        print("    - lineage impact check failed")
        failures += 1

    print("\nALL CONTRACTS VALID" if not failures else f"\n{failures} PROBLEM(S) FOUND")
    sys.exit(1 if failures else 0)
