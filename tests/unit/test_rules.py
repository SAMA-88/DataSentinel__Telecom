"""Unit tests for the quality library (databricks/notebooks/apply_quality/11_dq_lib.py), no Spark needed.

Covers the pure-Python parts: contract loading and validation, rule expansion, the Python twin of the
fix steps, and lineage. The Spark rule logic (rule_fail / apply_rules) and the Spark-vs-Python fix
parity are tested on Databricks by databricks/notebooks/apply_quality/13_rule_tests.py.

    python -m pytest tests/unit/test_rules.py -v
"""
import copy
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from sentinel_dq.quality.contract_loader import CONTRACT_DIR, load_contracts, load_dq_lib  # noqa: E402

lib = load_dq_lib()
CONTRACTS = load_contracts()


def minimal_contract(**overrides):
    c = {
        "dataset": "t", "version": "1.0", "owner": "tests",
        "source": {"kind": "cdc", "table": "bronze.x", "topic": "telecom.x", "key": ["id"]},
        "columns": [{"name": "id", "type": "bigint", "required": True},
                    {"name": "code", "type": "string"},
                    {"name": "amount", "type": "decimal(10,2)"}],
        "slo": {"min_pass_rate": 0.97, "circuit_breaker_min_pass_rate": 0.5},
        "rules": [{"id": "ID_NOT_NULL", "type": "not_null", "column": "id", "dimension": "completeness",
                   "severity": "critical", "action": "reject"}],
    }
    c.update(overrides)
    return c


def with_rule(**rule):
    base = {"id": "R1", "dimension": "validity", "severity": "medium", "action": "reject"}
    c = minimal_contract()
    c["rules"].append({**base, **rule})
    return c


# ---------------------------------------------------------------- the real contracts

def test_contracts_are_loaded_and_disabled_ones_skipped():
    assert "crm_customers" in CONTRACTS and "network_cdr" in CONTRACTS
    assert "app_events" not in CONTRACTS            # contract/_app_events.yaml is disabled by its "_" prefix
    assert (CONTRACT_DIR / "_app_events.yaml").exists()


@pytest.mark.parametrize("name", sorted(CONTRACTS))
def test_every_repo_contract_is_valid(name):
    assert lib.validate_contract(CONTRACTS[name]) == []


@pytest.mark.parametrize("name", sorted(CONTRACTS))
def test_every_contract_sets_both_slo_limits(name):
    slo = CONTRACTS[name].get("slo", {})
    assert 0 <= slo["circuit_breaker_min_pass_rate"] <= slo["min_pass_rate"] <= 1


FIX_CASES = [(name, fx, given, expected)
             for name, c in sorted(CONTRACTS.items())
             for fx in c.get("fixes", [])
             for given, expected in fx.get("examples", [])]


@pytest.mark.parametrize("name,fx,given,expected", FIX_CASES,
                         ids=[f"{n}.{fx['column']}:{g!r}" for n, fx, g, _ in FIX_CASES])
def test_contract_fix_examples(name, fx, given, expected):
    got = lib.apply_fix_steps_py(given, fx["steps"])
    assert got == expected
    for r in CONTRACTS[name]["rules"]:
        if r.get("type") == "regex" and r.get("column") == fx["column"]:
            assert re.search(r["pattern"], got), f"fixed value {got!r} still fails {r['id']}"


UNFIXABLE = [(name, fx, bad) for name, c in sorted(CONTRACTS.items())
             for fx in c.get("fixes", []) for bad in fx.get("unfixable", [])]


@pytest.mark.parametrize("name,fx,bad", UNFIXABLE, ids=[f"{n}.{fx['column']}:{b!r}" for n, fx, b in UNFIXABLE])
def test_unfixable_values_stay_invalid(name, fx, bad):
    got = lib.apply_fix_steps_py(bad, fx["steps"])
    for r in CONTRACTS[name]["rules"]:
        if r.get("type") == "regex" and r.get("column") == fx["column"]:
            assert not re.search(r["pattern"], got or ""), f"{bad!r} became valid: {got!r}"


# ---------------------------------------------------------------- validate_contract catches bad contracts

def test_minimal_contract_is_valid():
    assert lib.validate_contract(minimal_contract()) == []


@pytest.mark.parametrize("contract,expected", [
    ({k: v for k, v in minimal_contract().items() if k != "owner"}, "missing key 'owner'"),
    (minimal_contract(columns=[{"name": "id", "type": "bigint"}, {"name": "id", "type": "string"}]),
     "duplicate column names"),
    (minimal_contract(columns=[{"name": "id", "type": "integer"}]), "unknown type integer"),
    (minimal_contract(source={"kind": "parquet", "table": "x"}), "source.kind must be csv, json or cdc"),
    (minimal_contract(source={"kind": "csv", "table": "x"}), "source.file_source is required"),
    (minimal_contract(source={"kind": "cdc", "table": "x", "topic": "t", "key": ["nope"]}),
     "source.key column nope not in columns"),
    (with_rule(type="not_null", column="missing"), "column missing not in contract columns"),
    (with_rule(type="regex", column="code", pattern="([A-Z"), "bad regex"),
    (with_rule(type="domain", column="code"), "domain needs 'values'"),
    (with_rule(type="range", column="amount"), "range needs min and/or max"),
    (with_rule(type="compare", left="id", op="<>", right="amount"), "compare op must be one of"),
    (with_rule(type="compare", left="id", op="<", right="missing"), "compare columns must be contract columns"),
    (with_rule(type="unique", keys=[]), "unique needs 'keys'"),
    (with_rule(type="expr"), "expr needs 'valid_when'"),
    (with_rule(type="magic", column="id"), "unknown rule type magic"),
    (with_rule(type="not_null", column="id", action="delete"), "action must be one of"),
    (with_rule(type="not_null", column="id", dimension="vibes"), "unknown dimension vibes"),
    (with_rule(type="not_null", column="id", severity="urgent"), "unknown severity urgent"),
    (with_rule(id="ID_NOT_NULL", type="not_null", column="id"), "rule id missing or duplicated: ID_NOT_NULL"),
    (minimal_contract(slo={"min_pass_rate": 1.5}), "slo.min_pass_rate must be a number between 0 and 1"),
    (minimal_contract(slo={"min_pass_rate": 0.9, "circuit_breaker_min_pass_rate": "half"}),
     "slo.circuit_breaker_min_pass_rate must be a number"),
    (minimal_contract(slo={"min_pass_rate": 0.5, "circuit_breaker_min_pass_rate": 0.8}),
     "must not be above slo.min_pass_rate"),
    (minimal_contract(fixes=[{"column": "code", "steps": [{"op": "titlecase"}]}]), "unknown op titlecase"),
    (minimal_contract(fixes=[{"column": "nope", "steps": [{"op": "trim"}]}]), "fix column nope not in contract"),
    (minimal_contract(monitors={"freshness": {"frequency": "weekly"}}), "monitors.freshness needs"),
])
def test_validate_contract_reports_errors(contract, expected):
    errs = lib.validate_contract(contract)
    assert any(expected in e for e in errs), errs


def test_validate_contract_does_not_change_the_contract():
    c = minimal_contract()
    before = copy.deepcopy(c)
    lib.validate_contract(c)
    assert c == before


# ---------------------------------------------------------------- expand_rules

def test_expand_rules_adds_type_rule_for_every_non_string_column():
    rules = lib.expand_rules(minimal_contract())
    type_rules = {r["id"]: r for r in rules if r["type"] == "type"}
    assert set(type_rules) == {"TYPE_ID", "TYPE_AMOUNT"}          # 'code' is a string: no type rule
    assert all(r["action"] == "reject" and r["dimension"] == "validity" for r in type_rules.values())
    assert [r["id"] for r in rules][:2] == ["TYPE_ID", "TYPE_AMOUNT"]  # type rules come first


def test_expand_rules_type_action_can_be_set_in_the_contract():
    rules = lib.expand_rules(minimal_contract(type_action="flag"))
    assert {r["action"] for r in rules if r["type"] == "type"} == {"flag"}


def test_expand_rules_fills_defaults():
    c = minimal_contract(rules=[{"id": "R", "type": "not_null", "column": "code", "dimension": "completeness"}])
    r = [x for x in lib.expand_rules(c) if x["id"] == "R"][0]
    assert r["severity"] == "medium" and r["action"] == "reject" and r["description"] == "not_null on code"


def test_expand_rules_can_leave_out_uniqueness():
    c = with_rule(type="unique", keys=["code"])
    assert any(r["type"] == "unique" for r in lib.expand_rules(c))
    assert not any(r["type"] == "unique" for r in lib.expand_rules(c, include_unique=False))


@pytest.mark.parametrize("t,ok", [("string", True), ("bigint", True), ("date_days", True), ("decimal(10,2)", True),
                                  ("decimal(10)", False), ("varchar", False), (None, False)])
def test_is_valid_type(t, ok):
    assert lib.is_valid_type(t) is ok


# ---------------------------------------------------------------- fix steps (Python twin of fix_expression)

@pytest.mark.parametrize("value,expected", [
    ("  abc  ", "abc"), ("\tabc\n", "abc"), ("\r\nabc\x0b\x0c", "abc"),
    (" abc ", "abc"),                    # no-break space from Excel exports
    ("a b\tc", "a b\tc"),                          # trim never touches the inside of the value
    (" abc", " abc"),                    # other Unicode spaces are kept, exactly as Spark keeps them
    ("", ""),
])
def test_trim_strips_the_same_characters_as_spark(value, expected):
    assert lib.apply_fix_steps_py(value, [{"op": "trim"}]) == expected


def test_fix_steps_run_in_order():
    steps = [{"op": "trim"}, {"op": "upper"}, {"op": "regex_replace", "pattern": "-", "replacement": ""}]
    assert lib.apply_fix_steps_py(" ab-cd ", steps) == "ABCD"
    assert lib.apply_fix_steps_py("ABC", [{"op": "lower"}]) == "abc"


def test_fix_steps_keep_none():
    assert lib.apply_fix_steps_py(None, [{"op": "trim"}]) is None


@pytest.mark.parametrize("pattern,replacement,value,expected", [
    ("^0(6[0-9]{8})$", "+31$1", "0612345678", "+31612345678"),
    ("(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)", "$10-$1", "abcdefghij", "j-a"),   # two-digit group, Spark style
    ("^(\\w+)@(\\w+)$", "$2@$1", "user@host", "host@user"),
])
def test_regex_replace_uses_spark_group_syntax(pattern, replacement, value, expected):
    steps = [{"op": "regex_replace", "pattern": pattern, "replacement": replacement}]
    assert lib.apply_fix_steps_py(value, steps) == expected


# ---------------------------------------------------------------- lineage

def test_impacted_tables_follows_the_whole_chain():
    edges = [("a", "b"), ("b", "c"), ("c", "d"), ("x", "y"), ("c", "b")]   # includes a cycle b <-> c
    assert lib.impacted_tables("a", edges) == ["b", "c", "d"]
    assert lib.impacted_tables("d", edges) == []


def test_lineage_from_contracts():
    edges = lib.edges_from_contracts(CONTRACTS)
    vt = lib.validated_table(CONTRACTS["network_cdr"])
    assert vt == "data_sentinel.silver.network_cdr_validated"
    assert ("data_sentinel.bronze.network_cdr_raw", vt) in edges
    assert "data_sentinel.silver.fact_usage" in lib.impacted_tables(vt, edges)


def test_table_names():
    c = CONTRACTS["crm_customers"]
    assert lib.quarantine_table(c) == "data_sentinel.quarantine.crm_customers"
