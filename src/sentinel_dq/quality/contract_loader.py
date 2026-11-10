"""Load the quality library and the data contracts outside Databricks (no Spark, no Java needed).

The quality logic lives in one place only: databricks/notebooks/apply_quality/11_dq_lib.py, because the
Databricks notebooks load it with `%run ./11_dq_lib`. This module runs that same file locally with stand-ins
for pyspark and delta, so local tools test exactly the code that runs in production:

    scripts/validate_contracts.py   checks the contracts before they are uploaded
    tests/unit/test_rules.py        unit tests for the pure-Python parts (contracts, fixes, lineage)

Only the pure-Python functions are meaningful this way. The Spark parts (rule_fail, apply_rules,
fix_expression) are tested on Databricks by the notebook 13_rule_tests.
"""
import sys
import types
from functools import lru_cache
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[3]
LIB_PATH = ROOT / "databricks" / "notebooks" / "apply_quality" / "11_dq_lib.py"
CONTRACT_DIR = ROOT / "contract"


def _spark_stubs():
    m = mock.MagicMock()
    stubs = {
        "delta": {},
        "delta.tables": {"DeltaTable": m},
        "pyspark": {},
        "pyspark.sql": {"Window": m, "functions": m},
        "pyspark.sql.types": {k: m for k in ("DoubleType", "LongType", "StringType", "StructField",
                                             "StructType", "TimestampType")},
    }
    modules = {}
    for name, attrs in stubs.items():
        mod = types.ModuleType(name)
        mod.__dict__.update(attrs)
        modules[name] = mod
    return modules


@lru_cache(maxsize=None)
def load_dq_lib(path=LIB_PATH):
    """Return 11_dq_lib as a module object (lib.load_contracts, lib.validate_contract, ...)."""
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"quality library not found: {path}")
    lib = types.ModuleType("dq_lib")
    lib.__file__ = str(path)
    lib.spark = mock.MagicMock()           # the notebooks get `spark` from Databricks
    with mock.patch.dict(sys.modules, _spark_stubs()):
        exec(compile(path.read_text(encoding="utf-8"), str(path), "exec"), lib.__dict__)
    return lib


def load_contracts(directory=CONTRACT_DIR):
    """The repo's contracts, read with the same load_contracts the notebooks use."""
    return load_dq_lib().load_contracts(str(directory))
