import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "review_app"))

from logic import SOURCE_GROUPS, allowed_datasets, authenticate, diff_rows, hash_password, load_contracts, safe_name  # noqa: E402


def test_password_hash_and_verify():
    users = {"admin": {**hash_password("correct horse battery"), "role": "admin", "datasets": ["*"]}}
    assert authenticate(users, "admin", "correct horse battery")
    assert authenticate(users, "admin", "wrong") is None
    assert authenticate(users, "nobody", "correct horse battery") is None
    assert "correct" not in str(users)  # only the hash is stored


def test_allowed_datasets():
    all_ds = ["billing_payments", "crm_customers"]
    assert allowed_datasets({"role": "admin", "datasets": ["*"]}, all_ds) == all_ds
    assert allowed_datasets({"role": "owner", "datasets": ["billing_payments"]}, all_ds) == ["billing_payments"]
    assert allowed_datasets({"role": "owner", "datasets": []}, all_ds) == []


def test_diff_rows_returns_full_map_and_changes():
    cols = ["payment_id", "method", "amount"]
    original = [{"_row_id": "a", "payment_id": "1", "method": None, "amount": "35.00"},
                {"_row_id": "b", "payment_id": "2", "method": None, "amount": "48.50"}]
    edited = [{"_row_id": "a", "payment_id": "1", "method": "IDEAL", "amount": "35.00"},
              {"_row_id": "b", "payment_id": "2", "method": float("nan"), "amount": "48.50"}]
    out = diff_rows(original, edited, cols)
    assert len(out) == 1
    assert out[0]["values"] == {"payment_id": "1", "method": "IDEAL", "amount": "35.00"}
    assert out[0]["changes"] == [("method", None, "IDEAL")]


def test_contracts_load_and_names_are_safe():
    contracts = load_contracts(ROOT / "contract")
    assert "billing_payments" in contracts and "method" in contracts["billing_payments"]
    try:
        safe_name("x; DROP TABLE y")
        assert False
    except ValueError:
        pass


def test_every_dataset_belongs_to_exactly_one_source_user():
    datasets = sorted(load_contracts(ROOT / "contract"))
    grouped = sorted(d for ds in SOURCE_GROUPS.values() for d in ds)
    assert grouped == datasets
