"""Pure helpers for the quarantine review app (no Streamlit, no Databricks), so they can be unit tested.

Passwords: PBKDF2-HMAC-SHA256 with a random salt per user; only the salt and hash are stored.
Edits: a corrected row is written to the quarantine table's `_fixed` map with status FIXED_PENDING_REVALIDATION,
exactly what 21_quarantine_autofix produces, so 22_revalidate rechecks it with the contract rules.
"""
import hashlib
import hmac
import json
import os
import re
from pathlib import Path

import yaml

ITERATIONS = 310_000
# One review-app user per data source; groups follow incident_routing in 70_incident_setup (CRM, Billing, ... teams).
SOURCE_GROUPS = {
    "crm": ["crm_customers", "crm_plans"],
    "billing": ["billing_accounts", "billing_invoices", "billing_invoice_lines", "billing_payments",
                "invoices_file", "payments_file"],
    "network": ["network_cdr"],
    # "app": ["app_events"],   # app_events contract disabled (contract/_app_events.yaml)
    "partner": ["partner_usage"],
}
EDITABLE_STATUSES = ("PENDING_FIX", "REJECTED", "FIXED_PENDING_REVALIDATION")
ALL_STATUSES = EDITABLE_STATUSES + ("REVALIDATED_OK",)
FIXED_STATUS = "FIXED_PENDING_REVALIDATION"
_SAFE_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


# ---------- passwords and users ----------

def hash_password(password: str, salt: bytes | None = None) -> dict:
    salt = salt or os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, ITERATIONS)
    return {"salt": salt.hex(), "hash": digest.hex(), "iterations": ITERATIONS}


def verify_password(password: str, record: dict) -> bool:
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), bytes.fromhex(record["salt"]),
                                 int(record.get("iterations", ITERATIONS)))
    return hmac.compare_digest(digest.hex(), record["hash"])


def load_users(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def save_users(path: Path, users: dict) -> None:
    path.write_text(json.dumps(users, indent=2), encoding="utf-8")


def authenticate(users: dict, username: str, password: str) -> dict | None:
    """Return the user record when the password matches, else None (same answer for unknown users)."""
    rec = users.get(username)
    if rec is None:
        verify_password(password, hash_password("dummy"))  # keep timing similar for unknown names
        return None
    return rec if verify_password(password, rec) else None


def allowed_datasets(user: dict, all_datasets: list[str]) -> list[str]:
    if user.get("role") == "admin" or "*" in user.get("datasets", []):
        return list(all_datasets)
    return [d for d in all_datasets if d in user.get("datasets", [])]


# ---------- contracts ----------

def load_contracts(contract_dir: Path) -> dict:
    """dataset name -> list of column names, from contract/*.yaml (same files the quality gate uses)."""
    out = {}
    for f in sorted(contract_dir.glob("*.yaml")):
        if f.name.startswith("_"):   # disabled contract, same rule as load_contracts in 11_dq_lib
            continue
        c = yaml.safe_load(f.read_text(encoding="utf-8"))
        name = c["dataset"]
        cols = [x["name"] for x in c["columns"]]
        if _SAFE_NAME.match(name) and all(_SAFE_NAME.match(x) for x in cols):
            out[name] = cols
    return out

def safe_name(name: str) -> str:
    if not _SAFE_NAME.match(name):
        raise ValueError(f"unsafe identifier: {name!r}")
    return name


# ---------- edits ----------

def _norm(v):
    if v is None:
        return None
    try:
        if v != v:  # NaN from pandas
            return None
    except Exception:
        pass
    s = str(v)
    return None if s == "" else s


def diff_rows(original: list[dict], edited: list[dict], columns: list[str]) -> list[dict]:
    """Compare rows by _row_id; return [{row_id, values (full map), changes [(col, old, new)]}] for changed rows."""
    before = {r["_row_id"]: r for r in original}
    out = []
    for r in edited:
        old = before.get(r["_row_id"])
        if old is None:
            continue
        changes = [(c, _norm(old.get(c)), _norm(r.get(c))) for c in columns if _norm(old.get(c)) != _norm(r.get(c))]
        if changes:
            out.append({"row_id": r["_row_id"], "values": {c: _norm(r.get(c)) for c in columns}, "changes": changes})
    return out
