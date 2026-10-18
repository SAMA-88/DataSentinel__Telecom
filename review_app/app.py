"""Quarantine review app: sign in with a password, view quarantined rows, correct them, send them to recheck.

    streamlit run review_app/app.py

Corrected rows get status FIXED_PENDING_REVALIDATION; the next pipeline run (22_revalidate) applies the
contract rules again: rows that pass go to Silver, rows that still fail go back to REJECTED.
"""
import time
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

import db
from logic import (ALL_STATUSES, EDITABLE_STATUSES, allowed_datasets, authenticate, diff_rows, load_contracts,
                   load_users)

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
USERS_FILE = HERE / "users.json"
MAX_ATTEMPTS, LOCK_SECONDS, IDLE_LOGOUT_SECONDS = 5, 300, 1800

load_dotenv(ROOT / ".env")
load_dotenv(ROOT / "airflow" / ".env")  # DATABRICKS_TOKEN / DATABRICKS_HTTP_PATH already live here

st.set_page_config(page_title="DataSentinel · Quarantine review", page_icon="🛡️", layout="wide")
ss = st.session_state


def login_screen():
    st.title("🛡️ Quarantine review")
    st.caption("Authorized users only. Sign in to view and correct quarantined rows.")
    users = load_users(USERS_FILE)
    if not users:
        st.error("No users configured. Create one with:  python review_app/manage_users.py add <name> --role admin")
        st.stop()
    locked_until = ss.get("locked_until", 0)
    if time.time() < locked_until:
        st.error(f"Too many failed attempts. Try again in {int(locked_until - time.time())} s.")
        st.stop()
    with st.form("login"):
        username = st.text_input("User")
        password = st.text_input("Password", type="password")
        if st.form_submit_button("Sign in", type="primary"):
            user = authenticate(users, username.strip(), password)
            if user:
                ss.update(user=username.strip(), role=user.get("role", "owner"), datasets=user.get("datasets", []),
                          failed=0, last_seen=time.time())
                st.rerun()
            ss["failed"] = ss.get("failed", 0) + 1
            if ss["failed"] >= MAX_ATTEMPTS:
                ss["locked_until"], ss["failed"] = time.time() + LOCK_SECONDS, 0
            st.error("Wrong user or password.")
    st.stop()


@st.cache_resource
def get_conn():
    return db.connect()


if "user" not in ss or time.time() - ss.get("last_seen", 0) > IDLE_LOGOUT_SECONDS:
    ss.pop("user", None)
    login_screen()
ss["last_seen"] = time.time()

contracts = load_contracts(ROOT / "contract")
datasets = allowed_datasets({"role": ss["role"], "datasets": ss["datasets"]}, sorted(contracts))

with st.sidebar:
    st.markdown(f"**{ss['user']}** · {ss['role']}")
    if st.button("Sign out"):
        ss.clear()
        st.rerun()
    if not datasets:
        st.warning("Your account has no datasets assigned.")
        st.stop()
    dataset = st.selectbox("Dataset", datasets)
    statuses = st.multiselect("Status", ALL_STATUSES, default=list(EDITABLE_STATUSES))
    limit = st.slider("Max rows", 50, 1000, 200, step=50)

try:
    conn = get_conn()
except Exception as e:  # missing env vars or warehouse not reachable
    st.error(f"Cannot connect to Databricks: {e}")
    st.stop()

st.title(f"Quarantine · `{dataset}`")
if not db.table_exists(conn, dataset):
    st.info("No quarantine table for this dataset yet (the quality gate has not quarantined anything).")
    st.stop()

counts = db.status_counts(conn, dataset)
for col, s in zip(st.columns(len(ALL_STATUSES)), ALL_STATUSES):
    col.metric(s.replace("_", " ").title(), counts.get(s, 0))

columns = contracts[dataset]
rows = db.fetch_rows(conn, dataset, columns, statuses, limit)
if not rows:
    st.success("No rows with the selected status.")
    st.stop()

st.caption("Edit the values, then press **Save & send to recheck**. Rows with status REVALIDATED_OK are read-only. "
           "The next pipeline run rechecks the corrected rows with the contract rules.")
original = pd.DataFrame(rows)
locked_cols = [c for c in original.columns if c not in columns]
edited = st.data_editor(original, disabled=locked_cols, hide_index=True, use_container_width=True,
                        key=f"editor_{dataset}")

editable_ids = {r["_row_id"] for r in rows if r["_quarantine_status"] in EDITABLE_STATUSES}
changes = [e for e in diff_rows(rows, edited.to_dict("records"), columns) if e["row_id"] in editable_ids]
st.write(f"**{len(changes)}** row(s) changed.")
if st.button("Save & send to recheck", type="primary", disabled=not changes):
    n = db.save_fixes(conn, dataset, changes, ss["user"])
    st.success(f"{n} row(s) saved and marked FIXED_PENDING_REVALIDATION. They will be rechecked on the next run.")
    time.sleep(1)
    st.rerun()

with st.expander("Edit history"):
    hist = db.recent_edits(conn, dataset)
    if hist:
        st.dataframe(pd.DataFrame(hist), hide_index=True, use_container_width=True)
    else:
        st.write("No edits yet.")
