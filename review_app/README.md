# Quarantine review app

A password-protected web UI where an admin (or a dataset owner) views quarantined rows, corrects them and sends them back for a recheck.

## How it fits the pipeline

1. The quality gate (`20_quality_gate`) puts failing rows in `data_sentinel.quarantine.<dataset>`.
2. In this app the user corrects the values and presses **Save & send to recheck**. The app writes the full corrected row to `_fixed` and sets `_quarantine_status = FIXED_PENDING_REVALIDATION`. That's the same thing `21_quarantine_autofix` does.
3. The next pipeline run (`22_revalidate`) applies the contract rules again. Rows that pass go to `silver.<dataset>_validated` (`_dq_fixed = true`); rows that still fail go back to `REJECTED`.
4. Every changed cell is logged in `data_sentinel.monitoring.quarantine_edits` (who, when, row, column, old value, new value).

No notebook was changed.

## Setup (once)

```powershell
.venv\Scripts\pip install -r review_app\requirements.txt
.venv\Scripts\python review_app\manage_users.py add admin --role admin
.venv\Scripts\python review_app\manage_users.py sources      # one user per data source, each with its own password
```

`sources` creates these users (one password prompt each). Each one sees and edits only its own datasets:

| User | Datasets |
|---|---|
| `crm` | crm_customers, crm_plans |
| `billing` | billing_accounts, billing_invoices, billing_invoice_lines, billing_payments, invoices_file, payments_file |
| `network` | network_cdr |
| `app` | app_events |
| `partner` | partner_usage |

Other commands: `passwd <user>` changes a password, `remove <user>` deletes a user, `list` shows all users, `sources --only billing` creates just one of them. The groups are defined in `SOURCE_GROUPS` in `logic.py`.

Passwords are typed interactively (minimum 10 characters) and stored only as salted PBKDF2 hashes in `review_app/users.json`, which is git-ignored.

The Databricks connection uses `DATABRICKS_TOKEN` and `DATABRICKS_HTTP_PATH` from `airflow/.env` (or `.env` at the repo root). `DATABRICKS_HOST` defaults to the workspace already used by Airflow and dbt.

## Run

```powershell
.venv\Scripts\streamlit run review_app\app.py
```

Then open http://localhost:8501.

## Access rules

| Role | Sees | Can edit |
|---|---|---|
| `admin` | every dataset | rows in PENDING_FIX, REJECTED, FIXED_PENDING_REVALIDATION |
| `owner` | only the datasets listed for that user | same |

Rows already in REVALIDATED_OK are read-only. After 5 wrong passwords the session is locked for 5 minutes. Users are signed out after 30 minutes without activity.

## Security notes

- The app's own password check is the access control. The app talks to Databricks with one token, the one in `.env`, so protect that token and run the app only on a trusted machine or network. Streamlit serves plain HTTP.
- For stronger isolation in Databricks itself, also grant each team `SELECT`/`MODIFY` only on its own `quarantine.*` tables in Unity Catalog, and give each team its own token.

## Tests

```powershell
.venv\Scripts\python -m pytest -q tests\unit\test_review_app.py
```
