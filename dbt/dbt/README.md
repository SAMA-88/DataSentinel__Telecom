# dbt_simple - the Gold layer, written for beginners

dbt reads the Silver tables (made by the notebooks) and builds the Gold star schema and the dashboard tables.

    Silver tables -> staging (views) -> dims -> intermediate -> facts -> marts -> dashboards

## What is in each folder
| Folder | What it is | Built as |
|---|---|---|
| models/staging | stg_invoices, stg_payments: put the billing DB rows and billing file rows into one table (column `source`) | view |
| models/dims | dim_date, dim_plan, dim_customer: one row per thing, with a surrogate key. Key -1 = "unknown" | table |
| models/intermediate | int_customer_by_phone: one customer version per phone number (used by network facts) | view |
| models/facts | fact_usage, fact_invoice, fact_payment, fact_app_events, fact_partner_usage: one row per event, joined to the dims | table |
| models/marts | 12 small tables, one per dashboard panel | table |
| macros | generate_schema_name (use the schema name exactly), quarantine_union (stack all quarantine tables) | - |
| tests | 4 SQL checks. A test passes when it returns 0 rows | - |

## Read the models in this order
1. dim_date.sql - simplest, just a calendar.
2. dim_plan.sql - Silver plan table + a surrogate key.
3. dim_customer.sql - same, plus the -1 unknown row and the plan link.
4. facts/fact_invoice.sql - a fact: select columns, join to dim_customer, unknown customer becomes -1.
5. facts/fact_usage.sql - same, but joined by time (which customer version was valid on that day).
6. marts/mart_cell_quality.sql - a mart is just group by + count/sum.

Two ideas to remember:
- `{{ source('silver','table') }}` = a table made OUTSIDE dbt. `{{ ref('model') }}` = a model made BY dbt (dbt works out the order).
- Surrogate key = a number made with xxhash64 from the real id, so every dim row has one stable key.

## Run it (Windows PowerShell)
    cd dbt_simple
    python -m venv .venv ; .\.venv\Scripts\Activate.ps1
    pip install -r requirements.txt
    mkdir $env:USERPROFILE\.dbt -Force
    copy profiles.yml.example $env:USERPROFILE\.dbt\profiles.yml     # edit http_path
    $env:DATABRICKS_TOKEN = "dapi..."
    dbt debug        # must end with "All checks passed"
    dbt build        # models + tests in the right order

Needs a Databricks SQL warehouse (http_path is on its Connection details tab).
Run the Silver notebooks and the reconciliation first, and the incident phase once (so monitoring.incident_ops exists).

Then run verify/95_gold_verify.py as a notebook in Databricks.

## Known difference from the first version
int_customer_by_phone picks the lowest customer_id when one phone number belongs to several customers.
