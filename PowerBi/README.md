# DataSentinel Power BI project

Open `model/DataSentinel.pbip` in Power BI Desktop. The PBIP references the
`DataSentinel.Report` and `DataSentinel.SemanticModel` folders.

## Current state

- The semantic model imports the 12 `gold.mart_*` tables from Databricks.
  Next to them are three model-only tables:
  - `_Measures`: all DAX measures.
  - `Attention`: a calculated table that lists failing rules, breached or
    unassigned incidents and failing reconciliation checks, ordered by severity.
  - `Targets`: the business targets and critical thresholds (DQ pass rate, drop
    rate, SLA compliance, collection rate, crash share). Change a target here
    and every measure, status pill and color that uses it follows.
- The report has six pages in this order: Overview, Data Quality,
  Revenue Assurance, Incidents, Customer 360, and Network and App. A seventh
  hidden drill-through page provides the customer profile.
- Overview KPI cards navigate to their detail pages. Customer IDs support
  drill-through to the hidden profile page.
- Relationships (6):
  - `mart_recon_mismatches` and `mart_revenue_assurance_trend` link to
    `mart_revenue_assurance` on `check_id`, so selecting a check filters its
    cases and trend.
  - `mart_app_usage`, `mart_dq_trend`, `mart_network_daily` and
    `mart_partner_usage` each link to their own auto date table. There is no
    shared date table yet, so a date slicer filters only its own mart.

## Loading data

- The imported data is cached in `DataSentinel.SemanticModel/.pbi/cache.abf`.
  That file is gitignored, so a fresh clone opens with empty visuals.
- To load data: make sure the Databricks SQL warehouse is running and
  `dbt build` has created the `gold.mart_*` tables, then use Home > Refresh in
  Desktop and sign in with a Databricks personal access token.
- Calculated columns and the `Attention` and `Targets` tables are computed at
  refresh time, so values that depend on today's date (the payment age bands)
  are as of the last refresh.

## Still to do in Desktop

- Verify the loaded status values in `mart_dq_health` and `mart_incidents`.
  `Failing Rules` counts every status other than `PASS`, and `Open Incidents`
  counts `status = "OPEN"`.
- Set the unpaid-customer table to Top N 20.
- Apply a minimum voice-call threshold to the cell chart so low-volume cells do
  not dominate the ranking.

## About tools/build_report.py

The report was edited in Power BI Desktop after `tools/build_report.py`
generated it. The script still produces the older layout, does not build the
Customer Profile page, and deletes every page folder before it writes. Do not
run it on this model unless it has first been updated to match the current
report.
