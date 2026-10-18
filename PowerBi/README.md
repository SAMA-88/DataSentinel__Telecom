# DataSentinel Power BI project

Open `model/DataSentinel.pbip` in Power BI Desktop. The PBIP references the
`DataSentinel.Report` and `DataSentinel.SemanticModel` folders.

## Current state

- The semantic model contains schema definitions for the 12 `mart_*` tables
  and the `Measures` table.
- The supplied DAX measures are defined in `Measures`.
- The report has six pages in the requested order: Overview, Data Quality,
  Revenue Assurance, Incidents, Customer 360, and Network and App. A seventh
  hidden drill-through page provides the customer profile.
- Report visuals are defined for the cards, charts, slicers, and tables in the
  supplied page specification. Overview KPI cards navigate to their detail
  pages. Customer IDs support drill-through to the hidden profile page.
- Mart partitions navigate to their corresponding tables in the configured
  Databricks catalog and schema.
- No relationships are defined because the reporting marts are already shaped
  for their pages and the specification says no joins are needed.

## Required to load data

Authenticate with the Databricks connector through Power BI's local sign-in
flow, then refresh the model and validate source column types. The mart health
data determines the exact status values to exclude from the failing-checks
table; do not apply that filter until values are verified. After refresh, set
the unpaid-customer table to Top N 20 and apply a minimum voice-call threshold
to the cell chart so low-volume cells do not dominate the ranking. Add the
status-specific red/blue formatting and the matrix pass-rate color scale in
Desktop after confirming the loaded values; status labels are already present.