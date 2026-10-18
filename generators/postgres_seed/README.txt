POSTGRES SEED PACKAGE
1. Keep all files in this folder.
2. Start Postgres:  docker compose up -d postgres
3. Run:  powershell -ExecutionPolicy Bypass -File .\run_all.ps1

Options:
  -SkipProblems   load clean data only
  -Container / -User / -Db   if your names differ (defaults: cdc-postgres / postgres / telecom)
load_postgres.ps1 -NoTruncate   keeps existing rows (only if the tables are empty or IDs do not clash)

Expected counts after run_all (with problems): customers 1030, accounts 1010, invoices 3000,
invoice_lines 3886, payments 2679, plans 20, devices 1000.
Clean load only: customers 1000, accounts 1000.
