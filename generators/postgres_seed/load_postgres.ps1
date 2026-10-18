# Loads the 7 CSV files into the crm and billing tables.
# Usage:  powershell -ExecutionPolicy Bypass -File .\load_postgres.ps1
# Optional: -Container cdc-postgres -User postgres -Db telecom -NoTruncate
param(
  [string]$Container = "cdc-postgres",
  [string]$User = "postgres",
  [string]$Db = "telecom",
  [switch]$NoTruncate
)
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

$running = docker ps --format "{{.Names}}" | Where-Object { $_ -eq $Container }
if (-not $running) { throw "Container '$Container' is not running. Start it: docker compose up -d postgres" }

function Run($sql) {
  docker exec -i $Container psql -U $User -d $Db -v ON_ERROR_STOP=1 -c $sql
  if ($LASTEXITCODE -ne 0) { throw "SQL failed: $sql" }
}

foreach ($f in "plans","customers","devices","accounts","invoices","invoice_lines","payments") {
  docker cp ".\$f.csv" "${Container}:/tmp/$f.csv"
}

if (-not $NoTruncate) {
  Run "TRUNCATE crm.plans, crm.customers, crm.devices, billing.accounts, billing.invoices, billing.invoice_lines, billing.payments RESTART IDENTITY CASCADE;"
}

Run "COPY crm.plans (plan_id,plan_name,monthly_fee,data_gb,voice_minutes) FROM '/tmp/plans.csv' CSV HEADER;"
Run "COPY crm.customers (customer_id,first_name,last_name,email,msisdn,city,country,status,plan_id,created_at,updated_at) FROM '/tmp/customers.csv' CSV HEADER;"
Run "COPY crm.devices (device_id,customer_id,imei,model,created_at) FROM '/tmp/devices.csv' CSV HEADER;"
Run "COPY billing.accounts (account_id,msisdn,account_ref,status,created_at,updated_at) FROM '/tmp/accounts.csv' CSV HEADER;"
Run "COPY billing.invoices (invoice_id,account_id,period_start,period_end,total_amount,status,issued_at) FROM '/tmp/invoices.csv' CSV HEADER;"
Run "COPY billing.invoice_lines (line_id,invoice_id,line_type,description,amount) FROM '/tmp/invoice_lines.csv' CSV HEADER;"
Run "COPY billing.payments (payment_id,invoice_id,amount,method,status,paid_at) FROM '/tmp/payments.csv' CSV HEADER;"

foreach ($s in "crm.plans:plan_id","crm.customers:customer_id","crm.devices:device_id","billing.accounts:account_id","billing.invoices:invoice_id","billing.invoice_lines:line_id","billing.payments:payment_id") {
  $t,$col = $s.Split(":")
  Run "SELECT setval(pg_get_serial_sequence('$t','$col'), (SELECT max($col) FROM $t));" | Out-Null
}
Write-Host "Load finished." -ForegroundColor Green
