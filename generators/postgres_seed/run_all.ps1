# One command: load the clean data, inject the quality problems, print the counts.
# Usage:  powershell -ExecutionPolicy Bypass -File .\run_all.ps1
param([string]$Container = "cdc-postgres", [string]$User = "postgres", [string]$Db = "telecom", [switch]$SkipProblems)
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

& "$PSScriptRoot\load_postgres.ps1" -Container $Container -User $User -Db $Db

if (-not $SkipProblems) {
  Get-Content "$PSScriptRoot\inject_problems.sql" -Raw | docker exec -i $Container psql -U $User -d $Db -v ON_ERROR_STOP=1
  if ($LASTEXITCODE -ne 0) { throw "inject_problems.sql failed (rolled back, the clean data is still loaded)" }
}

$q = @"
SELECT 'crm.plans' AS tbl, count(*) FROM crm.plans UNION ALL
SELECT 'crm.customers', count(*) FROM crm.customers UNION ALL
SELECT 'crm.devices', count(*) FROM crm.devices UNION ALL
SELECT 'billing.accounts', count(*) FROM billing.accounts UNION ALL
SELECT 'billing.invoices', count(*) FROM billing.invoices UNION ALL
SELECT 'billing.invoice_lines', count(*) FROM billing.invoice_lines UNION ALL
SELECT 'billing.payments', count(*) FROM billing.payments;
"@
docker exec -i $Container psql -U $User -d $Db -c $q
