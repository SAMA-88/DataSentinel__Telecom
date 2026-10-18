-- STAGING: invoices from the billing database AND from the monthly billing file, in one list.
-- The column "source" says where each row came from. The reconciliation compares the two sources.
-- Inputs: silver.account, silver.invoice_line, silver.invoice, silver.invoice_file

-- Step 1: the accounts (we need the phone number and account id)
with account as (
    select account_id, account_ref, msisdn_hash
    from {{ source('silver', 'account') }}
),

-- Step 2: add up the lines of each invoice
invoice_lines as (
    select
        invoice_id,
        sum(amount) as lines_total,
        count(*) as lines_count
    from {{ source('silver', 'invoice_line') }}
    group by invoice_id
),

-- Step 3: one account_id per account_ref (the billing file only has the reference)
account_by_ref as (
    select account_ref, min(account_id) as account_id
    from account
    group by account_ref
),

-- Step 4: invoices from the billing database
db_invoices as (
    select
        'billing_db' as source,
        i.invoice_id,
        i.account_id,
        a.account_ref,
        a.msisdn_hash,
        i.period_start,
        i.period_end,
        cast(null as decimal(10,2)) as plan_fee,
        cast(null as decimal(10,2)) as overage,
        i.total_amount,
        i.status,
        i.issued_at,
        l.lines_total,
        l.lines_count
    from {{ source('silver', 'invoice') }} i
    left join account a on i.account_id = a.account_id
    left join invoice_lines l on i.invoice_id = l.invoice_id
),

-- Step 5: invoices from the billing file
file_invoices as (
    select
        'billing_file' as source,
        f.invoice_id,
        r.account_id,
        f.account_ref,
        f.msisdn_hash,
        f.period_start,
        f.period_end,
        f.plan_fee,
        f.overage,
        f.total_amount,
        f.status,
        cast(null as timestamp) as issued_at,
        cast(null as decimal(10,2)) as lines_total,
        cast(null as bigint) as lines_count
    from {{ source('silver', 'invoice_file') }} f
    left join account_by_ref r on f.account_ref = r.account_ref
)

-- Step 6: put both lists together
select * from db_invoices
union all
select * from file_invoices
