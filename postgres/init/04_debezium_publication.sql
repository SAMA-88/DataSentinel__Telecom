CREATE PUBLICATION crm_publication
FOR TABLE
    crm.plans,
    crm.customers,
    crm.devices;-- full row images, so Debezium sends the "before" value on UPDATE/DELETE (needed for SCD2)
ALTER TABLE crm.plans              REPLICA IDENTITY FULL;
ALTER TABLE crm.customers          REPLICA IDENTITY FULL;
ALTER TABLE crm.devices            REPLICA IDENTITY FULL;
ALTER TABLE billing.accounts       REPLICA IDENTITY FULL;
ALTER TABLE billing.invoices       REPLICA IDENTITY FULL;
ALTER TABLE billing.invoice_lines  REPLICA IDENTITY FULL;
ALTER TABLE billing.payments       REPLICA IDENTITY FULL;

-- the list of tables Debezium is allowed to stream
CREATE PUBLICATION dbz_publication FOR TABLE
    crm.plans, crm.customers, crm.devices,
    billing.accounts, billing.invoices, billing.invoice_lines, billing.payments;