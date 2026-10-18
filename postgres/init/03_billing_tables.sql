CREATE TABLE billing.accounts (
    account_id  BIGSERIAL PRIMARY KEY,   -- billing's own ID, different from CRM
    msisdn      TEXT,
    account_ref TEXT,
    status      TEXT DEFAULT 'OPEN',
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE billing.invoices (
    invoice_id   BIGSERIAL PRIMARY KEY,
    account_id   BIGINT REFERENCES billing.accounts(account_id),
    period_start DATE,
    period_end   DATE,
    total_amount NUMERIC(10,2),
    status       TEXT DEFAULT 'ISSUED',
    issued_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE billing.invoice_lines (
    line_id     BIGSERIAL PRIMARY KEY,
    invoice_id  BIGINT REFERENCES billing.invoices(invoice_id),
    line_type   TEXT,        -- PLAN_FEE, DATA_OVERAGE, VOICE, ROAMING
    description TEXT,
    amount      NUMERIC(10,2)
);

CREATE TABLE billing.payments (
    payment_id BIGSERIAL PRIMARY KEY,
    invoice_id BIGINT REFERENCES billing.invoices(invoice_id),
    amount     NUMERIC(10,2),
    method     TEXT,
    status     TEXT,
    paid_at    TIMESTAMPTZ DEFAULT now()
);


-- keep updated_at fresh on every update
CREATE FUNCTION billing.set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--Every time a customer is updated, automatically run set_updated_at() before saving the change.
CREATE TRIGGER trg_customers_updated
BEFORE UPDATE ON billing.accounts
FOR EACH ROW EXECUTE FUNCTION billing.set_updated_at();