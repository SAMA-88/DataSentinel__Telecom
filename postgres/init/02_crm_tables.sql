CREATE TABLE crm.plans (
    plan_id       SERIAL PRIMARY KEY,
    plan_name     TEXT NOT NULL,
    monthly_fee   NUMERIC(8,2) NOT NULL,
    data_gb       INT,
    voice_minutes INT
);

CREATE TABLE crm.customers (
    customer_id BIGSERIAL PRIMARY KEY,
    first_name  TEXT,
    last_name   TEXT,
    email       TEXT,
    msisdn      TEXT,
    city        TEXT,
    country     CHAR(2),
    status      TEXT DEFAULT 'ACTIVE',
    plan_id     INT REFERENCES crm.plans(plan_id),
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE crm.devices (
    device_id   BIGSERIAL PRIMARY KEY,
    customer_id BIGINT REFERENCES crm.customers(customer_id),
    imei        TEXT,
    model       TEXT,
    created_at  TIMESTAMPTZ DEFAULT now()
);

-- keep updated_at fresh on every update
CREATE FUNCTION crm.set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--Every time a customer is updated, automatically run set_updated_at() before saving the change.
CREATE TRIGGER trg_customers_updated
BEFORE UPDATE ON crm.customers
FOR EACH ROW EXECUTE FUNCTION crm.set_updated_at();


