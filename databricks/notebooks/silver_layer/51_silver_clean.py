# Databricks notebook source
# MAGIC %run ./49_silver_lib

# COMMAND ----------

# SILVER CLEAN TABLES: one row per business key (newest version), only rows that passed the quality gate,
# phone numbers replaced by their hash. Nothing is joined or aggregated here: that is Gold (dbt).
#   CDC datasets (Postgres)  -> newest change event per key
#   file / stream datasets   -> one row per id (duplicates were already rejected by the gate)

OPS = "_op IN ('r', 'c', 'u')"
LOADED = F.current_timestamp().alias("_loaded_at")


def with_hash(df):
    return df.withColumn("msisdn_hash", hash_pii("msisdn")).drop("msisdn")


# COMMAND ----------
acc = with_hash(latest_per_key(validated("billing_accounts").filter(OPS), ["account_id"]))
save(acc.select("account_id", "account_ref", "msisdn_hash", "status", "created_at", "updated_at", LOADED), T_ACCOUNT)

inv = latest_per_key(validated("billing_invoices").filter(OPS), ["invoice_id"])
save(inv.select("invoice_id", "account_id", "period_start", "period_end", "total_amount", "status", "issued_at", LOADED),
     T_INVOICE)

ln = latest_per_key(validated("billing_invoice_lines").filter(OPS), ["line_id"])
save(ln.select("line_id", "invoice_id", "line_type", "description", "amount", LOADED), T_INVOICE_LINE)

pay = latest_per_key(validated("billing_payments").filter(OPS), ["payment_id"])
save(pay.select("payment_id", "invoice_id", "amount", "method", "status", "paid_at", LOADED), T_PAYMENT)

# COMMAND ----------
fi = with_hash(latest_per_key(validated("invoices_file"), ["invoice_id"]))
save(fi.select("invoice_id", "account_ref", "msisdn_hash", "period_start", "period_end", "plan_fee", "overage",
               "total_amount", "status", LOADED), T_INVOICE_FILE)

pf = latest_per_key(validated("payments_file"), ["payment_id"])
save(pf.select("payment_id", "invoice_id", "amount", "method", "status", "paid_at", LOADED), T_PAYMENT_FILE)

# COMMAND ----------
net = with_hash(latest_per_key(validated("network_cdr"), ["cdr_id"]))
save(net.select("cdr_id", "msisdn_hash", "event_type", "event_ts", "duration_sec", "data_mb", "cell_id", "call_result",
                LOADED), T_NETWORK_CDR)

ev = latest_per_key(validated("app_events"), ["event_id"])
save(ev.select("event_id", "customer_id", "event_type", "app_version", "event_ts", LOADED), T_APP_EVENT)

pu = with_hash(latest_per_key(validated("partner_usage"), ["partner_record_id"]))
save(pu.select("partner_record_id", "msisdn_hash", "service", "amount", "currency", "period_start", LOADED),
     T_PARTNER_USAGE)
