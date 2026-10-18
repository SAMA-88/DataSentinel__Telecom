# Databricks notebook source
# MAGIC %run ./11_dq_lib

# COMMAND ----------

# Shared helpers for the Silver layer (it builds on 11_dq_lib: CATALOG, load_contracts, validated_table).
# Silver = rows that passed the quality gate, plus three things only: SCD2 history, PII masking, customer_xref.
# Dimensions, facts and marts are built in Gold by dbt, not here.

SILVER = f"{CATALOG}.silver"
T_CUSTOMER_HISTORY = f"{SILVER}.customer_history"
T_PLAN_HISTORY = f"{SILVER}.plan_history"
T_XREF = f"{SILVER}.customer_xref"
T_ACCOUNT = f"{SILVER}.account"
T_INVOICE = f"{SILVER}.invoice"
T_INVOICE_LINE = f"{SILVER}.invoice_line"
T_PAYMENT = f"{SILVER}.payment"
T_INVOICE_FILE = f"{SILVER}.invoice_file"
T_PAYMENT_FILE = f"{SILVER}.payment_file"
T_NETWORK_CDR = f"{SILVER}.network_cdr"
T_APP_EVENT = f"{SILVER}.app_event"
T_PARTNER_USAGE = f"{SILVER}.partner_usage"
SILVER_TABLES = [T_CUSTOMER_HISTORY, T_PLAN_HISTORY, T_XREF, T_ACCOUNT, T_INVOICE, T_INVOICE_LINE, T_PAYMENT,
                 T_INVOICE_FILE, T_PAYMENT_FILE, T_NETWORK_CDR, T_APP_EVENT, T_PARTNER_USAGE]
# tables of the first Silver version (dimensions and facts); they now belong to Gold (dbt) and are dropped by notebook 50
OLD_TABLES = [f"{SILVER}.{n}" for n in ("dim_customer", "dim_plan", "fact_usage", "fact_invoice", "fact_payment",
                                        "fact_app_events", "fact_partner_usage")]
END_OF_TIME = "9999-12-31 00:00:00"

# The salt must be the same in every notebook and must not be in the code in production.
# Create the secret once:  databricks secrets create-scope sentinel ; databricks secrets put-secret sentinel pii_salt
try:
    PII_SALT = dbutils.secrets.get("sentinel", "pii_salt")
except Exception:
    PII_SALT = "dev-salt-change-me"

CONTRACTS = load_contracts()


def validated(dataset):
    """The rows of a dataset that passed the quality gate (including the repaired ones)."""
    return spark.table(validated_table(CONTRACTS[dataset]))


def latest_per_key(df, key):
    """One row per business key: the newest change event (CDC) or the newest file row."""
    w = Window.partitionBy(*key).orderBy(F.col("_ts_ms").desc_nulls_last(), F.col("_offset").desc_nulls_last(),
                                         F.col("_file_name").desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


# ---------------- PII ----------------
def hash_pii(col, lower=False):
    v = F.trim(F.col(col))
    v = F.lower(v) if lower else v
    return F.sha2(F.concat(F.lit(PII_SALT), v), 256)


def mask_msisdn(col):      # +316****5678
    return F.concat(F.substring(F.col(col), 1, 4), F.lit("****"), F.substring(F.col(col), -2, 2))


def mask_email(col):       # l***@example.nl
    return F.concat(F.substring(F.col(col), 1, 1), F.lit("***@"), F.substring_index(F.col(col), "@", -1))


def mask_name(col):        # L***
    return F.concat(F.substring(F.col(col), 1, 1), F.lit("***"))


# ---------------- SCD2 ----------------
def scd2(events, key, tracked, created_col=None):
    """Turn ordered change events into history rows.
    events : one row per change event, with _ts_ms and _offset (CDC metadata)
    key    : business key column name
    tracked: columns whose change starts a new version
    A new version starts only when a tracked column really changes.
    valid_from of the first version is the creation time (when known), then the event time.
    valid_to is the next valid_from, or END_OF_TIME for the current version."""
    w = Window.partitionBy(key).orderBy(F.col("_ts_ms").asc(), F.col("_offset").asc())
    h = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("~null~")) for c in tracked]), 256)
    x = events.withColumn("_attr_hash", h)
    x = x.withColumn("_prev_hash", F.lag("_attr_hash").over(w))
    x = x.filter(F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("_attr_hash")))
    event_ts = F.expr("timestamp_millis(_ts_ms)")
    x = x.withColumn("version", F.row_number().over(w))
    first_from = event_ts if created_col is None else F.least(F.coalesce(F.col(created_col), event_ts), event_ts)
    x = x.withColumn("valid_from", F.when(F.col("version") == 1, first_from).otherwise(event_ts))
    w2 = Window.partitionBy(key).orderBy(F.col("version").asc())
    x = x.withColumn("valid_to", F.coalesce(F.lead("valid_from").over(w2), F.lit(END_OF_TIME).cast("timestamp")))
    x = x.withColumn("is_current", F.col("valid_to") == F.lit(END_OF_TIME).cast("timestamp"))
    return x.drop("_attr_hash", "_prev_hash")


def save(df, table):
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table))
    n = spark.table(table).count()
    print(f"{table}: {n:,} rows")
    return n
