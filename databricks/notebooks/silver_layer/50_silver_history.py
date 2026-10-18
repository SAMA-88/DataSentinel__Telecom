# Databricks notebook source
# MAGIC %run ./49_silver_lib

# COMMAND ----------

# SILVER HISTORY (SCD2) with masked PII.
#   plan_history     : one row per plan version
#   customer_history : one row per customer version; email / phone / name are hashed and masked, never stored raw
# Input: only rows that passed the quality gate. Rebuilt from the ordered change events, so a replay gives the same
# result (idempotent). Surrogate keys, the unknown member and the star schema are built in Gold by dbt.

# COMMAND ----------
# Remove the tables of the first Silver version: dimensions and facts now belong to Gold (dbt)
for t in OLD_TABLES:
    if spark.catalog.tableExists(t):
        spark.sql(f"DROP TABLE {t}")
        print("dropped", t)

# COMMAND ----------
plans = validated("crm_plans").filter("_op IN ('r', 'c', 'u')")
p = scd2(plans, "plan_id", ["plan_name", "monthly_fee", "data_gb", "voice_minutes"])
plan_history = p.select("plan_id", "version", "valid_from", "valid_to", "is_current", "plan_name", "monthly_fee",
                        "data_gb", "voice_minutes", F.current_timestamp().alias("_loaded_at"))
save(plan_history, T_PLAN_HISTORY)

# COMMAND ----------
cust = validated("crm_customers").filter("_op IN ('r', 'c', 'u')")
cust = (cust.withColumn("msisdn_hash", hash_pii("msisdn"))
            .withColumn("email_hash", hash_pii("email", lower=True)))
c2 = scd2(cust, "customer_id",
          ["msisdn_hash", "email_hash", "first_name", "last_name", "city", "country", "status", "plan_id"],
          created_col="created_at")
customer_history = c2.select(
    "customer_id", "version", "valid_from", "valid_to", "is_current",
    F.concat_ws(" ", mask_name("first_name"), mask_name("last_name")).alias("name_masked"),
    mask_email("email").alias("email_masked"), "email_hash",
    mask_msisdn("msisdn").alias("msisdn_masked"), "msisdn_hash",
    "city", "country", "status", "plan_id", "created_at",
    F.col("_dq_fixed").alias("_was_repaired"), F.current_timestamp().alias("_loaded_at"))
save(customer_history, T_CUSTOMER_HISTORY)
display(spark.table(T_CUSTOMER_HISTORY).orderBy("customer_id", "version").limit(20))
