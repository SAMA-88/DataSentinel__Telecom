# Databricks notebook source
# MAGIC %run ./49_silver_lib

# COMMAND ----------

# CUSTOMER_XREF: links the CRM customer id to the billing account id through the phone number.
# The ids differ by design (CRM customer_id is not the billing account_id), so the phone number is the only link.
# The phone number is compared as a hash, nothing raw is stored.
#   MATCHED      : one current customer and one account share the phone number
#   CRM_ONLY     : customer without a billing account   (reconciliation R1)
#   BILLING_ONLY : account without a customer           (reconciliation R2)
#   AMBIGUOUS    : the phone number belongs to more than one customer or account
# Run after 50 and 51.

# COMMAND ----------
crm = (spark.table(T_CUSTOMER_HISTORY).filter("is_current")
       .select("customer_id", "msisdn_hash", F.col("status").alias("crm_status"))
       .withColumn("_crm_n", F.count("*").over(Window.partitionBy("msisdn_hash"))))

acc = (spark.table(T_ACCOUNT).select("account_id", "account_ref", "msisdn_hash", F.col("status").alias("billing_status"))
       .withColumn("_acc_n", F.count("*").over(Window.partitionBy("msisdn_hash"))))

x = crm.alias("c").join(acc.alias("a"), F.col("c.msisdn_hash") == F.col("a.msisdn_hash"), "full_outer")
status = (F.when(F.col("c.customer_id").isNull(), "BILLING_ONLY")
           .when(F.col("a.account_id").isNull(), "CRM_ONLY")
           .when((F.col("_crm_n") == 1) & (F.col("_acc_n") == 1), "MATCHED")
           .otherwise("AMBIGUOUS"))
xref = x.select(F.coalesce(F.col("c.msisdn_hash"), F.col("a.msisdn_hash")).alias("msisdn_hash"),
                F.col("c.customer_id"), F.col("a.account_id"), F.col("a.account_ref"),
                F.col("c.crm_status"), F.col("a.billing_status"), status.alias("match_status"),
                F.current_timestamp().alias("_loaded_at"))
save(xref, T_XREF)
display(spark.table(T_XREF).groupBy("match_status").count().orderBy("match_status"))
