# Databricks notebook: Bronze for batch CSV files (network, billing, partner)
# Every business column is kept as STRING. Each file is also recorded in bronze.file_audit
# (file date, row count, column list) so the quality step can check freshness, volume and schema.
import re
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (DateType, LongType, StringType, StructField, StructType, TimestampType)

CATALOG = "data_sentinel"
BASE = f"/Volumes/{CATALOG}/bronze/raw/batch"
AUDIT = f"{CATALOG}.bronze.file_audit"
RELOAD = False        # True = drop the batch Bronze tables and the audit table, then load everything again

SOURCES = [
    dict(source="network", folder="network", table="network_cdr_raw", pattern=r"cdr_(\d{8})\.csv$", fmt="%Y%m%d"),
    dict(source="billing_invoices", folder="billing_invoices", table="billing_invoices_raw", pattern=r"invoices_(\d{6})\.csv$", fmt="%Y%m"),
    dict(source="billing_payments", folder="billing_payments", table="billing_payments_raw", pattern=r"payments_(\d{6})\.csv$", fmt="%Y%m"),
    dict(source="partner", folder="partner", table="partner_usage_raw", pattern=r"partner_usage_(\d{6})\.csv$", fmt="%Y%m"),
]

AUDIT_SCHEMA = StructType([
    StructField("source", StringType()), StructField("file_name", StringType()), StructField("file_path", StringType()),
    StructField("file_date", DateType()), StructField("row_count", LongType()), StructField("column_count", LongType()),
    StructField("columns", StringType()), StructField("size_bytes", LongType()), StructField("ingested_at", TimestampType()),
])

if RELOAD:
    for cfg in SOURCES:
        spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.bronze.{cfg['table']}")
    spark.sql(f"DROP TABLE IF EXISTS {AUDIT}")

loaded = set()
if spark.catalog.tableExists(AUDIT):
    loaded = {(r.source, r.file_name) for r in spark.table(AUDIT).select("source", "file_name").collect()}

new_files = 0
for cfg in SOURCES:
    target = f"{CATALOG}.bronze.{cfg['table']}"
    try:
        files = [f for f in dbutils.fs.ls(f"{BASE}/{cfg['folder']}") if f.name.lower().endswith(".csv")]
    except Exception as e:
        print(f"[skip] {cfg['folder']}: folder not found ({type(e).__name__})")
        continue

    for f in sorted(files, key=lambda x: x.name):
        if (cfg["source"], f.name) in loaded:
            continue
        m = re.search(cfg["pattern"], f.name)
        file_date = datetime.strptime(m.group(1), cfg["fmt"]).date() if m else None

        df = spark.read.option("header", "true").option("inferSchema", "false").csv(f.path)
        cols = list(df.columns)
        rows = df.count()

        bronze = (
            df.withColumn("_row_hash", F.sha2(F.to_json(F.struct(*[F.col(f"`{c}`") for c in cols])), 256))
              .withColumn("_source_system", F.lit(cfg["source"]))
              .withColumn("_source_file", F.col("_metadata.file_path"))
              .withColumn("_file_date", F.lit(file_date).cast("date"))
              .withColumn("_bronze_ingested_at", F.current_timestamp())
        )
        bronze.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target)

        audit_row = [(cfg["source"], f.name, f.path, file_date, rows, len(cols), ",".join(cols), f.size, datetime.utcnow())]
        spark.createDataFrame(audit_row, AUDIT_SCHEMA).write.format("delta").mode("append").saveAsTable(AUDIT)
        new_files += 1
        print(f"loaded {cfg['source']}/{f.name}: {rows:,} rows, {len(cols)} columns")

print(f"\nNew files loaded: {new_files}")
display(spark.table(AUDIT).orderBy("source", "file_date"))
for cfg in SOURCES:
    t = f"{CATALOG}.bronze.{cfg['table']}"
    if spark.catalog.tableExists(t):
        print(f"{t}: {spark.table(t).count():,} rows")
