# Databricks notebook source
# BRONZE CDC: load the Debezium change events that bridge.py wrote to the Volume into bronze.cdc_events.
#   source  /Volumes/data_sentinel/bronze/raw/cdc_<time>.jsonl  (one Kafka message per line, telecom.crm.* / telecom.billing.*)
#   target  data_sentinel.bronze.cdc_events                      (one row per Kafka message)
# Safe to run again: only messages not stored yet are inserted (key = topic + partition + offset), so nothing is
# loaded twice and nothing is lost. Taken from the manual cell in the 'reading_tables' notebook (2026-10-09).
# App events live in the app_events/ subfolder and are not read here.

from pyspark.sql import functions as F
from delta.tables import DeltaTable

RAW_DIR = "/Volumes/data_sentinel/bronze/raw"
SOURCE_PATH = f"{RAW_DIR}/*.jsonl"
TARGET_TABLE = "data_sentinel.bronze.cdc_events"

files = [f for f in dbutils.fs.ls(RAW_DIR) if f.name.endswith(".jsonl")]
if not files:
    dbutils.notebook.exit("no CDC files in the Volume yet (is bridge.py running?): nothing to load")
print(f"Found {len(files)} CDC files in the Volume")

bronze_df = (
    spark.read.json(SOURCE_PATH)
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_bronze_ingested_at", F.current_timestamp())
    .dropDuplicates(["topic", "partition", "offset"])
)

before = spark.table(TARGET_TABLE).count() if spark.catalog.tableExists(TARGET_TABLE) else 0
if before == 0 and not spark.catalog.tableExists(TARGET_TABLE):
    bronze_df.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
else:
    (DeltaTable.forName(spark, TARGET_TABLE).alias("t")
        .merge(bronze_df.alias("s"), "t.topic = s.topic AND t.partition = s.partition AND t.offset = s.offset")
        .whenNotMatchedInsertAll()
        .execute())

after = spark.table(TARGET_TABLE).count()
print(f"bronze.cdc_events: {before:,} -> {after:,} rows ({after - before:,} new)")
display(spark.table(TARGET_TABLE).groupBy("topic").count().orderBy("topic"))
