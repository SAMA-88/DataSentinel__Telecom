# Databricks notebook: Bronze for mobile app events (streamed from Kafka by bridge.py)
from pyspark.sql import functions as F
from delta.tables import DeltaTable

APP_DIR = "/Volumes/data_sentinel/bronze/raw/app_events"
TARGET_TABLE = "data_sentinel.bronze.app_events_raw"

files = [f for f in dbutils.fs.ls(APP_DIR) if f.name.endswith(".jsonl")]
assert files, "No .jsonl files in raw/app_events yet. Run bridge.py (and app_event_producer.py) first."
print(f"Found {len(files)} files")

raw_df = spark.read.json(f"{APP_DIR}/*.jsonl")

bronze_df = (
    raw_df
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_bronze_ingested_at", F.current_timestamp())
    .dropDuplicates(["topic", "partition", "offset"])
)

if not spark.catalog.tableExists(TARGET_TABLE):
    bronze_df.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
else:
    (
        DeltaTable.forName(spark, TARGET_TABLE).alias("t")
        .merge(
            bronze_df.alias("s"),
            "t.topic = s.topic AND t.partition = s.partition AND t.offset = s.offset",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

print("Bronze app events row count:", spark.table(TARGET_TABLE).count())

# Peek inside the raw message (parsing into typed columns happens later, in Silver)
display(
    spark.table(TARGET_TABLE)
    .select(
        "offset",
        F.get_json_object("value", "$.event_id").alias("event_id"),
        F.get_json_object("value", "$.customer_id").alias("customer_id"),
        F.get_json_object("value", "$.event_type").alias("event_type"),
        F.get_json_object("value", "$.app_version").alias("app_version"),
        F.get_json_object("value", "$.event_ts").alias("event_ts"),
    )
    .orderBy(F.col("offset").desc())
    .limit(10)
)
