"""Kafka -> Databricks Volume bridge.

CDC topics (telecom.*)  -> /Volumes/data_sentinel/bronze/raw/cdc_<time>.jsonl        (read by 01_bronze)
App events (app.events) -> /Volumes/data_sentinel/bronze/raw/app_events/app_events_<time>.jsonl  (read by 02_bronze_app_events)

Every line is one Kafka message wrapped in an envelope. "value" is kept as the raw message text.
"""
import json
import logging
import os
import time
from datetime import datetime, timezone
from urllib.parse import quote

import requests
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("data_sentinel-bridge")

HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN", "")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

VOLUME_ROOT = "/Volumes/data_sentinel/bronze/raw"
APP_DIR = f"{VOLUME_ROOT}/app_events"
BATCH_SIZE = 500
FLUSH_SECONDS = 10

CDC_TOPICS = [
    "telecom.crm.customers",
    "telecom.crm.devices",
    "telecom.crm.plans",
    "telecom.billing.accounts",
    "telecom.billing.invoices",
    "telecom.billing.invoice_lines",
    "telecom.billing.payments",
]
APP_TOPICS = ["app.events"]
TOPICS = CDC_TOPICS + APP_TOPICS


def route(topic):
    """Return (directory, file prefix) for a topic."""
    if topic in APP_TOPICS:
        return APP_DIR, "app_events"
    return VOLUME_ROOT, "cdc"


def split_events(events):
    groups = {}
    for event in events:
        groups.setdefault(route(event["topic"]), []).append(event)
    return groups


def api_headers(content_type=None):
    h = {"Authorization": f"Bearer {TOKEN}"}
    if content_type:
        h["Content-Type"] = content_type
    return h


def ensure_dir(path):
    url = f"{HOST}/api/2.0/fs/directories{quote(path, safe='/')}/"
    requests.put(url, headers=api_headers(), timeout=30).raise_for_status()


def put_file(directory, prefix, events):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    file_path = f"{directory}/{prefix}_{timestamp}.jsonl"
    url = f"{HOST}/api/2.0/fs/files{quote(file_path, safe='/')}"
    body = "".join(json.dumps(e, ensure_ascii=False) + "\n" for e in events).encode("utf-8")
    response = requests.put(
        url,
        params={"overwrite": "false"},
        headers=api_headers("application/octet-stream"),
        data=body,
        timeout=60,
    )
    response.raise_for_status()
    log.info("Uploaded %s events to %s", len(events), file_path)


def upload_batch(events):
    for (directory, prefix), group in split_events(events).items():
        put_file(directory, prefix, group)


def main():
    if not HOST or not TOKEN:
        raise RuntimeError("Set DATABRICKS_HOST and DATABRICKS_TOKEN in .env")

    ensure_dir(VOLUME_ROOT)
    ensure_dir(APP_DIR)

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        group_id="data_sentinel-bridge",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
    )

    events = []
    last_flush = time.monotonic()
    log.info("Bridge started; listening to %s topics", len(TOPICS))

    try:
        while True:
            records = consumer.poll(timeout_ms=1000)
            for _, messages in records.items():
                for message in messages:
                    events.append({
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                        "key": message.key.decode("utf-8", errors="replace") if message.key else None,
                        # Raw message text keeps the Bronze schema stable.
                        "value": message.value.decode("utf-8", errors="replace") if message.value is not None else None,
                        "kafka_timestamp_ms": message.timestamp,
                        "ingested_at": datetime.now(timezone.utc).isoformat(),
                    })

            elapsed = time.monotonic() - last_flush
            if events and (len(events) >= BATCH_SIZE or elapsed >= FLUSH_SECONDS):
                upload_batch(events)      # commit offsets only after a successful upload
                consumer.commit()
                events.clear()
                last_flush = time.monotonic()

    except KeyboardInterrupt:
        log.info("Stopping bridge")
    finally:
        if events:
            upload_batch(events)
            consumer.commit()
        consumer.close()


if __name__ == "__main__":
    main()
