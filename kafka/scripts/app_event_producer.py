"""Mobile app event generator (streaming source).

kafka mode : sends events to the Kafka topic app.events (bridge.py then uploads them)
    python app_event_producer.py kafka --rate 20 --total 500
file mode  : writes one JSON-lines file in the same envelope the bridge uses (fallback without Kafka)
    python app_event_producer.py file --date 2026-10-07 --rows 3000 --out data/landing/app_events
    then upload that folder to /Volumes/data_sentinel/bronze/raw/app_events/

Defects injected (for the quality gate):
  completeness  customer_id null (~2%)        uniqueness  repeated event_id (~1%)
  domain values unknown event_type (~1%)      timeliness  event_ts 3 days in the future (~0.5%)
  consistency   customer_id not in CRM (~0.5%)
  schema        --drift adds a new field device_os
"""
import argparse
import json
import random
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

TYPES = ["login", "view_bill", "topup", "crash", "plan_change"]
VERSIONS = ["5.1", "5.2", "6.0"]


def make_event(seq, ts, max_customer, state, drift):
    ev = {
        "event_id": f"EVT-{ts:%Y%m%d%H%M%S}-{seq:07d}",
        "customer_id": random.randint(1, max_customer),
        "event_type": random.choice(TYPES),
        "app_version": random.choice(VERSIONS),
        "event_ts": ts.isoformat(),
    }
    r = random.random()
    if r < 0.02:
        ev["customer_id"] = None
    elif r < 0.025:
        ev["customer_id"] = 900000 + random.randint(1, 999)
    if random.random() < 0.01:
        ev["event_type"] = "unknown_evt"
    if random.random() < 0.005:
        ev["event_ts"] = (ts + timedelta(days=3)).isoformat()
    if drift:
        ev["device_os"] = random.choice(["android", "ios"])
    if state["last"] and random.random() < 0.01:
        return dict(state["last"])
    state["last"] = ev
    return ev


def run_kafka(a):
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=a.bootstrap.split(","),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    state, i = {"last": None}, 0
    while a.total == 0 or i < a.total:
        ev = make_event(i, datetime.now(timezone.utc), a.customers, state, a.drift)
        producer.send("app.events", key=str(ev["customer_id"]), value=ev)
        i += 1
        time.sleep(1 / a.rate)
    producer.flush()
    print("sent", i, "events to app.events")


def run_file(a):
    d = date.fromisoformat(a.date)
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"app_events_{d:%Y%m%d}.jsonl"
    state = {"last": None}
    base = datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc)
    offset0 = int(f"{d:%Y%m%d}") * 1_000_000          # unique offsets per day
    with open(path, "w", encoding="utf-8") as f:
        for i in range(a.rows):
            ts = base + timedelta(seconds=random.randint(0, 86399))
            ev = make_event(i, ts, a.customers, state, a.drift)
            envelope = {
                "topic": "app.events",
                "partition": 0,
                "offset": offset0 + i,
                "key": str(ev["customer_id"]) if ev["customer_id"] is not None else None,
                "value": json.dumps(ev),
                "kafka_timestamp_ms": int(ts.timestamp() * 1000),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }
            f.write(json.dumps(envelope) + "\n")
    print("wrote", path, a.rows, "rows")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["kafka", "file"])
    ap.add_argument("--customers", type=int, default=1000)
    ap.add_argument("--drift", action="store_true")
    ap.add_argument("--rate", type=float, default=20, help="kafka: events per second")
    ap.add_argument("--total", type=int, default=0, help="kafka: 0 = run forever")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--date", default="2026-10-07")
    ap.add_argument("--rows", type=int, default=3000)
    ap.add_argument("--out", default="data/landing/app_events")
    a = ap.parse_args()
    run_kafka(a) if a.mode == "kafka" else run_file(a)
