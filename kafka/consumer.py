import json
import os
from kafka import KafkaConsumer
from deltalake import write_deltalake
import pyarrow as pa

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = "energy-raw"
BRONZE_PATH = "data/bronze/energy"

SCHEMA = pa.schema([
    pa.field("source", pa.string()),
    pa.field("series_id", pa.string()),
    pa.field("period", pa.string()),
    pa.field("state", pa.string()),
    pa.field("fuel_type", pa.string()),
    pa.field("metric", pa.string()),
    pa.field("value", pa.float64()),
    pa.field("unit", pa.string()),
    pa.field("ingested_at", pa.string()),
])

def run():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="energy-bronze-consumer",
        consumer_timeout_ms=10000  # stop after 10s of no messages
    )

    os.makedirs(BRONZE_PATH, exist_ok=True)
    batch = []

    print(f"Consuming from topic '{TOPIC}'...")

    for msg in consumer:
        record = msg.value
        # coerce value to float safely
        try:
            record["value"] = float(record["value"]) if record["value"] is not None else None
        except (ValueError, TypeError):
            record["value"] = None
        batch.append(record)

        if len(batch) % 500 == 0:
            print(f"  Consumed {len(batch)} records so far...")

    consumer.close()
    print(f"Total records consumed: {len(batch)}")

    if batch:
        table = pa.Table.from_pylist(batch, schema=SCHEMA)
        write_deltalake(
            BRONZE_PATH,
            table,
            mode="append",
            partition_by=["source", "period"]
        )
        print(f"Written to Bronze Delta Lake at {BRONZE_PATH}")
        print(f"Partitioned by source and period")

if __name__ == "__main__":
    run()