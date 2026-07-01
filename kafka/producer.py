import json
import os
import time
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = "energy-raw"

def get_latest_raw_file() -> str:
    files = sorted(os.listdir("data/raw"))
    if not files:
        raise FileNotFoundError("No raw files found in data/raw/")
    return f"data/raw/{files[-1]}"

def run():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3
    )

    raw_file = get_latest_raw_file()
    print(f"Loading records from {raw_file}")

    with open(raw_file) as f:
        records = json.load(f)

    print(f"Publishing {len(records)} records to topic '{TOPIC}'...")

    for i, record in enumerate(records):
        producer.send(TOPIC, value=record)
        if (i + 1) % 500 == 0:
            print(f"  Published {i + 1}/{len(records)}")
        time.sleep(0.001)

    producer.flush()
    print(f"Done. All {len(records)} records published to Kafka.")

if __name__ == "__main__":
    run()