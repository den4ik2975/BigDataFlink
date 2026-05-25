import csv
import json
import os
import re
import time
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


def file_number(path):
    if path.name == "MOCK_DATA.csv":
        return 0
    match = re.search(r"\((\d+)\)", path.name)
    return int(match.group(1)) if match else 999


def csv_files(source_dir):
    return sorted(Path(source_dir).glob("*.csv"), key=file_number)


def wait_for_admin(bootstrap_servers):
    last_error = None
    for _ in range(60):
        try:
            admin = AdminClient({"bootstrap.servers": bootstrap_servers})
            admin.list_topics(timeout=5)
            return admin
        except Exception as exc:
            last_error = exc
            time.sleep(2)
    raise RuntimeError(f"Kafka is unavailable: {last_error}")


def ensure_topic(bootstrap_servers, topic):
    admin = wait_for_admin(bootstrap_servers)
    futures = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    for future in futures.values():
        try:
            future.result()
        except Exception as exc:
            if "already exists" not in str(exc):
                raise


def wait_for_producer(bootstrap_servers):
    last_error = None
    for _ in range(60):
        try:
            producer = Producer({"bootstrap.servers": bootstrap_servers, "linger.ms": 20})
            producer.list_topics(timeout=5)
            return producer
        except Exception as exc:
            last_error = exc
            time.sleep(2)
    raise RuntimeError(f"Kafka producer cannot connect: {last_error}")


def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC", "pet-sales")
    source_dir = os.getenv("SOURCE_DIR", "/data")

    files = csv_files(source_dir)
    if len(files) != 10:
        raise RuntimeError(f"Expected 10 CSV files in {source_dir}, got {len(files)}")

    ensure_topic(bootstrap_servers, topic)
    producer = wait_for_producer(bootstrap_servers)

    source_raw_id = 0
    for path in files:
        number = file_number(path)
        with path.open(newline="", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)
            for row_number, row in enumerate(reader, start=1):
                source_raw_id += 1
                event = dict(row)
                event["source_raw_id"] = source_raw_id
                event["source_file"] = path.name
                event["source_file_number"] = number
                event["source_row_number"] = row_number
                producer.produce(
                    topic,
                    key=str(source_raw_id).encode("utf-8"),
                    value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
                )
                producer.poll(0)

    producer.flush()
    print(f"Produced {source_raw_id} messages to {topic}")


if __name__ == "__main__":
    main()
