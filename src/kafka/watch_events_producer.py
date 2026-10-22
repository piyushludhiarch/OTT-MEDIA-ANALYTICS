import argparse
import csv
import json
import os
from pathlib import Path

from kafka import KafkaProducer
from kafka.serializer import Serializer


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CSV_FILE = PROJECT_ROOT / "data" / "generated" / "watch_events.csv"
DEFAULT_BOOTSTRAP_SERVERS = (
    "localhost:9092,localhost:29092,localhost:39092"
)
REQUIRED_COLUMNS = {
    "event_id",
    "user_id",
    "content_id",
    "event_type",
    "event_time",
    "watch_seconds",
    "buffer_seconds",
    "device_type",
    "country",
}


class JsonSerializer(Serializer):
    def serialize(self, topic, headers, value):
        return json.dumps(value).encode("utf-8")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Publish generated OTT watch events to Kafka."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path(os.getenv("WATCH_EVENTS_CSV", DEFAULT_CSV_FILE)),
        help="Input CSV file (default: data/generated/watch_events.csv).",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS
        ),
        help="Comma-separated Kafka broker addresses.",
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("KAFKA_TOPIC", "watch_events_full"),
        help="Kafka topic to publish to.",
    )
    return parser.parse_args()


def read_events(csv_path):
    with csv_path.open(mode="r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        missing_columns = REQUIRED_COLUMNS.difference(reader.fieldnames or [])
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"{csv_path} is missing required columns: {missing}")

        for row_number, event in enumerate(reader, start=2):
            if any(not event.get(column) for column in REQUIRED_COLUMNS):
                raise ValueError(
                    f"{csv_path} contains an empty required value "
                    f"on row {row_number}"
                )
            yield event


def main():
    args = parse_args()
    if not args.input.is_file():
        raise FileNotFoundError(f"Watch-events CSV not found: {args.input}")

    producer = KafkaProducer(
        bootstrap_servers=[
            server.strip()
            for server in args.bootstrap_servers.split(",")
            if server.strip()
        ],
        value_serializer=JsonSerializer(),
        acks="all",
        retries=5,
    )

    sent_count = 0
    try:
        pending_sends = []
        for event in read_events(args.input):
            pending_sends.append(producer.send(args.topic, value=event))
            sent_count += 1

        for pending_send in pending_sends:
            pending_send.get(timeout=60)
    finally:
        producer.close()

    print(
        f"Published {sent_count} watch events to {args.topic} "
        f"using {args.bootstrap_servers}"
    )


if __name__ == "__main__":
    main()
