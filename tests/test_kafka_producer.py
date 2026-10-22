import csv
import json
from argparse import Namespace

import pytest

from src.kafka import watch_events_producer
from src.kafka.watch_events_producer import read_events


EVENT = {
    "event_id": "evt001",
    "user_id": "1001",
    "content_id": "101",
    "event_type": "play",
    "event_time": "2026-10-01 10:00:01",
    "watch_seconds": "120",
    "buffer_seconds": "0",
    "device_type": "Mobile",
    "country": "India",
}


def test_json_serializer_encodes_events():
    serialized = watch_events_producer.JsonSerializer().serialize(
        "watch_events_full", [], EVENT
    )

    assert json.loads(serialized.decode("utf-8")) == EVENT


def write_events_csv(path, headers, rows):
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def test_read_events_accepts_project_watch_event_schema(tmp_path):
    csv_path = tmp_path / "events.csv"
    write_events_csv(csv_path, list(EVENT), [EVENT])

    assert list(read_events(csv_path)) == [EVENT]


def test_read_events_reports_missing_required_columns(tmp_path):
    csv_path = tmp_path / "events.csv"
    write_events_csv(csv_path, ["event_id"], [{"event_id": "evt001"}])

    with pytest.raises(ValueError, match="missing required columns"):
        list(read_events(csv_path))


def test_read_events_reports_empty_required_values(tmp_path):
    csv_path = tmp_path / "events.csv"
    event = {**EVENT, "device_type": ""}
    write_events_csv(csv_path, list(EVENT), [event])

    with pytest.raises(ValueError, match="empty required value on row 2"):
        list(read_events(csv_path))


def test_main_waits_for_acknowledgements_and_closes_producer(
    monkeypatch, tmp_path
):
    csv_path = tmp_path / "events.csv"
    write_events_csv(csv_path, list(EVENT), [EVENT])
    sent_events = []
    acknowledged = []

    class SendResult:
        def get(self, timeout):
            acknowledged.append(timeout)

    class Producer:
        def __init__(self, **kwargs):
            assert kwargs["bootstrap_servers"] == ["localhost:9092"]
            assert kwargs["acks"] == "all"
            assert kwargs["retries"] == 5

        def send(self, topic, value):
            sent_events.append((topic, value))
            return SendResult()

        def close(self):
            pass

    monkeypatch.setattr(watch_events_producer, "KafkaProducer", Producer)
    monkeypatch.setattr(
        watch_events_producer,
        "parse_args",
        lambda: Namespace(
            input=csv_path,
            bootstrap_servers="localhost:9092",
            topic="watch_events_full",
        ),
    )

    watch_events_producer.main()

    assert sent_events == [("watch_events_full", EVENT)]
    assert acknowledged == [60]
