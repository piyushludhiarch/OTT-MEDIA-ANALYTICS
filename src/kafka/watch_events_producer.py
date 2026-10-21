import csv
import json

from kafka import KafkaProducer


CSV_FILE = "data/generated/watch_events.csv"
KAFKA_TOPIC = "watch_events_full"
KAFKA_SERVER = "localhost:9092"


producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


with open(CSV_FILE, mode="r", newline="", encoding="utf-8") as file:

    reader = csv.DictReader(file)

    for event in reader:
        producer.send(KAFKA_TOPIC, value=event)

        print(f"Sent: {event['event_id']}")


producer.flush()
producer.close()

print("All watch events sent successfully")