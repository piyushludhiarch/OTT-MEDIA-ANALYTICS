import csv
import random
from datetime import datetime, timedelta
from pathlib import Path


OUTPUT_FILE = Path("data/generated/watch_events.csv")

TOTAL_UNIQUE_EVENTS = 50_000
DUPLICATE_PERCENTAGE = 0.05

START_DATE = datetime(2026, 9, 1, 0, 0, 0)
TOTAL_DAYS = 30

USER_IDS = list(range(1001, 2001))
CONTENT_IDS = list(range(101, 401))

DEVICES = [
    "Mobile",
    "Web",
    "TV",
    "Tablet",
]

COUNTRIES = [
    "India",
    "USA",
    "UK",
    "Canada",
    "Australia",
    "Germany",
]

EVENT_TYPES = [
    "play",
    "pause",
    "resume",
    "buffer",
    "stop",
]


def generate_event_time():
    random_seconds = random.randint(
        0,
        TOTAL_DAYS * 24 * 60 * 60 - 1,
    )

    return START_DATE + timedelta(
        seconds=random_seconds
    )


def generate_event(event_number):
    event_id = f"evt{event_number:06d}"

    user_id = random.choice(USER_IDS)

    # Make some content significantly more popular.
    popularity_roll = random.random()

    if popularity_roll < 0.50:
        content_id = random.randint(101, 120)

    elif popularity_roll < 0.80:
        content_id = random.randint(121, 200)

    else:
        content_id = random.randint(201, 400)

    event_type = random.choices(
        EVENT_TYPES,
        weights=[
            55,  # play
            10,  # pause
            10,  # resume
            8,  # buffer
            17,   # stop
        ],
        k=1,
    )[0]

    event_time = generate_event_time()

    device_type = random.choice(DEVICES)
    country = random.choice(COUNTRIES)

    if event_type == "play":
        watch_seconds = random.randint(30, 600)
        buffer_seconds = random.choices(
            [0, random.randint(1, 3), random.randint(4, 8)],
            weights=[90, 8, 2],
            k=1,
        )[0]

    elif event_type == "buffer":
        watch_seconds = 0
        buffer_seconds = random.randint(2, 20)

    elif event_type in ["pause", "resume"]:
        watch_seconds = random.randint(0, 30)
        buffer_seconds = 0

    else:
        watch_seconds = random.randint(10, 120)
        buffer_seconds = 0

    return [
        event_id,
        user_id,
        content_id,
        event_type,
        event_time.strftime("%Y-%m-%d %H:%M:%S"),
        watch_seconds,
        buffer_seconds,
        device_type,
        country,
    ]


def generate_watch_events():
    random.seed(42)

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    events = []

    for event_number in range(
        1,
        TOTAL_UNIQUE_EVENTS + 1,
    ):
        events.append(
            generate_event(event_number)
        )

    duplicate_count = int(
        TOTAL_UNIQUE_EVENTS * DUPLICATE_PERCENTAGE
    )

    duplicate_events = random.sample(
        events,
        duplicate_count,
    )

    all_events = events + duplicate_events

    random.shuffle(all_events)

    with OUTPUT_FILE.open(
        mode="w",
        newline="",
        encoding="utf-8",
    ) as file:

        writer = csv.writer(file)

        writer.writerow(
            [
                "event_id",
                "user_id",
                "content_id",
                "event_type",
                "event_time",
                "watch_seconds",
                "buffer_seconds",
                "device_type",
                "country",
            ]
        )

        writer.writerows(all_events)

    print(f"Unique events generated: {len(events)}")
    print(f"Replay duplicates generated: {duplicate_count}")
    print(f"Total raw events: {len(all_events)}")
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    generate_watch_events()