import csv
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = (
    PROJECT_ROOT
    / "data"
    / "replay"
    / "movielens"
    / "ml-latest-small"
    / "ratings.csv"
)

OUTPUT_FILE = (
    PROJECT_ROOT
    / "data"
    / "replay"
    / "movielens"
    / "movielens_replay.csv"
)

def transform_rating(row, replay_index):
    original_timestamp = int(row["timestamp"])

    original_time = datetime.fromtimestamp(
        original_timestamp,
        tz=timezone.utc
    )

    replay_time = datetime(
        2026,
        10,
        1,
        0,
        0,
        0,
        tzinfo=timezone.utc
    )
    from datetime import timedelta
    replay_time = replay_time + timedelta(seconds=replay_index)

    return {
        "replay_id": f"ml_{replay_index:06d}",
        "user_id": int(row["userId"]),
        "content_id": int(row["movieId"]),
        "rating": float(row["rating"]),
        "original_event_time": original_time.isoformat(),
        "replay_event_time": replay_time.isoformat(),
    }
def main():
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with INPUT_FILE.open("r", newline="", encoding="utf-8") as input_file:
        reader = csv.DictReader(input_file)

        fieldnames = [
            "replay_id",
            "user_id",
            "content_id",
            "rating",
            "original_event_time",
            "replay_event_time",
        ]

        with OUTPUT_FILE.open(
            "w",
            newline="",
            encoding="utf-8"
        ) as output_file:

            writer = csv.DictWriter(
                output_file,
                fieldnames=fieldnames
            )

            writer.writeheader()

            for index, row in enumerate(reader, start=1):
                replay_event = transform_rating(row, index)
                writer.writerow(replay_event)


if __name__ == "__main__":
    main()