import csv
import random
from datetime import datetime, timedelta

OUTPUT_FILE = "data/generated/cdn_logs.csv"

random.seed(42)

regions = [
    "India",
    "USA",
    "UK",
    "Canada",
    "Australia",
    "Germany"
]

status_codes = [200, 200, 200, 200, 206, 206, 404, 500, 502, 503]

start_time = datetime(2026, 9, 1, 0, 0, 0)

total_records = 10000

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as file:

    writer = csv.writer(file)

    writer.writerow([
        "timestamp",
        "user_id",
        "content_id",
        "region",
        "status_code",
        "response_time_ms",
        "bytes_sent"
    ])

    for i in range(total_records):

        timestamp = start_time + timedelta(
            seconds=i * 30
        )

        region = random.choice(regions)

        status_code = random.choice(status_codes)

        # Create a deliberate HTTP error spike
        # in India during a specific time period.
        if (
            region == "India"
            and datetime(2026, 9, 1, 12, 0, 0)
            <= timestamp
            < datetime(2026, 9, 1, 13, 0, 0)
        ):
            status_code = random.choice([
                500,
                500,
                502,
                503,
                503
            ])

        writer.writerow([
            timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            random.randint(1001, 2000),
            random.randint(101, 400),
            region,
            status_code,
            random.randint(50, 2000),
            random.randint(10000, 500000)
        ])

print("CDN log generation completed.")
print(f"Records generated: {total_records}")
print(f"Output file: {OUTPUT_FILE}")