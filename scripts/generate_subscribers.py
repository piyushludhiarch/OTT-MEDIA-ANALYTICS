import csv
import random
from datetime import date, timedelta
from pathlib import Path


OUTPUT_FILE = Path("data/generated/subscribers.csv")

TOTAL_NEW_SUBSCRIBERS = 990

COUNTRIES = [
    "India",
    "USA",
    "UK",
    "Canada",
    "Australia",
    "Germany",
]

PLAN_IDS = [1, 2, 3]

FIRST_NAMES = [
    "Aarav", "Vihaan", "Aditya", "Arjun", "Rohan",
    "Rahul", "Vikram", "Kiran", "Nikhil", "Varun",
    "Ananya", "Diya", "Isha", "Priya", "Sneha",
    "Meera", "Kavya", "Aisha", "Neha", "Riya",
]

LAST_NAMES = [
    "Sharma", "Reddy", "Kumar", "Patel", "Singh",
    "Verma", "Rao", "Gupta", "Mehta", "Nair",
    "Das", "Joshi", "Kapoor", "Iyer", "Malhotra",
]

START_DATE = date(2025, 1, 1)
END_DATE = date(2026, 9, 30)


def random_signup_date():
    days = (END_DATE - START_DATE).days
    return START_DATE + timedelta(days=random.randint(0, days))


def generate_name():
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    return f"{first_name} {last_name}"


def generate_subscribers():
    random.seed(42)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    subscribers = []

    for subscriber_id in range(1011, 2001):
        name = generate_name()

        email = f"user{subscriber_id}@ottmedia.example"

        country = random.choice(COUNTRIES)

        plan_id = random.choices(
            PLAN_IDS,
            weights=[30, 45, 25],
            k=1,
        )[0]

        signup_date = random_signup_date()

        subscribers.append(
            [
                subscriber_id,
                name,
                email,
                country,
                plan_id,
                signup_date,
            ]
        )

    with OUTPUT_FILE.open(
        mode="w",
        newline="",
        encoding="utf-8",
    ) as file:

        writer = csv.writer(file)

        writer.writerow(
            [
                "subscriber_id",
                "name",
                "email",
                "country",
                "plan_id",
                "signup_date",
            ]
        )

        writer.writerows(subscribers)

    print(f"Generated {len(subscribers)} new subscribers")
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    generate_subscribers()