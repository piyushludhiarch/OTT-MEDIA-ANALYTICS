import csv
import random
from pathlib import Path


OUTPUT_FILE = Path("data/generated/content.csv")

TOTAL_NEW_CONTENT = 285

CONTENT_TYPES = [
    "Movie",
    "Series",
    "Documentary",
    "Sports",
]

GENRES = [
    "Action",
    "Adventure",
    "Comedy",
    "Drama",
    "Romance",
    "Thriller",
    "Horror",
    "Sci-Fi",
    "Fantasy",
    "Crime",
    "Documentary",
    "Animation",
    "Sports",
    "Nature",
    "Mystery",
]

TITLE_PREFIXES = [
    "The",
    "Beyond",
    "Hidden",
    "Last",
    "Final",
    "Dark",
    "Rising",
    "Lost",
    "Silent",
    "Golden",
    "Broken",
    "Infinite",
    "Secret",
    "Midnight",
    "Chasing",
]

TITLE_WORDS = [
    "Horizon",
    "Journey",
    "Legacy",
    "Empire",
    "Signal",
    "Mission",
    "Valley",
    "Kingdom",
    "Dream",
    "Storm",
    "Shadow",
    "Code",
    "Planet",
    "Warrior",
    "Connection",
    "Adventure",
    "Story",
    "Future",
    "Destiny",
    "World",
]

START_YEAR = 2015
END_YEAR = 2026


def generate_title(content_id):
    prefix = random.choice(TITLE_PREFIXES)
    word = random.choice(TITLE_WORDS)

    return f"{prefix} {word} {content_id}"


def generate_duration(content_type):
    if content_type == "Movie":
        return random.randint(80, 180)

    if content_type == "Series":
        return random.randint(20, 70)

    if content_type == "Documentary":
        return random.randint(30, 120)

    if content_type == "Sports":
        return random.randint(60, 240)

    return random.randint(30, 120)


def generate_content():
    random.seed(42)

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    content_records = []

    for content_id in range(116, 401):
        content_type = random.choice(CONTENT_TYPES)
        genre = random.choice(GENRES)
        release_year = random.randint(
            START_YEAR,
            END_YEAR,
        )

        duration_minutes = generate_duration(
            content_type
        )

        title = generate_title(content_id)

        content_records.append(
            [
                content_id,
                title,
                content_type,
                genre,
                release_year,
                duration_minutes,
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
                "content_id",
                "title",
                "content_type",
                "genre",
                "release_year",
                "duration_minutes",
            ]
        )

        writer.writerows(content_records)

    print(
        f"Generated {len(content_records)} new content records"
    )
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    generate_content()