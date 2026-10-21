import csv
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "data" / "generated" / "tmdb_trending.csv"
OUTPUT_FILE = PROJECT_ROOT / "data" / "generated" / "tmdb_trending_summary.csv"


def main():
    with INPUT_FILE.open("r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        movies = list(reader)

    movies.sort(
        key=lambda movie: float(movie["popularity"] or 0),
        reverse=True,
    )

    fields = [
        "trend_rank",
        "tmdb_id",
        "title",
        "release_date",
        "popularity",
        "vote_average",
        "vote_count",
        "original_language",
        "fetched_at",
    ]

    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()

        for rank, movie in enumerate(movies, start=1):
            writer.writerow(
                {
                    "trend_rank": rank,
                    "tmdb_id": movie["tmdb_id"],
                    "title": movie["title"],
                    "release_date": movie["release_date"],
                    "popularity": movie["popularity"],
                    "vote_average": movie["vote_average"],
                    "vote_count": movie["vote_count"],
                    "original_language": movie["original_language"],
                    "fetched_at": movie["fetched_at"],
                }
            )

    print(f"Processed {len(movies)} trending movies")
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()