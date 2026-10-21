import csv
import json
import os
from pathlib import Path
from urllib.request import Request, urlopen


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / "configs" / "tmdb" / ".env"
OUTPUT_FILE = PROJECT_ROOT / "data" / "generated" / "tmdb_trending.csv"


def load_token():
    if not ENV_FILE.exists():
        raise FileNotFoundError(f"TMDB environment file not found: {ENV_FILE}")

    for line in ENV_FILE.read_text().splitlines():
        line = line.strip()

        if line.startswith("TMDB_API_READ_ACCESS_TOKEN="):
            return line.split("=", 1)[1].strip()

    raise ValueError("TMDB_API_READ_ACCESS_TOKEN not found in .env")


def fetch_trending_movies(token):
    url = "https://api.themoviedb.org/3/trending/movie/day"

    request = Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "accept": "application/json",
        },
    )

    with urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))
def convert_json_to_csv():
    import json
    from datetime import datetime, timezone

    input_file = PROJECT_ROOT / "data" / "generated" / "tmdb_trending.json"
    output_file = PROJECT_ROOT / "data" / "generated" / "tmdb_trending.csv"

    data = json.loads(
        input_file.read_text(encoding="utf-8-sig")
    )

    results = data.get("results", [])

    fields = [
        "tmdb_id",
        "title",
        "release_date",
        "popularity",
        "vote_average",
        "vote_count",
        "original_language",
        "fetched_at",
    ]

    fetched_at = datetime.now(timezone.utc).isoformat()

    with output_file.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()

        for movie in results:
            writer.writerow(
                {
                    "tmdb_id": movie.get("id"),
                    "title": movie.get("title"),
                    "release_date": movie.get("release_date"),
                    "popularity": movie.get("popularity"),
                    "vote_average": movie.get("vote_average"),
                    "vote_count": movie.get("vote_count"),
                    "original_language": movie.get("original_language"),
                    "fetched_at": fetched_at,
                }
            )

    print(f"Converted {len(results)} movies to CSV")
    print(f"Output: {output_file}")    

def save_trending_movies(data):
    results = data.get("results", [])

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    fields = [
        "tmdb_id",
        "title",
        "release_date",
        "popularity",
        "vote_average",
        "vote_count",
        "original_language",
        "fetched_at",
    ]

    from datetime import datetime, timezone

    fetched_at = datetime.now(timezone.utc).isoformat()

    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()

        for movie in results:
            writer.writerow(
                {
                    "tmdb_id": movie.get("id"),
                    "title": movie.get("title"),
                    "release_date": movie.get("release_date"),
                    "popularity": movie.get("popularity"),
                    "vote_average": movie.get("vote_average"),
                    "vote_count": movie.get("vote_count"),
                    "original_language": movie.get("original_language"),
                    "fetched_at": fetched_at,
                }
            )

    print(f"Saved {len(results)} trending movies")
    print(f"Output: {OUTPUT_FILE}")

def main():
    convert_json_to_csv()


if __name__ == "__main__":
    main()