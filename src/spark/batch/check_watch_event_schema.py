EXPECTED_COLUMNS = {
    "event_id",
    "user_id",
    "content_id",
    "event_type",
    "event_time",
    "watch_seconds",
    "buffer_seconds",
    "device",
    "country",
}


def validate_schema(actual_columns):
    actual_columns = set(actual_columns)

    missing_columns = EXPECTED_COLUMNS - actual_columns
    unexpected_columns = actual_columns - EXPECTED_COLUMNS

    if missing_columns or unexpected_columns:
        print("\n===== SCHEMA DRIFT DETECTED =====")

        if missing_columns:
            print(
                "Missing columns:",
                sorted(missing_columns)
            )

        if unexpected_columns:
            print(
                "Unexpected columns:",
                sorted(unexpected_columns)
            )

        return False

    print("\n===== SCHEMA VALIDATION =====")
    print("Schema status: PASS")
    print("All expected columns are present.")
    return True


if __name__ == "__main__":

    valid_schema = [
        "event_id",
        "user_id",
        "content_id",
        "event_type",
        "event_time",
        "watch_seconds",
        "buffer_seconds",
        "device",
        "country",
    ]

    validate_schema(valid_schema)
