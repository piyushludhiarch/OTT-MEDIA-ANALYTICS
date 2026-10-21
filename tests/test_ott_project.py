def calculate_buffering_ratio(watch_seconds, buffer_seconds):
    total = watch_seconds + buffer_seconds
    return buffer_seconds / total if total > 0 else 0


def deduplicate_events(events):
    seen = set()
    result = []

    for event in events:
        event_id = event["event_id"]

        if event_id not in seen:
            seen.add(event_id)
            result.append(event)

    return result


def validate_event_schema(event):
    required_columns = {
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

    return required_columns.issubset(event.keys())


def calculate_precision_at_5(relevant_results, retrieved_results):
    if not retrieved_results:
        return 0.0

    relevant_count = sum(
        1
        for item in retrieved_results
        if item in relevant_results
    )

    return relevant_count / len(retrieved_results)


def test_buffering_ratio_meets_sla():
    ratio = calculate_buffering_ratio(
        9893021,
        53373
    )

    assert ratio < 0.008


def test_duplicate_events_are_removed():
    events = [
        {"event_id": "evt001"},
        {"event_id": "evt002"},
        {"event_id": "evt001"},
        {"event_id": "evt003"},
    ]

    result = deduplicate_events(events)

    assert len(result) == 3
    assert [event["event_id"] for event in result] == [
        "evt001",
        "evt002",
        "evt003",
    ]


def test_valid_event_schema():
    event = {
        "event_id": "evt001",
        "user_id": "user001",
        "content_id": "content001",
        "event_type": "play",
        "event_time": "2026-10-01T10:00:00",
        "watch_seconds": 120,
        "buffer_seconds": 2,
        "device": "mobile",
        "country": "India",
    }

    assert validate_event_schema(event)


def test_invalid_event_schema():
    event = {
        "event_id": "evt001",
        "user_id": "user001",
        "content_id": "content001",
    }

    assert not validate_event_schema(event)


def test_rag_precision_at_5():
    relevant = {
        "Shadow Protocol",
        "Hidden Destiny 356",
        "Secret Warrior 194",
    }

    retrieved = [
        "Shadow Protocol",
        "Hidden Destiny 356",
        "Secret Warrior 194",
        "Other Title",
        "Another Title",
    ]

    precision = calculate_precision_at_5(
        relevant,
        retrieved
    )

    assert precision == 0.60
