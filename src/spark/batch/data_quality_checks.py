from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim


spark = (
    SparkSession.builder
    .appName("OTTDataQualityChecks")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

bronze_watch_events_path = (
    "hdfs://localhost:9000/ott_media/bronze/watch_events"
)

silver_watch_events_path = (
    "hdfs://localhost:9000/ott_media/silver/watch_events"
)

gold_user_path = (
    "hdfs://localhost:9000/ott_media/gold/dim_user"
)

gold_content_path = (
    "hdfs://localhost:9000/ott_media/gold/dim_content"
)

gold_device_path = (
    "hdfs://localhost:9000/ott_media/gold/dim_device"
)

gold_fact_path = (
    "hdfs://localhost:9000/ott_media/gold/fact_watch_events"
)



bronze_df = (
    spark.read
    .format("delta")
    .load(bronze_watch_events_path)
)

silver_df = (
    spark.read
    .format("delta")
    .load(silver_watch_events_path)
)

dim_user_df = (
    spark.read
    .format("delta")
    .load(gold_user_path)
)

dim_content_df = (
    spark.read
    .format("delta")
    .load(gold_content_path)
)

dim_device_df = (
    spark.read
    .format("delta")
    .load(gold_device_path)
)

fact_df = (
    spark.read
    .format("delta")
    .load(gold_fact_path)
)

def check_rule(rule_name, condition, failure_count):
    if failure_count == 0:
        print(f"PASS - {rule_name}")
        return True

    print(f"FAIL - {rule_name} | Invalid records: {failure_count}")
    return False

print("\n===== OTT DATA QUALITY REPORT =====")

all_checks_passed = True


# --------------------------------------------------
# 1. Required fields
# --------------------------------------------------

invalid_event_id = silver_df.filter(
    col("event_id").isNull() | (trim(col("event_id")) == "")
).count()

all_checks_passed &= check_rule(
    "event_id is not null or empty",
    invalid_event_id == 0,
    invalid_event_id
)


invalid_user_id = silver_df.filter(
    col("user_id").isNull()
).count()

all_checks_passed &= check_rule(
    "user_id is not null",
    invalid_user_id == 0,
    invalid_user_id
)


invalid_content_id = silver_df.filter(
    col("content_id").isNull()
).count()

all_checks_passed &= check_rule(
    "content_id is not null",
    invalid_content_id == 0,
    invalid_content_id
)


invalid_event_time = silver_df.filter(
    col("event_time").isNull()
).count()

all_checks_passed &= check_rule(
    "event_time is not null",
    invalid_event_time == 0,
    invalid_event_time
)

# --------------------------------------------------
# 2. Valid event types
# --------------------------------------------------

valid_event_types = {
    "play",
    "pause",
    "resume",
    "buffer",
    "stop",
}

invalid_event_type = silver_df.filter(
    ~col("event_type").isin(valid_event_types)
    | col("event_type").isNull()
).count()

all_checks_passed &= check_rule(
    "event_type contains only allowed values",
    invalid_event_type == 0,
    invalid_event_type
)


# --------------------------------------------------
# 3. Non-negative watch seconds
# --------------------------------------------------

invalid_watch_seconds = silver_df.filter(
    col("watch_seconds").isNull()
    | (col("watch_seconds") < 0)
).count()

all_checks_passed &= check_rule(
    "watch_seconds is non-negative",
    invalid_watch_seconds == 0,
    invalid_watch_seconds
)


# --------------------------------------------------
# 4. Non-negative buffer seconds
# --------------------------------------------------

invalid_buffer_seconds = silver_df.filter(
    col("buffer_seconds").isNull()
    | (col("buffer_seconds") < 0)
).count()

all_checks_passed &= check_rule(
    "buffer_seconds is non-negative",
    invalid_buffer_seconds == 0,
    invalid_buffer_seconds
)

# --------------------------------------------------
# 5. Valid device types
# --------------------------------------------------

valid_device_types = {
    "Mobile",
    "Web",
    "TV",
    "Tablet",
}

invalid_device_type = silver_df.filter(
    ~col("device_type").isin(valid_device_types)
    | col("device_type").isNull()
).count()

all_checks_passed &= check_rule(
    "device_type contains only allowed values",
    invalid_device_type == 0,
    invalid_device_type
)


# --------------------------------------------------
# 6. Country must not be null or empty
# --------------------------------------------------

invalid_country = silver_df.filter(
    col("country").isNull()
    | (trim(col("country")) == "")
).count()

all_checks_passed &= check_rule(
    "country is not null or empty",
    invalid_country == 0,
    invalid_country
)

# --------------------------------------------------
# 7. Silver event_id uniqueness
# --------------------------------------------------

silver_total_count = silver_df.count()

silver_unique_event_ids = (
    silver_df
    .select("event_id")
    .distinct()
    .count()
)

duplicate_silver_event_ids = (
    silver_total_count - silver_unique_event_ids
)

all_checks_passed &= check_rule(
    "Silver event_id values are unique",
    duplicate_silver_event_ids == 0,
    duplicate_silver_event_ids
)

# --------------------------------------------------
# 8. Fact -> User dimension integrity
# --------------------------------------------------

missing_users = (
    fact_df
    .select("user_id")
    .distinct()
    .join(
        dim_user_df.select("user_id").distinct(),
        on="user_id",
        how="left_anti"
    )
    .count()
)

all_checks_passed &= check_rule(
    "Every fact user_id exists in dim_user",
    missing_users == 0,
    missing_users
)


# --------------------------------------------------
# 9. Fact -> Content dimension integrity
# --------------------------------------------------

missing_content = (
    fact_df
    .select("content_id")
    .distinct()
    .join(
        dim_content_df.select("content_id").distinct(),
        on="content_id",
        how="left_anti"
    )
    .count()
)

all_checks_passed &= check_rule(
    "Every fact content_id exists in dim_content",
    missing_content == 0,
    missing_content
)


# --------------------------------------------------
# 10. Fact -> Device dimension integrity
# --------------------------------------------------

missing_devices = (
    fact_df
    .select("device_key")
    .distinct()
    .join(
        dim_device_df.select("device_key").distinct(),
        on="device_key",
        how="left_anti"
    )
    .count()
)

all_checks_passed &= check_rule(
    "Every fact device_key exists in dim_device",
    missing_devices == 0,
    missing_devices
)

# --------------------------------------------------
# 11. Bronze -> Silver reconciliation
# --------------------------------------------------

bronze_count = bronze_df.count()
silver_count = silver_df.count()

expected_duplicates_removed = bronze_count - silver_count

print("\n===== RECORD RECONCILIATION =====")
print("Bronze records :", bronze_count)
print("Silver records :", silver_count)
print("Records removed:", expected_duplicates_removed)

reconciliation_ok = (
    bronze_count == 52500
    and silver_count == 50000
    and expected_duplicates_removed == 2500
)

all_checks_passed &= check_rule(
    "Bronze to Silver reconciliation is 52,500 -> 50,000",
    reconciliation_ok,
    0 if reconciliation_ok else abs(
        (bronze_count - 52500) + (silver_count - 50000)
    )
)

# --------------------------------------------------
# Final DQ result
# --------------------------------------------------

print("\n===== FINAL DATA QUALITY RESULT =====")

if all_checks_passed:
    print("ALL DATA QUALITY CHECKS PASSED")
else:
    print("DATA QUALITY CHECKS FAILED")
    spark.stop()
    raise SystemExit(1)


spark.stop()