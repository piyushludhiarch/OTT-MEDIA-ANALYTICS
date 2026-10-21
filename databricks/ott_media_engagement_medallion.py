# Databricks notebook source
# MAGIC %md
# MAGIC # OTT Media Engagement — Databricks Medallion Pipeline
# MAGIC
# MAGIC Runs the project's generated watch events, subscribers, and content data through Bronze, Silver, Gold, and data-quality checks using Unity Catalog managed Delta tables.

# COMMAND ----------

from typing import Any, Callable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Databricks provides these globals when notebook cells execute.
spark: SparkSession
dbutils: Any
display: Callable[..., Any]

CATALOG = "workspace"
SCHEMA = "default"
VOLUME = "ott_media_engagement"
INPUT_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

TABLES = {
    "bronze_events": "ott_bronze_watch_events",
    "bronze_subscribers": "ott_bronze_subscribers",
    "bronze_content": "ott_bronze_content",
    "silver_events": "ott_silver_watch_events",
    "silver_subscribers": "ott_silver_subscribers",
    "silver_content": "ott_silver_content",
    "dim_user": "ott_dim_user",
    "dim_content": "ott_dim_content",
    "dim_device": "ott_dim_device",
    "fact_events": "ott_fact_watch_events",
}

EVENTS_PATH = f"{INPUT_ROOT}/watch_events.csv"
SUBSCRIBERS_PATH = f"{INPUT_ROOT}/subscribers.csv"
CONTENT_PATH = f"{INPUT_ROOT}/content.csv"

available_input_files = {entry.name for entry in dbutils.fs.ls(INPUT_ROOT)}
required_input_files = {
    "watch_events.csv",
    "subscribers.csv",
    "content.csv",
}
missing_input_files = required_input_files - available_input_files
if missing_input_files:
    raise FileNotFoundError(
        f"Missing input files in {INPUT_ROOT}: {sorted(missing_input_files)}"
    )

print(f"Catalog/schema: {CATALOG}.{SCHEMA}")
print(f"Input volume: {INPUT_ROOT}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze — load source CSVs as Delta tables

# COMMAND ----------

events_schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("content_id", IntegerType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("watch_seconds", IntegerType(), True),
        StructField("buffer_seconds", IntegerType(), True),
        StructField("device_type", StringType(), True),
        StructField("country", StringType(), True),
    ]
)

subscribers_schema = StructType(
    [
        StructField("subscriber_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("plan_id", IntegerType(), True),
        StructField("signup_date", DateType(), True),
    ]
)

content_schema = StructType(
    [
        StructField("content_id", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("content_type", StringType(), True),
        StructField("genre", StringType(), True),
        StructField("release_year", IntegerType(), True),
        StructField("duration_minutes", IntegerType(), True),
    ]
)

source_data = {
    "bronze_events": spark.read.option("header", True).schema(events_schema).csv(EVENTS_PATH),
    "bronze_subscribers": spark.read.option("header", True).schema(subscribers_schema).csv(SUBSCRIBERS_PATH),
    "bronze_content": spark.read.option("header", True).schema(content_schema).csv(CONTENT_PATH),
}

for key, dataframe in source_data.items():
    dataframe.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(TABLES[key])

bronze_events = spark.table(TABLES["bronze_events"])
print(f"Bronze watch events: {bronze_events.count():,}")
display(bronze_events.limit(10))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver — standardize and deduplicate records

# COMMAND ----------

silver_events = (
    spark.table(TABLES["bronze_events"])
    .filter(F.col("event_id").isNotNull() & (F.trim("event_id") != ""))
    .filter(F.col("user_id").isNotNull() & F.col("content_id").isNotNull())
    .withColumn("event_id", F.trim("event_id"))
    .withColumn("event_type", F.trim("event_type"))
    .withColumn("device_type", F.trim("device_type"))
    .withColumn("country", F.trim("country"))
    .dropDuplicates(["event_id"])
)

silver_subscribers = (
    spark.table(TABLES["bronze_subscribers"])
    .filter(F.col("subscriber_id").isNotNull())
    .dropDuplicates(["subscriber_id"])
    .withColumn("name", F.trim("name"))
    .withColumn("email", F.trim("email"))
    .withColumn("country", F.trim("country"))
)

silver_content = (
    spark.table(TABLES["bronze_content"])
    .filter(F.col("content_id").isNotNull())
    .dropDuplicates(["content_id"])
    .withColumn("title", F.trim("title"))
    .withColumn("content_type", F.trim("content_type"))
    .withColumn("genre", F.trim("genre"))
)

for key, dataframe in (
    ("silver_events", silver_events),
    ("silver_subscribers", silver_subscribers),
    ("silver_content", silver_content),
):
    dataframe.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(TABLES[key])

bronze_count = spark.table(TABLES["bronze_events"]).count()
silver_count = spark.table(TABLES["silver_events"]).count()
print(
    f"Bronze -> Silver watch events: {bronze_count:,} -> {silver_count:,} "
    f"({bronze_count - silver_count:,} duplicates removed)"
)
display(spark.table(TABLES["silver_events"]).limit(10))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold — dimensions and partitioned watch-event fact table

# COMMAND ----------

users = spark.table(TABLES["silver_subscribers"])
content = spark.table(TABLES["silver_content"])
events = spark.table(TABLES["silver_events"])

dim_user = users.select(
    F.col("subscriber_id").alias("user_id"),
    "name",
    "email",
    "plan_id",
    "country",
    "signup_date",
).dropDuplicates(["user_id"])

missing_user_ids = (
    events.select("user_id")
    .distinct()
    .join(users.select("subscriber_id"), users.subscriber_id == F.col("user_id"), "left_anti")
    .select("user_id")
)
missing_users = missing_user_ids.select(
    "user_id",
    F.lit(None).cast(StringType()).alias("name"),
    F.lit(None).cast(StringType()).alias("email"),
    F.lit(None).cast(IntegerType()).alias("plan_id"),
    F.lit(None).cast(StringType()).alias("country"),
    F.lit(None).cast(DateType()).alias("signup_date"),
)
dim_user = dim_user.unionByName(missing_users).dropDuplicates(["user_id"])

dim_content = content.select(
    "content_id",
    "title",
    "content_type",
    "genre",
    "duration_minutes",
    "release_year",
).dropDuplicates(["content_id"])

missing_content_ids = (
    events.select("content_id")
    .distinct()
    .join(content.select("content_id"), on="content_id", how="left_anti")
)
missing_content = missing_content_ids.select(
    "content_id",
    F.concat(F.lit("Legacy Content "), F.col("content_id")).alias("title"),
    F.lit("Unknown").alias("content_type"),
    F.lit(None).cast(StringType()).alias("genre"),
    F.lit(None).cast(IntegerType()).alias("duration_minutes"),
    F.lit(None).cast(IntegerType()).alias("release_year"),
)
dim_content = dim_content.unionByName(missing_content).dropDuplicates(["content_id"])

dim_device = (
    events.select("device_type")
    .filter(F.col("device_type").isNotNull())
    .dropDuplicates(["device_type"])
    .withColumn(
        "device_key",
        F.when(F.col("device_type") == "Mobile", 1)
        .when(F.col("device_type") == "TV", 2)
        .when(F.col("device_type") == "Tablet", 3)
        .when(F.col("device_type") == "Web", 4),
    )
    .select("device_key", "device_type")
)

fact_events = (
    events.join(dim_device, on="device_type", how="left")
    .select(
        "event_id",
        "user_id",
        "content_id",
        "device_key",
        "event_type",
        "event_time",
        "watch_seconds",
        "buffer_seconds",
        "country",
        F.to_date("event_time").alias("event_date"),
    )
)

for key, dataframe in (
    ("dim_user", dim_user),
    ("dim_content", dim_content),
    ("dim_device", dim_device),
):
    dataframe.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(TABLES[key])

fact_events.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).partitionBy("event_date", "country").saveAsTable(TABLES["fact_events"])

gold_counts = {
    key: spark.table(TABLES[key]).count()
    for key in ("dim_user", "dim_content", "dim_device", "fact_events")
}
print("Gold table row counts:", gold_counts)
print(f"User dimension rows with missing source profiles: {missing_user_ids.count()}")
print(f"Content dimension rows with missing source metadata: {missing_content_ids.count()}")
display(spark.table(TABLES["fact_events"]).limit(10))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data quality and reconciliation

# COMMAND ----------

silver = spark.table(TABLES["silver_events"])
fact = spark.table(TABLES["fact_events"])
gold_users = spark.table(TABLES["dim_user"])
gold_content = spark.table(TABLES["dim_content"])
gold_devices = spark.table(TABLES["dim_device"])

checks = {
    "Bronze has 52,500 source rows": bronze_count == 52_500,
    "Silver has 50,000 unique events": silver_count == 50_000,
    "Exactly 2,500 duplicate events removed": bronze_count - silver_count == 2_500,
    "1,000 users in user dimension": gold_counts["dim_user"] == 1_000,
    "300 titles in content dimension": gold_counts["dim_content"] == 300,
    "All 50,000 silver events reach the fact table": gold_counts["fact_events"] == silver_count,
    "Silver event IDs are unique": silver.select("event_id").distinct().count() == silver_count,
    "All fact users exist in the user dimension": fact.select("user_id").distinct().join(
        gold_users.select("user_id").distinct(), "user_id", "left_anti"
    ).count() == 0,
    "All fact content IDs exist in the content dimension": fact.select("content_id").distinct().join(
        gold_content.select("content_id").distinct(), "content_id", "left_anti"
    ).count() == 0,
    "All fact device keys exist in the device dimension": fact.select("device_key").distinct().join(
        gold_devices.select("device_key").distinct(), "device_key", "left_anti"
    ).count() == 0,
}

for check_name, passed in checks.items():
    print(f"{'PASS' if passed else 'FAIL'} - {check_name}")

assert all(checks.values()), "One or more Databricks data-quality checks failed."

qoe = fact.agg(
    F.sum("watch_seconds").alias("watch_seconds"),
    F.sum("buffer_seconds").alias("buffer_seconds"),
).first()
buffering_ratio_pct = (
    100 * qoe["buffer_seconds"] / (qoe["watch_seconds"] + qoe["buffer_seconds"])
    if qoe["watch_seconds"] + qoe["buffer_seconds"]
    else 0.0
)
print(f"Watch seconds: {qoe['watch_seconds']:,}")
print(f"Buffer seconds: {qoe['buffer_seconds']:,}")
print(f"Buffering ratio: {buffering_ratio_pct:.4f}%")
print(f"All {len(checks)} Databricks data-quality checks passed.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Output tables
# MAGIC
# MAGIC All results are managed Delta tables in `workspace.default`: Bronze source tables, Silver cleaned tables, and Gold dimensions/fact. The generated subscriber and content CSVs contain 990 and 285 rows, while watch events reference 1,000 users and 300 content IDs. Gold dimensions therefore retain the event-referenced IDs with explicit null source attributes for the 10 missing subscriber profiles and placeholder metadata for the 15 missing content records. Re-running this notebook overwrites only the project-prefixed `ott_*` tables.
