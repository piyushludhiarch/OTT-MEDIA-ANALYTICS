from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    when
)


spark = (
    SparkSession.builder
    .appName("OTTCreateGoldLayer")
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


# ============================================================
# Silver paths
# ============================================================

silver_subscribers_path = (
    "hdfs://localhost:9000/ott_media/silver/subscribers"
)

silver_content_path = (
    "hdfs://localhost:9000/ott_media/silver/content"
)

silver_watch_events_path = (
    "hdfs://localhost:9000/ott_media/silver/watch_events"
)


# ============================================================
# Gold paths
# ============================================================

gold_base_path = (
    "hdfs://localhost:9000/ott_media/gold"
)

gold_user_path = f"{gold_base_path}/dim_user"
gold_content_path = f"{gold_base_path}/dim_content"
gold_device_path = f"{gold_base_path}/dim_device"
gold_fact_path = f"{gold_base_path}/fact_watch_events"


# ============================================================
# Read Silver tables
# ============================================================

subscribers_df = (
    spark.read
    .format("delta")
    .load(silver_subscribers_path)
)

content_df = (
    spark.read
    .format("delta")
    .load(silver_content_path)
)

watch_events_df = (
    spark.read
    .format("delta")
    .load(silver_watch_events_path)
)


# ============================================================
# 1. Dimension: User
# ============================================================

dim_user = (
    subscribers_df
    .select(
        col("subscriber_id").alias("user_id"),
        col("name"),
        col("email"),
        col("plan_id"),
        col("country"),
        col("signup_date")
    )
    .dropDuplicates(["user_id"])
)


# ============================================================
# 2. Dimension: Content
# ============================================================

dim_content = (
    content_df
    .select(
        col("content_id"),
        col("title"),
        col("content_type"),
        col("genre"),
        col("duration_minutes"),
        col("release_year")
    )
    .dropDuplicates(["content_id"])
)


# ============================================================
# 3. Dimension: Device
# ============================================================

dim_device = (
    watch_events_df
    .select("device_type")
    .filter(col("device_type").isNotNull())
    .dropDuplicates(["device_type"])
    .withColumn(
        "device_key",
        when(col("device_type") == "Mobile", 1)
        .when(col("device_type") == "TV", 2)
        .when(col("device_type") == "Tablet", 3)
        .when(col("device_type") == "Web", 4)
    )
    .select(
        "device_key",
        "device_type"
    )
)

# ============================================================
# 4. Fact: Watch Events
# ============================================================

fact_watch_events = (
    watch_events_df
    .join(
        dim_device,
        on="device_type",
        how="left"
    )
    .select(
        col("event_id"),
        col("user_id"),
        col("content_id"),
        col("device_key"),
        col("event_type"),
        col("event_time"),
        col("watch_seconds"),
        col("buffer_seconds"),
        col("country"),
        to_date(col("event_time")).alias("event_date")
    )
)


# ============================================================
# Validation
# ============================================================

print("\n===== GOLD LAYER VALIDATION =====")

print("dim_user records       :", dim_user.count())
print("dim_content records    :", dim_content.count())
print("dim_device records     :", dim_device.count())
print("fact_watch_events      :", fact_watch_events.count())

print("\n===== DEVICE VALUES =====")
dim_device.orderBy("device_key").show(
    truncate=False
)


# ============================================================
# Write Gold Delta tables
# ============================================================

(
    dim_user.write
    .format("delta")
    .mode("overwrite")
    .save(gold_user_path)
)

(
    dim_content.write
    .format("delta")
    .mode("overwrite")
    .save(gold_content_path)
)

(
    dim_device.write
    .format("delta")
    .mode("overwrite")
    .save(gold_device_path)
)

(
    fact_watch_events.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("event_date", "country")
    .save(gold_fact_path)
)


print("\n===== GOLD LAYER WRITTEN SUCCESSFULLY =====")
print("dim_user        :", gold_user_path)
print("dim_content     :", gold_content_path)
print("dim_device      :", gold_device_path)
print("fact_watch_events:", gold_fact_path)


spark.stop()