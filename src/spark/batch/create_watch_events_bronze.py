from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType
)


# --------------------------------------------------
# Spark + Delta Lake configuration
# --------------------------------------------------

spark = SparkSession.builder \
    .appName("OTTCreateWatchEventsBronze") \
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    ) \
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    ) \
    .getOrCreate()


# --------------------------------------------------
# HDFS Raw Watch Events Path
# --------------------------------------------------

raw_watch_events_path = (
    "hdfs://localhost:9000/ott_media/raw/watch_events_generated/watch_events.csv"
)


# --------------------------------------------------
# Bronze Delta Path
# --------------------------------------------------

bronze_watch_events_path = (
    "hdfs://localhost:9000/ott_media/bronze/watch_events"
)


# --------------------------------------------------
# Watch Events Schema
# --------------------------------------------------

watch_events_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("content_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("watch_seconds", IntegerType(), True),
    StructField("buffer_seconds", IntegerType(), True),
    StructField("device_type", StringType(), True),
    StructField("country", StringType(), True)
])


# --------------------------------------------------
# Read Watch Events from HDFS
# --------------------------------------------------

watch_events_df = spark.read \
    .option("header", "true") \
    .schema(watch_events_schema) \
    .csv(raw_watch_events_path)


# --------------------------------------------------
# Display Raw Watch Events
# --------------------------------------------------

print("\n===== RAW WATCH EVENTS =====")
watch_events_df.show(truncate=False)


# --------------------------------------------------
# Write Bronze Delta Table
# --------------------------------------------------

watch_events_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(bronze_watch_events_path)


print("\n===== WATCH EVENTS BRONZE WRITTEN SUCCESSFULLY =====")
print("Path:", bronze_watch_events_path)


# --------------------------------------------------
# Stop Spark
# --------------------------------------------------

spark.stop()