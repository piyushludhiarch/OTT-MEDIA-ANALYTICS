from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim


# --------------------------------------------------
# Spark + Delta Lake configuration
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("OTTCreateWatchEventsSilver")
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


# --------------------------------------------------
# Paths
# --------------------------------------------------

bronze_watch_events_path = (
    "hdfs://localhost:9000/ott_media/bronze/watch_events"
)

silver_watch_events_path = (
    "hdfs://localhost:9000/ott_media/silver/watch_events"
)


# --------------------------------------------------
# Read Bronze Watch Events
# --------------------------------------------------

watch_events_df = (
    spark.read
    .format("delta")
    .load(bronze_watch_events_path)
)


# --------------------------------------------------
# Silver transformations
# --------------------------------------------------

silver_watch_events_df = (
    watch_events_df
    .filter(col("event_id").isNotNull())
    .filter(col("user_id").isNotNull())
    .filter(col("content_id").isNotNull())
    .withColumn("event_id", trim(col("event_id")))
    .withColumn("event_type", trim(col("event_type")))
    .withColumn("device_type", trim(col("device_type")))
    .withColumn("country", trim(col("country")))
    .dropDuplicates(["event_id"])
)


# --------------------------------------------------
# Validation
# --------------------------------------------------

input_count = watch_events_df.count()
output_count = silver_watch_events_df.count()

print("\n===== WATCH EVENTS SILVER VALIDATION =====")
print("Bronze records :", input_count)
print("Silver records :", output_count)
print("Duplicates removed:", input_count - output_count)


# --------------------------------------------------
# Write Silver Delta
# --------------------------------------------------

(
    silver_watch_events_df.write
    .format("delta")
    .mode("overwrite")
    .save(silver_watch_events_path)
)

print("\n===== WATCH EVENTS SILVER WRITTEN SUCCESSFULLY =====")
print("Path:", silver_watch_events_path)


# --------------------------------------------------
# Stop Spark
# --------------------------------------------------

spark.stop()