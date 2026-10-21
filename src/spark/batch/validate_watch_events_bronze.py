from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("OTTValidateWatchEventsBronze") \
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    ) \
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    ) \
    .getOrCreate()


bronze_watch_events_path = (
    "hdfs://localhost:9000/ott_media/bronze/watch_events"
)


watch_events_df = spark.read \
    .format("delta") \
    .load(bronze_watch_events_path)


print("\n===== WATCH EVENTS BRONZE SCHEMA =====")
watch_events_df.printSchema()


print("\n===== WATCH EVENTS BRONZE DATA =====")
watch_events_df.show(truncate=False)


print("\n===== WATCH EVENTS BRONZE COUNT =====")
print("Total records:", watch_events_df.count())


print("\n===== CHECK DUPLICATE EVENT =====")
watch_events_df \
    .filter(watch_events_df.event_id == "evt006") \
    .show(truncate=False)

from pyspark.sql.functions import sum as _sum

result = watch_events_df.select(
    _sum("watch_seconds").alias("total_watch_seconds"),
    _sum("buffer_seconds").alias("total_buffer_seconds")
).collect()[0]

watch = result["total_watch_seconds"]
buffer = result["total_buffer_seconds"]
ratio = buffer / (watch + buffer) if (watch + buffer) else 0

print("\n===== BRONZE BUFFERING VALIDATION =====")
print(f"Total watch seconds:  {watch:,}")
print(f"Total buffer seconds: {buffer:,}")
print(f"Buffering ratio:      {ratio:.4%}")

spark.stop()