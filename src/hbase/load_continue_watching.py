from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


# ---------------------------------------------------------
# Spark Session
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("OTTContinueWatchingHBase")
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


# ---------------------------------------------------------
# Paths
# ---------------------------------------------------------

silver_watch_events_path = (
    "hdfs://localhost:9000/ott_media/silver/watch_events"
)


# ---------------------------------------------------------
# Read Silver Watch Events
# ---------------------------------------------------------

watch_events = (
    spark.read
    .format("delta")
    .load(silver_watch_events_path)
)


# ---------------------------------------------------------
# Get Latest Watch Event for Each User
# ---------------------------------------------------------

window_spec = (
    Window
    .partitionBy("user_id")
    .orderBy(col("event_time").desc())
)

latest_events = (
    watch_events
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)


# ---------------------------------------------------------
# Validation
# ---------------------------------------------------------

print("\n===== CONTINUE WATCHING VALIDATION =====")

print(
    "Silver records       :",
    watch_events.count()
)

print(
    "Unique users         :",
    latest_events.count()
)


# ---------------------------------------------------------
# Show Sample Records
# ---------------------------------------------------------

latest_events.select(
    "user_id",
    "content_id",
    "event_type",
    "watch_seconds",
    "event_time",
    "device_type"
).show(10, truncate=False)


# ---------------------------------------------------------
# Write Latest Events to HBase
# ---------------------------------------------------------

def write_to_hbase(partition):

    import happybase

    # Connect to HBase Thrift server
    connection = happybase.Connection(
        host="localhost",
        port=9090,
        timeout=10000
    )

    connection.open()

    table = connection.table("continue_watching")

    # Use a batch for efficient writes
    batch = table.batch()

    count = 0

    for row in partition:

        user_id = str(row["user_id"])

        batch.put(
            user_id.encode("utf-8"),
            {
                b"watch:content_id":
                    str(row["content_id"]).encode("utf-8"),

                b"watch:event_type":
                    str(row["event_type"]).encode("utf-8"),

                b"watch:watch_seconds":
                    str(row["watch_seconds"]).encode("utf-8"),

                b"watch:event_time":
                    str(row["event_time"]).encode("utf-8"),

                b"watch:device_type":
                    str(row["device_type"]).encode("utf-8")
            }
        )

        count += 1

    # Flush all writes
    batch.send()

    connection.close()


# ---------------------------------------------------------
# Load into HBase
# ---------------------------------------------------------

latest_events.rdd.foreachPartition(write_to_hbase)


# ---------------------------------------------------------
# Final Validation
# ---------------------------------------------------------

print("\n===== HBASE LOAD COMPLETED =====")

print(
    "Records written to continue_watching:",
    latest_events.count()
)


# ---------------------------------------------------------
# Stop Spark
# ---------------------------------------------------------

spark.stop()