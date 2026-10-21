from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, TimestampType
)
from pyspark.sql.functions import from_json, col


spark = (
    SparkSession.builder
    .appName("OTTWatchEventsStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "watch_events_full")
    .option("startingOffsets", "earliest")
    .load()
)


event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("content_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("watch_seconds", StringType(), False),
    StructField("buffer_seconds", StringType(), False),
    StructField("device_type", StringType(), False),
    StructField("country", StringType(), False),
])

events_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json("json_value", event_schema).alias("event"))
    .select("event.*")
)

from pyspark.sql.functions import from_json, col, window, count

events_df = (
    events_df
    .withColumn("user_id", col("user_id").cast("int"))
    .withColumn("content_id", col("content_id").cast("int"))
    .withColumn("watch_seconds", col("watch_seconds").cast("int"))   #converting the strings to integers
    .withColumn("buffer_seconds", col("buffer_seconds").cast("int"))
)


#events_df = events_df.dropDuplicates(["event_id"])   # this is for removing duplicates from the stream based on event_id
# if we get 50000 we have an evidencde that the duplicates are removed and we have only unique records in the batch
# it is proved that deduplication is worked and 50000 unique records are present in the batch and duplicates are removed from the stream based on event_id
events_df = (
    events_df
    .withWatermark("event_time", "10 minutes")
    .dropDuplicates(["event_id"])                      #deduplication + 10mins watermarking to handle late arriving data and remove duplicates based on event_id
)
# from pyspark.sql.functions import window, count

# trending_df = (
#     events_df
#     .filter(events_df.event_type.isin("play", "resume"))
#     .groupBy(
#         window("event_time", "15 minutes"),                  # Number of play/resume events for each content title within every 15-minute window.
#         "content_id"
#     )
#     .agg(
#         count("*").alias("play_count")
#     )
#     .orderBy("window", "play_count", ascending=False)
# )

#def show_batch_count(batch_df, batch_id):
  #  print(f"Batch {batch_id} record count: {batch_df.count()}")      # this is to check weather the depluicated records are removed  and count of records which are unique in the batch


def show_trending(batch_df, batch_id):
    print(f"Batch {batch_id} record count after dedup: {batch_df.count()}")

    total_watch = batch_df.selectExpr(
        "sum(watch_seconds) as total_watch"
    ).collect()[0]["total_watch"] or 0
                                                   #Buffering Ratio = total buffer seconds ÷ (total watch seconds + total buffer seconds)
    total_buffer = batch_df.selectExpr(
        "sum(buffer_seconds) as total_buffer"
    ).collect()[0]["total_buffer"] or 0

    buffering_ratio = (
        total_buffer / (total_watch + total_buffer)
        if (total_watch + total_buffer) > 0
        else 0
    )

    print(f"Total watch seconds: {total_watch}")
    print(f"Total buffer seconds: {total_buffer}")
    print(f"Buffering ratio: {buffering_ratio:.4f}")

    trending = (
        batch_df
        .filter(batch_df.event_type.isin("play", "resume"))
        .groupBy(
            window("event_time", "15 minutes"),              #see the actual 15-minute Trending Now results.
            "content_id"
        )
        .agg(
            count("*").alias("play_count")
        )
        .orderBy("play_count", ascending=False)
    )

    trending.show(20, truncate=False)

query = (
    events_df.writeStream
    .foreachBatch(show_trending)
    .outputMode("append")
    .option("checkpointLocation", "/ott_media/checkpoints/watch_events")
    .trigger(once=True)
    .start()
)

query.awaitTermination()

spark.stop()