from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("OTTPowerBIExport")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

FACT_PATH = "hdfs://localhost:9000/ott_media/hive_gold/fact_watch_events"
CONTENT_PATH = "hdfs://localhost:9000/ott_media/hive_gold/dim_content"
USER_PATH = "hdfs://localhost:9000/ott_media/hive_gold/dim_user"

OUTPUT_PATH = (
    "file:///home/mohit/ott-media-engagement/powerbi/data/"
    "ott_watch_events_csv"
)

fact = spark.read.parquet(FACT_PATH)
content = spark.read.parquet(CONTENT_PATH)
users = spark.read.parquet(USER_PATH)

powerbi_df = (
    fact
    .join(
        content,
        fact.content_id == content.content_id,
        "left"
    )
    .join(
        users,
        fact.user_id == users.user_id,
        "left"
    )
    .select(
        fact.event_id,
        fact.user_id,
        fact.content_id,
        fact.device_key,
        fact.event_type,
        fact.event_time,
        fact.watch_seconds,
        fact.buffer_seconds,
        fact.event_date,
        fact.country,
        content.title,
        content.content_type,
        content.genre,
        content.duration_minutes,
        content.release_year,
        users.plan_id,
        users.signup_date
    )
)

print("===== POWER BI DATASET =====")
print("Rows:", powerbi_df.count())
print("Columns:", len(powerbi_df.columns))

(
    powerbi_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(OUTPUT_PATH)
)

print("Power BI export completed.")
print("Output:", OUTPUT_PATH)

spark.stop()