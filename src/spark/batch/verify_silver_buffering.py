from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum

spark = (
    SparkSession.builder
    .appName("VerifySilverBuffering")
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

df = spark.read.format("delta").load(
    "hdfs://localhost:9000/ott_media/silver/watch_events"
)

result = df.select(
    _sum("watch_seconds").alias("total_watch_seconds"),
    _sum("buffer_seconds").alias("total_buffer_seconds")
).collect()[0]

watch = result["total_watch_seconds"]
buffer = result["total_buffer_seconds"]

ratio = (
    buffer / (watch + buffer)
    if (watch + buffer) > 0
    else 0
)

print("\n===== SILVER BUFFERING VALIDATION =====")
print(f"Silver records:        {df.count():,}")
print(f"Total watch seconds:   {watch:,}")
print(f"Total buffer seconds:  {buffer:,}")
print(f"Buffering ratio:       {ratio:.4%}")
print(f"SLA target:            < 0.80%")
print(f"SLA status:            {'PASS' if ratio < 0.008 else 'FAIL'}")

spark.stop()
