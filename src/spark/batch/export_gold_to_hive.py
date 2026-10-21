from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("OTTExportGoldToHive")
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


gold_base_path = "hdfs://localhost:9000/ott_media/gold"
hive_gold_base_path = "hdfs://localhost:9000/ott_media/hive_gold"


tables = [
    "dim_user",
    "dim_content",
    "dim_device",
    "fact_watch_events"
]


for table in tables:
    source_path = f"{gold_base_path}/{table}"
    target_path = f"{hive_gold_base_path}/{table}"

    print(f"\nExporting {table}...")

    df = (
        spark.read
        .format("delta")
        .load(source_path)
    )

    print(f"{table} records: {df.count()}")

    if table == "fact_watch_events":
        (
            df.write
            .format("parquet")
            .mode("overwrite")
            .partitionBy("event_date", "country")
            .save(target_path)
        )
    else:
        (
            df.write
            .format("parquet")
            .mode("overwrite")
            .save(target_path)
        )

    print(f"{table} exported to: {target_path}")


print("\n===== GOLD PARQUET EXPORT COMPLETED =====")


spark.stop()