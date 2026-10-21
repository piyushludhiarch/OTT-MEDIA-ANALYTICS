from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("OTTInspectContent")
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

content_path = "hdfs://localhost:9000/ott_media/gold/dim_content"

content_df = (
    spark.read
    .format("delta")
    .load(content_path)
)

print("\n===== OTT CONTENT SCHEMA =====")
content_df.printSchema()

print("\n===== SAMPLE CONTENT RECORDS =====")
content_df.show(20, truncate=False)

print("\n===== CONTENT COUNT =====")
print("Total content:", content_df.count())

spark.stop()