from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ValidateOTTBronze") \
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    ) \
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    ) \
    .getOrCreate()

    
plans_path = "hdfs://localhost:9000/ott_media/bronze/plans"
subscribers_path = "hdfs://localhost:9000/ott_media/bronze/subscribers"
content_path = "hdfs://localhost:9000/ott_media/bronze/content"

plans_df = spark.read.format("delta").load(plans_path)
subscribers_df = spark.read.format("delta").load(subscribers_path)
content_df = spark.read.format("delta").load(content_path)

print("\n===== PLANS BRONZE =====")
plans_df.printSchema()
plans_df.show()

print("\n===== SUBSCRIBERS BRONZE =====")
subscribers_df.printSchema()
subscribers_df.show()

print("\n===== CONTENT BRONZE =====")
content_df.printSchema()
content_df.show()

print("\n===== RECORD COUNTS =====")
print("Plans:", plans_df.count())
print("Subscribers:", subscribers_df.count())
print("Content:", content_df.count())

spark.stop()