from pyspark.sql import SparkSession


# --------------------------------------------------
# Spark + Delta Lake configuration
# --------------------------------------------------

spark = SparkSession.builder \
    .appName("OTTValidateSilver") \
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
# Silver Delta Layer Paths
# --------------------------------------------------

silver_plans_path = "hdfs://localhost:9000/ott_media/silver/plans"
silver_subscribers_path = "hdfs://localhost:9000/ott_media/silver/subscribers"
silver_content_path = "hdfs://localhost:9000/ott_media/silver/content"


# --------------------------------------------------
# Read Silver Delta Tables
# --------------------------------------------------

plans_df = spark.read \
    .format("delta") \
    .load(silver_plans_path)

subscribers_df = spark.read \
    .format("delta") \
    .load(silver_subscribers_path)

content_df = spark.read \
    .format("delta") \
    .load(silver_content_path)


# --------------------------------------------------
# Display Schemas
# --------------------------------------------------

print("\n===== SILVER PLANS SCHEMA =====")
plans_df.printSchema()

print("\n===== SILVER SUBSCRIBERS SCHEMA =====")
subscribers_df.printSchema()

print("\n===== SILVER CONTENT SCHEMA =====")
content_df.printSchema()


# --------------------------------------------------
# Display Data
# --------------------------------------------------

print("\n===== SILVER PLANS =====")
plans_df.show()

print("\n===== SILVER SUBSCRIBERS =====")
subscribers_df.show()

print("\n===== SILVER CONTENT =====")
content_df.show()


# --------------------------------------------------
# Record Counts
# --------------------------------------------------

print("\n===== SILVER RECORD COUNTS =====")
print("Plans      :", plans_df.count())
print("Subscribers:", subscribers_df.count())
print("Content    :", content_df.count())


# --------------------------------------------------
# Stop Spark
# --------------------------------------------------

spark.stop()