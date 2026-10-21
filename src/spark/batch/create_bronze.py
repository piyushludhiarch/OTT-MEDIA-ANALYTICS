from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DecimalType,
    DateType
)



# --------------------------------------------------
# Spark + Delta Lake configuration
# --------------------------------------------------

builder = SparkSession.builder \
    .appName("OTTCreateBronze") \
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    ) \
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
spark = builder.getOrCreate()


# --------------------------------------------------
# HDFS Raw Layer Paths
# --------------------------------------------------

plans_path = "hdfs://localhost:9000/ott_media/raw/plans"
subscribers_path = "hdfs://localhost:9000/ott_media/raw/subscribers"
content_path = "hdfs://localhost:9000/ott_media/raw/content"


# --------------------------------------------------
# Schemas
# --------------------------------------------------

plans_schema = StructType([
    StructField("duration_days", IntegerType(), True),
    StructField("plan_id", IntegerType(), True),
    StructField("plan_name", StringType(), True),
    StructField("price", DecimalType(10, 2), True)
])

subscribers_schema = StructType([
    StructField("country", StringType(), True),
    StructField("email", StringType(), True),
    StructField("name", StringType(), True),
    StructField("plan_id", IntegerType(), True),
    StructField("signup_date", DateType(), True),
    StructField("subscriber_id", IntegerType(), True)
])

content_schema = StructType([
    StructField("content_id", IntegerType(), True),
    StructField("content_type", StringType(), True),
    StructField("duration_minutes", IntegerType(), True),
    StructField("genre", StringType(), True),
    StructField("release_year", IntegerType(), True),
    StructField("title", StringType(), True)
])


# --------------------------------------------------
# Read Plans from Raw HDFS
# --------------------------------------------------

plans_df = spark.read \
    .option("header", "false") \
    .schema(plans_schema) \
    .csv(plans_path)

print("\n===== PLANS =====")
plans_df.printSchema()
plans_df.show()


# --------------------------------------------------
# Write Plans to Bronze Delta
# --------------------------------------------------

bronze_plans_path = "hdfs://localhost:9000/ott_media/bronze/plans"

plans_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(bronze_plans_path)


# --------------------------------------------------
# Read Subscribers from Raw HDFS
# --------------------------------------------------

subscribers_df = spark.read \
    .option("header", "false") \
    .schema(subscribers_schema) \
    .csv(subscribers_path)

print("\n===== SUBSCRIBERS =====")
subscribers_df.printSchema()
subscribers_df.show()


# --------------------------------------------------
# Write Subscribers to Bronze Delta
# --------------------------------------------------

bronze_subscribers_path = "hdfs://localhost:9000/ott_media/bronze/subscribers"

subscribers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(bronze_subscribers_path)


# --------------------------------------------------
# Read Content from Raw HDFS
# --------------------------------------------------

content_df = spark.read \
    .option("header", "false") \
    .schema(content_schema) \
    .csv(content_path)

print("\n===== CONTENT =====")
content_df.printSchema()
content_df.show()


# --------------------------------------------------
# Write Content to Bronze Delta
# --------------------------------------------------

bronze_content_path = "hdfs://localhost:9000/ott_media/bronze/content"

content_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(bronze_content_path)


# --------------------------------------------------
# Stop Spark
# --------------------------------------------------

spark.stop()