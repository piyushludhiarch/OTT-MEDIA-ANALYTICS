from pyspark.sql import SparkSession
from pyspark.sql.functions import trim


# --------------------------------------------------
# Spark + Delta Lake configuration
# --------------------------------------------------

spark = SparkSession.builder \
    .appName("OTTCreateSilver") \
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
# Bronze Delta Layer Paths
# --------------------------------------------------

bronze_plans_path = "hdfs://localhost:9000/ott_media/bronze/plans"
bronze_subscribers_path = "hdfs://localhost:9000/ott_media/bronze/subscribers"
bronze_content_path = "hdfs://localhost:9000/ott_media/bronze/content"


# --------------------------------------------------
# Silver Delta Layer Paths
# --------------------------------------------------

silver_plans_path = "hdfs://localhost:9000/ott_media/silver/plans"
silver_subscribers_path = "hdfs://localhost:9000/ott_media/silver/subscribers"
silver_content_path = "hdfs://localhost:9000/ott_media/silver/content"


# --------------------------------------------------
# Read Plans from Bronze
# --------------------------------------------------

plans_df = spark.read \
    .format("delta") \
    .load(bronze_plans_path)

print("\n===== BRONZE PLANS =====")
plans_df.show()


# --------------------------------------------------
# Clean Plans Data
# --------------------------------------------------

plans_silver_df = plans_df \
    .filter(plans_df.plan_id.isNotNull()) \
    .dropDuplicates(["plan_id"])


# --------------------------------------------------
# Standardize Plan Name
# --------------------------------------------------

plans_silver_df = plans_silver_df \
    .withColumn("plan_name", trim(plans_silver_df.plan_name))

print("\n===== STANDARDIZED SILVER PLANS =====")
plans_silver_df.show()




# --------------------------------------------------
# Read Subscribers from Bronze
# --------------------------------------------------

subscribers_df = spark.read \
    .format("delta") \
    .load(bronze_subscribers_path)

print("\n===== BRONZE SUBSCRIBERS =====")
subscribers_df.show()


# --------------------------------------------------
# Clean Subscribers Data
# --------------------------------------------------

subscribers_silver_df = subscribers_df \
    .filter(subscribers_df.subscriber_id.isNotNull()) \
    .dropDuplicates(["subscriber_id"])


# --------------------------------------------------
# Standardize Subscriber Text Fields
# --------------------------------------------------

subscribers_silver_df = subscribers_silver_df \
    .withColumn("name", trim(subscribers_silver_df.name)) \
    .withColumn("email", trim(subscribers_silver_df.email)) \
    .withColumn("country", trim(subscribers_silver_df.country))

print("\n===== CLEANED SILVER SUBSCRIBERS =====")
subscribers_silver_df.show()

# --------------------------------------------------
# Read Content from Bronze
# --------------------------------------------------

content_df = spark.read \
    .format("delta") \
    .load(bronze_content_path)

print("\n===== BRONZE CONTENT =====")
content_df.show()


# --------------------------------------------------
# Clean Content Data
# --------------------------------------------------

content_silver_df = content_df \
    .filter(content_df.content_id.isNotNull()) \
    .dropDuplicates(["content_id"])


# --------------------------------------------------
# Standardize Content Text Fields
# --------------------------------------------------

content_silver_df = content_silver_df \
    .withColumn("title", trim(content_silver_df.title)) \
    .withColumn("content_type", trim(content_silver_df.content_type)) \
    .withColumn("genre", trim(content_silver_df.genre))

print("\n===== CLEANED SILVER CONTENT =====")
content_silver_df.show()



# --------------------------------------------------
# Write Silver Delta Tables
# --------------------------------------------------

plans_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(silver_plans_path)

subscribers_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(silver_subscribers_path)

content_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(silver_content_path)

print("\n===== SILVER DELTA TABLES WRITTEN SUCCESSFULLY =====")
print("Plans      :", silver_plans_path)
print("Subscribers:", silver_subscribers_path)
print("Content    :", silver_content_path)


# --------------------------------------------------
# Stop Spark
# --------------------------------------------------

spark.stop()