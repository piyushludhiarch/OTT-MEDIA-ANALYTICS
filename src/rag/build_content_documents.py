from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, lit


spark = (
    SparkSession.builder
    .appName("OTTBuildRAGDocuments")
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


content_path = (
    "hdfs://localhost:9000/ott_media/gold/dim_content"
)

output_path = (
    "hdfs://localhost:9000/ott_media/rag/content_documents"
)


content_df = (
    spark.read
    .format("delta")
    .load(content_path)
)


documents_df = (
    content_df
    .select(
        "content_id",
        "title",
        "content_type",
        "genre",
        "duration_minutes",
        "release_year"
    )
    .withColumn(
        "document",
        concat(
            lit("Title: "), col("title"), lit(". "),
            lit("Type: "), col("content_type"), lit(". "),
            lit("Genre: "), col("genre"), lit(". "),
            lit("Duration: "), col("duration_minutes"), lit(" minutes. "),
            lit("Release year: "), col("release_year"), lit(".")
        )
    )
)


print("\n===== RAG DOCUMENT SAMPLE =====")

documents_df.select(
    "content_id",
    "title",
    "document"
).show(10, truncate=False)


print("\n===== DOCUMENT COUNT =====")
print("Documents:", documents_df.count())


(
    documents_df
    .select(
        "content_id",
        "title",
        "content_type",
        "genre",    
        "document"
    )
    .write
    .mode("overwrite")
    .parquet(output_path)
)


print("\n===== RAG DOCUMENTS WRITTEN =====")
print("Path:", output_path)


spark.stop()