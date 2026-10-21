from pyspark.sql import SparkSession
from sentence_transformers import SentenceTransformer
import numpy as np
import json
import os


spark = (
    SparkSession.builder
    .appName("OTTCreateEmbeddings")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


documents_path = (
    "hdfs://localhost:9000/ott_media/rag/content_documents"
)

output_dir = "data/generated/rag"


os.makedirs(output_dir, exist_ok=True)


print("\n===== LOADING RAG DOCUMENTS =====")

documents_df = (
    spark.read
    .parquet(documents_path)
    .select(
        "content_id",
        "title",
        "content_type",
        "genre",
        "document"
    )
)

records = documents_df.collect()

print("Documents loaded:", len(records))


print("\n===== LOADING EMBEDDING MODEL =====")

model = SentenceTransformer("all-MiniLM-L6-v2")

print("Embedding model loaded")
print("Embedding dimension:", model.get_embedding_dimension())


documents = [row["document"] for row in records]

print("\n===== CREATING EMBEDDINGS =====")

embeddings = model.encode(
    documents,
    show_progress_bar=True,
    normalize_embeddings=True
)

embeddings = np.asarray(embeddings, dtype="float32")

print("Embedding shape:", embeddings.shape)


metadata = []

for row in records:
    metadata.append(
        {
            "content_id": row["content_id"],
            "title": row["title"],
            "document": row["document"],
            "content_type": row["content_type"],
            "genre": row["genre"]
        }
    )


np.save(
    f"{output_dir}/content_embeddings.npy",
    embeddings
)

with open(
    f"{output_dir}/content_metadata.json",
    "w",
    encoding="utf-8"
) as f:
    json.dump(
        metadata,
        f,
        indent=2,
        ensure_ascii=False
    )


print("\n===== EMBEDDINGS CREATED SUCCESSFULLY =====")
print("Vectors:", len(embeddings))
print("Dimensions:", embeddings.shape[1])
print("Embedding file:", f"{output_dir}/content_embeddings.npy")
print("Metadata file:", f"{output_dir}/content_metadata.json")


spark.stop()