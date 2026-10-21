import faiss
import json
import numpy as np

from sentence_transformers import SentenceTransformer


INDEX_FILE = "data/generated/rag/content_index.faiss"
METADATA_FILE = "data/generated/rag/content_metadata.json"


print("\n===== LOADING RETRIEVAL COMPONENTS =====")

index = faiss.read_index(INDEX_FILE)

with open(
    METADATA_FILE,
    "r",
    encoding="utf-8"
) as f:
    metadata = json.load(f)

model = SentenceTransformer("all-MiniLM-L6-v2")

print("FAISS vectors:", index.ntotal)
print("Metadata records:", len(metadata))


test_cases = [
    ("action movies", "genre", "Action"),
    ("science fiction movies", "genre", "Sci-Fi"),
    ("comedy movies", "genre", "Comedy"),
    ("romance movies", "genre", "Romance"),
    ("documentary content", "content_type", "Documentary"),
    ("thriller movies", "genre", "Thriller"),
]


def search(query, top_k=5):

    query_embedding = model.encode(
        [query],
        normalize_embeddings=True
    )

    query_embedding = np.asarray(
        query_embedding,
        dtype="float32"
    )

    scores, indices = index.search(
        query_embedding,
        top_k
    )

    return scores[0], indices[0]


print("\n===== PRECISION@5 EVALUATION =====")


total_relevant = 0
total_possible = 0


for query, field, expected_value in test_cases:

    scores, indices = search(query, top_k=5)

    relevant_count = 0

    print(f"\nQuery: {query}")
    print(f"Expected {field}: {expected_value}")

    for rank, (score, idx) in enumerate(
        zip(scores, indices),
        start=1
    ):

        result = metadata[idx]

        is_relevant = (
            result[field] == expected_value
        )

        if is_relevant:
            relevant_count += 1

        marker = "RELEVANT" if is_relevant else "NOT RELEVANT"

        print(
            f"{rank}. {result['title']} "
            f"| {result[field]} "
            f"| score={score:.4f} "
            f"| {marker}"
        )

    precision = relevant_count / 5

    print(
        f"Precision@5: {precision:.2f}"
    )

    total_relevant += relevant_count
    total_possible += 5


overall_precision = (
    total_relevant / total_possible
)


print("\n===== OVERALL PRECISION@5 =====")

print(
    f"Relevant results: {total_relevant}/{total_possible}"
)

print(
    f"Overall Precision@5: {overall_precision:.2f}"
)


target = 0.60

if overall_precision >= target:
    print(
        "RESULT: PASS - Precision@5 target achieved"
    )
else:
    print(
        "RESULT: NOT YET - Precision@5 target not achieved"
    )