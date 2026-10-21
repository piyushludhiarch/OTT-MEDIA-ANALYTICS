import argparse
import contextlib
from functools import lru_cache
import json
from pathlib import Path
import sys

import faiss
import numpy as np

from sentence_transformers import SentenceTransformer


@lru_cache(maxsize=1)
def load_search_resources():
    import json

    project_root = Path(__file__).resolve().parents[2]
    index_file = project_root / "data/generated/rag/content_index.faiss"
    metadata_file = project_root / "data/generated/rag/content_metadata.json"

    index = faiss.read_index(str(index_file))
    with metadata_file.open("r", encoding="utf-8") as metadata_file_handle:
        metadata = json.load(metadata_file_handle)
    model = SentenceTransformer("all-MiniLM-L6-v2")

    return index, metadata, model


def search_titles(query, top_k=5):
    if not query.strip():
        raise ValueError("Search query cannot be empty.")
    if top_k < 1:
        raise ValueError("top_k must be at least 1.")

    index, metadata, model = load_search_resources()
    top_k = min(top_k, index.ntotal)
    results = []
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

    print("\n===== SEARCH RESULTS =====")
    print(f"Query: {query}")

    for rank, (score, idx) in enumerate(
        zip(scores[0], indices[0]),
        start=1
    ):
        if idx < 0:
            continue

        result = metadata[idx]

        print(
            f"\n{rank}. {result['title']}"
        )

        print(
            f"   Content ID: {result['content_id']}"
        )

        print(
            f"   Score: {score:.4f}"
        )

        print(
            f"   {result['document']}"
        )
        results.append(
            {
                "title": result["title"],
                "content_id": result["content_id"],
                "document": result["document"],
                "score": float(score),
            }
        )

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Search OTT content titles.")
    parser.add_argument("--query", default="action movies")
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print search results as JSON for programmatic callers.",
    )
    args = parser.parse_args()

    if args.json:
        with contextlib.redirect_stdout(sys.stderr):
            results = search_titles(args.query, top_k=args.top_k)
        print(json.dumps(results))
    else:
        print("\n===== LOADING FAISS INDEX =====")
        index, metadata, _ = load_search_resources()
        print("Vectors in index:", index.ntotal)
        print("Vector dimension:", index.d)
        print("Metadata records:", len(metadata))
        print("Embedding model loaded")
        search_titles(args.query, top_k=args.top_k)