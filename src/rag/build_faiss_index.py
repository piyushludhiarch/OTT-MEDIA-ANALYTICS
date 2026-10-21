import faiss
import numpy as np
import os


EMBEDDING_FILE = "data/generated/rag/content_embeddings.npy"
INDEX_FILE = "data/generated/rag/content_index.faiss"


print("\n===== LOADING EMBEDDINGS =====")

embeddings = np.load(EMBEDDING_FILE).astype("float32")

print("Embedding shape:", embeddings.shape)


print("\n===== BUILDING FAISS INDEX =====")

dimension = embeddings.shape[1]

index = faiss.IndexFlatIP(dimension)

index.add(embeddings)

print("FAISS index created")
print("Vectors in index:", index.ntotal)
print("Vector dimension:", index.d)


os.makedirs(
    os.path.dirname(INDEX_FILE),
    exist_ok=True
)

faiss.write_index(index, INDEX_FILE)


print("\n===== FAISS INDEX WRITTEN SUCCESSFULLY =====")
print("Index file:", INDEX_FILE)