from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import faiss
import json

claim_index = faiss.read_index("output/faiss/claim_index.faiss")
evidence_index = faiss.read_index("output/faiss/evidence_index.faiss")

def calculate_similarity(claim_idx, evidence_idx):
    claim_embedding = np.zeros((claim_index.d,), dtype=np.float32)
    evidence_embedding = np.zeros((evidence_index.d,), dtype=np.float32)
    claim_index.reconstruct(claim_idx, claim_embedding)
    evidence_index.reconstruct(evidence_idx, evidence_embedding)
    claim_embedding /= np.linalg.norm(claim_embedding)
    evidence_embedding /= np.linalg.norm(evidence_embedding)
    return np.dot(claim_embedding, evidence_embedding)



with open('output/data_with_faiss_indices.json', 'r') as f:
    data = json.load(f)

for claim_id, claim_data in data.items():
    claim_faiss_index = claim_data["claim_faiss_index"]
    for evidence in claim_data["evidence"]:
        evidence_faiss_index = evidence["evidence_faiss_index"]
        similarity_score = float(calculate_similarity(claim_faiss_index, evidence_faiss_index))
        evidence["similarity_score"] = similarity_score

with open("output/data_with_similarities.json", "w", encoding="utf-8") as f:
    json.dump(data, f, indent=4, ensure_ascii=False)

