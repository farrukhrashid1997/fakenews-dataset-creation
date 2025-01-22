import json
from sentence_transformers import SentenceTransformer
import faiss
import numpy as np
from tqdm import tqdm

with open('output/search_results_latest.json', 'r') as f:
    data = json.load(f)

model = SentenceTransformer('avsolatorio/GIST-Embedding-v0')

embedding_dim = 768 

#TODO:
## Research/Read about the indexing types offered by FAISS - IndexFlatL2 is a simple L2 distance index which is in simple words: a very brute 
## force way of finding the nearest neighbors.

claim_index = faiss.IndexFlatL2(embedding_dim)
evidence_index = faiss.IndexFlatL2(embedding_dim)


claim_metadata = []
evidence_metadata = [] 


for claim_id, claim_data in tqdm(data.items(), desc="Processing Claims"):
    claim_text = claim_data["claim"]
    claim_embedding = np.array(model.encode(claim_text), dtype=np.float32)
    claim_index.add(np.array([claim_embedding]))
    claim_faiss_index = claim_index.ntotal - 1  # FAISS index of the claim
    claim_data["claim_faiss_index"] = claim_faiss_index 
    

    for evidence in tqdm(claim_data["evidence"], desc=f"Processing Evidence for Claim {claim_id}", leave=False):
        snippet = evidence["snippet"]
        evidence_embedding = np.array(model.encode(snippet), dtype=np.float32)
        evidence_index.add(np.array([evidence_embedding]))
        evidence_faiss_index = evidence_index.ntotal - 1
        evidence["evidence_faiss_index"] = evidence_faiss_index


faiss.write_index(claim_index, "output/faiss/claim_index.faiss")
faiss.write_index(evidence_index, "output/faiss/evidence_index.faiss")

# Save updated JSON
with open("output/data_with_faiss_indices.json", "w", encoding="utf-8") as f:
    json.dump(data, f, indent=4, ensure_ascii=False)
