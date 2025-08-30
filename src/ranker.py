import math
import numpy as np
import pickle
from sentence_transformers import SentenceTransformer

class BM25:
    def __init__(self, index, k1=1.5, b=0.75):
        self.index = index
        self.k1 = k1
        self.b = b
        self.avgdl = sum([v['length'] for v in index.doc_stats.values()]) / max(1, len(index.doc_stats))
        self.N = index.N

    def idf(self, term):
        df = self.index.term_doc_freq.get(term, 0)
        return math.log(1 + (self.N - df + 0.5) / (df + 0.5))

    def score(self, query, doc_id):
        doc = self.index.doc_table[doc_id]
        tokens = doc['tokens']
        score = 0.0
        for term in query:
            tf = tokens.count(term)
            idf = self.idf(term)
            denom = tf + self.k1 * (1 - self.b + self.b * len(tokens) / self.avgdl)
            score += idf * tf * (self.k1 + 1) / denom if denom else 0
        return score

class SemanticRanker:
    def __init__(self, index, embedding_path):
        self.index = index
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        with open(embedding_path, 'rb') as f:
            self.embeddings = pickle.load(f)

    def embed(self, text):
        return self.model.encode(text)

    def score(self, query, doc_id):
        q_vec = self.embed(query)
        d_vec = self.embeddings[doc_id]
        return float(np.dot(q_vec, d_vec) / (np.linalg.norm(q_vec) * np.linalg.norm(d_vec)))

class HybridRanker:
    def __init__(self, bm25, semantic, alpha=0.7):
        self.bm25 = bm25
        self.semantic = semantic
        self.alpha = alpha

    def score(self, query, doc_id):
        bm25_score = self.bm25.score(query, doc_id)
        semantic_score = self.semantic.score(' '.join(query), doc_id)
        return self.alpha * bm25_score + (1 - self.alpha) * semantic_score
