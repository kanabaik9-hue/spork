import pickle
import json
from fastapi import FastAPI, Request
from pydantic import BaseModel
from indexer import InvertedIndex

from ranker import BM25, SemanticRanker, HybridRanker
app = FastAPI()

class SearchRequest(BaseModel):
    query: str
    topK: int = 10
    useSemantic: bool = True
    site: str = None
    dateRange: str = None

with open('data/index.pkl', 'rb') as f:
    idx_data = pickle.load(f)
index = InvertedIndex()
index.index = idx_data['index']
index.doc_stats = idx_data['doc_stats']
index.term_doc_freq = idx_data['term_doc_freq']
index.doc_table = idx_data['doc_table']
index.N = idx_data['N']

bm25 = BM25(index)
semantic = None
hybrid = None

def get_semantic():
    global semantic, hybrid
    if semantic is None:
        semantic = SemanticRanker(index, 'data/embeddings.pkl')
        hybrid = HybridRanker(bm25, semantic)
    return semantic, hybrid

@app.post('/search')
def search(req: SearchRequest):
    query_tokens = req.query.lower().split()
    candidates = []
    if req.useSemantic:
        _, hybrid_ranker = get_semantic()
    for doc_id in index.doc_table:
        if req.site and req.site not in index.doc_table[doc_id]['url']:
            continue
        score = hybrid_ranker.score(query_tokens, doc_id) if req.useSemantic else bm25.score(query_tokens, doc_id)
        candidates.append((score, doc_id))
    candidates.sort(reverse=True)
    results = []
    for score, doc_id in candidates[:req.topK]:
        doc = index.doc_table[doc_id]
        snippet = doc['body'][:300]
        highlights = [t for t in query_tokens if t in doc['tokens']]
        results.append({
            'title': doc['title'],
            'url': doc['url'],
            'snippet': snippet,
            'score': score,
            'highlights': highlights
        })
    return {'hits': results}
