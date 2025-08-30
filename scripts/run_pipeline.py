import sys
import os
import asyncio
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from crawler import Crawler
from parser import Parser
from dedupe import Deduplicator
from indexer import Indexer
from embedder import Embedder

# Directories
RAW_DIR = 'data/raw'
PARSED_DIR = 'data/parsed'
DEDUPED_DIR = 'data/deduped'
INDEX_PATH = 'data/index.pkl'
EMBED_PATH = 'data/embeddings.pkl'

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PARSED_DIR, exist_ok=True)
os.makedirs(DEDUPED_DIR, exist_ok=True)

# 1. Crawl
seeds = [
    'https://fastapi.tiangolo.com/',
    'https://spacy.io/',
    'https://www.sentence-transformers.org/',
    'https://www.python.org/'
]
crawler = Crawler(RAW_DIR, max_concurrency=3)
print('Crawling...')
asyncio.run(crawler.crawl(seeds, limit=20))
print('Crawling complete.')

# 2. Parse
print('Parsing...')
parser = Parser(RAW_DIR, PARSED_DIR)
parser.parse_all()
print('Parsing complete.')

# 3. Dedupe
print('Deduplicating...')
dedupe = Deduplicator(PARSED_DIR, DEDUPED_DIR)
dedupe.dedupe()
print('Deduplication complete.')

# 4. Index
print('Indexing...')
indexer = Indexer(DEDUPED_DIR, INDEX_PATH)
indexer.build()
print('Indexing complete.')

# 5. Embeddings
print('Embedding...')
embedder = Embedder(DEDUPED_DIR, EMBED_PATH)
embedder.build()
print('Embedding complete.')

print('Pipeline finished. You can now run the API server.')
