import os
import json
import math
import pickle
from collections import defaultdict, Counter

class InvertedIndex:
    def __init__(self):
        self.index = defaultdict(list)
        self.doc_stats = {}
        self.term_doc_freq = Counter()
        self.doc_table = {}
        self.N = 0

    def add_document(self, doc_id, doc):
        tokens = doc['tokens']
        self.doc_stats[doc_id] = {
            'length': len(tokens),
            'title': doc['title'],
            'url': doc['url']
        }
        self.doc_table[doc_id] = doc
        positions = defaultdict(list)
        for pos, token in enumerate(tokens):
            positions[token].append(pos)
        for token, pos_list in positions.items():
            self.index[token].append((doc_id, pos_list))
            self.term_doc_freq[token] += 1
        self.N += 1

    def save(self, path):
        with open(path, 'wb') as f:
            pickle.dump({
                'index': self.index,
                'doc_stats': self.doc_stats,
                'term_doc_freq': self.term_doc_freq,
                'doc_table': self.doc_table,
                'N': self.N
            }, f)

    def load(self, path):
        with open(path, 'rb') as f:
            data = pickle.load(f)
            self.index = data['index']
            self.doc_stats = data['doc_stats']
            self.term_doc_freq = data['term_doc_freq']
            self.doc_table = data['doc_table']
            self.N = data['N']

class Indexer:
    def __init__(self, input_dir, index_path):
        self.input_dir = input_dir
        self.index_path = index_path
        self.idx = InvertedIndex()

    def build(self):
        for fname in os.listdir(self.input_dir):
            if fname.endswith('.json'):
                with open(os.path.join(self.input_dir, fname), 'r', encoding='utf-8') as f:
                    doc = json.load(f)
                doc_id = fname.replace('.json', '')
                self.idx.add_document(doc_id, doc)
        self.idx.save(self.index_path)
