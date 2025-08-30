import os
import hashlib
import json
from datasketch import MinHash, MinHashLSH

class Deduplicator:
    def __init__(self, input_dir, output_dir, threshold=0.9):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.threshold = threshold
        os.makedirs(output_dir, exist_ok=True)
        self.lsh = MinHashLSH(threshold=threshold, num_perm=128)
        self.hashes = {}

    def dedupe(self):
        for fname in os.listdir(self.input_dir):
            if fname.endswith('.json'):
                with open(os.path.join(self.input_dir, fname), 'r', encoding='utf-8') as f:
                    doc = json.load(f)
                tokens = set(doc['tokens'])
                m = MinHash(num_perm=128)
                for t in tokens:
                    m.update(t.encode('utf8'))
                h = fname.replace('.json', '')
                if len(self.lsh.query(m)) == 0:
                    self.lsh.insert(h, m)
                    with open(os.path.join(self.output_dir, fname), 'w', encoding='utf-8') as f:
                        json.dump(doc, f)
