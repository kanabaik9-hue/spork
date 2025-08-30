import os
import json
import pickle
from sentence_transformers import SentenceTransformer

class Embedder:
    def __init__(self, input_dir, output_path):
        self.input_dir = input_dir
        self.output_path = output_path
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        self.embeddings = {}

    def build(self):
        for fname in os.listdir(self.input_dir):
            if fname.endswith('.json'):
                with open(os.path.join(self.input_dir, fname), 'r', encoding='utf-8') as f:
                    doc = json.load(f)
                doc_id = fname.replace('.json', '')
                text = doc['title'] + ' ' + ' '.join(doc['headings']) + ' ' + doc['body']
                self.embeddings[doc_id] = self.model.encode(text)
        with open(self.output_path, 'wb') as f:
            pickle.dump(self.embeddings, f)
