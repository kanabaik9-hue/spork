import re
from bs4 import BeautifulSoup
import hashlib
import os
import json
from urllib.parse import urlparse
from collections import Counter
import spacy

nlp = spacy.load('en_core_web_sm', disable=['ner', 'parser'])
STOPWORDS = set(nlp.Defaults.stop_words)

class Document:
    def __init__(self, url, html):
        self.url = url
        self.html = html
        self.title = ''
        self.headings = []
        self.body = ''
        self.tokens = []
        self.metadata = {}
        self.parse()

    def parse(self):
        soup = BeautifulSoup(self.html, 'lxml')
        self.title = soup.title.string if soup.title else ''
        self.headings = [h.get_text() for h in soup.find_all(['h1', 'h2', 'h3'])]
        self.body = ' '.join([p.get_text() for p in soup.find_all('p')])
        self.metadata = {
            'url': self.url,
            'title': self.title,
            'headings': self.headings,
            'content_length': len(self.body),
            'lang': soup.html.get('lang') if soup.html else 'en',
            'canonical_url': soup.find('link', rel='canonical')['href'] if soup.find('link', rel='canonical') else self.url,
            'outbound_links': [a['href'] for a in soup.find_all('a', href=True)]
        }
        self.tokens = self.tokenize(self.body)

    def tokenize(self, text):
        doc = nlp(text)
        tokens = [t.lemma_.lower() for t in doc if t.is_alpha and t.lemma_.lower() not in STOPWORDS]
        return tokens

    def to_dict(self):
        return {
            'url': self.url,
            'title': self.title,
            'headings': self.headings,
            'body': self.body,
            'tokens': self.tokens,
            'metadata': self.metadata
        }

class Parser:
    def __init__(self, storage_dir, output_dir):
        self.storage_dir = storage_dir
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def parse_all(self):
        for fname in os.listdir(self.storage_dir):
            if fname.endswith('.html'):
                with open(os.path.join(self.storage_dir, fname), 'r', encoding='utf-8') as f:
                    html = f.read()
                meta_fname = fname.replace('.html', '.meta')
                with open(os.path.join(self.storage_dir, meta_fname), 'r', encoding='utf-8') as f:
                    meta = eval(f.read())
                doc = Document(meta['url'], html)
                h = hashlib.sha256(meta['url'].encode()).hexdigest()
                with open(os.path.join(self.output_dir, f'{h}.json'), 'w', encoding='utf-8') as f:
                    json.dump(doc.to_dict(), f)
