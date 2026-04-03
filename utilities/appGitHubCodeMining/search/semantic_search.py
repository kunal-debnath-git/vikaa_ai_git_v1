# Semantic Search Module (Embeddings based)

from sentence_transformers import SentenceTransformer, util
import numpy as np

class SemanticCodeSearcher:
    def __init__(self, model_name='all-MiniLM-L6-v2'):
        self.model = SentenceTransformer(model_name)
        self.index = []
        self.code_snippets = []

    def add_code(self, code_snippet):
        embedding = self.model.encode(code_snippet, convert_to_tensor=True)
        self.index.append(embedding)
        self.code_snippets.append(code_snippet)

    def search(self, query, top_k=3):
        query_embedding = self.model.encode(query, convert_to_tensor=True)
        hits = util.semantic_search(query_embedding, self.index, top_k=top_k)[0]
        results = [self.code_snippets[hit['corpus_id']] for hit in hits]
        return results

# Example Usage
if __name__ == "__main__":
    searcher = SemanticCodeSearcher()
    searcher.add_code("def resize_image(img, size): return img.resize(size)")
    searcher.add_code("def download_file(url): pass")
    searcher.add_code("def parse_json(data): return json.loads(data)")

    print("Search results:", searcher.search("resize picture"))