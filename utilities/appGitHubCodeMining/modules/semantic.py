"""Lightweight semantic search wrapper (pluggable).

Encapsulates SentenceTransformers usage with a simple in-memory corpus.
Call add(text, payload) to index; search(query) to retrieve top matches.

Debugging notes:
- If sentence-transformers isn't installed, an ImportError is raised on init.
- Embeddings are kept in-memory; restart loses state. TODO: add persistence.
"""
try:
    from sentence_transformers import SentenceTransformer, util
except Exception:  # pragma: no cover
    SentenceTransformer = None  # type: ignore
    util = None  # type: ignore

class Semantic:
    """In-memory semantic index using SentenceTransformers.

    Attributes:
        model: the embedding model.
        embeddings: list of tensors aligned with items.
        items: list of payload dicts containing at least {'text': str}.
    """
    def __init__(self, model_name: str = 'all-MiniLM-L6-v2'):
        if SentenceTransformer is None:
            raise ImportError("sentence-transformers is required for Semantic search. Please install it.")
        self.model = SentenceTransformer(model_name)
        self.embeddings = []
        self.items = []

    def add(self, text: str, payload: dict):
        """Add a text snippet and optional payload to the index."""
        emb = self.model.encode(text, convert_to_tensor=True)
        self.embeddings.append(emb)
        self.items.append({"text": text, **payload})

    def search(self, query: str, top_k: int = 5):
        """Return top_k payloads sorted by similarity."""
        q = self.model.encode(query, convert_to_tensor=True)
        hits = util.semantic_search(q, self.embeddings, top_k=top_k)[0]
        return [self.items[h['corpus_id']] for h in hits]

    def search_with_scores(self, query: str, top_k: int = 5):
        """Return a list of dicts with item and similarity score.

        [{ 'item': <payload>, 'score': <float> }, ...]
        """
        if not self.embeddings:
            return []
        q = self.model.encode(query, convert_to_tensor=True)
        hits = util.semantic_search(q, self.embeddings, top_k=top_k)[0]
        out = []
        for h in hits:
            idx = h['corpus_id']
            score = float(h.get('score', 0.0))
            out.append({"item": self.items[idx], "score": score})
        return out
