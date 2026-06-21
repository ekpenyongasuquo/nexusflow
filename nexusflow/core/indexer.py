"""
nexusflow/core/indexer.py
Hybrid vector + keyword search index.
Combines FAISS (dense embeddings) with BM25 (keyword) using
Reciprocal Rank Fusion. CPU-only — no GPU required.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

import numpy as np
from rank_bm25 import BM25Okapi

logger = logging.getLogger(__name__)

# Lazy import — sentence_transformers is large, load only when needed
_model = None


def _get_embedding_model():
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        # all-MiniLM-L6-v2: 22MB, 384-dim, fast on CPU
        _model = SentenceTransformer("all-MiniLM-L6-v2")
        logger.info("Embedding model loaded: all-MiniLM-L6-v2")
    return _model


@dataclass
class IndexedDocument:
    doc_id: str
    text: str
    source: str        # "slack" | "jira" | "github"
    metadata: dict = field(default_factory=dict)


class HybridIndex:
    """
    Hybrid BM25 + FAISS index over a corpus of documents.
    Build once per pipeline run — lightweight, in-memory.

    Usage:
        index = HybridIndex()
        index.build(documents)
        results = index.search("budget variance Q3", top_k=10)
    """

    def __init__(self, rrf_k: int = 60):
        self._rrf_k = rrf_k
        self._documents: list[IndexedDocument] = []
        self._bm25: BM25Okapi | None = None
        self._faiss_index = None
        self._embeddings: np.ndarray | None = None

    def build(self, documents: list[IndexedDocument]) -> None:
        """Build both BM25 and FAISS indexes from the document list."""
        if not documents:
            logger.warning("HybridIndex: no documents to index")
            return

        self._documents = documents
        texts = [doc.text for doc in documents]

        # ── BM25 index ────────────────────────────────────────────────────────
        tokenised = [text.lower().split() for text in texts]
        self._bm25 = BM25Okapi(tokenised)

        # ── FAISS dense index ─────────────────────────────────────────────────
        try:
            import faiss
            model = _get_embedding_model()
            self._embeddings = model.encode(texts, show_progress_bar=False)
            dim = self._embeddings.shape[1]
            self._faiss_index = faiss.IndexFlatIP(dim)  # Inner product (cosine after normalise)
            faiss.normalize_L2(self._embeddings)
            self._faiss_index.add(self._embeddings.astype(np.float32))
            logger.info(
                "HybridIndex built: %d docs, FAISS dim=%d", len(documents), dim
            )
        except ImportError:
            logger.warning("faiss not available — falling back to BM25 only")
            self._faiss_index = None

    def search(self, query: str, top_k: int = 10) -> list[tuple[IndexedDocument, float]]:
        """
        Hybrid search using Reciprocal Rank Fusion of BM25 + FAISS scores.
        Returns list of (document, rrf_score) sorted by relevance desc.
        """
        if not self._documents:
            return []

        n = len(self._documents)
        rrf_scores: dict[int, float] = {i: 0.0 for i in range(n)}

        # ── BM25 ranking ──────────────────────────────────────────────────────
        if self._bm25:
            bm25_scores = self._bm25.get_scores(query.lower().split())
            bm25_ranked = np.argsort(bm25_scores)[::-1]
            for rank, idx in enumerate(bm25_ranked):
                rrf_scores[int(idx)] += 1.0 / (self._rrf_k + rank + 1)

        # ── FAISS ranking ─────────────────────────────────────────────────────
        if self._faiss_index is not None:
            try:
                import faiss
                model = _get_embedding_model()
                q_emb = model.encode([query], show_progress_bar=False)
                faiss.normalize_L2(q_emb)
                _, faiss_indices = self._faiss_index.search(
                    q_emb.astype(np.float32), min(top_k * 2, n)
                )
                for rank, idx in enumerate(faiss_indices[0]):
                    if idx >= 0:
                        rrf_scores[int(idx)] += 1.0 / (self._rrf_k + rank + 1)
            except Exception as e:
                logger.warning("FAISS search failed: %s — using BM25 only", e)

        # ── Merge and return top_k ────────────────────────────────────────────
        sorted_indices = sorted(rrf_scores.keys(), key=lambda i: rrf_scores[i], reverse=True)
        return [
            (self._documents[i], rrf_scores[i])
            for i in sorted_indices[:top_k]
            if rrf_scores[i] > 0
        ]
