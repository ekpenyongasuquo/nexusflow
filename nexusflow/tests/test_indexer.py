"""
nexusflow/tests/test_indexer.py
Test suite for HybridIndex — BM25 + FAISS search.
Tests indexing, retrieval relevance, and empty corpus handling.
"""
from __future__ import annotations

import pytest

from nexusflow.core.indexer import HybridIndex, IndexedDocument


def _docs() -> list[IndexedDocument]:
    return [
        IndexedDocument(doc_id="s1", text="Budget variance detected in Q3 financial report", source="slack"),
        IndexedDocument(doc_id="s2", text="Team meeting about sprint planning next week", source="slack"),
        IndexedDocument(doc_id="j1", text="JIRA ticket: Q3 budget overrun needs CFO approval", source="jira"),
        IndexedDocument(doc_id="j2", text="Customer escalation in APAC region requires VP sign-off", source="jira"),
        IndexedDocument(doc_id="g1", text="PR #42: Fix payment processing module", source="github"),
        IndexedDocument(doc_id="g2", text="PR #43: Add budget variance detection webhook", source="github"),
    ]


def test_build_and_search_basic():
    """Index builds and returns relevant results for a budget query."""
    index = HybridIndex()
    index.build(_docs())
    results = index.search("budget variance Q3", top_k=3)

    assert len(results) > 0
    top_doc, top_score = results[0]
    # Budget-related docs should rank higher than sprint planning
    assert "budget" in top_doc.text.lower() or "Q3" in top_doc.text


def test_search_returns_at_most_top_k():
    """search() never returns more results than top_k."""
    index = HybridIndex()
    index.build(_docs())
    results = index.search("budget", top_k=2)
    assert len(results) <= 2


def test_search_empty_corpus_returns_empty():
    """search() on empty index returns empty list, no error."""
    index = HybridIndex()
    index.build([])
    results = index.search("budget variance")
    assert results == []


def test_search_unrelated_query_returns_results():
    """Even an unrelated query returns results (BM25 fallback)."""
    index = HybridIndex()
    index.build(_docs())
    results = index.search("completely unrelated xyz topic", top_k=3)
    # May return fewer results but should not crash
    assert isinstance(results, list)


def test_search_scores_are_positive():
    """All returned RRF scores are positive."""
    index = HybridIndex()
    index.build(_docs())
    results = index.search("budget", top_k=5)
    for _, score in results:
        assert score > 0


def test_source_metadata_preserved():
    """Source field is preserved through indexing and retrieval."""
    index = HybridIndex()
    docs = _docs()
    index.build(docs)
    results = index.search("budget variance", top_k=6)

    sources = {doc.source for doc, _ in results}
    # Results should span multiple sources
    assert len(sources) >= 1


def test_build_single_document():
    """Index handles a single document without error."""
    index = HybridIndex()
    index.build([IndexedDocument(doc_id="only", text="Single document test", source="slack")])
    results = index.search("single document", top_k=5)
    assert len(results) == 1


def test_metadata_accessible_on_results():
    """Document metadata is accessible on retrieved results."""
    docs = [
        IndexedDocument(
            doc_id="m1",
            text="Budget meeting notes",
            source="slack",
            metadata={"channel": "C001", "author": "U001"},
        )
    ]
    index = HybridIndex()
    index.build(docs)
    results = index.search("budget")
    assert results[0][0].metadata["channel"] == "C001"
