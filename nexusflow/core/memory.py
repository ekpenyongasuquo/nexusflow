"""
nexusflow/core/memory.py
Episodic Memory Store — turns NexusFlow from stateless automation
into adaptive intelligence.

After each completed pipeline a compact episode is persisted to the
audit SQLite DB (table ``episodic_memories``).  On the next synthesis
run the top-3 most similar past episodes are retrieved and injected
into the LLM prompt as "Past Decision Context".

Retrieval strategy
------------------
* ``recall(trigger_type)``     — fast indexed lookup by trigger type,
                                  most-recent-first.
* ``recall_similar(summary)``  — BM25 keyword scoring over stored
                                  context summaries; no embeddings needed.

Both methods check the in-process ``deque`` cache (maxlen=500) first,
falling back to the SQLite DB only on a cache miss.

Thread / task safety
--------------------
``MemoryStore`` is designed for one instance per process.  The cache
is a plain ``collections.deque``; all async DB calls use the shared
``AuditSessionFactory`` from ``nexusflow.db.session``.
"""
from __future__ import annotations

import logging
import math
import re
import uuid
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from nexusflow.core.models import PipelineState
from nexusflow.db.models import EpisodicMemoryRecord
from nexusflow.db.session import AuditSessionFactory

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Dataclass (in-process representation)
# ---------------------------------------------------------------------------

@dataclass
class EpisodicMemory:
    """
    A single completed pipeline distilled into a compact memory episode.

    Fields
    ------
    memory_id:
        UUID-4 string assigned on creation.
    pipeline_id:
        Source pipeline that generated this episode.
    trigger_type:
        Enum value string, e.g. ``"BUDGET_VARIANCE"``.
    context_summary:
        First 300 characters of ``DecisionBrief.context_summary`` —
        enough to support BM25 keyword matching without bloating the store.
    outcome:
        Human decision outcome string, e.g. ``"APPROVED"``.
    option_selected:
        Title of the chosen ``DecisionOption``, or ``""`` when none.
    confidence_score:
        ``DecisionBrief.confidence_score`` at time of synthesis (0–1).
    duration_seconds:
        Wall-clock seconds from pipeline creation to completion.
    created_at:
        UTC datetime when this episode was stored.
    tags:
        Flat list of searchable labels: trigger_type value plus any
        affected_systems from the brief.
    """
    memory_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_id: str = ""
    trigger_type: str = ""
    context_summary: str = ""
    outcome: str = ""
    option_selected: str = ""
    confidence_score: float = 0.0
    duration_seconds: float = 0.0
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    tags: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# BM25 helpers (self-contained, zero extra dependencies)
# ---------------------------------------------------------------------------

def _tokenise(text: str) -> list[str]:
    """Lowercase, strip punctuation, split on whitespace."""
    return re.findall(r"[a-z0-9]+", text.lower())


def _bm25_score(
    query_tokens: list[str],
    doc_tokens: list[str],
    avg_dl: float,
    k1: float = 1.5,
    b: float = 0.75,
) -> float:
    """
    Compute a BM25 relevance score between a query token list and a
    document token list.

    Parameters
    ----------
    query_tokens:
        Tokenised search query.
    doc_tokens:
        Tokenised document text.
    avg_dl:
        Average document length (tokens) across the corpus.
    k1, b:
        Standard BM25 tuning parameters.
    """
    tf_map = Counter(doc_tokens)
    dl = len(doc_tokens)
    score = 0.0
    for token in query_tokens:
        tf = tf_map.get(token, 0)
        if tf == 0:
            continue
        idf = math.log(1 + 1 / (tf + 0.5))   # simplified IDF (single-doc)
        numerator = tf * (k1 + 1)
        denominator = tf + k1 * (1 - b + b * dl / max(avg_dl, 1))
        score += idf * numerator / denominator
    return score


# ---------------------------------------------------------------------------
# MemoryStore
# ---------------------------------------------------------------------------

class MemoryStore:
    """
    Episodic memory store — persists completed pipeline episodes to SQLite
    and serves fast recall for the Synthesiser Agent.

    Parameters
    ----------
    cache_maxlen:
        Maximum number of episodes held in the in-process LRU-style deque.
        The deque is populated on first use by loading the most recent
        ``cache_maxlen`` rows from the DB.
    """

    def __init__(self, cache_maxlen: int = 500) -> None:
        self._cache: deque[EpisodicMemory] = deque(maxlen=cache_maxlen)
        self._cache_loaded = False

    # ── private helpers ────────────────────────────────────────────────────

    @staticmethod
    def _state_to_episode(state: PipelineState) -> EpisodicMemory | None:
        """
        Distil a completed ``PipelineState`` into an ``EpisodicMemory``.
        Returns ``None`` when the state lacks the minimum required fields.
        """
        if not state.brief:
            return None

        # Duration: completion time minus creation time
        now = datetime.now(timezone.utc)
        created = state.created_at
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        duration = (now - created).total_seconds()

        # Outcome
        outcome = ""
        if state.human_decision:
            outcome = str(state.human_decision.outcome)

        # Option selected
        option_selected = ""
        if (
            state.recommendation
            and state.human_decision
            and state.human_decision.selected_option_id
        ):
            for opt in state.recommendation.options:
                if opt.option_id == state.human_decision.selected_option_id:
                    option_selected = opt.title
                    break

        # Tags: trigger type + affected systems from brief
        tags: list[str] = [str(state.trigger_type)]
        tags.extend(state.brief.affected_systems or [])

        return EpisodicMemory(
            pipeline_id=state.pipeline_id,
            trigger_type=str(state.trigger_type),
            context_summary=state.brief.context_summary[:300],
            outcome=outcome,
            option_selected=option_selected,
            confidence_score=state.brief.confidence_score,
            duration_seconds=round(duration, 2),
            created_at=now,
            tags=tags,
        )

    @staticmethod
    def _record_to_episode(record: EpisodicMemoryRecord) -> EpisodicMemory:
        """Materialise an ORM record into an ``EpisodicMemory`` dataclass."""
        created = record.created_at
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        return EpisodicMemory(
            memory_id=record.memory_id,
            pipeline_id=record.pipeline_id,
            trigger_type=record.trigger_type,
            context_summary=record.context_summary,
            outcome=record.outcome,
            option_selected=record.option_selected,
            confidence_score=record.confidence_score,
            duration_seconds=record.duration_seconds,
            created_at=created,
            tags=list(record.tags or []),
        )

    async def _ensure_cache(self) -> None:
        """
        Warm the in-process cache from the DB on first access.
        Subsequent calls are no-ops.
        """
        if self._cache_loaded:
            return
        async with AuditSessionFactory() as session:
            result = await session.execute(
                select(EpisodicMemoryRecord)
                .order_by(EpisodicMemoryRecord.created_at.desc())
                .limit(self._cache.maxlen)
            )
            rows = result.scalars().all()
        # Load oldest-first into the deque so most recent is at right end
        for row in reversed(rows):
            self._cache.append(self._record_to_episode(row))
        self._cache_loaded = True
        logger.info(
            "[MemoryStore] Cache warmed — %d episodes loaded", len(self._cache)
        )

    # ── public API ─────────────────────────────────────────────────────────

    async def store(self, state: PipelineState) -> EpisodicMemory | None:
        """
        Extract an episode from a completed pipeline state and persist it.

        The episode is appended to the in-process cache immediately and
        written to the ``episodic_memories`` SQLite table.

        Parameters
        ----------
        state:
            A ``PipelineState`` that has reached a terminal status
            (``COMPLETE`` or ``FAILED``).

        Returns
        -------
        EpisodicMemory | None
            The stored episode, or ``None`` when the state has no brief
            (nothing meaningful to remember).
        """
        episode = self._state_to_episode(state)
        if episode is None:
            logger.debug(
                "[MemoryStore] Skipping store — no brief on pipeline %s",
                state.pipeline_id,
            )
            return None

        # Persist to SQLite
        record = EpisodicMemoryRecord(
            memory_id=episode.memory_id,
            pipeline_id=episode.pipeline_id,
            trigger_type=episode.trigger_type,
            context_summary=episode.context_summary,
            outcome=episode.outcome,
            option_selected=episode.option_selected,
            confidence_score=episode.confidence_score,
            duration_seconds=episode.duration_seconds,
            created_at=episode.created_at,
            tags=episode.tags,
        )
        async with AuditSessionFactory() as session:
            session.add(record)
            await session.commit()

        # Update in-process cache
        await self._ensure_cache()
        self._cache.append(episode)

        logger.info(
            "[MemoryStore] Stored episode %s (pipeline=%s, trigger=%s, outcome=%s)",
            episode.memory_id,
            episode.pipeline_id,
            episode.trigger_type,
            episode.outcome,
        )
        return episode

    async def recall(
        self,
        trigger_type: str,
        limit: int = 5,
    ) -> list[EpisodicMemory]:
        """
        Return the most recent episodes that match ``trigger_type``.

        Checks the in-process cache first; if the cache yields fewer
        than ``limit`` hits it falls back to a DB query.

        Parameters
        ----------
        trigger_type:
            Exact trigger type string to filter on, e.g. ``"BUDGET_VARIANCE"``.
        limit:
            Maximum number of episodes to return.  Defaults to 5.

        Returns
        -------
        list[EpisodicMemory]
            Matched episodes, most-recent-first.
        """
        await self._ensure_cache()

        # Cache path — walk deque from right (most-recent)
        hits = [
            ep for ep in reversed(self._cache)
            if ep.trigger_type == trigger_type
        ][:limit]

        if len(hits) >= limit:
            return hits

        # DB fallback
        async with AuditSessionFactory() as session:
            result = await session.execute(
                select(EpisodicMemoryRecord)
                .where(EpisodicMemoryRecord.trigger_type == trigger_type)
                .order_by(EpisodicMemoryRecord.created_at.desc())
                .limit(limit)
            )
            rows = result.scalars().all()

        return [self._record_to_episode(r) for r in rows]

    async def recall_similar(
        self,
        context_summary: str,
        limit: int = 3,
    ) -> list[EpisodicMemory]:
        """
        Return the ``limit`` episodes whose stored context summaries are
        most relevant to ``context_summary`` using BM25 keyword scoring.

        No embeddings or vector index are required.  The search corpus is
        the in-process cache, which holds up to 500 recent episodes.

        Parameters
        ----------
        context_summary:
            Free-text query — typically the freshly generated
            ``DecisionBrief.context_summary``.
        limit:
            Maximum number of results to return.  Defaults to 3.

        Returns
        -------
        list[EpisodicMemory]
            Top-scoring episodes, highest-score-first.  Episodes with a
            score of 0 (no keyword overlap) are excluded.
        """
        await self._ensure_cache()

        if not self._cache:
            return []

        query_tokens = _tokenise(context_summary)
        if not query_tokens:
            return []

        # Pre-tokenise all cached summaries for scoring
        corpus: list[tuple[EpisodicMemory, list[str]]] = [
            (ep, _tokenise(ep.context_summary)) for ep in self._cache
        ]
        avg_dl = sum(len(toks) for _, toks in corpus) / len(corpus)

        scored: list[tuple[float, EpisodicMemory]] = []
        for ep, doc_tokens in corpus:
            score = _bm25_score(query_tokens, doc_tokens, avg_dl)
            if score > 0.0:
                scored.append((score, ep))

        scored.sort(key=lambda t: t[0], reverse=True)
        return [ep for _, ep in scored[:limit]]

    async def summary_stats(self) -> dict[str, Any]:
        """
        Return aggregate statistics over all stored episodes.

        The result is computed directly from the DB (not the cache) so it
        reflects the full history, not just the last 500 entries.

        Returns
        -------
        dict
            Keys:
            - ``total_episodes``   — int, total row count
            - ``by_trigger``       — dict[trigger_type, dict] each with:
                  ``count``, ``avg_confidence``, ``avg_duration_seconds``,
                  ``outcome_distribution`` (dict[outcome, int])
        """
        async with AuditSessionFactory() as session:
            result = await session.execute(select(EpisodicMemoryRecord))
            all_rows: list[EpisodicMemoryRecord] = result.scalars().all()

        if not all_rows:
            return {"total_episodes": 0, "by_trigger": {}}

        by_trigger: dict[str, dict[str, Any]] = {}

        for row in all_rows:
            bucket = by_trigger.setdefault(
                row.trigger_type,
                {
                    "count": 0,
                    "confidence_sum": 0.0,
                    "duration_sum": 0.0,
                    "outcome_distribution": {},
                },
            )
            bucket["count"] += 1
            bucket["confidence_sum"] += row.confidence_score
            bucket["duration_sum"] += row.duration_seconds
            outcome = row.outcome or "UNKNOWN"
            bucket["outcome_distribution"][outcome] = (
                bucket["outcome_distribution"].get(outcome, 0) + 1
            )

        # Convert sums → averages; remove internal accumulator keys
        summary: dict[str, Any] = {"total_episodes": len(all_rows), "by_trigger": {}}
        for trigger, bucket in by_trigger.items():
            n = bucket["count"]
            summary["by_trigger"][trigger] = {
                "count": n,
                "avg_confidence": round(bucket["confidence_sum"] / n, 4),
                "avg_duration_seconds": round(bucket["duration_sum"] / n, 2),
                "outcome_distribution": bucket["outcome_distribution"],
            }

        return summary


# ---------------------------------------------------------------------------
# Module-level singleton — import and use directly
# ---------------------------------------------------------------------------

memory_store = MemoryStore()
