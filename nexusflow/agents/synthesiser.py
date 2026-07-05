"""
nexusflow/agents/synthesiser.py
L2 Synthesiser Agent — cross-references collected corpus and
generates a structured DecisionBrief with context summary,
causal chain, and risk matrix.

Uses hybrid FAISS index for relevant passage retrieval,
then calls an LLM for synthesis. Falls back to extractive
summary if no LLM is configured.

Episodic memory: the top-3 most similar past episodes are retrieved
from ``MemoryStore`` and injected into the LLM prompt as a
"Past Decision Context" block, giving the system institutional memory.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import httpx

from nexusflow.core.indexer import HybridIndex, IndexedDocument
from nexusflow.core.memory import EpisodicMemory, memory_store
from nexusflow.core.models import (
    DecisionBrief,
    PipelineState,
    PipelineStatus,
    RiskLevel,
    RiskMatrixEntry,
)
from nexusflow.core.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# System prompt for the Synthesiser LLM call
_SYSTEM_PROMPT = """You are NexusFlow's Synthesiser Agent — an enterprise decision intelligence system.
You receive a corpus of enterprise signals (Slack messages, JIRA tickets, GitHub PRs) and must produce a structured decision brief.

When "Past Decision Context" is provided, use it to calibrate your confidence score and refine recommendations — similar past outcomes are strong priors. Do not repeat past decisions verbatim; extract patterns and apply them to the current situation.

Respond ONLY with valid JSON matching this exact schema:
{
  "context_summary": "string (max 500 tokens — clear narrative of the situation)",
  "causal_chain": ["step 1", "step 2", "step 3"],
  "risk_matrix": [
    {
      "factor": "string",
      "likelihood": "LOW|MEDIUM|HIGH|CRITICAL",
      "impact": "LOW|MEDIUM|HIGH|CRITICAL",
      "mitigation": "string"
    }
  ],
  "affected_systems": ["system1", "system2"],
  "estimated_impact_usd": null,
  "confidence_score": 0.0
}

Be precise, factual, and grounded only in the provided signals. Do not hallucinate facts not present in the corpus."""


async def run_synthesiser_agent(state: PipelineState) -> PipelineState:
    """
    L2 Synthesiser Agent entry point.

    Input:  PipelineState with corpus populated
    Output: PipelineState with brief populated

    Episodic memory is consulted before the LLM call: the top-3 most
    similar past episodes are formatted as "Past Decision Context" and
    prepended to the user message so the model can learn from history.
    """
    logger.info("[L2-SYNTHESISER] Pipeline %s — starting synthesis", state.pipeline_id)
    state.status = PipelineStatus.SYNTHESISING

    if not state.corpus or state.corpus.total_items == 0:
        state.status = PipelineStatus.FAILED
        state.error_stage = "SYNTHESISER"
        state.error_message = "No corpus available for synthesis"
        return state

    # ── Build hybrid index ────────────────────────────────────────────────────
    documents = _corpus_to_documents(state)
    index = HybridIndex()
    index.build(documents)

    # ── Retrieve most relevant passages for the trigger ───────────────────────
    query = _build_query(state)
    top_docs = index.search(query, top_k=20)
    context_passages = "\n\n".join(
        f"[{doc.source.upper()}] {doc.text}" for doc, _ in top_docs
    )

    # ── Recall similar past episodes from episodic memory ────────────────────
    past_episodes = await _recall_past_episodes(query)

    # ── LLM synthesis ─────────────────────────────────────────────────────────
    brief_data = await _call_llm(context_passages, state.trigger_type, past_episodes)

    if brief_data is None:
        # Fallback: extractive summary from top passages
        brief_data = _extractive_fallback(top_docs, state)

    brief = DecisionBrief(
        pipeline_id=state.pipeline_id,
        generated_at=datetime.now(timezone.utc),
        context_summary=brief_data.get("context_summary", ""),
        causal_chain=brief_data.get("causal_chain", []),
        risk_matrix=[
            RiskMatrixEntry(**r) for r in brief_data.get("risk_matrix", [])
        ],
        affected_systems=brief_data.get("affected_systems", []),
        estimated_impact_usd=brief_data.get("estimated_impact_usd"),
        confidence_score=float(brief_data.get("confidence_score", 0.5)),
        source_item_count=state.corpus.total_items,
    )

    state.brief = brief
    logger.info(
        "[L2-SYNTHESISER] Pipeline %s — brief generated. Confidence: %.2f "
        "(informed by %d past episode(s))",
        state.pipeline_id,
        brief.confidence_score,
        len(past_episodes),
    )
    return state


async def _recall_past_episodes(query: str) -> list[EpisodicMemory]:
    """
    Query episodic memory for the top-3 episodes most similar to ``query``.
    Errors are suppressed — a warm memory is helpful but never blocking.
    """
    try:
        return await memory_store.recall_similar(query, limit=3)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[L2-SYNTHESISER] Memory recall failed (non-fatal): %s", exc)
        return []


def _format_past_episodes(episodes: list[EpisodicMemory]) -> str:
    """
    Render a list of episodes as a compact human-readable block suitable
    for injection into the LLM user message.

    Returns an empty string when there are no episodes so the prompt
    remains unchanged when memory is empty.
    """
    if not episodes:
        return ""

    lines = ["--- Past Decision Context (most similar episodes) ---"]
    for i, ep in enumerate(episodes, start=1):
        lines.append(
            f"{i}. [{ep.trigger_type}] outcome={ep.outcome} "
            f"confidence={ep.confidence_score:.2f} "
            f"option_selected={ep.option_selected!r}\n"
            f"   Summary: {ep.context_summary}"
        )
    lines.append("--- End Past Decision Context ---")
    return "\n".join(lines)


async def _call_llm(
    context: str,
    trigger_type: str,
    past_episodes: list[EpisodicMemory] | None = None,
) -> dict | None:
    """
    Call OpenRouter (Llama 3.3 70B) for synthesis.

    Parameters
    ----------
    context:
        Concatenated top-k retrieved passages from the corpus.
    trigger_type:
        Pipeline trigger type string.
    past_episodes:
        Up to 3 similar past episodes from episodic memory.
        When present they are prepended to the user message as
        "Past Decision Context" so the model can reason from history.

    Returns
    -------
    dict | None
        Parsed JSON brief dict, or ``None`` when the LLM is unavailable.
    """
    if not settings.openrouter_api_key:
        logger.info("[L2-SYNTHESISER] No LLM configured — using extractive fallback")
        return None

    past_context_block = _format_past_episodes(past_episodes or [])
    user_message = (
        f"{past_context_block}\n\n" if past_context_block else ""
    ) + (
        f"Trigger Type: {trigger_type}\n\n"
        f"Enterprise Signals Corpus:\n{context}\n\n"
        "Produce the decision brief JSON now."
    )

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                f"{settings.openrouter_base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {settings.openrouter_api_key}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://nexusflow.dev",
                    "X-Title": "NexusFlow Synthesiser Agent",
                },
                json={
                    "model": "meta-llama/llama-3.3-70b-instruct",
                    "messages": [
                        {"role": "system", "content": _SYSTEM_PROMPT},
                        {"role": "user", "content": user_message},
                    ],
                    "temperature": 0.1,
                    "max_tokens": 1500,
                    "response_format": {"type": "json_object"},
                },
            )

        if resp.status_code != 200:
            logger.warning("[L2-SYNTHESISER] LLM call failed: %s", resp.status_code)
            return None

        content = resp.json()["choices"][0]["message"]["content"]
        return json.loads(content)

    except (httpx.HTTPError, json.JSONDecodeError, KeyError) as e:
        logger.warning("[L2-SYNTHESISER] LLM error: %s", e)
        return None


def _corpus_to_documents(state: PipelineState) -> list[IndexedDocument]:
    """Convert typed corpus into IndexedDocument list for indexing."""
    docs: list[IndexedDocument] = []

    for msg in (state.corpus.slack_messages or []):
        docs.append(IndexedDocument(
            doc_id=f"slack-{msg.id}",
            text=f"{msg.author}: {msg.content}",
            source="slack",
            metadata={"timestamp": msg.timestamp.isoformat(), "author": msg.author},
        ))

    for ticket in (state.corpus.jira_tickets or []):
        text = ticket.summary
        if ticket.description:
            text += f"\n{ticket.description}"
        docs.append(IndexedDocument(
            doc_id=f"jira-{ticket.key}",
            text=text,
            source="jira",
            metadata={"key": ticket.key, "status": ticket.status},
        ))

    for pr in (state.corpus.github_prs or []):
        text = pr.title
        if pr.body:
            text += f"\n{pr.body}"
        docs.append(IndexedDocument(
            doc_id=f"github-{pr.number}",
            text=text,
            source="github",
            metadata={"number": pr.number, "state": pr.state},
        ))

    return docs


def _build_query(state: PipelineState) -> str:
    """Build a retrieval query from the pipeline trigger."""
    trigger_queries = {
        "BUDGET_VARIANCE": "budget overspend financial variance cost overrun",
        "PROJECT_STALL": "project blocked delayed stalled dependency issue",
        "CUSTOMER_ESCALATION": "customer escalation complaint urgent critical",
        "COMPLIANCE_DEADLINE": "compliance regulatory deadline audit requirement",
        "ANOMALY_DETECTED": "anomaly error failure spike unusual pattern",
        "MANUAL": "decision review analysis",
    }
    base = trigger_queries.get(str(state.trigger_type), "enterprise decision")
    extra = " ".join(str(v) for v in state.trigger_metadata.values() if isinstance(v, str))
    return f"{base} {extra}".strip()


def _extractive_fallback(
    top_docs: list[tuple[IndexedDocument, float]],
    state: PipelineState,
) -> dict:
    """
    Extractive fallback when no LLM is available.
    Produces a minimal but valid brief from top retrieved passages.
    """
    summary_lines = [
        f"• [{doc.source.upper()}] {doc.text[:200]}"
        for doc, _ in top_docs[:5]
    ]
    return {
        "context_summary": (
            f"Trigger: {state.trigger_type}. "
            "Top signals retrieved from enterprise corpus:\n"
            + "\n".join(summary_lines)
        ),
        "causal_chain": [
            f"Trigger detected: {state.trigger_type}",
            f"Relevant signals found across {len(top_docs)} sources",
            "Manual review required — LLM synthesis unavailable",
        ],
        "risk_matrix": [
            {
                "factor": "Decision quality",
                "likelihood": "MEDIUM",
                "impact": "HIGH",
                "mitigation": "Configure OPENROUTER_API_KEY for full LLM synthesis",
            }
        ],
        "affected_systems": list({doc.source for doc, _ in top_docs}),
        "estimated_impact_usd": None,
        "confidence_score": 0.3,
    }
