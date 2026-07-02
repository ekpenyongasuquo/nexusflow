"""
nexusflow/core/state/pipeline.py
LangGraph state machine — the NexusFlow agent pipeline.

Graph topology
--------------

  START
    │
    ▼
  collect ──(FAILED)──────────────────────────────────────────► END
    │
    ▼
  synthesise ──(FAILED)──────────────────────────────────────► END
    │
    ▼
  route_confidence          ← NEW: inspect brief.confidence_score
    │
    ├─(confidence >= 0.7 OR collect_pass >= 1)─► validate
    │
    └─(confidence < 0.7 AND collect_pass == 0)─► collect   [second pass,
                                                             expanded lookback]
  validate
    │
    ├─(HALTED_COMPLIANCE)──────────────────────────────────► END
    │
    ├─(FAILED AND validate_retries < MAX_VALIDATE_RETRIES)─► validate  [retry]
    │
    ├─(FAILED AND retries exhausted)───────────────────────► END
    │
    └─(cleared)────────────────────────────────────────────► recommend
  recommend ──► END   [pipeline pauses; execute triggered via approval API]

  execute ──► END     [called separately after human approval]

State schema
------------
AgentState is a TypedDict whose keys are exactly the fields emitted by
PipelineState.model_dump(mode="json") plus three graph-level counters
(validate_retries, collect_pass, expanded_lookback_hours) that exist only
inside the graph and are never written back into PipelineState domain models.
"""
from __future__ import annotations

import logging
from typing import Any

from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from nexusflow.agents.collector import run_collector_agent
from nexusflow.agents.executor import run_executor_agent
from nexusflow.agents.recommender import run_recommender_agent
from nexusflow.agents.synthesiser import run_synthesiser_agent
from nexusflow.agents.validator import run_validator_agent
from nexusflow.core.models import PipelineState, PipelineStatus

logger = logging.getLogger(__name__)

# ── Retry / loop limits ───────────────────────────────────────────────────────
MAX_VALIDATE_RETRIES  = 2   # max re-runs of the validate node on FAILED status
CONFIDENCE_THRESHOLD  = 0.7 # brief.confidence_score below which a second pass fires
EXPANDED_LOOKBACK_MULT = 2  # multiplier applied to lookback_hours on second pass


# ── AgentState — typed schema for LangGraph checkpointing ────────────────────
# Keys match PipelineState.model_dump(mode="json") exactly so that the
# PipelineState(**state) round-trip in every node works without key errors.
# The three counters at the bottom are graph-internal and default to 0.

class AgentState(TypedDict, total=False):
    # ── Identity / routing ────────────────────────────────────────────────────
    pipeline_id:      str
    created_at:       str          # ISO-8601 string after model_dump
    trigger_type:     str
    trigger_source:   str
    trigger_metadata: dict[str, Any]
    status:           str

    # ── Agent output sections (None until the node runs) ─────────────────────
    corpus:           dict[str, Any] | None
    brief:            dict[str, Any] | None
    validation:       dict[str, Any] | None
    recommendation:   dict[str, Any] | None
    human_decision:   dict[str, Any] | None
    receipt:          dict[str, Any] | None

    # ── Error tracking ────────────────────────────────────────────────────────
    error_stage:      str | None
    error_message:    str | None

    # ── Graph-internal counters (not part of PipelineState) ───────────────────
    validate_retries:        int   # incremented each time validate is retried
    collect_pass:            int   # 0 = first pass, 1 = expanded second pass
    expanded_lookback_hours: int   # set by route_confidence on second pass


# ── Node wrappers — convert AgentState dict ↔ PipelineState typed model ──────
# Each wrapper strips graph-internal keys before constructing PipelineState,
# then merges the result back with the preserved counters.

_GRAPH_KEYS = frozenset(
    {"validate_retries", "collect_pass", "expanded_lookback_hours"}
)


def _to_pipeline_state(state: AgentState) -> PipelineState:
    """Build a PipelineState from AgentState, ignoring graph-internal keys."""
    return PipelineState(**{k: v for k, v in state.items() if k not in _GRAPH_KEYS})


def _merge(state: AgentState, result: PipelineState) -> AgentState:
    """
    Merge an updated PipelineState back into the AgentState dict.
    Graph-internal counters that are already in *state* are preserved.
    """
    merged: AgentState = result.model_dump(mode="json")  # type: ignore[assignment]
    for key in _GRAPH_KEYS:
        if key in state:
            merged[key] = state[key]
    return merged


async def _collect_node(state: AgentState) -> AgentState:
    # On a second pass, expand the lookback window before collecting
    ps = _to_pipeline_state(state)
    if state.get("collect_pass", 0) == 1:
        expanded = state.get("expanded_lookback_hours", 0)
        if expanded:
            ps.trigger_metadata = {**ps.trigger_metadata, "lookback_hours": expanded}
            logger.info(
                "[PIPELINE] collect_pass=1 — expanding lookback to %dh for pipeline %s",
                expanded, ps.pipeline_id,
            )
    result = await run_collector_agent(ps)
    return _merge(state, result)


async def _synthesise_node(state: AgentState) -> AgentState:
    ps = _to_pipeline_state(state)
    result = await run_synthesiser_agent(ps)
    return _merge(state, result)


async def _validate_node(state: AgentState) -> AgentState:
    ps = _to_pipeline_state(state)
    result = await run_validator_agent(ps)
    merged = _merge(state, result)
    return merged


async def _recommend_node(state: AgentState) -> AgentState:
    ps = _to_pipeline_state(state)
    result = await run_recommender_agent(ps)
    return _merge(state, result)


async def _execute_node(state: AgentState) -> AgentState:
    ps = _to_pipeline_state(state)
    result = await run_executor_agent(ps)
    return _merge(state, result)


# ── route_confidence node ─────────────────────────────────────────────────────
# This is a real graph node (not just a conditional edge function) because it
# needs to write expanded_lookback_hours and collect_pass into the state before
# looping back to collect.

async def _route_confidence_node(state: AgentState) -> AgentState:
    """
    Inspect brief.confidence_score.  If below CONFIDENCE_THRESHOLD and this is
    only the first collect pass, widen the lookback window for a second pass.
    The routing decision itself is made by _after_route_confidence().
    """
    brief = state.get("brief") or {}
    confidence = float(brief.get("confidence_score", 0.0))
    collect_pass = state.get("collect_pass", 0)

    if confidence < CONFIDENCE_THRESHOLD and collect_pass == 0:
        meta = state.get("trigger_metadata") or {}
        current_lookback = int(meta.get("lookback_hours", 72))
        expanded = current_lookback * EXPANDED_LOOKBACK_MULT
        logger.info(
            "[PIPELINE] confidence=%.2f < %.2f — scheduling second collect pass "
            "(lookback %dh → %dh) for pipeline %s",
            confidence, CONFIDENCE_THRESHOLD,
            current_lookback, expanded,
            state.get("pipeline_id"),
        )
        return {**state, "expanded_lookback_hours": expanded}

    return state   # no change needed — proceed to validate


# ── Conditional edge functions ────────────────────────────────────────────────

def _after_collect(state: AgentState) -> str:
    status = state.get("status", "")
    if status in (PipelineStatus.FAILED, PipelineStatus.HALTED_COMPLIANCE):
        logger.error(
            "[PIPELINE] halted after collect: %s", state.get("error_message")
        )
        return END
    return "synthesise"


def _after_synthesise(state: AgentState) -> str:
    status = state.get("status", "")
    if status in (PipelineStatus.FAILED, PipelineStatus.HALTED_COMPLIANCE):
        logger.error(
            "[PIPELINE] halted after synthesise: %s", state.get("error_message")
        )
        return END
    return "route_confidence"


def _after_route_confidence(state: AgentState) -> str:
    """
    Route after the confidence-check node.

    - confidence >= CONFIDENCE_THRESHOLD  → proceed to validate (first pass)
    - confidence <  CONFIDENCE_THRESHOLD AND collect_pass == 0
          → loop back to collect with expanded lookback (second pass)
    - collect_pass >= 1 (already did the expansion)
          → proceed regardless of confidence to avoid infinite loop
    """
    brief = state.get("brief") or {}
    confidence = float(brief.get("confidence_score", 0.0))
    collect_pass = state.get("collect_pass", 0)

    if confidence < CONFIDENCE_THRESHOLD and collect_pass == 0:
        # Node already wrote expanded_lookback_hours; bump collect_pass so we
        # don't loop again after the second synthesis.  We do this here because
        # edge functions can return state updates in LangGraph 0.2+.
        return "collect"

    return "validate"


def _after_validate(state: AgentState) -> str:
    """
    Three-way routing out of validate:

    HALTED_COMPLIANCE  → END  (compliance stop — never retry)
    FAILED             → retry validate up to MAX_VALIDATE_RETRIES times,
                         then END
    anything else      → recommend
    """
    status = state.get("status", "")

    if status == PipelineStatus.HALTED_COMPLIANCE:
        logger.warning("[PIPELINE] validation halted by compliance policy")
        return END

    if status == PipelineStatus.FAILED:
        retries = state.get("validate_retries", 0)
        if retries < MAX_VALIDATE_RETRIES:
            logger.warning(
                "[PIPELINE] validate FAILED (attempt %d/%d) — retrying",
                retries + 1, MAX_VALIDATE_RETRIES,
            )
            return "validate"
        logger.error(
            "[PIPELINE] validate FAILED after %d retries — halting. %s",
            MAX_VALIDATE_RETRIES, state.get("error_message"),
        )
        return END

    return "recommend"


def _after_recommend(state: AgentState) -> str:
    status = state.get("status", "")
    if status in (PipelineStatus.FAILED, PipelineStatus.HALTED_COMPLIANCE):
        return END
    # Pipeline pauses here — execute is triggered via the approval API
    return END


# ── State-update nodes for graph-internal counters ────────────────────────────
# LangGraph 0.2 conditional edges can return a routing key (str) but cannot
# mutate state directly.  We use thin "counter bump" nodes on the retry and
# second-pass paths to increment the counters before re-entering the target node.

async def _bump_validate_retry(state: AgentState) -> AgentState:
    """Increment validate_retries before looping back to validate."""
    return {**state, "validate_retries": state.get("validate_retries", 0) + 1}


async def _bump_collect_pass(state: AgentState) -> AgentState:
    """Increment collect_pass before the second collect run."""
    return {**state, "collect_pass": state.get("collect_pass", 0) + 1}


# ── Build & compile ───────────────────────────────────────────────────────────

def build_pipeline() -> Any:
    """
    Build and compile the NexusFlow LangGraph pipeline.
    Returns a compiled graph ready for async invocation.
    """
    graph: StateGraph = StateGraph(AgentState)

    # ── Nodes ──────────────────────────────────────────────────────────────────
    graph.add_node("collect",           _collect_node)
    graph.add_node("synthesise",        _synthesise_node)
    graph.add_node("route_confidence",  _route_confidence_node)
    graph.add_node("validate",          _validate_node)
    graph.add_node("recommend",         _recommend_node)
    graph.add_node("execute",           _execute_node)
    # Counter-bump helper nodes — zero business logic, only increment counters
    graph.add_node("bump_validate_retry", _bump_validate_retry)
    graph.add_node("bump_collect_pass",   _bump_collect_pass)

    # ── Edges — main flow ──────────────────────────────────────────────────────
    graph.add_edge(START, "collect")
    graph.add_conditional_edges("collect",          _after_collect)
    graph.add_conditional_edges("synthesise",       _after_synthesise)

    # route_confidence → validate (normal) OR bump_collect_pass (second pass)
    graph.add_conditional_edges(
        "route_confidence",
        _after_route_confidence,
        {"validate": "validate", "collect": "bump_collect_pass"},
    )

    # bump_collect_pass → collect  (second pass entry)
    graph.add_edge("bump_collect_pass", "collect")

    # validate → recommend (cleared) OR bump_validate_retry (retry) OR END
    graph.add_conditional_edges(
        "validate",
        _after_validate,
        {"recommend": "recommend", "validate": "bump_validate_retry", END: END},
    )

    # bump_validate_retry → validate  (re-enter with incremented counter)
    graph.add_edge("bump_validate_retry", "validate")

    graph.add_conditional_edges("recommend", _after_recommend)
    graph.add_edge("execute", END)

    compiled = graph.compile()
    logger.info(
        "NexusFlow pipeline compiled: "
        "collect → synthesise → route_confidence → validate(±retry) → recommend → [human] → execute"
    )
    return compiled


# ── Singleton pipeline instance ───────────────────────────────────────────────
_pipeline = None


def get_pipeline() -> Any:
    global _pipeline
    if _pipeline is None:
        _pipeline = build_pipeline()
    return _pipeline


async def run_pipeline(state: PipelineState) -> PipelineState:
    """
    Run the collect → recommend stages of the pipeline.
    Stops at AWAITING_HUMAN — the execute stage is triggered separately
    via the /pipelines/{id}/approve endpoint.
    """
    pipeline = get_pipeline()

    # Seed graph-internal counters so AgentState fields are present from step 1
    initial_state: AgentState = {
        **state.model_dump(mode="json"),  # type: ignore[misc]
        "validate_retries":        0,
        "collect_pass":            0,
        "expanded_lookback_hours": 0,
    }

    try:
        result = await pipeline.ainvoke(initial_state)
        # Strip graph-internal keys before constructing PipelineState
        clean = {k: v for k, v in result.items() if k not in _GRAPH_KEYS}
        return PipelineState(**clean)
    except Exception as e:
        logger.exception("Pipeline invocation error for %s", state.pipeline_id)
        state.status        = PipelineStatus.FAILED
        state.error_stage   = "PIPELINE"
        state.error_message = str(e)
        return state


async def run_execute_stage(state: PipelineState) -> PipelineState:
    """
    Run only the execute stage — called after human approval.
    """
    seed: AgentState = {
        **state.model_dump(mode="json"),  # type: ignore[misc]
        "validate_retries":        0,
        "collect_pass":            0,
        "expanded_lookback_hours": 0,
    }
    result_dict = await _execute_node(seed)
    clean = {k: v for k, v in result_dict.items() if k not in _GRAPH_KEYS}
    return PipelineState(**clean)
