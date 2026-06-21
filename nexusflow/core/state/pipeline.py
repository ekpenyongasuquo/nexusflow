"""
nexusflow/core/state/pipeline.py
LangGraph state machine — the NexusFlow agent pipeline.
Five nodes: collect → synthesise → validate → recommend → execute
No shared mutable state between nodes.
Each node receives and returns the full PipelineState.
"""
from __future__ import annotations

import logging
from typing import Any

from langgraph.graph import END, START, StateGraph

from nexusflow.agents.collector import run_collector_agent
from nexusflow.agents.executor import run_executor_agent
from nexusflow.agents.recommender import run_recommender_agent
from nexusflow.agents.synthesiser import run_synthesiser_agent
from nexusflow.agents.validator import run_validator_agent
from nexusflow.core.models import PipelineState, PipelineStatus

logger = logging.getLogger(__name__)


# ── Node wrappers — convert PipelineState ↔ LangGraph dict ───────────────────
# LangGraph passes state as a dict; we convert to/from our typed model.

async def _collect_node(state: dict) -> dict:
    ps = PipelineState(**state)
    result = await run_collector_agent(ps)
    return result.model_dump(mode="json")


async def _synthesise_node(state: dict) -> dict:
    ps = PipelineState(**state)
    result = await run_synthesiser_agent(ps)
    return result.model_dump(mode="json")


async def _validate_node(state: dict) -> dict:
    ps = PipelineState(**state)
    result = await run_validator_agent(ps)
    return result.model_dump(mode="json")


async def _recommend_node(state: dict) -> dict:
    ps = PipelineState(**state)
    result = await run_recommender_agent(ps)
    return result.model_dump(mode="json")


async def _execute_node(state: dict) -> dict:
    ps = PipelineState(**state)
    result = await run_executor_agent(ps)
    return result.model_dump(mode="json")


# ── Conditional edges — halt pipeline on failure or compliance hold ───────────

def _after_collect(state: dict) -> str:
    status = state.get("status", "")
    if status in (PipelineStatus.FAILED, PipelineStatus.HALTED_COMPLIANCE):
        logger.error("Pipeline halted after COLLECT: %s", state.get("error_message"))
        return END
    return "synthesise"


def _after_synthesise(state: dict) -> str:
    status = state.get("status", "")
    if status in (PipelineStatus.FAILED, PipelineStatus.HALTED_COMPLIANCE):
        logger.error("Pipeline halted after SYNTHESISE: %s", state.get("error_message"))
        return END
    return "validate"


def _after_validate(state: dict) -> str:
    status = state.get("status", "")
    if status == PipelineStatus.HALTED_COMPLIANCE:
        logger.warning("Pipeline halted by compliance validator")
        return END
    if status == PipelineStatus.FAILED:
        logger.error("Pipeline failed after VALIDATE: %s", state.get("error_message"))
        return END
    return "recommend"


def _after_recommend(state: dict) -> str:
    status = state.get("status", "")
    if status in (PipelineStatus.FAILED, PipelineStatus.HALTED_COMPLIANCE):
        return END
    # AWAITING_HUMAN — pipeline pauses here for human input
    # The execute node is triggered externally via the approval API
    return END


def build_pipeline() -> Any:
    """
    Build and compile the NexusFlow LangGraph pipeline.
    Returns a compiled graph ready for async invocation.
    """
    graph = StateGraph(dict)

    # ── Add nodes ─────────────────────────────────────────────────────────────
    graph.add_node("collect", _collect_node)
    graph.add_node("synthesise", _synthesise_node)
    graph.add_node("validate", _validate_node)
    graph.add_node("recommend", _recommend_node)
    graph.add_node("execute", _execute_node)

    # ── Add edges ─────────────────────────────────────────────────────────────
    graph.add_edge(START, "collect")
    graph.add_conditional_edges("collect", _after_collect)
    graph.add_conditional_edges("synthesise", _after_synthesise)
    graph.add_conditional_edges("validate", _after_validate)
    graph.add_conditional_edges("recommend", _after_recommend)
    graph.add_edge("execute", END)

    compiled = graph.compile()
    logger.info("NexusFlow pipeline compiled: collect → synthesise → validate → recommend → [human] → execute")
    return compiled


# ── Singleton pipeline instance ───────────────────────────────────────────────
_pipeline = None


def get_pipeline():
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
    initial_state = state.model_dump(mode="json")

    try:
        result = await pipeline.ainvoke(initial_state)
        return PipelineState(**result)
    except Exception as e:
        logger.exception("Pipeline invocation error for %s", state.pipeline_id)
        state.status = PipelineStatus.FAILED
        state.error_stage = "PIPELINE"
        state.error_message = str(e)
        return state


async def run_execute_stage(state: PipelineState) -> PipelineState:
    """
    Run only the execute stage — called after human approval.
    """
    result_dict = await _execute_node(state.model_dump(mode="json"))
    return PipelineState(**result_dict)
