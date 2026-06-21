"""
nexusflow/tests/agents/test_recommender.py
Test suite for L4 Recommender Agent.
Tests rule-based fallback and option generation.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from nexusflow.agents.recommender import _rule_based_fallback, run_recommender_agent
from nexusflow.core.models import (
    DecisionBrief,
    PipelineState,
    PipelineStatus,
    TriggerType,
    ValidationResult,
)


def _make_validated_state(trigger: TriggerType = TriggerType.BUDGET_VARIANCE) -> PipelineState:
    state = PipelineState(trigger_type=trigger)
    state.brief = DecisionBrief(
        pipeline_id=state.pipeline_id,
        context_summary="Q3 budget exceeded by 18% — $75K variance",
        causal_chain=["Vendor costs rose 22%", "Contingency buffer exhausted"],
        estimated_impact_usd=75000.0,
        confidence_score=0.80,
        source_item_count=20,
    )
    state.validation = ValidationResult(
        pipeline_id=state.pipeline_id,
        required_approver_role="CFO",
        is_cleared=True,
    )
    return state


def test_rule_based_fallback_budget_variance():
    """Rule-based fallback generates 3 options for BUDGET_VARIANCE."""
    state = _make_validated_state(TriggerType.BUDGET_VARIANCE)
    result = _rule_based_fallback(state)

    assert len(result["options"]) == 3
    assert result["options"][0]["label"] == "Option A"
    assert result["options"][1]["label"] == "Option B"
    assert result["options"][2]["label"] == "Option C"


def test_rule_based_fallback_project_stall():
    """Rule-based fallback generates 3 options for PROJECT_STALL."""
    state = _make_validated_state(TriggerType.PROJECT_STALL)
    result = _rule_based_fallback(state)
    assert len(result["options"]) == 3


def test_rule_based_fallback_unknown_trigger():
    """Rule-based fallback generates generic 3 options for unknown trigger."""
    state = _make_validated_state(TriggerType.MANUAL)
    result = _rule_based_fallback(state)
    assert len(result["options"]) == 3


def test_rule_based_option_a_highest_confidence():
    """Option A always has higher confidence than Option C in fallback."""
    state = _make_validated_state(TriggerType.BUDGET_VARIANCE)
    result = _rule_based_fallback(state)
    conf_a = result["options"][0]["confidence"]
    conf_c = result["options"][2]["confidence"]
    assert conf_a > conf_c


@pytest.mark.asyncio
@patch("nexusflow.agents.recommender._call_llm", new_callable=AsyncMock)
async def test_recommender_builds_typed_options(mock_llm):
    """Recommender converts LLM JSON into typed DecisionOption objects."""
    mock_llm.return_value = {
        "options": [
            {"label": "Option A", "title": "Reallocate contingency",
             "description": "Use Q3 contingency reserves to cover variance.",
             "confidence": 0.82, "risk_level": "LOW",
             "implementation_steps": ["Step 1", "Step 2"],
             "time_to_implement_days": 1},
            {"label": "Option B", "title": "Defer vendor contracts",
             "description": "Postpone non-critical vendor payments.",
             "confidence": 0.65, "risk_level": "MEDIUM",
             "implementation_steps": ["Step 1"],
             "time_to_implement_days": 3},
            {"label": "Option C", "title": "Board reserve escalation",
             "description": "Request emergency board reserve.",
             "confidence": 0.45, "risk_level": "HIGH",
             "implementation_steps": ["Step 1"],
             "time_to_implement_days": 7},
        ],
        "recommended_option_id": None,
        "reasoning": "Option A provides lowest disruption at highest confidence.",
    }

    state = _make_validated_state()
    result = await run_recommender_agent(state)

    assert result.recommendation is not None
    assert len(result.recommendation.options) == 3
    assert result.recommendation.options[0].confidence == 0.82
    assert result.status == PipelineStatus.AWAITING_HUMAN
    assert result.recommendation.recommended_option_id is not None


@pytest.mark.asyncio
async def test_recommender_fails_without_brief():
    """Recommender fails gracefully when no brief is present."""
    state = PipelineState(trigger_type=TriggerType.MANUAL)
    result = await run_recommender_agent(state)
    assert result.status == PipelineStatus.FAILED
    assert result.error_stage == "RECOMMENDER"


@pytest.mark.asyncio
async def test_recommender_skips_when_validation_halted():
    """Recommender does not run if validation halted the pipeline."""
    state = _make_validated_state()
    state.validation.is_cleared = False
    state.validation.halt_reason = "PII compliance violation"

    result = await run_recommender_agent(state)

    # Should not have set recommendation
    assert result.recommendation is None


@pytest.mark.asyncio
@patch("nexusflow.agents.recommender._call_llm", new_callable=AsyncMock)
async def test_recommender_falls_back_when_llm_unavailable(mock_llm):
    """Recommender uses rule-based fallback when LLM returns None."""
    mock_llm.return_value = None

    state = _make_validated_state(TriggerType.PROJECT_STALL)
    result = await run_recommender_agent(state)

    assert result.recommendation is not None
    assert len(result.recommendation.options) == 3
    assert result.status == PipelineStatus.AWAITING_HUMAN
