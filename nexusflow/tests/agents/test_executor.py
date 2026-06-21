"""
nexusflow/tests/agents/test_executor.py
Test suite for L5 Executor Agent.
Tests all decision outcomes and audit receipt generation.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from nexusflow.agents.executor import run_executor_agent
from nexusflow.core.models import (
    DecisionBrief,
    DecisionOption,
    DecisionOutcome,
    HumanDecision,
    PipelineState,
    PipelineStatus,
    RecommendationPackage,
    TriggerType,
    ValidationResult,
)


def _make_approved_state(outcome: DecisionOutcome = DecisionOutcome.APPROVED) -> PipelineState:
    state = PipelineState(
        trigger_type=TriggerType.BUDGET_VARIANCE,
        trigger_metadata={"slack_channel_id": "C001", "jira_project_key": ""},
        status=PipelineStatus.AWAITING_HUMAN,
    )
    state.brief = DecisionBrief(
        pipeline_id=state.pipeline_id,
        context_summary="Q3 budget variance of 18%",
        causal_chain=["Costs rose", "No buffer"],
        confidence_score=0.80,
        source_item_count=10,
    )
    option = DecisionOption(
        label="Option A",
        title="Reallocate contingency",
        description="Draw from Q3 contingency to cover variance.",
        confidence=0.82,
        implementation_steps=["Identify buffer", "Raise request", "CFO approval"],
    )
    state.recommendation = RecommendationPackage(
        pipeline_id=state.pipeline_id,
        options=[option],
        recommended_option_id=option.option_id,
    )
    state.human_decision = HumanDecision(
        pipeline_id=state.pipeline_id,
        approver_id="user-cfo-001",
        approver_role="CFO",
        outcome=outcome,
        selected_option_id=option.option_id,
    )
    return state


@pytest.mark.asyncio
@patch("nexusflow.agents.executor.SlackAdapter")
async def test_executor_approved_posts_slack(MockSlack):
    """Approved decision posts Slack notification."""
    mock_adapter = MockSlack.return_value
    mock_adapter.post_message = AsyncMock(return_value=True)

    state = _make_approved_state(DecisionOutcome.APPROVED)
    result = await run_executor_agent(state)

    assert result.status == PipelineStatus.COMPLETE
    mock_adapter.post_message.assert_called_once()
    call_args = mock_adapter.post_message.call_args
    assert "C001" in str(call_args)
    assert "NexusFlow Decision Executed" in str(call_args)


@pytest.mark.asyncio
@patch("nexusflow.agents.executor.SlackAdapter")
async def test_executor_creates_receipt(MockSlack):
    """Executor always creates an audit receipt regardless of outcome."""
    mock_adapter = MockSlack.return_value
    mock_adapter.post_message = AsyncMock(return_value=True)

    state = _make_approved_state(DecisionOutcome.APPROVED)
    result = await run_executor_agent(state)

    assert result.receipt is not None
    assert result.receipt.pipeline_id == state.pipeline_id
    assert result.receipt.human_decision == DecisionOutcome.APPROVED
    assert result.receipt.trigger_type == TriggerType.BUDGET_VARIANCE


@pytest.mark.asyncio
async def test_executor_rejected_logs_and_completes():
    """Rejected decision logs action and marks pipeline COMPLETE — no API calls."""
    state = _make_approved_state(DecisionOutcome.REJECTED)
    result = await run_executor_agent(state)

    assert result.status == PipelineStatus.COMPLETE
    assert result.receipt is not None
    rejection_actions = [
        a for a in result.receipt.actions_taken
        if a.operation == "log_rejection"
    ]
    assert len(rejection_actions) == 1


@pytest.mark.asyncio
async def test_executor_deferred_completes():
    """Deferred decision marks pipeline COMPLETE with defer action."""
    state = _make_approved_state(DecisionOutcome.DEFERRED)
    result = await run_executor_agent(state)

    assert result.status == PipelineStatus.COMPLETE
    defer_actions = [
        a for a in result.receipt.actions_taken
        if a.operation == "defer"
    ]
    assert len(defer_actions) == 1


@pytest.mark.asyncio
async def test_executor_escalated_completes():
    """Escalated decision marks pipeline COMPLETE with escalate action."""
    state = _make_approved_state(DecisionOutcome.ESCALATED)
    result = await run_executor_agent(state)

    assert result.status == PipelineStatus.COMPLETE
    escalate_actions = [
        a for a in result.receipt.actions_taken
        if a.operation == "escalate"
    ]
    assert len(escalate_actions) == 1


@pytest.mark.asyncio
async def test_executor_fails_without_human_decision():
    """Executor fails gracefully when no human decision is present."""
    state = PipelineState(trigger_type=TriggerType.MANUAL)
    result = await run_executor_agent(state)

    assert result.status == PipelineStatus.FAILED
    assert result.error_stage == "EXECUTOR"


@pytest.mark.asyncio
@patch("nexusflow.agents.executor.SlackAdapter")
async def test_executor_tolerates_slack_failure(MockSlack):
    """Executor continues and completes even if Slack post fails."""
    mock_adapter = MockSlack.return_value
    mock_adapter.post_message = AsyncMock(return_value=False)

    state = _make_approved_state(DecisionOutcome.APPROVED)
    result = await run_executor_agent(state)

    # Pipeline still completes
    assert result.status == PipelineStatus.COMPLETE

    # Slack action recorded as FAILED
    slack_actions = [a for a in result.receipt.actions_taken if a.tool == "slack"]
    assert slack_actions[0].status == "FAILED"


@pytest.mark.asyncio
@patch("nexusflow.agents.executor.SlackAdapter")
async def test_executor_approved_actions_include_internal_log(MockSlack):
    """Executor always logs a nexusflow internal decision record."""
    mock_adapter = MockSlack.return_value
    mock_adapter.post_message = AsyncMock(return_value=True)

    state = _make_approved_state(DecisionOutcome.APPROVED)
    result = await run_executor_agent(state)

    internal_logs = [
        a for a in result.receipt.actions_taken
        if a.tool == "nexusflow" and a.operation == "log_decision"
    ]
    assert len(internal_logs) == 1
    assert internal_logs[0].status == "SUCCESS"
