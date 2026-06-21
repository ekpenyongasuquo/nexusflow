"""
nexusflow/tests/agents/test_collector.py
Test suite for L1 Collector Agent.
Tests concurrent adapter orchestration, partial failure tolerance,
and zero-item halt behaviour.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from nexusflow.agents.collector import run_collector_agent
from nexusflow.core.models import (
    GitHubPR,
    JiraTicket,
    PipelineState,
    PipelineStatus,
    SlackMessage,
    TriggerType,
)


def _slack_msg(idx: int) -> SlackMessage:
    return SlackMessage(
        id=str(idx),
        channel_id="C001",
        author=f"U{idx:03d}",
        timestamp=datetime.now(timezone.utc),
        content=f"Message {idx}",
    )


def _jira_ticket(idx: int) -> JiraTicket:
    return JiraTicket(
        id=str(idx),
        key=f"PROJ-{idx}",
        summary=f"Ticket {idx}",
        status="In Progress",
        updated=datetime.now(timezone.utc),
    )


def _github_pr(idx: int) -> GitHubPR:
    return GitHubPR(
        id=idx,
        number=idx,
        title=f"PR #{idx}",
        state="open",
        author=f"dev{idx}",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )


@pytest.mark.asyncio
@patch("nexusflow.agents.collector._safe_slack_collect", new_callable=AsyncMock)
@patch("nexusflow.agents.collector._safe_jira_collect", new_callable=AsyncMock)
@patch("nexusflow.agents.collector._safe_github_collect", new_callable=AsyncMock)
async def test_collector_aggregates_all_sources(
    mock_github, mock_jira, mock_slack
):
    """Collector aggregates results from all three adapters into corpus."""
    mock_slack.return_value = [_slack_msg(1), _slack_msg(2)]
    mock_jira.return_value = [_jira_ticket(1)]
    mock_github.return_value = [_github_pr(1), _github_pr(2), _github_pr(3)]

    state = PipelineState(
        trigger_type=TriggerType.BUDGET_VARIANCE,
        trigger_metadata={"slack_channel_id": "C001"},
    )
    result = await run_collector_agent(state)

    assert result.corpus is not None
    assert result.corpus.total_items == 6
    assert len(result.corpus.slack_messages) == 2
    assert len(result.corpus.jira_tickets) == 1
    assert len(result.corpus.github_prs) == 3
    assert result.status == PipelineStatus.COLLECTING


@pytest.mark.asyncio
@patch("nexusflow.agents.collector._safe_slack_collect", new_callable=AsyncMock)
@patch("nexusflow.agents.collector._safe_jira_collect", new_callable=AsyncMock)
@patch("nexusflow.agents.collector._safe_github_collect", new_callable=AsyncMock)
async def test_collector_tolerates_partial_failure(
    mock_github, mock_jira, mock_slack
):
    """Collector continues when one adapter fails — partial corpus is valid."""
    mock_slack.return_value = [_slack_msg(1), _slack_msg(2), _slack_msg(3)]
    mock_jira.return_value = []  # JIRA failed silently
    mock_github.return_value = []  # GitHub failed silently

    state = PipelineState(trigger_type=TriggerType.PROJECT_STALL)
    result = await run_collector_agent(state)

    # Pipeline continues — 3 items collected from Slack
    assert result.corpus.total_items == 3
    assert result.status == PipelineStatus.COLLECTING
    assert result.error_message is None


@pytest.mark.asyncio
@patch("nexusflow.agents.collector._safe_slack_collect", new_callable=AsyncMock)
@patch("nexusflow.agents.collector._safe_jira_collect", new_callable=AsyncMock)
@patch("nexusflow.agents.collector._safe_github_collect", new_callable=AsyncMock)
async def test_collector_halts_on_zero_items(
    mock_github, mock_jira, mock_slack
):
    """Collector halts pipeline when all adapters return zero items."""
    mock_slack.return_value = []
    mock_jira.return_value = []
    mock_github.return_value = []

    state = PipelineState(trigger_type=TriggerType.MANUAL)
    result = await run_collector_agent(state)

    assert result.status == PipelineStatus.FAILED
    assert result.error_stage == "COLLECTOR"
    assert result.corpus.total_items == 0


@pytest.mark.asyncio
@patch("nexusflow.agents.collector._safe_slack_collect", new_callable=AsyncMock)
@patch("nexusflow.agents.collector._safe_jira_collect", new_callable=AsyncMock)
@patch("nexusflow.agents.collector._safe_github_collect", new_callable=AsyncMock)
async def test_collector_records_errors_in_corpus(
    mock_github, mock_jira, mock_slack
):
    """Collection errors are recorded in corpus.collection_errors."""
    mock_slack.side_effect = lambda meta, errors: (
        errors.append("Slack: rate limit") or []
    )
    mock_jira.return_value = [_jira_ticket(1)]
    mock_github.return_value = []

    state = PipelineState(trigger_type=TriggerType.CUSTOMER_ESCALATION)
    result = await run_collector_agent(state)

    assert result.corpus.total_items == 1
    assert any("Slack" in e for e in result.corpus.collection_errors)


@pytest.mark.asyncio
@patch("nexusflow.agents.collector._safe_slack_collect", new_callable=AsyncMock)
@patch("nexusflow.agents.collector._safe_jira_collect", new_callable=AsyncMock)
@patch("nexusflow.agents.collector._safe_github_collect", new_callable=AsyncMock)
async def test_collector_sets_pipeline_id_on_corpus(
    mock_github, mock_jira, mock_slack
):
    """Corpus pipeline_id matches the parent pipeline state."""
    mock_slack.return_value = [_slack_msg(1)]
    mock_jira.return_value = []
    mock_github.return_value = []

    state = PipelineState(trigger_type=TriggerType.ANOMALY_DETECTED)
    result = await run_collector_agent(state)

    assert result.corpus.pipeline_id == state.pipeline_id
