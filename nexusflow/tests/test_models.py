"""
nexusflow/tests/test_models.py
Test suite for core domain models and PipelineState contracts.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from nexusflow.core.models import (
    CollectedCorpus,
    DecisionBrief,
    DecisionOption,
    PipelineState,
    PipelineStatus,
    RiskLevel,
    TriggerType,
)


def test_pipeline_state_defaults():
    """PipelineState initialises with expected defaults."""
    state = PipelineState()
    assert state.status == PipelineStatus.PENDING
    assert state.pipeline_id  # auto-generated UUID
    assert state.corpus is None
    assert state.brief is None
    assert state.validation is None
    assert state.recommendation is None
    assert state.human_decision is None
    assert state.receipt is None


def test_pipeline_state_trigger_types():
    """All TriggerType values are valid PipelineState trigger_type."""
    for trigger in TriggerType:
        state = PipelineState(trigger_type=trigger)
        assert state.trigger_type == trigger


def test_collected_corpus_total_items_auto_computed():
    """CollectedCorpus total_items is computed from all list lengths."""
    from nexusflow.core.models import SlackMessage, JiraTicket
    from datetime import datetime, timezone

    corpus = CollectedCorpus(
        pipeline_id="test-id",
        slack_messages=[
            SlackMessage(id="1", channel_id="C1", author="U1",
                        timestamp=datetime.now(timezone.utc), content="test"),
            SlackMessage(id="2", channel_id="C1", author="U2",
                        timestamp=datetime.now(timezone.utc), content="test2"),
        ],
        jira_tickets=[],
        github_prs=[],
    )
    assert corpus.total_items == 2


def test_decision_option_confidence_bounds():
    """DecisionOption confidence must be between 0 and 1."""
    with pytest.raises(ValidationError):
        DecisionOption(
            label="Option A",
            title="Test",
            description="Test description",
            confidence=1.5,  # invalid
        )


def test_decision_option_valid_confidence():
    """DecisionOption accepts valid confidence values."""
    opt = DecisionOption(
        label="Option A",
        title="Reallocate budget",
        description="Move funds from contingency to cover variance",
        confidence=0.82,
        risk_level=RiskLevel.LOW,
    )
    assert opt.confidence == 0.82
    assert opt.option_id  # auto-generated


def test_decision_brief_serialisation():
    """DecisionBrief serialises and deserialises cleanly."""
    from nexusflow.core.models import RiskMatrixEntry

    brief = DecisionBrief(
        pipeline_id="test-pipeline-123",
        context_summary="Q3 budget exceeded by 18%",
        causal_chain=["Vendor costs rose", "No contingency"],
        risk_matrix=[
            RiskMatrixEntry(
                factor="Budget overrun",
                likelihood=RiskLevel.HIGH,
                impact=RiskLevel.CRITICAL,
                mitigation="Reallocate contingency reserves",
            )
        ],
        confidence_score=0.75,
        source_item_count=22,
    )

    serialised = brief.model_dump(mode="json")
    restored = DecisionBrief(**serialised)

    assert restored.pipeline_id == brief.pipeline_id
    assert restored.confidence_score == brief.confidence_score
    assert len(restored.risk_matrix) == 1
    assert restored.risk_matrix[0].likelihood == RiskLevel.HIGH


def test_pipeline_state_json_roundtrip():
    """Full PipelineState serialises and deserialises without data loss."""
    state = PipelineState(
        trigger_type=TriggerType.BUDGET_VARIANCE,
        trigger_source="netsuite-webhook",
        trigger_metadata={"amount": 75000, "department": "Engineering"},
    )

    serialised = state.model_dump(mode="json")
    restored = PipelineState(**serialised)

    assert restored.pipeline_id == state.pipeline_id
    assert restored.trigger_type == state.trigger_type
    assert restored.trigger_metadata["amount"] == 75000
