"""
nexusflow/tests/agents/test_validator.py
Test suite for L3 Validator Agent.
Tests PII scanning, policy enforcement, authority graph routing.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from nexusflow.agents.validator import (
    _resolve_authority,
    _scan_pii,
    _pseudonymise,
    run_validator_agent,
)
from nexusflow.core.models import (
    CollectedCorpus,
    DecisionBrief,
    PipelineState,
    PipelineStatus,
    RiskLevel,
    TriggerType,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_state(trigger: TriggerType = TriggerType.BUDGET_VARIANCE) -> PipelineState:
    state = PipelineState(trigger_type=trigger)
    state.brief = DecisionBrief(
        pipeline_id=state.pipeline_id,
        context_summary="Q3 budget has exceeded allocated limits by 18%.",
        causal_chain=["Vendor costs increased", "No contingency buffer", "CFO not notified"],
        estimated_impact_usd=75000.0,
        confidence_score=0.82,
        source_item_count=15,
    )
    state.corpus = CollectedCorpus(pipeline_id=state.pipeline_id)
    return state


# ── PII Scanning Tests ────────────────────────────────────────────────────────

def test_pii_scan_detects_email():
    findings = _scan_pii("Contact john.doe@company.com for details")
    assert any(f.entity_type == "EMAIL" for f in findings)


def test_pii_scan_detects_phone():
    findings = _scan_pii("Call us at 555-123-4567 for assistance")
    assert any(f.entity_type == "PHONE_NUMBER" for f in findings)


def test_pii_scan_detects_ssn():
    findings = _scan_pii("SSN on file: 123-45-6789")
    assert any(f.entity_type == "SSN" for f in findings)


def test_pii_scan_clean_text():
    findings = _scan_pii("Budget variance of 18% detected in Q3 finance reports")
    assert len(findings) == 0


def test_pseudonymise_replaces_email():
    text = "Contact admin@nexusflow.dev for access"
    findings = _scan_pii(text)
    result = _pseudonymise(text, findings)
    assert "admin@nexusflow.dev" not in result
    assert "[EMAIL_" in result


def test_pseudonymise_preserves_non_pii():
    text = "Budget is 18% over Q3 targets"
    findings = _scan_pii(text)
    result = _pseudonymise(text, findings)
    assert result == text  # no PII — unchanged


# ── Authority Graph Tests ─────────────────────────────────────────────────────

MOCK_POLICY = {
    "authority_rules": [
        {"trigger_type": "BUDGET_VARIANCE", "min_value_usd": 0, "max_value_usd": 9999,
         "required_role": "MANAGER", "fallback_role": "DIRECTOR"},
        {"trigger_type": "BUDGET_VARIANCE", "min_value_usd": 10000, "max_value_usd": 49999,
         "required_role": "DIRECTOR", "fallback_role": "VP"},
        {"trigger_type": "BUDGET_VARIANCE", "min_value_usd": 50000, "max_value_usd": None,
         "required_role": "CFO", "fallback_role": "CEO"},
        {"trigger_type": "COMPLIANCE_DEADLINE", "min_value_usd": None, "max_value_usd": None,
         "required_role": "CLO", "fallback_role": "CFO"},
    ]
}


def test_authority_small_budget_routes_to_manager():
    role, fallback = _resolve_authority("BUDGET_VARIANCE", 5000.0, MOCK_POLICY)
    assert role == "MANAGER"
    assert fallback == "DIRECTOR"


def test_authority_mid_budget_routes_to_director():
    role, fallback = _resolve_authority("BUDGET_VARIANCE", 25000.0, MOCK_POLICY)
    assert role == "DIRECTOR"
    assert fallback == "VP"


def test_authority_large_budget_routes_to_cfo():
    role, fallback = _resolve_authority("BUDGET_VARIANCE", 75000.0, MOCK_POLICY)
    assert role == "CFO"
    assert fallback == "CEO"


def test_authority_compliance_routes_to_clo():
    role, fallback = _resolve_authority("COMPLIANCE_DEADLINE", None, MOCK_POLICY)
    assert role == "CLO"
    assert fallback == "CFO"


def test_authority_unknown_trigger_defaults_to_manager():
    role, _ = _resolve_authority("UNKNOWN_TRIGGER", None, MOCK_POLICY)
    assert role == "MANAGER"


# ── Validator Agent Integration Tests ────────────────────────────────────────

@pytest.mark.asyncio
@patch("nexusflow.agents.validator._load_policy", return_value=MOCK_POLICY)
async def test_validator_clears_clean_brief(mock_policy):
    """Clean brief with no PII or violations is cleared for routing."""
    state = _make_state(TriggerType.BUDGET_VARIANCE)
    result = await run_validator_agent(state)

    assert result.validation is not None
    assert result.validation.is_cleared is True
    assert result.validation.required_approver_role == "CFO"  # $75K impact
    assert result.status != PipelineStatus.HALTED_COMPLIANCE


@pytest.mark.asyncio
@patch("nexusflow.agents.validator._load_policy", return_value=MOCK_POLICY)
async def test_validator_pseudonymises_pii_and_continues(mock_policy):
    """Brief containing PII gets pseudonymised — pipeline continues (not halted)."""
    state = _make_state(TriggerType.BUDGET_VARIANCE)
    state.brief.context_summary = "Finance lead john.doe@company.com flagged Q3 issue"

    result = await run_validator_agent(state)

    # PII pseudonymised
    assert "john.doe@company.com" not in result.brief.context_summary
    assert result.validation.pii_findings  # findings recorded
    # Pipeline continues (PII was pseudonymised, not a halt condition)
    assert result.validation.is_cleared is True


@pytest.mark.asyncio
@patch("nexusflow.agents.validator._load_policy", return_value=MOCK_POLICY)
async def test_validator_warns_on_low_confidence(mock_policy):
    """Brief with low confidence generates a warning violation but doesn't halt."""
    state = _make_state(TriggerType.BUDGET_VARIANCE)
    state.brief.confidence_score = 0.25  # below 0.4 threshold

    result = await run_validator_agent(state)

    warning_violations = [
        v for v in result.validation.policy_violations
        if v.rule_id == "POL-004"
    ]
    assert len(warning_violations) == 1


@pytest.mark.asyncio
@patch("nexusflow.agents.validator._load_policy", return_value=MOCK_POLICY)
async def test_validator_fails_with_no_brief(mock_policy):
    """Validator fails gracefully when no brief is present."""
    state = PipelineState(trigger_type=TriggerType.MANUAL)
    result = await run_validator_agent(state)

    assert result.status == PipelineStatus.FAILED
    assert result.error_stage == "VALIDATOR"
