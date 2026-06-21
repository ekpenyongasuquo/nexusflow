"""
nexusflow/tests/test_audit.py
Test suite for the cryptographic audit receipt chain.
Tests SHA-256 hashing, chain linking, and integrity verification.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nexusflow.core.models import (
    AuditReceipt,
    DecisionOutcome,
    PipelineState,
    PipelineStatus,
    TriggerType,
)
from nexusflow.db.audit import _sha256, write_audit_receipt


def test_sha256_deterministic():
    """SHA-256 hash is deterministic for the same input."""
    data = "NexusFlow audit test string"
    assert _sha256(data) == _sha256(data)
    assert len(_sha256(data)) == 64  # 256 bits = 64 hex chars


def test_sha256_unique_for_different_inputs():
    """Different inputs produce different SHA-256 hashes."""
    assert _sha256("pipeline-abc") != _sha256("pipeline-xyz")


@pytest.mark.asyncio
async def test_write_audit_receipt_genesis():
    """First receipt in chain has chain_hash='GENESIS'."""
    state = PipelineState(
        trigger_type=TriggerType.BUDGET_VARIANCE,
        status=PipelineStatus.COMPLETE,
    )
    state.human_decision = MagicMock()
    state.human_decision.outcome = DecisionOutcome.APPROVED
    state.receipt = MagicMock()
    state.receipt.actions_taken = []

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=MagicMock(scalar_one_or_none=MagicMock(return_value=None)))

    receipt = await write_audit_receipt(state, mock_session)

    assert receipt.chain_hash == "GENESIS"
    assert len(receipt.sha256_hash) == 64
    assert receipt.pipeline_id == state.pipeline_id


@pytest.mark.asyncio
async def test_write_audit_receipt_chains_to_previous():
    """Second receipt chains to first receipt's SHA-256 hash."""
    state = PipelineState(
        trigger_type=TriggerType.PROJECT_STALL,
        status=PipelineStatus.COMPLETE,
    )
    state.human_decision = MagicMock()
    state.human_decision.outcome = DecisionOutcome.APPROVED
    state.receipt = MagicMock()
    state.receipt.actions_taken = []

    prev_hash = "a" * 64  # simulate previous receipt hash

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(
        return_value=MagicMock(scalar_one_or_none=MagicMock(return_value=prev_hash))
    )

    receipt = await write_audit_receipt(state, mock_session)

    assert receipt.chain_hash == prev_hash
    assert receipt.sha256_hash != prev_hash


@pytest.mark.asyncio
async def test_write_audit_receipt_hash_covers_content():
    """SHA-256 hash changes when receipt content changes."""
    def _make_state(trigger):
        s = PipelineState(trigger_type=trigger, status=PipelineStatus.COMPLETE)
        s.human_decision = MagicMock()
        s.human_decision.outcome = DecisionOutcome.APPROVED
        s.receipt = MagicMock()
        s.receipt.actions_taken = []
        return s

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(
        return_value=MagicMock(scalar_one_or_none=MagicMock(return_value=None))
    )

    state_a = _make_state(TriggerType.BUDGET_VARIANCE)
    state_b = _make_state(TriggerType.CUSTOMER_ESCALATION)

    receipt_a = await write_audit_receipt(state_a, mock_session)
    receipt_b = await write_audit_receipt(state_b, mock_session)

    assert receipt_a.sha256_hash != receipt_b.sha256_hash


def test_audit_receipt_serialises_cleanly():
    """AuditReceipt Pydantic model serialises to JSON without error."""
    receipt = AuditReceipt(
        pipeline_id="test-pipeline",
        trigger_type=TriggerType.COMPLIANCE_DEADLINE,
        final_status=PipelineStatus.COMPLETE,
        human_decision=DecisionOutcome.APPROVED,
        sha256_hash="a" * 64,
        chain_hash="GENESIS",
    )
    data = receipt.model_dump(mode="json")
    json_str = json.dumps(data, default=str)
    assert "test-pipeline" in json_str
    assert "COMPLIANCE_DEADLINE" in json_str
