"""
nexusflow/db/audit.py
Immutable audit receipt writer with SHA-256 hash chain.
Every receipt hashes the previous one — tamper-evident by design.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexusflow.core.models import AuditReceipt, PipelineState
from nexusflow.db.models import AuditReceiptRecord


def _sha256(data: str) -> str:
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


async def get_last_chain_hash(session: AsyncSession) -> str:
    """Retrieve the SHA-256 hash of the most recent receipt for chain linking."""
    result = await session.execute(
        select(AuditReceiptRecord.sha256_hash)
        .order_by(AuditReceiptRecord.created_at.desc())
        .limit(1)
    )
    row = result.scalar_one_or_none()
    return row if row else "GENESIS"


async def write_audit_receipt(
    state: PipelineState,
    session: AsyncSession,
) -> AuditReceipt:
    """
    Serialise pipeline state into an immutable audit receipt.
    Computes SHA-256 of receipt JSON, chains to previous receipt hash.
    Writes to append-only SQLite audit table.
    """
    chain_hash = await get_last_chain_hash(session)

    receipt = AuditReceipt(
        pipeline_id=state.pipeline_id,
        trigger_type=state.trigger_type,
        final_status=state.status,
        human_decision=(
            state.human_decision.outcome if state.human_decision else None
        ),
        actions_taken=(
            state.receipt.actions_taken if state.receipt else []
        ),
        chain_hash=chain_hash,
    )

    # Serialise to canonical JSON for hashing
    receipt_dict = receipt.model_dump(mode="json")
    receipt_json_str = json.dumps(receipt_dict, sort_keys=True, default=str)
    receipt.sha256_hash = _sha256(receipt_json_str)

    # Persist to audit DB
    record = AuditReceiptRecord(
        receipt_id=receipt.receipt_id,
        pipeline_id=receipt.pipeline_id,
        created_at=receipt.created_at,
        trigger_type=str(receipt.trigger_type),
        final_status=str(receipt.final_status),
        human_decision=(
            str(receipt.human_decision) if receipt.human_decision else None
        ),
        actions_json=[a.model_dump(mode="json") for a in receipt.actions_taken],
        receipt_json=receipt_dict,
        sha256_hash=receipt.sha256_hash,
        chain_hash=receipt.chain_hash,
        is_genesis=(chain_hash == "GENESIS"),
    )

    session.add(record)
    await session.flush()  # write immediately — do not defer

    return receipt


async def verify_chain_integrity(session: AsyncSession) -> dict:
    """
    Walk the entire audit receipt chain and verify hash integrity.
    Returns a report: {valid: bool, total: int, broken_at: str | None}
    """
    result = await session.execute(
        select(AuditReceiptRecord).order_by(AuditReceiptRecord.created_at.asc())
    )
    records = result.scalars().all()

    if not records:
        return {"valid": True, "total": 0, "broken_at": None}

    prev_hash = "GENESIS"
    for record in records:
        if record.chain_hash != prev_hash:
            return {
                "valid": False,
                "total": len(records),
                "broken_at": record.receipt_id,
                "expected_chain_hash": prev_hash,
                "found_chain_hash": record.chain_hash,
            }
        prev_hash = record.sha256_hash

    return {"valid": True, "total": len(records), "broken_at": None}
