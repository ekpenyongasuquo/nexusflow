"""
nexusflow/api/routes/admin.py
Admin routes — authority rules management, policy reload, audit export.
Restricted to ADMIN role.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexusflow.api.middleware.auth import require_role
from nexusflow.db.models import AuthorityRule, AuditReceiptRecord, User
from nexusflow.db.session import get_audit_session, get_main_session

router = APIRouter()


class AuthorityRuleCreate(BaseModel):
    trigger_type: str
    min_value_usd: float | None = None
    max_value_usd: float | None = None
    required_role: str
    fallback_role: str | None = None


@router.get("/authority-rules")
async def list_authority_rules(
    current_user: User = Depends(require_role("ADMIN")),
    session: AsyncSession = Depends(get_main_session),
):
    result = await session.execute(
        select(AuthorityRule).where(AuthorityRule.is_active == True)
    )
    rules = result.scalars().all()
    return [
        {
            "id": r.id,
            "trigger_type": r.trigger_type,
            "min_value_usd": r.min_value_usd,
            "max_value_usd": r.max_value_usd,
            "required_role": r.required_role,
            "fallback_role": r.fallback_role,
        }
        for r in rules
    ]


@router.post("/authority-rules", status_code=201)
async def create_authority_rule(
    body: AuthorityRuleCreate,
    current_user: User = Depends(require_role("ADMIN")),
    session: AsyncSession = Depends(get_main_session),
):
    rule = AuthorityRule(
        trigger_type=body.trigger_type.upper(),
        min_value_usd=body.min_value_usd,
        max_value_usd=body.max_value_usd,
        required_role=body.required_role.upper(),
        fallback_role=body.fallback_role.upper() if body.fallback_role else None,
    )
    session.add(rule)
    await session.flush()
    return {"id": rule.id, "created": True}


@router.delete("/authority-rules/{rule_id}")
async def delete_authority_rule(
    rule_id: str,
    current_user: User = Depends(require_role("ADMIN")),
    session: AsyncSession = Depends(get_main_session),
):
    result = await session.execute(
        select(AuthorityRule).where(AuthorityRule.id == rule_id)
    )
    rule = result.scalar_one_or_none()
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")
    rule.is_active = False
    return {"deleted": True}


@router.get("/audit-receipts")
async def list_audit_receipts(
    limit: int = 50,
    current_user: User = Depends(require_role("ADMIN", "CFO", "CLO")),
    audit_session: AsyncSession = Depends(get_audit_session),
):
    result = await audit_session.execute(
        select(AuditReceiptRecord)
        .order_by(AuditReceiptRecord.created_at.desc())
        .limit(min(limit, 500))
    )
    records = result.scalars().all()
    return [
        {
            "receipt_id": r.receipt_id,
            "pipeline_id": r.pipeline_id,
            "created_at": r.created_at,
            "trigger_type": r.trigger_type,
            "final_status": r.final_status,
            "human_decision": r.human_decision,
            "sha256_hash": r.sha256_hash,
            "chain_hash": r.chain_hash,
            "is_genesis": r.is_genesis,
        }
        for r in records
    ]


@router.post("/policy/reload")
async def reload_policy(
    current_user: User = Depends(require_role("ADMIN", "CLO")),
):
    """
    Force hot-reload of the governance policy YAML.
    The Validator Agent auto-reloads on next invocation,
    but this endpoint invalidates the cache immediately.
    """
    import nexusflow.agents.validator as v
    v._POLICY_CACHE = None
    v._POLICY_MTIME = 0.0
    return {"reloaded": True, "message": "Policy cache cleared — next validation will reload from disk"}
