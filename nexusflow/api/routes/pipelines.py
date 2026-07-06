"""
nexusflow/api/routes/pipelines.py
Pipeline routes — trigger, status, approve, audit chain verification.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexusflow.api.middleware.auth import get_current_user, require_role
from nexusflow.core.models import (
    DecisionOutcome,
    HumanDecision,
    PipelineState,
    PipelineStatus,
    TriggerType,
)
from nexusflow.core.observability import MetricsCollector, PipelineMetrics
from nexusflow.core.state.pipeline import run_execute_stage, run_pipeline
from nexusflow.db.audit import verify_chain_integrity, write_audit_receipt
from nexusflow.db.models import Pipeline, User
from nexusflow.db.session import get_audit_session, get_main_session

logger = logging.getLogger(__name__)
router = APIRouter()


# ── Request / Response schemas ────────────────────────────────────────────────

class TriggerRequest(BaseModel):
    trigger_type: TriggerType = TriggerType.MANUAL
    trigger_source: str = ""
    trigger_metadata: dict = {}


class ApproveRequest(BaseModel):
    outcome: DecisionOutcome
    selected_option_id: str | None = None
    notes: str | None = None


class PipelineResponse(BaseModel):
    pipeline_id: str
    status: str
    trigger_type: str
    created_at: datetime
    duration_seconds: float | None = None
    brief_summary: str | None = None
    options_count: int = 0
    error_message: str | None = None


# ── Routes ────────────────────────────────────────────────────────────────────

@router.post("/trigger", response_model=PipelineResponse, status_code=202)
async def trigger_pipeline(
    body: TriggerRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_main_session),
):
    """
    Trigger a new NexusFlow pipeline.
    Runs asynchronously — returns pipeline_id immediately.
    Poll /pipelines/{id} for status updates.
    """
    state = PipelineState(
        trigger_type=body.trigger_type,
        trigger_source=body.trigger_source or current_user.email,
        trigger_metadata=body.trigger_metadata,
    )

    # Persist initial pipeline record
    pipeline_record = Pipeline(
        id=state.pipeline_id,
        trigger_type=str(body.trigger_type),
        trigger_source=state.trigger_source,
        trigger_metadata=body.trigger_metadata,
        status=PipelineStatus.PENDING,
    )
    session.add(pipeline_record)
    await session.flush()

    # Run pipeline in background
    background_tasks.add_task(
        _run_pipeline_background,
        state=state,
        pipeline_id=state.pipeline_id,
    )

    logger.info(
        "Pipeline %s triggered by %s (type=%s)",
        state.pipeline_id, current_user.email, body.trigger_type
    )

    return PipelineResponse(
        pipeline_id=state.pipeline_id,
        status=PipelineStatus.PENDING,
        trigger_type=str(body.trigger_type),
        created_at=state.created_at,
    )


@router.get("/{pipeline_id}", response_model=PipelineResponse)
async def get_pipeline(
    pipeline_id: str,
    current_user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_main_session),
):
    """Get current status and summary of a pipeline."""
    result = await session.execute(
        select(Pipeline).where(Pipeline.id == pipeline_id)
    )
    record = result.scalar_one_or_none()

    if not record:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    brief_summary = None
    options_count = 0

    if record.brief_json:
        brief_summary = record.brief_json.get("context_summary", "")[:200]

    if record.recommendation_json:
        options_count = len(record.recommendation_json.get("options", []))

    return PipelineResponse(
        pipeline_id=record.id,
        status=record.status,
        trigger_type=record.trigger_type,
        created_at=record.created_at,
        duration_seconds=record.duration_seconds,
        brief_summary=brief_summary,
        options_count=options_count,
        error_message=record.error_message,
    )


@router.get("/{pipeline_id}/detail")
async def get_pipeline_detail(
    pipeline_id: str,
    current_user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_main_session),
):
    """Get full pipeline detail including brief, options, and validation."""
    result = await session.execute(
        select(Pipeline).where(Pipeline.id == pipeline_id)
    )
    record = result.scalar_one_or_none()

    if not record:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    return {
        "pipeline_id": record.id,
        "status": record.status,
        "trigger_type": record.trigger_type,
        "trigger_metadata": record.trigger_metadata,
        "created_at": record.created_at,
        "duration_seconds": record.duration_seconds,
        "brief": record.brief_json,
        "validation": record.validation_json,
        "recommendation": record.recommendation_json,
        "decision": record.decision_json,
        "error_stage": record.error_stage,
        "error_message": record.error_message,
    }


@router.post("/{pipeline_id}/approve", response_model=PipelineResponse)
async def approve_pipeline(
    pipeline_id: str,
    body: ApproveRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_main_session),
    audit_session: AsyncSession = Depends(get_audit_session),
):
    """
    Human approval endpoint — the one-tap decision interface.
    Triggers the L5 Executor Agent.
    """
    result = await session.execute(
        select(Pipeline).where(Pipeline.id == pipeline_id)
    )
    record = result.scalar_one_or_none()

    if not record:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    if record.status != PipelineStatus.AWAITING_HUMAN:
        raise HTTPException(
            status_code=400,
            detail=f"Pipeline is not awaiting approval (status: {record.status})",
        )

    # Reconstruct state from DB
    state = PipelineState(
        pipeline_id=pipeline_id,
        trigger_type=record.trigger_type.replace("TriggerType.", ""),
        trigger_source=record.trigger_source,
        trigger_metadata=record.trigger_metadata or {},
        status=PipelineStatus.AWAITING_HUMAN,
    )

    # Attach stored agent outputs
    from nexusflow.core.models import DecisionBrief, RecommendationPackage, ValidationResult
    if record.brief_json:
        state.brief = DecisionBrief(**record.brief_json)
    if record.validation_json:
        state.validation = ValidationResult(**record.validation_json)
    if record.recommendation_json:
        state.recommendation = RecommendationPackage(**record.recommendation_json)

    # Attach human decision
    state.human_decision = HumanDecision(
        pipeline_id=pipeline_id,
        approver_id=current_user.id,
        approver_role=current_user.role,
        outcome=body.outcome,
        selected_option_id=body.selected_option_id,
        notes=body.notes,
    )

    # Run executor in background
    background_tasks.add_task(
        _run_execute_background,
        state=state,
        pipeline_id=pipeline_id,
        approver_id=current_user.id,
    )

    # Update DB record
    record.status = PipelineStatus.EXECUTING
    record.approver_id = current_user.id
    record.decision_json = body.model_dump()

    return PipelineResponse(
        pipeline_id=record.id,
        status=record.status,
        trigger_type=record.trigger_type.replace("TriggerType.", ""),
        created_at=record.created_at,
    )


@router.get("/audit/chain-integrity")
async def check_audit_chain(
    current_user: User = Depends(require_role("ADMIN", "CFO", "CLO")),
    audit_session: AsyncSession = Depends(get_audit_session),
):
    """Verify the cryptographic integrity of the entire audit receipt chain."""
    report = await verify_chain_integrity(audit_session)
    return report


@router.get("/")
async def list_pipelines(
    limit: int = 20,
    current_user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_main_session),
):
    """List recent pipelines."""
    result = await session.execute(
        select(Pipeline)
        .order_by(Pipeline.created_at.desc())
        .limit(min(limit, 100))
    )
    records = result.scalars().all()

    return [
        {
            "pipeline_id": r.id,
            "status": r.status,
            "trigger_type": r.trigger_type,
            "created_at": r.created_at,
            "duration_seconds": r.duration_seconds,
            "error_message": r.error_message,
        }
        for r in records
    ]


# ── Background task runners ───────────────────────────────────────────────────

async def _run_pipeline_background(state: PipelineState, pipeline_id: str):
    """Background task: run collect → recommend stages, persist result."""
    from nexusflow.db.session import MainSessionFactory

    start_time = datetime.now(timezone.utc)
    try:
        result_state = await run_pipeline(state)
        MetricsCollector.instance().record(PipelineMetrics.from_state(result_state))

        async with MainSessionFactory() as session:
            db_result = await session.execute(
                select(Pipeline).where(Pipeline.id == pipeline_id)
            )
            record = db_result.scalar_one_or_none()
            if record:
                record.status = result_state.status
                record.brief_json = result_state.brief.model_dump(mode="json") if result_state.brief else None
                record.validation_json = result_state.validation.model_dump(mode="json") if result_state.validation else None
                record.recommendation_json = result_state.recommendation.model_dump(mode="json") if result_state.recommendation else None
                record.error_stage = result_state.error_stage
                record.error_message = result_state.error_message
                duration = (datetime.now(timezone.utc) - start_time).total_seconds()
                record.duration_seconds = duration
                await session.commit()

        logger.info(
            "Pipeline %s completed stage 1–4 in %.1fs. Status: %s",
            pipeline_id, (datetime.now(timezone.utc) - start_time).total_seconds(), result_state.status
        )

    except Exception as e:
        logger.exception("Background pipeline error for %s: %s", pipeline_id, e)
        async with MainSessionFactory() as session:
            db_result = await session.execute(
                select(Pipeline).where(Pipeline.id == pipeline_id)
            )
            record = db_result.scalar_one_or_none()
            if record:
                record.status = PipelineStatus.FAILED
                record.error_stage = "BACKGROUND"
                record.error_message = str(e)
                await session.commit()


async def _run_execute_background(state: PipelineState, pipeline_id: str, approver_id: str):
    """Background task: run execute stage, write audit receipt."""
    from nexusflow.db.session import AuditSessionFactory, MainSessionFactory

    start_time = datetime.now(timezone.utc)
    try:
        result_state = await run_execute_stage(state)

        # Write audit receipt
        async with AuditSessionFactory() as audit_session:
            receipt = await write_audit_receipt(result_state, audit_session)
            await audit_session.commit()

        # Update pipeline record
        async with MainSessionFactory() as session:
            db_result = await session.execute(
                select(Pipeline).where(Pipeline.id == pipeline_id)
            )
            record = db_result.scalar_one_or_none()
            if record:
                record.status = result_state.status
                record.completed_at = datetime.now(timezone.utc)
                duration = (datetime.now(timezone.utc) - start_time).total_seconds()
                record.duration_seconds = (record.duration_seconds or 0) + duration
                await session.commit()

        logger.info(
            "Pipeline %s execute complete. Receipt: %s (SHA256: %s...)",
            pipeline_id, receipt.receipt_id, receipt.sha256_hash[:16]
        )

    except Exception as e:
        logger.exception("Execute background error for %s: %s", pipeline_id, e)
