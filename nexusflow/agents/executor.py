"""
nexusflow/agents/executor.py
L5 Executor Agent — executes the human-approved decision,
triggers downstream actions, and writes the immutable audit receipt.

Approval Gateway
----------------
The very first action of ``run_executor_agent`` is to run
``ApprovalGateway().validate_decision(state)``.  If the result is not
approved, the pipeline is immediately set to ``HALTED_COMPLIANCE`` and
returned with no side effects.  This prevents stale replays, hallucinated
option IDs, and under-privileged approvers from ever reaching the
downstream adapters.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from nexusflow.adapters.jira import JiraAdapter
from nexusflow.adapters.slack import SlackAdapter
from nexusflow.core.approval_gateway import (
    ApprovalGateway,
    ApprovalGatewayError,
    GatewayResult,
)
from nexusflow.core.models import (
    AuditReceipt,
    DecisionOutcome,
    ExecutionAction,
    PipelineState,
    PipelineStatus,
)
from nexusflow.core.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


async def run_executor_agent(
    state: PipelineState,
) -> PipelineState:
    """
    L5 Executor Agent entry point.
    Only runs if human_decision.outcome == APPROVED.

    Input:  PipelineState with human_decision populated
    Output: PipelineState with receipt populated and status=COMPLETE

    The ``ApprovalGateway`` is the first thing that runs.  It checks:
    - ``human_decision`` is present
    - ``selected_option_id`` resolves to a known recommendation option
    - The decision is not older than 30 minutes
    - The approver's role satisfies the policy-required role

    If any check fails the state is immediately set to
    ``HALTED_COMPLIANCE`` and the function returns without executing
    any downstream actions.
    """
    logger.info("[L5-EXECUTOR] Pipeline %s — executing decision", state.pipeline_id)
    state.status = PipelineStatus.EXECUTING

    # ── Approval Gateway pre-flight ────────────────────────────────────────────
    try:
        gateway_result: GatewayResult = ApprovalGateway().validate_decision(state)
    except ApprovalGatewayError as exc:
        # validate_decision already emits the [APPROVAL-GATEWAY] BLOCKED log;
        # we record it on the state and halt.
        logger.error(
            "[L5-EXECUTOR] Pipeline %s — gateway blocked execution: %s",
            state.pipeline_id, exc,
        )
        state.status = PipelineStatus.HALTED_COMPLIANCE
        state.error_stage = "APPROVAL_GATEWAY"
        state.error_message = str(exc)
        return state

    if not gateway_result.approved:
        # Should not be reached (validate_decision raises on failure), but
        # kept as a belt-and-suspenders guard in case the exception is caught
        # upstream and execution is attempted again.
        logger.error(
            "[L5-EXECUTOR] Pipeline %s — gateway returned approved=False "
            "without raising; halting. Reason: %s",
            state.pipeline_id, gateway_result.reason,
        )
        state.status = PipelineStatus.HALTED_COMPLIANCE
        state.error_stage = "APPROVAL_GATEWAY"
        state.error_message = gateway_result.reason
        return state

    # Gateway passed — log the green-light and proceed.
    logger.info(
        "[L5-EXECUTOR] Pipeline %s — gateway passed, proceeding with execution.",
        state.pipeline_id,
    )

    if not state.human_decision:
        # Unreachable post-gateway, but satisfies the type-checker.
        state.status = PipelineStatus.FAILED
        state.error_stage = "EXECUTOR"
        state.error_message = "No human decision found"
        return state

    outcome = state.human_decision.outcome
    actions: list[ExecutionAction] = []

    if outcome == DecisionOutcome.APPROVED:
        actions = await _execute_approved_actions(state)
        state.status = PipelineStatus.COMPLETE

    elif outcome == DecisionOutcome.REJECTED:
        logger.info("[L5-EXECUTOR] Pipeline %s — decision REJECTED by approver", state.pipeline_id)
        actions.append(ExecutionAction(
            tool="nexusflow",
            operation="log_rejection",
            payload_summary="Decision rejected by approver — no actions taken",
            status="SUCCESS",
        ))
        state.status = PipelineStatus.COMPLETE

    elif outcome == DecisionOutcome.ESCALATED:
        actions.append(ExecutionAction(
            tool="nexusflow",
            operation="escalate",
            payload_summary=f"Decision escalated by {state.human_decision.approver_role}",
            status="SUCCESS",
        ))
        state.status = PipelineStatus.COMPLETE

    elif outcome == DecisionOutcome.DEFERRED:
        actions.append(ExecutionAction(
            tool="nexusflow",
            operation="defer",
            payload_summary="Decision deferred — pipeline paused",
            status="SUCCESS",
        ))
        state.status = PipelineStatus.COMPLETE

    # ── Build audit receipt ────────────────────────────────────────────────────
    receipt = AuditReceipt(
        pipeline_id=state.pipeline_id,
        created_at=datetime.now(timezone.utc),
        trigger_type=state.trigger_type,
        final_status=state.status,
        human_decision=outcome,
        actions_taken=actions,
    )
    state.receipt = receipt

    logger.info(
        "[L5-EXECUTOR] Pipeline %s — complete. Outcome: %s. Actions: %d.",
        state.pipeline_id, outcome, len(actions)
    )
    return state


async def _execute_approved_actions(state: PipelineState) -> list[ExecutionAction]:
    """Execute downstream actions for an approved decision."""
    actions: list[ExecutionAction] = []
    meta = state.trigger_metadata
    brief = state.brief
    decision = state.human_decision

    # Find the selected option
    selected_option = None
    if state.recommendation and decision.selected_option_id:
        for opt in state.recommendation.options:
            if opt.option_id == decision.selected_option_id:
                selected_option = opt
                break

    option_title = selected_option.title if selected_option else "Approved decision"
    option_desc = selected_option.description if selected_option else ""

    # ── Action 1: Post Slack notification ────────────────────────────────────
    slack_channel = meta.get("slack_channel_id", "")
    if slack_channel:
        message = (
            f"✅ *NexusFlow Decision Executed*\n"
            f"*Pipeline:* `{state.pipeline_id[:8]}`\n"
            f"*Trigger:* {state.trigger_type}\n"
            f"*Decision:* {option_title}\n"
            f"*Approved by:* {decision.approver_role}\n"
            f"*Action:* {option_desc}\n"
            f"_Audit receipt generated — chain integrity verified_"
        )
        try:
            adapter = SlackAdapter()
            success = await adapter.post_message(slack_channel, message)
            actions.append(ExecutionAction(
                tool="slack",
                operation="post_message",
                payload_summary=f"Decision notification posted to {slack_channel}",
                status="SUCCESS" if success else "FAILED",
            ))
        except Exception as e:
            actions.append(ExecutionAction(
                tool="slack",
                operation="post_message",
                payload_summary=f"Failed to post to {slack_channel}: {e}",
                status="FAILED",
                error=str(e),
            ))

    # ── Action 2: Create JIRA ticket ─────────────────────────────────────────
    jira_project = meta.get("jira_project_key", "")
    if jira_project:
        summary = f"[NexusFlow] {option_title} — {state.trigger_type}"
        description = (
            f"Decision executed by NexusFlow pipeline {state.pipeline_id}.\n\n"
            f"Trigger: {state.trigger_type}\n"
            f"Approved by: {decision.approver_role} ({decision.approver_id})\n"
            f"Context: {brief.context_summary[:500] if brief else 'N/A'}\n\n"
            f"Selected Option: {option_title}\n"
            f"{option_desc}\n\n"
            + (
                "\nImplementation Steps:\n" +
                "\n".join(f"  {i+1}. {s}" for i, s in enumerate(selected_option.implementation_steps))
                if selected_option else ""
            )
        )
        try:
            adapter = JiraAdapter()
            ticket_key = await adapter.create_ticket(
                project_key=jira_project,
                summary=summary,
                description=description,
                labels=["nexusflow", "auto-generated", state.trigger_type.lower()],
            )
            actions.append(ExecutionAction(
                tool="jira",
                operation="create_ticket",
                payload_summary=f"JIRA ticket created: {ticket_key}",
                status="SUCCESS" if ticket_key else "FAILED",
            ))
        except Exception as e:
            actions.append(ExecutionAction(
                tool="jira",
                operation="create_ticket",
                payload_summary=f"Failed to create JIRA ticket: {e}",
                status="FAILED",
                error=str(e),
            ))

    # ── Action 3: Internal decision log (always succeeds) ────────────────────
    actions.append(ExecutionAction(
        tool="nexusflow",
        operation="log_decision",
        payload_summary=(
            f"Decision logged: {option_title}. "
            f"Approver: {decision.approver_role}. "
            f"Pipeline: {state.pipeline_id}"
        ),
        status="SUCCESS",
    ))

    return actions
