"""
nexusflow/core/approval_gateway.py
Approval Gateway — mandatory pre-flight gate before the L5 Executor runs.

Problem it solves
-----------------
The Executor Agent has real-world side effects (Slack messages, JIRA tickets,
downstream integrations).  Without a gate, a stale replay, a role mismatch,
or a hallucinated ``selected_option_id`` that was never offered to the human
approver could cause irreversible, incorrect actions.

The gateway runs four deterministic checks — no LLM, no network, no DB:

1. ``human_decision`` is present and not None.
2. ``selected_option_id`` (when present) resolves to a real option that
   was produced by the Recommender Agent for *this* pipeline.
3. The decision was made within the last ``DECISION_MAX_AGE_MINUTES``
   minutes — prevents stale approvals from being replayed.
4. ``approver_role`` satisfies the ``required_approver_role`` recorded by
   the Validator Agent — prevents under-privileged approvers from
   authorising high-impact decisions.

All four checks must pass to get ``GatewayResult(approved=True)``.
Any failure produces ``GatewayResult(approved=False, reason=<detail>)``
and also raises a typed exception so callers can distinguish timeout
failures from role failures in upstream error handling.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from nexusflow.core.models import PipelineState

logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

DECISION_MAX_AGE_MINUTES: int = 30
"""Decisions older than this are considered stale and rejected."""


# ── Exceptions ────────────────────────────────────────────────────────────────

class ApprovalGatewayError(Exception):
    """Base class for all approval gateway failures."""

    def __init__(self, message: str, pipeline_id: str = "") -> None:
        super().__init__(message)
        self.pipeline_id = pipeline_id


class ApprovalTimeoutError(ApprovalGatewayError):
    """
    Raised when the human decision timestamp is older than
    ``DECISION_MAX_AGE_MINUTES`` minutes.

    This prevents stale approvals — collected long before the Executor
    finally runs — from triggering side effects.
    """

    def __init__(self, decided_at: datetime, age_minutes: float, pipeline_id: str = "") -> None:
        super().__init__(
            f"Human decision is {age_minutes:.1f} minutes old "
            f"(decided_at={decided_at.isoformat()}); "
            f"maximum allowed age is {DECISION_MAX_AGE_MINUTES} minutes.",
            pipeline_id=pipeline_id,
        )
        self.decided_at = decided_at
        self.age_minutes = age_minutes


class RoleViolationError(ApprovalGatewayError):
    """
    Raised when the approver's role does not satisfy the minimum role
    required by the Validator Agent's policy result.

    This prevents a user with insufficient authority (e.g. a MEMBER)
    from approving decisions that policy mandates must be reviewed by
    a CFO or CLO.
    """

    def __init__(
        self,
        approver_role: str,
        required_role: str,
        pipeline_id: str = "",
    ) -> None:
        super().__init__(
            f"Approver role '{approver_role}' does not satisfy the required "
            f"role '{required_role}' set by the Validator Agent policy.",
            pipeline_id=pipeline_id,
        )
        self.approver_role = approver_role
        self.required_role = required_role


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class GatewayResult:
    """
    Immutable result returned by :meth:`ApprovalGateway.validate_decision`.

    Fields
    ------
    approved:
        ``True`` when all checks pass and the Executor may proceed.
        ``False`` when any check fails.
    reason:
        Human-readable explanation of the result — always populated,
        even on success, for audit log legibility.
    checked_at:
        UTC datetime when the gateway ran.
    pipeline_id:
        The pipeline this result applies to.
    """
    approved: bool
    reason: str
    checked_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    pipeline_id: str = ""


# ── Role hierarchy ────────────────────────────────────────────────────────────

# Maps each role to its authority level.  An approver satisfies a required
# role when their own level is >= the required level.
_ROLE_LEVELS: dict[str, int] = {
    "MEMBER":    10,
    "MANAGER":   20,
    "DIRECTOR":  30,
    "VP":        40,
    "CFO":       50,
    "CLO":       50,
    "CTO":       50,
    "CEO":       60,
    "ADMIN":     70,
}


def _role_satisfies(approver_role: str, required_role: str) -> bool:
    """
    Return ``True`` when *approver_role* has authority >= *required_role*.

    Unknown roles are treated as level 0 (insufficient for everything).
    """
    approver_level = _ROLE_LEVELS.get(approver_role.upper(), 0)
    required_level = _ROLE_LEVELS.get(required_role.upper(), 0)
    return approver_level >= required_level


# ── Gateway ───────────────────────────────────────────────────────────────────

class ApprovalGateway:
    """
    Runs pre-flight validation checks on a ``PipelineState`` immediately
    before the L5 Executor Agent is allowed to run.

    Usage
    -----
    .. code-block:: python

        gateway = ApprovalGateway()
        result = gateway.validate_decision(state)
        if not result.approved:
            state.status = PipelineStatus.HALTED_COMPLIANCE
            return state
        # … proceed with execution

    All methods are synchronous — the gateway performs zero I/O.
    """

    # ── individual checks ──────────────────────────────────────────────────

    @staticmethod
    def _check_decision_present(state: PipelineState) -> str | None:
        """
        Check 1 — human decision exists.

        Returns an error string on failure, ``None`` on pass.
        """
        if state.human_decision is None:
            return "No human decision is attached to this pipeline state."
        return None

    @staticmethod
    def _check_option_valid(state: PipelineState) -> str | None:
        """
        Check 2 — ``selected_option_id`` resolves to a known option.

        When the approver selects an option, its ``option_id`` must appear
        in ``recommendation.options`` for *this* pipeline.  An unrecognised
        ID is a strong signal of a hallucinated or tampered payload.

        If no option was selected (``selected_option_id`` is ``None``) the
        check is skipped — some outcomes (REJECTED, DEFERRED) need no
        selection.

        Returns an error string on failure, ``None`` on pass.
        """
        decision = state.human_decision
        if decision is None or decision.selected_option_id is None:
            return None  # nothing to validate

        if state.recommendation is None:
            return (
                f"selected_option_id='{decision.selected_option_id}' was provided "
                "but no RecommendationPackage exists on the pipeline state."
            )

        known_ids = {opt.option_id for opt in state.recommendation.options}
        if decision.selected_option_id not in known_ids:
            return (
                f"selected_option_id='{decision.selected_option_id}' is not among "
                f"the known option IDs for this pipeline: {sorted(known_ids)}.  "
                "Possible hallucination or replay attack — execution blocked."
            )
        return None

    @staticmethod
    def _check_decision_freshness(
        state: PipelineState,
    ) -> tuple[str | None, ApprovalTimeoutError | None]:
        """
        Check 3 — decision is not stale.

        Computes the age of ``human_decision.decided_at`` relative to now.
        Returns ``(error_str, exception)`` — both are ``None`` on pass.
        """
        decision = state.human_decision
        if decision is None:
            return None, None

        decided_at = decision.decided_at
        if decided_at.tzinfo is None:
            decided_at = decided_at.replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)
        age = now - decided_at
        age_minutes = age.total_seconds() / 60

        if age_minutes > DECISION_MAX_AGE_MINUTES:
            exc = ApprovalTimeoutError(
                decided_at=decided_at,
                age_minutes=age_minutes,
                pipeline_id=state.pipeline_id,
            )
            return str(exc), exc

        return None, None

    @staticmethod
    def _check_role(
        state: PipelineState,
    ) -> tuple[str | None, RoleViolationError | None]:
        """
        Check 4 — approver role satisfies policy requirements.

        Reads ``required_approver_role`` from ``state.validation``.
        When no validation result exists the check is skipped (the
        Validator may not have been able to assign a required role, which
        is a separate pipeline concern).

        Returns ``(error_str, exception)`` — both are ``None`` on pass.
        """
        decision = state.human_decision
        if decision is None or state.validation is None:
            return None, None

        required_role = state.validation.required_approver_role
        approver_role = decision.approver_role

        if not _role_satisfies(approver_role, required_role):
            exc = RoleViolationError(
                approver_role=approver_role,
                required_role=required_role,
                pipeline_id=state.pipeline_id,
            )
            return str(exc), exc

        return None, None

    # ── public API ─────────────────────────────────────────────────────────

    def validate_decision(self, state: PipelineState) -> GatewayResult:
        """
        Run all four pre-flight checks against ``state``.

        Checks run in dependency order:

        1. Decision present  (prerequisite for all subsequent checks)
        2. Option ID valid   (anti-hallucination / anti-replay)
        3. Decision fresh    (anti-stale-replay; raises ``ApprovalTimeoutError``)
        4. Role sufficient   (authority enforcement; raises ``RoleViolationError``)

        The first failing check short-circuits the remaining ones.
        The raised exception is re-raised *after* the ``GatewayResult`` is
        assembled so callers that only check ``result.approved`` still work,
        while callers that want typed error handling can catch the exception.

        Parameters
        ----------
        state:
            The ``PipelineState`` to validate.  Must have ``human_decision``
            populated before this method is called.

        Returns
        -------
        GatewayResult
            ``approved=True`` when all checks pass.
            ``approved=False`` with a descriptive ``reason`` on any failure.

        Raises
        ------
        ApprovalTimeoutError
            When the decision age exceeds ``DECISION_MAX_AGE_MINUTES``.
        RoleViolationError
            When the approver's role is insufficient for the required role.
        ApprovalGatewayError
            For all other validation failures (no decision, bad option ID).
        """
        pipeline_id = state.pipeline_id
        pending_exception: ApprovalGatewayError | None = None

        # ── Check 1: decision present ──────────────────────────────────────
        err = self._check_decision_present(state)
        if err:
            result = GatewayResult(
                approved=False,
                reason=err,
                pipeline_id=pipeline_id,
            )
            self._log(result)
            raise ApprovalGatewayError(err, pipeline_id=pipeline_id)

        # ── Check 2: option ID valid ───────────────────────────────────────
        err = self._check_option_valid(state)
        if err:
            result = GatewayResult(
                approved=False,
                reason=err,
                pipeline_id=pipeline_id,
            )
            self._log(result)
            raise ApprovalGatewayError(err, pipeline_id=pipeline_id)

        # ── Check 3: decision freshness ────────────────────────────────────
        err, timeout_exc = self._check_decision_freshness(state)
        if err:
            result = GatewayResult(
                approved=False,
                reason=err,
                pipeline_id=pipeline_id,
            )
            self._log(result)
            raise timeout_exc  # type: ignore[misc]

        # ── Check 4: role sufficient ───────────────────────────────────────
        err, role_exc = self._check_role(state)
        if err:
            result = GatewayResult(
                approved=False,
                reason=err,
                pipeline_id=pipeline_id,
            )
            self._log(result)
            raise role_exc  # type: ignore[misc]

        # ── All checks passed ──────────────────────────────────────────────
        decision = state.human_decision  # guaranteed non-None at this point
        reason = (
            f"All checks passed. "
            f"Approver: {decision.approver_role} ({decision.approver_id}). "
            f"Outcome: {decision.outcome}."
        )
        result = GatewayResult(
            approved=True,
            reason=reason,
            pipeline_id=pipeline_id,
        )
        self._log(result)
        return result

    # ── logging helper ─────────────────────────────────────────────────────

    @staticmethod
    def _log(result: GatewayResult) -> None:
        """Emit a structured log line with the [APPROVAL-GATEWAY] prefix."""
        if result.approved:
            logger.info(
                "[APPROVAL-GATEWAY] APPROVED pipeline=%s | %s",
                result.pipeline_id,
                result.reason,
            )
        else:
            logger.warning(
                "[APPROVAL-GATEWAY] BLOCKED pipeline=%s | %s",
                result.pipeline_id,
                result.reason,
            )
