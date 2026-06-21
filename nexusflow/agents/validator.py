"""
nexusflow/agents/validator.py
L3 Validator Agent — PII scanning, policy enforcement, authority graph routing.
Hot-reloadable policy ruleset from YAML.
No external dependencies for PII scanning — pure regex + pattern matching.
"""
from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

import yaml

from nexusflow.core.models import (
    PIIFinding,
    PipelineState,
    PipelineStatus,
    PolicyViolation,
    RiskLevel,
    ValidationResult,
)
from nexusflow.core.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# ── PII Patterns ──────────────────────────────────────────────────────────────
_PII_PATTERNS: dict[str, re.Pattern] = {
    "EMAIL": re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Z|a-z]{2,}\b"),
    "PHONE_NUMBER": re.compile(r"\b(?:\+?\d{1,3}[\s\-]?)?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{4}\b"),
    "SSN": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "CREDIT_CARD": re.compile(r"\b(?:\d{4}[\s\-]?){3}\d{4}\b"),
    "IP_ADDRESS": re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"),
    "ACCOUNT_NUMBER": re.compile(r"\b[Aa]cc(?:ount)?[\s#:]*\d{6,}\b"),
}

_POLICY_CACHE: dict | None = None
_POLICY_MTIME: float = 0.0


def _load_policy() -> dict:
    """Load policy YAML with hot-reload — checks file modification time."""
    global _POLICY_CACHE, _POLICY_MTIME

    policy_path = Path(settings.policy_file_path)
    if not policy_path.exists():
        logger.warning("Policy file not found: %s — using empty policy", policy_path)
        return {}

    mtime = policy_path.stat().st_mtime
    if _POLICY_CACHE is None or mtime > _POLICY_MTIME:
        with policy_path.open() as f:
            _POLICY_CACHE = yaml.safe_load(f)
        _POLICY_MTIME = mtime
        logger.info("Policy loaded/reloaded from %s", policy_path)

    return _POLICY_CACHE or {}


def _scan_pii(text: str) -> list[PIIFinding]:
    """Scan text for PII using regex patterns. Returns findings with pseudonyms."""
    findings: list[PIIFinding] = []
    for entity_type, pattern in _PII_PATTERNS.items():
        for match in pattern.finditer(text):
            findings.append(PIIFinding(
                entity_type=entity_type,
                value_pseudonym=f"[{entity_type}_{uuid.uuid4().hex[:6].upper()}]",
                location="decision_brief",
            ))
    return findings


def _pseudonymise(text: str, findings: list[PIIFinding]) -> str:
    """Replace detected PII with pseudonyms in text."""
    result = text
    for entity_type, pattern in _PII_PATTERNS.items():
        replacement_idx = 0
        relevant = [f for f in findings if f.entity_type == entity_type]
        for match in pattern.finditer(text):
            if replacement_idx < len(relevant):
                result = result.replace(
                    match.group(), relevant[replacement_idx].value_pseudonym, 1
                )
                replacement_idx += 1
    return result


def _resolve_authority(trigger_type: str, impact_usd: float | None, policy: dict) -> tuple[str, str | None]:
    """
    Walk authority_rules in policy to find required approver role.
    Returns (required_role, fallback_role).
    """
    rules = policy.get("authority_rules", [])
    impact = impact_usd or 0.0

    for rule in rules:
        if rule.get("trigger_type") != str(trigger_type):
            continue
        min_v = rule.get("min_value_usd")
        max_v = rule.get("max_value_usd")
        if min_v is not None and impact < min_v:
            continue
        if max_v is not None and impact > max_v:
            continue
        return rule["required_role"], rule.get("fallback_role")

    return "MANAGER", None  # default


async def run_validator_agent(state: PipelineState) -> PipelineState:
    """
    L3 Validator Agent entry point.

    Input:  PipelineState with brief populated
    Output: PipelineState with validation populated
    """
    logger.info("[L3-VALIDATOR] Pipeline %s — starting validation", state.pipeline_id)
    state.status = PipelineStatus.VALIDATING

    if not state.brief:
        state.status = PipelineStatus.FAILED
        state.error_stage = "VALIDATOR"
        state.error_message = "No decision brief available for validation"
        return state

    policy = _load_policy()
    pii_findings: list[PIIFinding] = []
    violations: list[PolicyViolation] = []

    # ── PII Scan ──────────────────────────────────────────────────────────────
    scan_text = state.brief.context_summary + " " + " ".join(state.brief.causal_chain)
    pii_findings = _scan_pii(scan_text)

    if pii_findings:
        logger.warning(
            "[L3-VALIDATOR] Pipeline %s — %d PII findings. Pseudonymising.",
            state.pipeline_id, len(pii_findings)
        )
        # Pseudonymise the brief in-place
        state.brief.context_summary = _pseudonymise(
            state.brief.context_summary, pii_findings
        )
        state.brief.causal_chain = [
            _pseudonymise(step, pii_findings) for step in state.brief.causal_chain
        ]

        # Check POL-001 — if PII was found, log but don't halt (it's now pseudonymised)
        violations.append(PolicyViolation(
            rule_id="POL-001",
            rule_name="PII detected and pseudonymised",
            severity=RiskLevel.HIGH,
            description=f"{len(pii_findings)} PII entities pseudonymised before routing",
            remediation="Review source data collection to minimise PII ingestion",
        ))

    # ── Policy Rule Checks ────────────────────────────────────────────────────
    policy_rules = policy.get("policy_rules", [])
    trigger = str(state.trigger_type)
    confidence = state.brief.confidence_score

    for rule in policy_rules:
        rule_triggers = rule.get("trigger_types", [])
        if rule_triggers and trigger not in rule_triggers:
            continue

        rule_id = rule["id"]
        action = rule.get("action", "WARN")

        # POL-002: budget decisions need impact estimate
        if rule_id == "POL-002" and state.brief.estimated_impact_usd is None:
            violations.append(PolicyViolation(
                rule_id=rule_id,
                rule_name=rule["name"],
                severity=RiskLevel[rule["severity"]],
                description=rule["description"],
                remediation="Ensure LLM synthesis includes financial impact estimate",
            ))

        # POL-004: confidence threshold
        if rule_id == "POL-004":
            threshold = rule.get("confidence_threshold", 0.4)
            if confidence < threshold:
                violations.append(PolicyViolation(
                    rule_id=rule_id,
                    rule_name=rule["name"],
                    severity=RiskLevel[rule["severity"]],
                    description=f"Brief confidence {confidence:.2f} below threshold {threshold}",
                    remediation="Collect more signals or review trigger metadata",
                ))

    # ── Authority Graph Resolution ────────────────────────────────────────────
    required_role, fallback_role = _resolve_authority(
        trigger, state.brief.estimated_impact_usd, policy
    )

    # ── Build Validation Result ───────────────────────────────────────────────
    # Check if any CRITICAL violation with HALT action requires stopping
    critical_halts = [
        v for v in violations
        if v.severity == RiskLevel.CRITICAL
    ]

    is_cleared = len(critical_halts) == 0
    halt_reason = None
    if not is_cleared:
        halt_reason = "; ".join(v.description for v in critical_halts)
        state.status = PipelineStatus.HALTED_COMPLIANCE

    validation = ValidationResult(
        pipeline_id=state.pipeline_id,
        validated_at=datetime.now(timezone.utc),
        pii_findings=pii_findings,
        policy_violations=violations,
        required_approver_role=required_role,
        is_cleared=is_cleared,
        halt_reason=halt_reason,
    )

    state.validation = validation

    if not is_cleared:
        logger.error(
            "[L3-VALIDATOR] Pipeline %s — HALTED. Reason: %s",
            state.pipeline_id, halt_reason
        )
    else:
        logger.info(
            "[L3-VALIDATOR] Pipeline %s — cleared. Approver role: %s. Violations (non-critical): %d",
            state.pipeline_id, required_role, len(violations)
        )

    return state
