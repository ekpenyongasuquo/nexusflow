"""
nexusflow/core/models.py
All domain entities, typed contracts between agents.
Every agent receives and returns these — no dicts, no ambiguity.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# ── Enums ─────────────────────────────────────────────────────────────────────

class PipelineStatus(str, Enum):
    PENDING = "PENDING"
    COLLECTING = "COLLECTING"
    SYNTHESISING = "SYNTHESISING"
    VALIDATING = "VALIDATING"
    RECOMMENDING = "RECOMMENDING"
    AWAITING_HUMAN = "AWAITING_HUMAN"
    EXECUTING = "EXECUTING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"
    HALTED_COMPLIANCE = "HALTED_COMPLIANCE"


class TriggerType(str, Enum):
    BUDGET_VARIANCE = "BUDGET_VARIANCE"
    PROJECT_STALL = "PROJECT_STALL"
    CUSTOMER_ESCALATION = "CUSTOMER_ESCALATION"
    COMPLIANCE_DEADLINE = "COMPLIANCE_DEADLINE"
    ANOMALY_DETECTED = "ANOMALY_DETECTED"
    MANUAL = "MANUAL"


class RiskLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class DecisionOutcome(str, Enum):
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    ESCALATED = "ESCALATED"
    DEFERRED = "DEFERRED"


# ── Source Events (from MCP Adapters) ─────────────────────────────────────────

class SlackMessage(BaseModel):
    id: str
    channel_id: str
    author: str
    timestamp: datetime
    content: str
    thread_ts: str | None = None
    has_pii: bool = False


class JiraTicket(BaseModel):
    id: str
    key: str
    summary: str
    status: str
    assignee: str | None = None
    updated: datetime
    labels: list[str] = Field(default_factory=list)
    description: str | None = None


class GitHubPR(BaseModel):
    id: int
    number: int
    title: str
    state: str
    author: str
    created_at: datetime
    updated_at: datetime
    body: str | None = None
    labels: list[str] = Field(default_factory=list)


class CollectedCorpus(BaseModel):
    """Output of L1 Collector Agents — typed corpus of all raw signals."""
    pipeline_id: str
    collected_at: datetime = Field(default_factory=datetime.utcnow)
    slack_messages: list[SlackMessage] = Field(default_factory=list)
    jira_tickets: list[JiraTicket] = Field(default_factory=list)
    github_prs: list[GitHubPR] = Field(default_factory=list)
    total_items: int = 0
    collection_errors: list[str] = Field(default_factory=list)

    def model_post_init(self, __context: Any) -> None:
        self.total_items = (
            len(self.slack_messages)
            + len(self.jira_tickets)
            + len(self.github_prs)
        )


# ── Decision Brief (from L2 Synthesiser) ─────────────────────────────────────

class RiskMatrixEntry(BaseModel):
    factor: str
    likelihood: RiskLevel
    impact: RiskLevel
    mitigation: str


class DecisionBrief(BaseModel):
    """Output of L2 Synthesiser Agent."""
    pipeline_id: str
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    context_summary: str
    causal_chain: list[str] = Field(default_factory=list)
    risk_matrix: list[RiskMatrixEntry] = Field(default_factory=list)
    affected_systems: list[str] = Field(default_factory=list)
    estimated_impact_usd: float | None = None
    confidence_score: float = Field(ge=0.0, le=1.0, default=0.0)
    source_item_count: int = 0


# ── Validation Result (from L3 Validator) ────────────────────────────────────

class PIIFinding(BaseModel):
    entity_type: str      # e.g. "EMAIL", "PERSON_NAME", "ACCOUNT_NUMBER"
    value_pseudonym: str  # pseudonymised replacement
    location: str         # which field it was found in


class PolicyViolation(BaseModel):
    rule_id: str
    rule_name: str
    severity: RiskLevel
    description: str
    remediation: str


class ValidationResult(BaseModel):
    """Output of L3 Validator Agent."""
    pipeline_id: str
    validated_at: datetime = Field(default_factory=datetime.utcnow)
    pii_findings: list[PIIFinding] = Field(default_factory=list)
    policy_violations: list[PolicyViolation] = Field(default_factory=list)
    required_approver_role: str = "MANAGER"
    required_approver_id: str | None = None
    is_cleared: bool = False
    halt_reason: str | None = None


# ── Decision Options (from L4 Recommender) ───────────────────────────────────

class DecisionOption(BaseModel):
    option_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    label: str                    # e.g. "Option A"
    title: str
    description: str
    projected_roi_usd: float | None = None
    projected_roi_percent: float | None = None
    confidence: float = Field(ge=0.0, le=1.0)
    implementation_steps: list[str] = Field(default_factory=list)
    risk_level: RiskLevel = RiskLevel.MEDIUM
    time_to_implement_days: int | None = None


class RecommendationPackage(BaseModel):
    """Output of L4 Recommender Agent."""
    pipeline_id: str
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    options: list[DecisionOption] = Field(default_factory=list)
    recommended_option_id: str | None = None
    reasoning: str = ""


# ── Human Decision (approval interface output) ────────────────────────────────

class HumanDecision(BaseModel):
    pipeline_id: str
    approver_id: str
    approver_role: str
    decided_at: datetime = Field(default_factory=datetime.utcnow)
    outcome: DecisionOutcome
    selected_option_id: str | None = None
    notes: str | None = None


# ── Execution Receipt (from L5 Executor) ─────────────────────────────────────

class ExecutionAction(BaseModel):
    action_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    tool: str           # e.g. "slack", "jira", "netsuite"
    operation: str      # e.g. "post_message", "create_ticket"
    payload_summary: str
    status: str         # "SUCCESS" | "FAILED" | "SKIPPED"
    executed_at: datetime = Field(default_factory=datetime.utcnow)
    error: str | None = None


class AuditReceipt(BaseModel):
    """Immutable audit receipt — written to append-only SQLite."""
    receipt_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_id: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    trigger_type: TriggerType
    final_status: PipelineStatus
    human_decision: DecisionOutcome | None = None
    actions_taken: list[ExecutionAction] = Field(default_factory=list)
    sha256_hash: str = ""           # computed after serialisation
    chain_hash: str = ""            # hash of previous receipt — chain integrity


# ── Pipeline State (LangGraph node) ──────────────────────────────────────────

class PipelineState(BaseModel):
    """
    The single state object passed between all LangGraph nodes.
    Each agent reads what it needs and writes its output section.
    No shared mutable state outside this object.
    """
    pipeline_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = Field(default_factory=datetime.utcnow)
    trigger_type: TriggerType = TriggerType.MANUAL
    trigger_source: str = ""
    trigger_metadata: dict[str, Any] = Field(default_factory=dict)
    status: PipelineStatus = PipelineStatus.PENDING

    # Agent outputs — populated as pipeline progresses
    corpus: CollectedCorpus | None = None
    brief: DecisionBrief | None = None
    validation: ValidationResult | None = None
    recommendation: RecommendationPackage | None = None
    human_decision: HumanDecision | None = None
    receipt: AuditReceipt | None = None

    # Error tracking
    error_stage: str | None = None
    error_message: str | None = None

    class Config:
        use_enum_values = True
