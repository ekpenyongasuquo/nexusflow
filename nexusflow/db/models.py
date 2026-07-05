"""
nexusflow/db/models.py
SQLAlchemy ORM models for PostgreSQL (main) and SQLite (audit).
"""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean, DateTime, Float, ForeignKey, Index, Integer,
    String, Text, JSON, Enum as SAEnum
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


def _uuid() -> str:
    return str(uuid.uuid4())


# ── Users & Auth ──────────────────────────────────────────────────────────────

class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    hashed_password: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[str] = mapped_column(String(64), default="MEMBER")
    department: Mapped[str | None] = mapped_column(String(128), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    pipelines: Mapped[list[Pipeline]] = relationship(
        "Pipeline", back_populates="approver", foreign_keys="Pipeline.approver_id"
    )


# ── Authority Graph ───────────────────────────────────────────────────────────

class AuthorityRule(Base):
    """
    Defines who must approve decisions of a given type/value.
    e.g. budget_variance > $50K → CFO
    """
    __tablename__ = "authority_rules"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    trigger_type: Mapped[str] = mapped_column(String(64), nullable=False)
    min_value_usd: Mapped[float | None] = mapped_column(Float, nullable=True)
    max_value_usd: Mapped[float | None] = mapped_column(Float, nullable=True)
    required_role: Mapped[str] = mapped_column(String(64), nullable=False)
    fallback_role: Mapped[str | None] = mapped_column(String(64), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


# ── Pipeline ──────────────────────────────────────────────────────────────────

class Pipeline(Base):
    __tablename__ = "pipelines"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    trigger_type: Mapped[str] = mapped_column(String(64), nullable=False)
    trigger_source: Mapped[str] = mapped_column(String(255), default="")
    trigger_metadata: Mapped[dict] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String(64), default="PENDING")

    # Stage outputs stored as JSON blobs
    corpus_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    brief_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    validation_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    recommendation_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    decision_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    approver_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("users.id"), nullable=True
    )
    approver: Mapped[User | None] = relationship(
        "User", back_populates="pipelines", foreign_keys=[approver_id]
    )

    error_stage: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    # Duration in seconds — computed on completion
    duration_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)


# ── Audit Receipt (append-only SQLite) ───────────────────────────────────────

class EpisodicMemoryRecord(Base):
    """
    Persisted episodic memory episode.  Written once after each completed
    pipeline; never updated.  Lives in the audit SQLite DB alongside
    ``AuditReceiptRecord``.

    Indexed on ``trigger_type`` for fast ``recall()`` lookups and on
    ``created_at`` for chronological ordering.
    """
    __tablename__ = "episodic_memories"
    __table_args__ = (
        Index("ix_episodic_memories_trigger_type", "trigger_type"),
        Index("ix_episodic_memories_created_at", "created_at"),
    )

    memory_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    pipeline_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    trigger_type: Mapped[str] = mapped_column(String(64), nullable=False)
    # First 300 chars of DecisionBrief.context_summary — supports BM25 search
    context_summary: Mapped[str] = mapped_column(Text, nullable=False, default="")
    outcome: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    option_selected: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    confidence_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    duration_seconds: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    # Flat list: [trigger_type, *affected_systems]
    tags: Mapped[list] = mapped_column(JSON, default=list)


class AuditReceiptRecord(Base):
    """
    Immutable audit log. Written once, never updated.
    Stored in a separate SQLite DB for portability.
    """
    __tablename__ = "audit_receipts"

    receipt_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    pipeline_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    trigger_type: Mapped[str] = mapped_column(String(64), nullable=False)
    final_status: Mapped[str] = mapped_column(String(64), nullable=False)
    human_decision: Mapped[str | None] = mapped_column(String(64), nullable=True)
    actions_json: Mapped[list] = mapped_column(JSON, default=list)
    receipt_json: Mapped[dict] = mapped_column(JSON, default=dict)

    # Cryptographic integrity
    sha256_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    chain_hash: Mapped[str] = mapped_column(String(64), default="GENESIS")

    # Immutability guard — application enforces no UPDATE on this table
    is_genesis: Mapped[bool] = mapped_column(Boolean, default=False)
