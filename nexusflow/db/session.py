"""
nexusflow/db/session.py
Async SQLAlchemy engine and session factories.
Two databases: PostgreSQL (main) + SQLite (audit — append-only).
"""
from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from nexusflow.core.settings import get_settings
from nexusflow.db.models import Base

settings = get_settings()

# ── Main database engine (PostgreSQL in production, SQLite in dev) ─────────────
_is_sqlite = settings.database_url.startswith("sqlite")

_main_engine_kwargs = dict(
    echo=settings.app_env == "development",
)
if not _is_sqlite:
    _main_engine_kwargs["pool_pre_ping"] = True
    _main_engine_kwargs["pool_size"] = 10
    _main_engine_kwargs["max_overflow"] = 20

main_engine = create_async_engine(
    settings.database_url,
    **_main_engine_kwargs,
)

MainSessionFactory = async_sessionmaker(
    main_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

# ── Audit database engine (SQLite — always, for portability) ──────────────────
audit_engine = create_async_engine(
    settings.audit_db_url,
    echo=False,
    connect_args={"check_same_thread": False},
)

AuditSessionFactory = async_sessionmaker(
    audit_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def init_db() -> None:
    """Create all tables on startup."""
    async with main_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with audit_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_main_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency — yields a main DB session."""
    async with MainSessionFactory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def get_audit_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency — yields an audit DB session."""
    async with AuditSessionFactory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise