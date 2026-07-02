"""
nexusflow/api/main.py
FastAPI application entry point.
Registers all routers, middleware, and startup/shutdown hooks.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from nexusflow.api.routes import auth, pipelines, admin
from nexusflow.core.observability import get_metrics_summary
from nexusflow.core.settings import get_settings
from nexusflow.db.session import init_db

settings = get_settings()

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan — startup and shutdown."""
    logger.info("NexusFlow starting up (env=%s)", settings.app_env)
    await init_db()
    logger.info("Database initialised")
    yield
    logger.info("NexusFlow shutting down")


app = FastAPI(
    title="NexusFlow API",
    description="Autonomous Decision Intelligence for the Distributed Enterprise",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# ── CORS ──────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if not settings.is_production else ["https://nexusflow.dev"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(auth.router, prefix="/auth", tags=["Authentication"])
app.include_router(pipelines.router, prefix="/pipelines", tags=["Pipelines"])
app.include_router(admin.router, prefix="/admin", tags=["Admin"])


# ── Health check ──────────────────────────────────────────────────────────────
@app.get("/health", tags=["Health"])
async def health_check():
    return {
        "status": "healthy",
        "service": "nexusflow-api",
        "version": "0.1.0",
        "environment": settings.app_env,
        "ibm_bob_enabled": settings.ibm_bob_enabled,
    }


@app.get("/", tags=["Root"])
async def root():
    return {
        "service": "NexusFlow",
        "tagline": "Autonomous Decision Intelligence for the Distributed Enterprise",
        "docs": "/docs",
        "health": "/health",
    }


# ── Observability ─────────────────────────────────────────────────────────────
@app.get("/metrics", tags=["Observability"])
async def metrics():
    return get_metrics_summary()


# ── Global exception handler ──────────────────────────────────────────────────
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.exception("Unhandled exception: %s", exc)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "type": type(exc).__name__},
    )
