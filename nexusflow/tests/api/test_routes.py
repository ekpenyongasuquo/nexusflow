"""
nexusflow/tests/api/test_routes.py
Test suite for FastAPI routes.
Uses AsyncClient with in-memory SQLite DB.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from nexusflow.api.main import app
from nexusflow.core.models import PipelineStatus, TriggerType
from nexusflow.db.session import init_db


@pytest.fixture(scope="module", autouse=True)
async def setup_db():
    """Initialise in-memory test DB once per module."""
    await init_db()


@pytest.fixture
async def client():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


@pytest.fixture
async def auth_headers(client):
    """Register and login a test user, return auth headers."""
    await client.post("/auth/register", json={
        "email": "testuser@nexusflow.dev",
        "full_name": "Test User",
        "password": "testpassword123",
        "role": "MANAGER",
    })
    resp = await client.post("/auth/login", json={
        "email": "testuser@nexusflow.dev",
        "password": "testpassword123",
    })
    token = resp.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


# ── Health ────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_health_check(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert data["service"] == "nexusflow-api"


@pytest.mark.asyncio
async def test_root(client):
    resp = await client.get("/")
    assert resp.status_code == 200
    assert "NexusFlow" in resp.json()["service"]


# ── Auth routes ───────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_register_new_user(client):
    resp = await client.post("/auth/register", json={
        "email": "newuser@nexusflow.dev",
        "full_name": "New User",
        "password": "securepass456",
        "role": "MEMBER",
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["email"] == "newuser@nexusflow.dev"
    assert data["role"] == "MEMBER"


@pytest.mark.asyncio
async def test_register_duplicate_email_returns_409(client):
    await client.post("/auth/register", json={
        "email": "duplicate@nexusflow.dev",
        "full_name": "First",
        "password": "pass123",
    })
    resp = await client.post("/auth/register", json={
        "email": "duplicate@nexusflow.dev",
        "full_name": "Second",
        "password": "pass456",
    })
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_login_valid_credentials(client):
    await client.post("/auth/register", json={
        "email": "logintest@nexusflow.dev",
        "full_name": "Login Test",
        "password": "mypassword",
    })
    resp = await client.post("/auth/login", json={
        "email": "logintest@nexusflow.dev",
        "password": "mypassword",
    })
    assert resp.status_code == 200
    assert "access_token" in resp.json()


@pytest.mark.asyncio
async def test_login_wrong_password_returns_401(client):
    await client.post("/auth/register", json={
        "email": "wrongpass@nexusflow.dev",
        "full_name": "Wrong Pass",
        "password": "correctpass",
    })
    resp = await client.post("/auth/login", json={
        "email": "wrongpass@nexusflow.dev",
        "password": "wrongpass",
    })
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_get_me_requires_auth(client):
    resp = await client.get("/auth/me")
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_get_me_returns_user_profile(client, auth_headers):
    resp = await client.get("/auth/me", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["email"] == "testuser@nexusflow.dev"
    assert data["role"] == "MANAGER"


# ── Pipeline routes ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_trigger_pipeline_requires_auth(client):
    resp = await client.post("/pipelines/trigger", json={
        "trigger_type": "MANUAL",
    })
    assert resp.status_code == 403


@pytest.mark.asyncio
@patch("nexusflow.api.routes.pipelines._run_pipeline_background", new_callable=AsyncMock)
async def test_trigger_pipeline_returns_202(mock_bg, client, auth_headers):
    resp = await client.post("/pipelines/trigger", json={
        "trigger_type": "MANUAL",
        "trigger_source": "test",
        "trigger_metadata": {},
    }, headers=auth_headers)
    assert resp.status_code == 202
    data = resp.json()
    assert "pipeline_id" in data
    assert data["status"] == "PENDING"


@pytest.mark.asyncio
@patch("nexusflow.api.routes.pipelines._run_pipeline_background", new_callable=AsyncMock)
async def test_get_pipeline_status(mock_bg, client, auth_headers):
    # Trigger first
    trigger_resp = await client.post("/pipelines/trigger", json={
        "trigger_type": "BUDGET_VARIANCE",
        "trigger_metadata": {"amount": 75000},
    }, headers=auth_headers)
    pipeline_id = trigger_resp.json()["pipeline_id"]

    # Poll status
    resp = await client.get(f"/pipelines/{pipeline_id}", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["pipeline_id"] == pipeline_id
    assert data["trigger_type"] == "BUDGET_VARIANCE"


@pytest.mark.asyncio
async def test_get_nonexistent_pipeline_returns_404(client, auth_headers):
    resp = await client.get("/pipelines/nonexistent-id-xyz", headers=auth_headers)
    assert resp.status_code == 404


@pytest.mark.asyncio
@patch("nexusflow.api.routes.pipelines._run_pipeline_background", new_callable=AsyncMock)
async def test_list_pipelines(mock_bg, client, auth_headers):
    # Trigger a pipeline
    await client.post("/pipelines/trigger", json={
        "trigger_type": "MANUAL",
    }, headers=auth_headers)

    resp = await client.get("/pipelines/", headers=auth_headers)
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)
    assert len(resp.json()) >= 1
