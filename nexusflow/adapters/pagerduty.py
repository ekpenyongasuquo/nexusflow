"""
nexusflow/adapters/pagerduty.py
PagerDuty MCP Adapter — L1 Collector Agent tool.
Pure httpx, no PagerDuty SDK dependency.
Handles offset/limit pagination, 429 rate limiting, and 401 auth failure.
API reference: https://developer.pagerduty.com/api-reference/
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from nexusflow.core.models import PagerDutyAlert, PagerDutyIncident
from nexusflow.core.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

PAGERDUTY_API = "https://api.pagerduty.com"
PAGE_SIZE = 100  # maximum allowed by PagerDuty REST API v2


class CollectorError(Exception):
    """Raised when a Collector Agent fails to retrieve data."""

    def __init__(self, message: str, retry_after: int = 60) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class PagerDutyAdapter:
    """
    Fetches incidents and alerts from PagerDuty REST API v2.
    Returns typed PagerDutyIncident / PagerDutyAlert objects —
    no raw dicts escape this adapter.

    Authentication uses the ``Token token=<API_KEY>`` scheme required
    by the PagerDuty REST API v2.  The key is read from
    ``settings.pagerduty_api_key`` (env var ``PAGERDUTY_API_KEY``) unless
    overridden via the constructor.
    """

    def __init__(self, api_key: str | None = None) -> None:
        self._api_key = api_key or settings.pagerduty_api_key
        self._headers = {
            "Authorization": f"Token token={self._api_key}",
            "Accept": "application/vnd.pagerduty+json;version=2",
            "Content-Type": "application/json",
        }

    # ── helpers ────────────────────────────────────────────────────────────────

    def _check_auth(self) -> None:
        """Raise CollectorError immediately if the API key is absent."""
        if not self._api_key:
            raise CollectorError(
                "PAGERDUTY_API_KEY not configured", retry_after=0
            )

    @staticmethod
    def _parse_dt(value: str) -> datetime:
        """Parse an ISO-8601 timestamp returned by PagerDuty into a UTC datetime."""
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    def _raise_for_status(self, resp: httpx.Response, context: str) -> None:
        """
        Translate HTTP error codes into CollectorError with useful messages.
        Must be called *before* resp.raise_for_status().
        """
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            raise CollectorError(
                f"PagerDuty rate limit hit ({context})",
                retry_after=retry_after,
            )
        if resp.status_code == 401:
            raise CollectorError(
                "PagerDuty authentication failed — check PAGERDUTY_API_KEY",
                retry_after=0,
            )
        resp.raise_for_status()

    # ── public methods ─────────────────────────────────────────────────────────

    async def fetch_incidents(
        self,
        lookback_hours: int = 72,
        statuses: list[str] | None = None,
    ) -> list[PagerDutyIncident]:
        """
        Fetch all incidents matching ``statuses`` created within the last
        ``lookback_hours``.

        Handles offset/limit pagination automatically; each page requests
        up to ``PAGE_SIZE`` (100) records, which is the maximum permitted
        by the PagerDuty REST API v2.

        Parameters
        ----------
        lookback_hours:
            How far back in time to search.  Defaults to 72 hours.
        statuses:
            List of incident statuses to include.  Defaults to
            ``["triggered", "acknowledged"]``.

        Returns
        -------
        list[PagerDutyIncident]
            Typed incident objects, ordered oldest-first as returned by the API.

        Raises
        ------
        CollectorError
            On missing API key, 401 auth failure, or 429 rate limit.
        """
        self._check_auth()

        if statuses is None:
            statuses = ["triggered", "acknowledged"]

        since = (
            datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        ).isoformat()

        incidents: list[PagerDutyIncident] = []
        offset = 0

        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                params: dict[str, Any] = {
                    "statuses[]": statuses,
                    "since": since,
                    "limit": PAGE_SIZE,
                    "offset": offset,
                    "sort_by": "created_at:asc",
                }

                resp = await client.get(
                    f"{PAGERDUTY_API}/incidents",
                    headers=self._headers,
                    params=params,
                )
                self._raise_for_status(resp, "fetch_incidents")

                data = resp.json()

                for raw in data.get("incidents", []):
                    assignees = [
                        a["assignee"]["summary"]
                        for a in raw.get("assignments", [])
                        if a.get("assignee")
                    ]
                    incidents.append(
                        PagerDutyIncident(
                            id=raw["id"],
                            title=raw["title"],
                            status=raw["status"],
                            urgency=raw["urgency"],
                            created_at=self._parse_dt(raw["created_at"]),
                            service_name=raw["service"]["summary"],
                            assigned_to=assignees,
                            html_url=raw["html_url"],
                        )
                    )

                # Pagination — PagerDuty uses offset/limit with a `more` flag
                if not data.get("more", False):
                    break
                offset += PAGE_SIZE

        logger.info(
            "PagerDutyAdapter: fetched %d incidents (last %dh, statuses=%s)",
            len(incidents),
            lookback_hours,
            statuses,
        )
        return incidents

    async def fetch_alerts(self, incident_id: str) -> list[PagerDutyAlert]:
        """
        Fetch all alerts belonging to a single incident.

        Parameters
        ----------
        incident_id:
            The PagerDuty incident ID (e.g. ``"P1ABCDE"``).

        Returns
        -------
        list[PagerDutyAlert]
            Typed alert objects for the given incident.

        Raises
        ------
        CollectorError
            On missing API key, 401 auth failure, or 429 rate limit.
        """
        self._check_auth()

        alerts: list[PagerDutyAlert] = []
        offset = 0

        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                params: dict[str, Any] = {
                    "limit": PAGE_SIZE,
                    "offset": offset,
                }

                resp = await client.get(
                    f"{PAGERDUTY_API}/incidents/{incident_id}/alerts",
                    headers=self._headers,
                    params=params,
                )
                self._raise_for_status(resp, f"fetch_alerts incident={incident_id}")

                data = resp.json()

                for raw in data.get("alerts", []):
                    body: dict[str, Any] = raw.get("body", {}) or {}
                    alerts.append(
                        PagerDutyAlert(
                            id=raw["id"],
                            summary=raw["summary"],
                            severity=raw.get("severity", "unknown"),
                            created_at=self._parse_dt(raw["created_at"]),
                            body_details=body.get("details", {}),
                        )
                    )

                if not data.get("more", False):
                    break
                offset += PAGE_SIZE

        logger.info(
            "PagerDutyAdapter: fetched %d alerts for incident %s",
            len(alerts),
            incident_id,
        )
        return alerts
