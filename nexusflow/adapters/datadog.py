"""
nexusflow/adapters/datadog.py
Datadog MCP Adapter — L1 Collector Agent tool.
Pure httpx, no Datadog SDK dependency.
Handles page/page_size pagination, 429 rate limiting, and 403 auth failure.
API reference: https://docs.datadoghq.com/api/latest/
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from nexusflow.core.models import DatadogEvent, DatadogMonitor
from nexusflow.core.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

DATADOG_API = "https://api.datadoghq.com"
PAGE_SIZE = 100  # records per page for monitors endpoint


class CollectorError(Exception):
    """Raised when a Collector Agent fails to retrieve data."""

    def __init__(self, message: str, retry_after: int = 60) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class DatadogAdapter:
    """
    Fetches monitors and events from the Datadog REST API (v1/v2).
    Returns typed ``DatadogMonitor`` / ``DatadogEvent`` objects —
    no raw dicts escape this adapter.

    Authentication uses two headers required by the Datadog API:
    ``DD-API-KEY`` and ``DD-APPLICATION-KEY``.  Both are read from
    ``settings.datadog_api_key`` / ``settings.datadog_app_key``
    (env vars ``DATADOG_API_KEY`` / ``DATADOG_APP_KEY``) unless
    overridden via the constructor.
    """

    def __init__(
        self,
        api_key: str | None = None,
        app_key: str | None = None,
    ) -> None:
        self._api_key = api_key or settings.datadog_api_key
        self._app_key = app_key or settings.datadog_app_key
        self._headers = {
            "DD-API-KEY": self._api_key,
            "DD-APPLICATION-KEY": self._app_key,
            "Content-Type": "application/json",
        }

    # ── helpers ────────────────────────────────────────────────────────────────

    def _check_auth(self) -> None:
        """Raise CollectorError immediately if either credential is absent."""
        if not self._api_key:
            raise CollectorError(
                "DATADOG_API_KEY not configured", retry_after=0
            )
        if not self._app_key:
            raise CollectorError(
                "DATADOG_APP_KEY not configured", retry_after=0
            )

    @staticmethod
    def _parse_dt(value: str | int | float) -> datetime:
        """
        Parse a Datadog timestamp into a UTC datetime.

        Datadog returns ISO-8601 strings for monitors and Unix epoch
        integers/floats for events.
        """
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, tz=timezone.utc)
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))

    def _raise_for_status(self, resp: httpx.Response, context: str) -> None:
        """
        Translate HTTP error codes into CollectorError with useful messages.
        Must be called *before* resp.raise_for_status().
        """
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            raise CollectorError(
                f"Datadog rate limit hit ({context})",
                retry_after=retry_after,
            )
        if resp.status_code == 403:
            raise CollectorError(
                "Datadog authentication failed — check DATADOG_API_KEY and DATADOG_APP_KEY",
                retry_after=0,
            )
        resp.raise_for_status()

    # ── public methods ─────────────────────────────────────────────────────────

    async def fetch_monitors(
        self,
        lookback_hours: int = 72,
        tags: list[str] | None = None,
    ) -> list[DatadogMonitor]:
        """
        Fetch all monitors (optionally filtered by tags) whose state was last
        updated within the last ``lookback_hours``.

        Uses the Datadog v1 ``GET /api/v1/monitor`` endpoint.  Handles
        ``page`` / ``page_size`` pagination automatically; each page requests
        up to ``PAGE_SIZE`` (100) records.

        Parameters
        ----------
        lookback_hours:
            How far back in time to filter by ``modified`` timestamp.
            Defaults to 72 hours.
        tags:
            Optional list of tag strings (``"env:prod"``, ``"team:sre"``) to
            pass to the ``monitor_tags`` query parameter.  When ``None`` all
            monitors are returned.

        Returns
        -------
        list[DatadogMonitor]
            Typed monitor objects whose ``updated_at`` falls within the
            lookback window, ordered as returned by the API.

        Raises
        ------
        CollectorError
            On missing credentials, 403 auth failure, or 429 rate limit.
        """
        self._check_auth()

        since: datetime = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        monitors: list[DatadogMonitor] = []
        page = 0

        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                params: dict[str, Any] = {
                    "page": page,
                    "page_size": PAGE_SIZE,
                }
                if tags:
                    # Datadog accepts repeated monitor_tags[] values
                    params["monitor_tags"] = ",".join(tags)

                resp = await client.get(
                    f"{DATADOG_API}/api/v1/monitor",
                    headers=self._headers,
                    params=params,
                )
                self._raise_for_status(resp, "fetch_monitors")

                raw_list: list[dict[str, Any]] = resp.json()

                for raw in raw_list:
                    updated_at = self._parse_dt(raw["modified"])
                    # Client-side time filter: only keep monitors active in window
                    if updated_at < since:
                        continue
                    monitors.append(
                        DatadogMonitor(
                            id=raw["id"],
                            name=raw["name"],
                            status=raw.get("overall_state", "unknown"),
                            type=raw["type"],
                            message=raw.get("message", ""),
                            created_at=self._parse_dt(raw["created"]),
                            updated_at=updated_at,
                            tags=raw.get("tags") or [],
                            query=raw.get("query", ""),
                        )
                    )

                # Pagination — stop when the page returned fewer records than requested
                if len(raw_list) < PAGE_SIZE:
                    break
                page += 1

        logger.info(
            "DatadogAdapter: fetched %d monitors (last %dh, tags=%s)",
            len(monitors),
            lookback_hours,
            tags,
        )
        return monitors

    async def fetch_events(
        self,
        lookback_hours: int = 24,
        priority: str = "normal",
        tags: list[str] | None = None,
    ) -> list[DatadogEvent]:
        """
        Fetch events from the Datadog v1 events stream.

        Uses the Datadog v1 ``GET /api/v1/events`` endpoint with Unix epoch
        ``start`` / ``end`` time bounds.

        Parameters
        ----------
        lookback_hours:
            How far back in time to search.  Defaults to 24 hours.
        priority:
            Event priority filter — ``"normal"`` or ``"low"``.
            Defaults to ``"normal"``.
        tags:
            Optional list of tag filter strings (e.g. ``"env:prod"``).
            Multiple tags are AND-combined by Datadog.

        Returns
        -------
        list[DatadogEvent]
            Typed event objects, ordered as returned by the API.

        Raises
        ------
        CollectorError
            On missing credentials, 403 auth failure, or 429 rate limit.
        """
        self._check_auth()

        now = datetime.now(timezone.utc)
        start = int((now - timedelta(hours=lookback_hours)).timestamp())
        end = int(now.timestamp())

        params: dict[str, Any] = {
            "start": start,
            "end": end,
            "priority": priority,
        }
        if tags:
            params["tags"] = ",".join(tags)

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{DATADOG_API}/api/v1/events",
                headers=self._headers,
                params=params,
            )
            self._raise_for_status(resp, "fetch_events")

            data = resp.json()

        events: list[DatadogEvent] = []
        for raw in data.get("events", []):
            events.append(
                DatadogEvent(
                    id=str(raw["id"]),
                    title=raw.get("title", ""),
                    text=raw.get("text", ""),
                    priority=raw.get("priority", priority),
                    status=raw.get("alert_type", "info"),
                    created_at=self._parse_dt(raw["date_happened"]),
                    tags=raw.get("tags") or [],
                    host=raw.get("host", ""),
                )
            )

        logger.info(
            "DatadogAdapter: fetched %d events (last %dh, priority=%s, tags=%s)",
            len(events),
            lookback_hours,
            priority,
            tags,
        )
        return events
