"""
nexusflow/adapters/sentry.py
Sentry MCP Adapter — L1 Collector Agent tool.
Pure httpx, no Sentry SDK dependency.
Handles cursor pagination via Link response header, 429 rate limiting,
and 401 auth failure.
API reference: https://docs.sentry.io/api/
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from nexusflow.core.models import SentryEvent, SentryIssue
from nexusflow.core.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

SENTRY_API = "https://sentry.io/api/0"

# Regex that extracts the cursor value from a Sentry Link header.
# Example header value:
#   <https://sentry.io/api/0/.../?cursor=0:100:0>; rel="next"; results="true"
_NEXT_CURSOR_RE = re.compile(
    r'<[^>]+[?&]cursor=([^&>]+)[^>]*>;\s*rel="next";\s*results="true"'
)


class CollectorError(Exception):
    """Raised when a Collector Agent fails to retrieve data."""

    def __init__(self, message: str, retry_after: int = 60) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class SentryAdapter:
    """
    Fetches issues and events from the Sentry REST API.
    Returns typed ``SentryIssue`` / ``SentryEvent`` objects —
    no raw dicts escape this adapter.

    Authentication uses the ``Authorization: Bearer <AUTH_TOKEN>`` header
    required by the Sentry REST API.  The token is read from
    ``settings.sentry_auth_token`` (env var ``SENTRY_AUTH_TOKEN``) unless
    overridden via the constructor.

    The default ``organization_slug`` for ``fetch_issues`` falls back to
    ``settings.sentry_org_slug`` (env var ``SENTRY_ORG_SLUG``) when not
    supplied per-call.
    """

    def __init__(self, auth_token: str | None = None) -> None:
        self._auth_token = auth_token or settings.sentry_auth_token
        self._headers = {
            "Authorization": f"Bearer {self._auth_token}",
            "Content-Type": "application/json",
        }

    # ── helpers ────────────────────────────────────────────────────────────────

    def _check_auth(self) -> None:
        """Raise CollectorError immediately if the auth token is absent."""
        if not self._auth_token:
            raise CollectorError(
                "SENTRY_AUTH_TOKEN not configured", retry_after=0
            )

    @staticmethod
    def _parse_dt(value: str) -> datetime:
        """Parse an ISO-8601 timestamp returned by Sentry into a UTC datetime."""
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    def _raise_for_status(self, resp: httpx.Response, context: str) -> None:
        """
        Translate HTTP error codes into CollectorError with useful messages.
        Must be called *before* resp.raise_for_status().
        """
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            raise CollectorError(
                f"Sentry rate limit hit ({context})",
                retry_after=retry_after,
            )
        if resp.status_code == 401:
            raise CollectorError(
                "Sentry authentication failed — check SENTRY_AUTH_TOKEN",
                retry_after=0,
            )
        resp.raise_for_status()

    @staticmethod
    def _next_cursor(link_header: str | None) -> str | None:
        """
        Parse the ``Link`` response header and return the next-page cursor
        string, or ``None`` when there are no further pages.

        Sentry signals end-of-results with ``results="false"`` on the next
        rel, so the regex only matches when ``results="true"``.
        """
        if not link_header:
            return None
        match = _NEXT_CURSOR_RE.search(link_header)
        return match.group(1) if match else None

    # ── public methods ─────────────────────────────────────────────────────────

    async def fetch_issues(
        self,
        organization_slug: str,
        project_slug: str | None = None,
        lookback_hours: int = 72,
        status: str = "unresolved",
    ) -> list[SentryIssue]:
        """
        Fetch issues for an organization (optionally scoped to a project)
        whose ``lastSeen`` falls within the last ``lookback_hours``.

        Uses ``GET /api/0/organizations/{org}/issues/`` with cursor-based
        pagination driven by the ``Link`` response header.

        Parameters
        ----------
        organization_slug:
            The Sentry organization slug (e.g. ``"my-org"``).  Overrides
            ``settings.sentry_org_slug`` for this call.
        project_slug:
            Optional project slug to narrow results.  When ``None`` all
            projects in the organization are queried.
        lookback_hours:
            How far back in time to search by ``lastSeen``.
            Defaults to 72 hours.
        status:
            Issue status filter — ``"unresolved"``, ``"resolved"``, or
            ``"ignored"``.  Defaults to ``"unresolved"``.

        Returns
        -------
        list[SentryIssue]
            Typed issue objects ordered as returned by the API.

        Raises
        ------
        CollectorError
            On missing auth token, 401 auth failure, or 429 rate limit.
        """
        self._check_auth()

        since: str = (
            datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        ).isoformat()

        params: dict[str, Any] = {
            "query": f"is:{status}",
            "limit": 100,
            "start": since,
        }
        if project_slug:
            params["project"] = project_slug

        url = f"{SENTRY_API}/organizations/{organization_slug}/issues/"
        issues: list[SentryIssue] = []

        async with httpx.AsyncClient(timeout=30.0) as client:
            while url:
                resp = await client.get(
                    url,
                    headers=self._headers,
                    params=params,
                )
                self._raise_for_status(resp, "fetch_issues")

                for raw in resp.json():
                    assignee_raw = raw.get("assignedTo")
                    if isinstance(assignee_raw, dict):
                        assignee: str | None = assignee_raw.get("name") or assignee_raw.get("email")
                    else:
                        assignee = None

                    issues.append(
                        SentryIssue(
                            id=raw["id"],
                            title=raw["title"],
                            culprit=raw.get("culprit", ""),
                            status=raw["status"],
                            level=raw.get("level", "error"),
                            first_seen=self._parse_dt(raw["firstSeen"]),
                            last_seen=self._parse_dt(raw["lastSeen"]),
                            count=int(raw.get("count", 0)),
                            assignee=assignee,
                            project_slug=raw["project"]["slug"],
                            url=raw["permalink"],
                        )
                    )

                # Cursor pagination — follow next link until results="false"
                cursor = self._next_cursor(resp.headers.get("Link"))
                if cursor:
                    # Subsequent requests embed cursor in params; clear url params
                    params = {**params, "cursor": cursor}
                else:
                    break

        logger.info(
            "SentryAdapter: fetched %d issues (org=%s, project=%s, last %dh, status=%s)",
            len(issues),
            organization_slug,
            project_slug,
            lookback_hours,
            status,
        )
        return issues

    async def fetch_events(
        self,
        issue_id: str,
        limit: int = 10,
    ) -> list[SentryEvent]:
        """
        Fetch the most recent events belonging to a single Sentry issue.

        Uses ``GET /api/0/issues/{issue_id}/events/``.  No pagination is
        performed — the response is capped at ``limit`` records via the
        ``?limit`` query parameter.

        Parameters
        ----------
        issue_id:
            The Sentry issue ID (numeric string, e.g. ``"123456789"``).
        limit:
            Maximum number of events to return.  Defaults to 10.

        Returns
        -------
        list[SentryEvent]
            Typed event objects for the given issue, newest first.

        Raises
        ------
        CollectorError
            On missing auth token, 401 auth failure, or 429 rate limit.
        """
        self._check_auth()

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{SENTRY_API}/issues/{issue_id}/events/",
                headers=self._headers,
                params={"limit": limit, "full": "true"},
            )
            self._raise_for_status(resp, f"fetch_events issue={issue_id}")

            raw_list: list[dict[str, Any]] = resp.json()

        events: list[SentryEvent] = []
        for raw in raw_list:
            # tags arrive as [{"key": "...", "value": "..."}, ...]
            raw_tags = raw.get("tags") or []
            tag_strings: list[str] = [
                f"{t['key']}:{t['value']}" for t in raw_tags if "key" in t and "value" in t
            ]
            events.append(
                SentryEvent(
                    id=raw["eventID"],
                    message=raw.get("message", ""),
                    platform=raw.get("platform", ""),
                    timestamp=self._parse_dt(raw["dateCreated"]),
                    tags=tag_strings,
                    environment=raw.get("environment", ""),
                )
            )

        logger.info(
            "SentryAdapter: fetched %d events for issue %s",
            len(events),
            issue_id,
        )
        return events
