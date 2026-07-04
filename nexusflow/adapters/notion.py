"""
nexusflow/adapters/notion.py
Notion MCP Adapter — L1 Collector Agent tool.
Pure httpx, no Notion SDK dependency.
Handles has_more/next_cursor pagination, 429 rate limiting, and 401 auth failure.
API reference: https://developers.notion.com/reference/intro
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from nexusflow.core.models import NotionComment, NotionPage
from nexusflow.core.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"  # required Notion-Version header


class CollectorError(Exception):
    """Raised when a Collector Agent fails to retrieve data."""

    def __init__(self, message: str, retry_after: int = 60) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class NotionAdapter:
    """
    Fetches pages and comments from the Notion REST API v1.
    Returns typed ``NotionPage`` / ``NotionComment`` objects —
    no raw dicts escape this adapter.

    Authentication uses the ``Authorization: Bearer <SECRET>`` header
    required by the Notion API.  The secret is read from
    ``settings.notion_secret`` (env var ``NOTION_SECRET``) unless
    overridden via the constructor.

    The default ``database_id`` for ``fetch_pages`` falls back to
    ``settings.notion_database_id`` (env var ``NOTION_DATABASE_ID``) when not
    supplied per-call.
    """

    def __init__(self, secret: str | None = None) -> None:
        self._secret = secret or settings.notion_secret
        self._headers = {
            "Authorization": f"Bearer {self._secret}",
            "Notion-Version": NOTION_VERSION,
            "Content-Type": "application/json",
        }

    # ── helpers ────────────────────────────────────────────────────────────────

    def _check_auth(self) -> None:
        """Raise CollectorError immediately if the integration secret is absent."""
        if not self._secret:
            raise CollectorError(
                "NOTION_SECRET not configured", retry_after=0
            )

    @staticmethod
    def _parse_dt(value: str) -> datetime:
        """Parse an ISO-8601 timestamp returned by Notion into a UTC datetime."""
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    def _raise_for_status(self, resp: httpx.Response, context: str) -> None:
        """
        Translate HTTP error codes into CollectorError with useful messages.
        Must be called *before* resp.raise_for_status().
        """
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            raise CollectorError(
                f"Notion rate limit hit ({context})",
                retry_after=retry_after,
            )
        if resp.status_code == 401:
            raise CollectorError(
                "Notion authentication failed — check NOTION_SECRET",
                retry_after=0,
            )
        resp.raise_for_status()

    @staticmethod
    def _extract_title(properties: dict[str, Any]) -> str:
        """
        Walk the properties dict and return the plain-text value of the first
        ``title``-type property, falling back to an empty string.

        Notion pages always have exactly one title property, but its key name
        is user-defined (e.g. ``"Name"``, ``"Title"``, ``"Page"``).
        """
        for prop in properties.values():
            if prop.get("type") == "title":
                parts = prop.get("title") or []
                return "".join(p.get("plain_text", "") for p in parts)
        return ""

    @staticmethod
    def _extract_status(properties: dict[str, Any]) -> str | None:
        """
        Return the plain-text value of the first ``status`` or ``select``
        property found, or ``None`` when neither type is present.
        """
        for prop in properties.values():
            ptype = prop.get("type")
            if ptype == "status":
                node = prop.get("status") or {}
                return node.get("name")
            if ptype == "select":
                node = prop.get("select") or {}
                return node.get("name")
        return None

    @staticmethod
    def _extract_assignee(properties: dict[str, Any]) -> str | None:
        """
        Return the name of the first person in the first ``people``-type
        property found, or ``None`` when unassigned.
        """
        for prop in properties.values():
            if prop.get("type") == "people":
                people = prop.get("people") or []
                if people:
                    person = people[0]
                    return (
                        person.get("name")
                        or (person.get("person") or {}).get("email")
                    )
        return None

    # ── public methods ─────────────────────────────────────────────────────────

    async def fetch_pages(
        self,
        database_id: str,
        lookback_hours: int = 72,
        filter_property: str | None = None,
    ) -> list[NotionPage]:
        """
        Query a Notion database and return pages last edited within the last
        ``lookback_hours``.

        Uses ``POST /v1/databases/{database_id}/query`` with
        ``has_more`` / ``next_cursor`` pagination.  Each request fetches the
        maximum of 100 pages permitted by the Notion API.

        Parameters
        ----------
        database_id:
            The Notion database UUID (with or without hyphens).
        lookback_hours:
            How far back to filter by ``last_edited_time``.
            Defaults to 72 hours.
        filter_property:
            Optional name of a ``status`` or ``select`` property to include
            in a compound filter.  When ``None`` only the time-range filter
            is applied.

        Returns
        -------
        list[NotionPage]
            Typed page objects whose ``updated_at`` falls within the lookback
            window, ordered as returned by the API.

        Raises
        ------
        CollectorError
            On missing secret, 401 auth failure, or 429 rate limit.
        """
        self._check_auth()

        since: str = (
            datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        ).isoformat()

        # Build the Notion filter — always include the time-range condition
        time_filter: dict[str, Any] = {
            "timestamp": "last_edited_time",
            "last_edited_time": {"on_or_after": since},
        }

        if filter_property:
            body: dict[str, Any] = {
                "filter": {
                    "and": [
                        time_filter,
                        {
                            "property": filter_property,
                            "status": {"is_not_empty": True},
                        },
                    ]
                },
                "page_size": 100,
            }
        else:
            body = {"filter": time_filter, "page_size": 100}

        pages: list[NotionPage] = []

        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                resp = await client.post(
                    f"{NOTION_API}/databases/{database_id}/query",
                    headers=self._headers,
                    json=body,
                )
                self._raise_for_status(resp, "fetch_pages")

                data: dict[str, Any] = resp.json()

                for raw in data.get("results", []):
                    props: dict[str, Any] = raw.get("properties") or {}
                    pages.append(
                        NotionPage(
                            id=raw["id"],
                            title=self._extract_title(props),
                            status=self._extract_status(props),
                            assignee=self._extract_assignee(props),
                            created_at=self._parse_dt(raw["created_time"]),
                            updated_at=self._parse_dt(raw["last_edited_time"]),
                            url=raw["url"],
                            properties=props,
                        )
                    )

                # Pagination — Notion returns has_more + next_cursor
                if data.get("has_more") and data.get("next_cursor"):
                    body["start_cursor"] = data["next_cursor"]
                else:
                    break

        logger.info(
            "NotionAdapter: fetched %d pages (db=%s, last %dh, filter_property=%s)",
            len(pages),
            database_id,
            lookback_hours,
            filter_property,
        )
        return pages

    async def fetch_comments(
        self,
        page_id: str,
    ) -> list[NotionComment]:
        """
        Fetch all comments attached to a Notion page or block.

        Uses ``GET /v1/comments?block_id={page_id}`` with
        ``has_more`` / ``next_cursor`` pagination.

        Parameters
        ----------
        page_id:
            The Notion page or block UUID whose comments to retrieve.

        Returns
        -------
        list[NotionComment]
            Typed comment objects ordered oldest-first as returned by the API.

        Raises
        ------
        CollectorError
            On missing secret, 401 auth failure, or 429 rate limit.
        """
        self._check_auth()

        params: dict[str, Any] = {"block_id": page_id}
        comments: list[NotionComment] = []

        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                resp = await client.get(
                    f"{NOTION_API}/comments",
                    headers=self._headers,
                    params=params,
                )
                self._raise_for_status(resp, f"fetch_comments page={page_id}")

                data: dict[str, Any] = resp.json()

                for raw in data.get("results", []):
                    # rich_text is a list of text nodes; join their plain_text
                    rich_text = raw.get("rich_text") or []
                    text = "".join(
                        node.get("plain_text", "") for node in rich_text
                    )

                    created_by = raw.get("created_by") or {}
                    author = (
                        created_by.get("name")
                        or created_by.get("id", "unknown")
                    )

                    comments.append(
                        NotionComment(
                            id=raw["id"],
                            text=text,
                            author=author,
                            created_at=self._parse_dt(raw["created_time"]),
                        )
                    )

                # Pagination — follow next_cursor when has_more is True
                if data.get("has_more") and data.get("next_cursor"):
                    params["start_cursor"] = data["next_cursor"]
                else:
                    break

        logger.info(
            "NotionAdapter: fetched %d comments for page %s",
            len(comments),
            page_id,
        )
        return comments
