"""
nexusflow/adapters/confluence.py
Confluence MCP Adapter — L1 Collector Agent tool.
Pure httpx, no Atlassian SDK dependency.
Handles REST API v2 cursor pagination (_links.next), 429 rate limiting,
and 401 auth failure.
API reference: https://developer.atlassian.com/cloud/confluence/rest/v2/
"""
from __future__ import annotations

import base64
import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urljoin

import httpx

from nexusflow.core.models import ConfluenceComment, ConfluencePage
from nexusflow.core.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# Maximum results per page allowed by Confluence REST API v2
_PAGE_SIZE = 50
# Confluence REST API v2 base path suffix
_API_PATH = "/wiki/api/v2"


class CollectorError(Exception):
    """Raised when a Collector Agent fails to retrieve data."""

    def __init__(self, message: str, retry_after: int = 60) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class ConfluenceAdapter:
    """
    Fetches pages and inline comments from the Confluence Cloud REST API v2.
    Returns typed ``ConfluencePage`` / ``ConfluenceComment`` objects —
    no raw dicts escape this adapter.

    Authentication uses HTTP Basic auth encoded as ``base64(email:api_token)``,
    which is the same scheme used by the Jira adapter.  Credentials are read
    from ``settings.confluence_base_url``, ``settings.confluence_email``, and
    ``settings.confluence_api_token`` (env vars ``CONFLUENCE_BASE_URL``,
    ``CONFLUENCE_EMAIL``, ``CONFLUENCE_API_TOKEN``) unless overridden via the
    constructor.
    """

    def __init__(
        self,
        base_url: str | None = None,
        email: str | None = None,
        api_token: str | None = None,
    ) -> None:
        self._base_url = (
            base_url or settings.confluence_base_url
        ).rstrip("/")
        self._email = email or settings.confluence_email
        self._api_token = api_token or settings.confluence_api_token

        # Basic auth: base64(email:api_token) — identical to jira.py
        credentials = base64.b64encode(
            f"{self._email}:{self._api_token}".encode()
        ).decode()
        self._headers = {
            "Authorization": f"Basic {credentials}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    # ── helpers ────────────────────────────────────────────────────────────────

    def _check_auth(self) -> None:
        """Raise CollectorError immediately if any required credential is absent."""
        missing = [
            name
            for name, val in (
                ("CONFLUENCE_BASE_URL", self._base_url),
                ("CONFLUENCE_EMAIL", self._email),
                ("CONFLUENCE_API_TOKEN", self._api_token),
            )
            if not val
        ]
        if missing:
            raise CollectorError(
                f"Confluence credentials not configured: {', '.join(missing)}",
                retry_after=0,
            )

    @staticmethod
    def _parse_dt(value: str) -> datetime:
        """Parse an ISO-8601 timestamp returned by Confluence into a UTC datetime."""
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    def _api_url(self, path: str) -> str:
        """Build a full Confluence REST API v2 URL from a relative ``path``."""
        return f"{self._base_url}{_API_PATH}{path}"

    def _raise_for_status(self, resp: httpx.Response, context: str) -> None:
        """
        Translate HTTP error codes into CollectorError with useful messages.
        Must be called *before* ``resp.raise_for_status()``.
        """
        if resp.status_code == 401:
            raise CollectorError(
                "Confluence authentication failed — check CONFLUENCE_EMAIL "
                "and CONFLUENCE_API_TOKEN",
                retry_after=0,
            )
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            raise CollectorError(
                f"Confluence rate limit hit ({context})",
                retry_after=retry_after,
            )
        resp.raise_for_status()

    @staticmethod
    def _extract_excerpt(body: dict[str, Any] | None) -> str:
        """
        Return the first 500 characters of plain-text body content.

        Confluence REST API v2 returns body under ``storage.value`` (HTML/XHTML)
        or ``atlas_doc_format.value`` (ADF JSON).  We do a lightweight strip of
        common HTML tags to produce readable plain text without an HTML parser
        dependency.
        """
        if not body:
            return ""

        # Prefer the plain 'atlas_doc_format' representation if present,
        # otherwise fall back to 'storage' (HTML).
        raw: str = ""
        if body.get("atlas_doc_format", {}).get("value"):
            raw = body["atlas_doc_format"]["value"]
        elif body.get("storage", {}).get("value"):
            raw = body["storage"]["value"]

        # Strip the most common HTML/XML tags with a minimal inline regex-free
        # approach (no external dependency required).
        import re  # noqa: PLC0415 — imported here to keep module-level imports minimal

        text = re.sub(r"<[^>]+>", " ", raw)
        text = re.sub(r"\s+", " ", text).strip()
        return text[:500]

    # ── public methods ─────────────────────────────────────────────────────────

    async def fetch_pages(
        self,
        space_key: str,
        lookback_hours: int = 72,
        limit: int = 50,
    ) -> list[ConfluencePage]:
        """
        Fetch pages from ``space_key`` updated within the last ``lookback_hours``.

        Uses Confluence REST API v2 cursor pagination via ``_links.next``
        automatically.  Each page requests up to ``limit`` records (capped at
        ``_PAGE_SIZE``).

        Parameters
        ----------
        space_key:
            The Confluence space key (e.g. ``"PROJ"`` or ``"~username"``).
        lookback_hours:
            How far back to search based on ``lastModifiedDate``.
            Defaults to 72 hours.
        limit:
            Records per API page.  Values above ``_PAGE_SIZE`` are clamped.
            Defaults to 50.

        Returns
        -------
        list[ConfluencePage]
            Typed page objects, ordered as returned by the API.

        Raises
        ------
        CollectorError
            On missing credentials, 401 auth failure, or 429 rate limit.
        """
        self._check_auth()

        page_limit = min(limit, _PAGE_SIZE)
        since: str = (
            datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        ).strftime("%Y-%m-%dT%H:%M:%S.000Z")

        pages: list[ConfluencePage] = []

        # Start URL — REST API v2 /pages endpoint filtered by space key
        next_url: str | None = self._api_url(
            f"/pages?spaceKey={space_key}"
            f"&limit={page_limit}"
            f"&sort=-modified-date"
            f"&body-format=storage"
        )

        async with httpx.AsyncClient(timeout=30.0) as client:
            while next_url:
                resp = await client.get(next_url, headers=self._headers)
                self._raise_for_status(resp, f"fetch_pages space={space_key}")

                data: dict[str, Any] = resp.json()
                results: list[dict[str, Any]] = data.get("results", [])

                done = False
                for raw in results:
                    version = raw.get("version", {})
                    updated_str: str = version.get("createdAt", "")
                    created_str: str = raw.get("createdAt", updated_str)

                    # Stop paging once results fall outside the lookback window
                    if updated_str and self._parse_dt(updated_str) < (
                        datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
                    ):
                        done = True
                        break

                    author_obj = version.get("authorId") or {}
                    # v2 API returns authorId as a plain string, not an object
                    author_str = (
                        author_obj
                        if isinstance(author_obj, str)
                        else author_obj.get("displayName", "unknown")
                    )

                    pages.append(
                        ConfluencePage(
                            id=str(raw["id"]),
                            title=raw.get("title", ""),
                            space_key=space_key,
                            created_at=self._parse_dt(created_str) if created_str else datetime.now(timezone.utc),
                            updated_at=self._parse_dt(updated_str) if updated_str else datetime.now(timezone.utc),
                            author=author_str or "unknown",
                            url=(
                                f"{self._base_url}/wiki"
                                + raw.get("_links", {}).get("webui", "")
                            ),
                            excerpt=self._extract_excerpt(raw.get("body")),
                        )
                    )

                if done:
                    break

                # Cursor pagination: follow _links.next if present
                links: dict[str, str] = data.get("_links", {})
                raw_next: str | None = links.get("next")
                if raw_next:
                    # next may be a relative path like /wiki/api/v2/pages?cursor=…
                    next_url = (
                        raw_next
                        if raw_next.startswith("http")
                        else f"{self._base_url}{raw_next}"
                    )
                else:
                    next_url = None

        logger.info(
            "ConfluenceAdapter: fetched %d pages (space=%s, last %dh)",
            len(pages),
            space_key,
            lookback_hours,
        )
        return pages

    async def fetch_comments(self, page_id: str) -> list[ConfluenceComment]:
        """
        Fetch all inline/footer comments belonging to a single page.

        Uses ``_links.next`` cursor pagination automatically.

        Parameters
        ----------
        page_id:
            The Confluence page ID (numeric string, e.g. ``"123456789"``).

        Returns
        -------
        list[ConfluenceComment]
            Typed comment objects for the given page.

        Raises
        ------
        CollectorError
            On missing credentials, 401 auth failure, or 429 rate limit.
        """
        self._check_auth()

        comments: list[ConfluenceComment] = []

        next_url: str | None = self._api_url(
            f"/pages/{page_id}/footer-comments?limit={_PAGE_SIZE}&body-format=storage"
        )

        async with httpx.AsyncClient(timeout=30.0) as client:
            while next_url:
                resp = await client.get(next_url, headers=self._headers)
                self._raise_for_status(
                    resp, f"fetch_comments page={page_id}"
                )

                data: dict[str, Any] = resp.json()

                for raw in data.get("results", []):
                    version = raw.get("version", {})
                    created_str: str = raw.get("createdAt", version.get("createdAt", ""))
                    author_val = version.get("authorId", "unknown")
                    author_str = (
                        author_val
                        if isinstance(author_val, str)
                        else author_val.get("displayName", "unknown")
                    )

                    body_obj = raw.get("body", {})
                    body_text = (
                        body_obj.get("storage", {}).get("value", "")
                        or body_obj.get("atlas_doc_format", {}).get("value", "")
                    )

                    comments.append(
                        ConfluenceComment(
                            id=str(raw["id"]),
                            body=body_text,
                            author=author_str or "unknown",
                            created_at=(
                                self._parse_dt(created_str)
                                if created_str
                                else datetime.now(timezone.utc)
                            ),
                        )
                    )

                links: dict[str, str] = data.get("_links", {})
                raw_next: str | None = links.get("next")
                if raw_next:
                    next_url = (
                        raw_next
                        if raw_next.startswith("http")
                        else f"{self._base_url}{raw_next}"
                    )
                else:
                    next_url = None

        logger.info(
            "ConfluenceAdapter: fetched %d comments for page %s",
            len(comments),
            page_id,
        )
        return comments
