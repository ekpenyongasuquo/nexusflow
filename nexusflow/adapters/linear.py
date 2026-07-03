"""
nexusflow/adapters/linear.py
Linear MCP Adapter — L1 Collector Agent tool.
Pure httpx, no Linear SDK dependency.
Handles GraphQL cursor pagination, 429 rate limiting, and 401 auth failure.
API reference: https://developers.linear.app/docs/graphql/working-with-the-graphql-api
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from nexusflow.core.models import LinearComment, LinearIssue
from nexusflow.core.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

LINEAR_API = "https://api.linear.app/graphql"
PAGE_SIZE = 50  # comfortable page size; Linear's default max is 250


class CollectorError(Exception):
    """Raised when a Collector Agent fails to retrieve data."""

    def __init__(self, message: str, retry_after: int = 60) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class LinearAdapter:
    """
    Fetches issues and comments from the Linear GraphQL API.
    Returns typed ``LinearIssue`` / ``LinearComment`` objects —
    no raw dicts escape this adapter.

    Authentication uses the ``Authorization: Bearer <API_KEY>`` scheme
    required by Linear.  The key is read from
    ``settings.linear_api_key`` (env var ``LINEAR_API_KEY``) unless
    overridden via the constructor.
    """

    def __init__(self, api_key: str | None = None) -> None:
        self._api_key = api_key or settings.linear_api_key
        self._headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }

    # ── helpers ────────────────────────────────────────────────────────────────

    def _check_auth(self) -> None:
        """Raise CollectorError immediately if the API key is absent."""
        if not self._api_key:
            raise CollectorError(
                "LINEAR_API_KEY not configured", retry_after=0
            )

    @staticmethod
    def _parse_dt(value: str) -> datetime:
        """Parse an ISO-8601 timestamp returned by Linear into a UTC datetime."""
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    async def _gql(
        self,
        client: httpx.AsyncClient,
        query: str,
        variables: dict[str, Any],
        context: str,
    ) -> dict[str, Any]:
        """
        Execute a GraphQL request and return the ``data`` payload.

        Raises
        ------
        CollectorError
            On 401 auth failure or 429 rate limit.
        httpx.HTTPStatusError
            On any other HTTP error.
        """
        resp = await client.post(
            LINEAR_API,
            headers=self._headers,
            json={"query": query, "variables": variables},
        )

        if resp.status_code == 401:
            raise CollectorError(
                "Linear authentication failed — check LINEAR_API_KEY",
                retry_after=0,
            )
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            raise CollectorError(
                f"Linear rate limit hit ({context})",
                retry_after=retry_after,
            )
        resp.raise_for_status()

        payload: dict[str, Any] = resp.json()
        if "errors" in payload:
            messages = "; ".join(e.get("message", "") for e in payload["errors"])
            raise CollectorError(
                f"Linear GraphQL error ({context}): {messages}", retry_after=0
            )
        return payload["data"]

    # ── GraphQL queries ────────────────────────────────────────────────────────

    _ISSUES_QUERY = """
    query FetchIssues(
        $filter: IssueFilter!
        $first: Int!
        $after: String
    ) {
        issues(filter: $filter, first: $first, after: $after, orderBy: createdAt) {
            pageInfo {
                hasNextPage
                endCursor
            }
            nodes {
                id
                title
                state { name }
                priority
                assignee { displayName }
                createdAt
                updatedAt
                team { name }
                url
                labels { nodes { name } }
            }
        }
    }
    """

    _COMMENTS_QUERY = """
    query FetchComments($issueId: String!, $first: Int!, $after: String) {
        issue(id: $issueId) {
            comments(first: $first, after: $after) {
                pageInfo {
                    hasNextPage
                    endCursor
                }
                nodes {
                    id
                    body
                    user { displayName }
                    createdAt
                }
            }
        }
    }
    """

    # ── public methods ─────────────────────────────────────────────────────────

    async def fetch_issues(
        self,
        lookback_hours: int = 72,
        states: list[str] | None = None,
        team_id: str | None = None,
    ) -> list[LinearIssue]:
        """
        Fetch all issues matching ``states`` updated within the last
        ``lookback_hours``.

        Uses GraphQL cursor pagination (``endCursor`` / ``hasNextPage``)
        automatically; each page requests up to ``PAGE_SIZE`` records.

        Parameters
        ----------
        lookback_hours:
            How far back in time to search (based on ``updatedAt``).
            Defaults to 72 hours.
        states:
            List of state names to include.  Defaults to
            ``["In Progress", "Todo", "Blocked"]``.
        team_id:
            Optional Linear team ID to scope the search.  When ``None``
            all teams accessible to the API key are searched.

        Returns
        -------
        list[LinearIssue]
            Typed issue objects, ordered oldest-first as returned by the API.

        Raises
        ------
        CollectorError
            On missing API key, 401 auth failure, or 429 rate limit.
        """
        self._check_auth()

        if states is None:
            states = ["In Progress", "Todo", "Blocked"]

        since = (
            datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Build the GraphQL filter object
        gql_filter: dict[str, Any] = {
            "state": {"name": {"in": states}},
            "updatedAt": {"gte": since},
        }
        if team_id is not None:
            gql_filter["team"] = {"id": {"eq": team_id}}

        issues: list[LinearIssue] = []
        cursor: str | None = None

        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                variables: dict[str, Any] = {
                    "filter": gql_filter,
                    "first": PAGE_SIZE,
                }
                if cursor is not None:
                    variables["after"] = cursor

                data = await self._gql(client, self._ISSUES_QUERY, variables, "fetch_issues")
                page = data["issues"]

                for node in page["nodes"]:
                    issues.append(
                        LinearIssue(
                            id=node["id"],
                            title=node["title"],
                            state=node["state"]["name"],
                            priority=node["priority"],
                            assignee=(
                                node["assignee"]["displayName"]
                                if node.get("assignee")
                                else None
                            ),
                            created_at=self._parse_dt(node["createdAt"]),
                            updated_at=self._parse_dt(node["updatedAt"]),
                            team_name=node["team"]["name"],
                            url=node["url"],
                            labels=[
                                lbl["name"]
                                for lbl in node.get("labels", {}).get("nodes", [])
                            ],
                        )
                    )

                page_info = page["pageInfo"]
                if not page_info["hasNextPage"]:
                    break
                cursor = page_info["endCursor"]

        logger.info(
            "LinearAdapter: fetched %d issues (last %dh, states=%s, team=%s)",
            len(issues),
            lookback_hours,
            states,
            team_id,
        )
        return issues

    async def fetch_comments(self, issue_id: str) -> list[LinearComment]:
        """
        Fetch all comments belonging to a single issue.

        Uses GraphQL cursor pagination automatically.

        Parameters
        ----------
        issue_id:
            The Linear issue ID (e.g. ``"abc123"``).

        Returns
        -------
        list[LinearComment]
            Typed comment objects for the given issue.

        Raises
        ------
        CollectorError
            On missing API key, 401 auth failure, or 429 rate limit.
        """
        self._check_auth()

        comments: list[LinearComment] = []
        cursor: str | None = None

        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                variables: dict[str, Any] = {
                    "issueId": issue_id,
                    "first": PAGE_SIZE,
                }
                if cursor is not None:
                    variables["after"] = cursor

                data = await self._gql(
                    client, self._COMMENTS_QUERY, variables,
                    f"fetch_comments issue={issue_id}",
                )
                page = data["issue"]["comments"]

                for node in page["nodes"]:
                    comments.append(
                        LinearComment(
                            id=node["id"],
                            body=node["body"],
                            author=node["user"]["displayName"] if node.get("user") else "unknown",
                            created_at=self._parse_dt(node["createdAt"]),
                        )
                    )

                page_info = page["pageInfo"]
                if not page_info["hasNextPage"]:
                    break
                cursor = page_info["endCursor"]

        logger.info(
            "LinearAdapter: fetched %d comments for issue %s",
            len(comments),
            issue_id,
        )
        return comments
