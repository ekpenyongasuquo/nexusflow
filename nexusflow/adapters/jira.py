"""
nexusflow/adapters/jira.py
JIRA MCP Adapter — L1 Collector Agent tool.
Uses JIRA Cloud REST API v3. Pure httpx, no SDK.
"""
from __future__ import annotations

import base64
import logging
from datetime import datetime, timedelta, timezone

import httpx

from nexusflow.core.models import JiraTicket
from nexusflow.core.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class JiraAdapter:
    """
    Fetches recently updated JIRA tickets matching a JQL query.
    Returns typed JiraTicket objects.
    """

    def __init__(
        self,
        base_url: str | None = None,
        email: str | None = None,
        api_token: str | None = None,
    ):
        self._base_url = (base_url or settings.jira_base_url).rstrip("/")
        _email = email or settings.jira_email
        _token = api_token or settings.jira_api_token

        # Basic auth: base64(email:api_token)
        credentials = base64.b64encode(f"{_email}:{_token}".encode()).decode()
        self._headers = {
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _build_jql(self, labels: list[str], updated_days: int) -> str:
        """Build a JQL query for recently updated tickets matching labels."""
        since = (
            datetime.now(timezone.utc) - timedelta(days=updated_days)
        ).strftime("%Y-%m-%d")

        label_clause = ""
        if labels:
            quoted = ", ".join(f'"{lbl}"' for lbl in labels)
            label_clause = f" AND labels IN ({quoted})"

        return f'updated >= "{since}"{label_clause} ORDER BY updated DESC'

    async def fetch_tickets(
        self,
        labels: list[str] | None = None,
        jql: str | None = None,
        updated_days: int = 7,
        max_results: int = 100,
    ) -> list[JiraTicket]:
        """
        Fetch JIRA tickets by JQL or label filter.
        At least one of `labels` or `jql` should be provided.
        """
        if not self._base_url:
            logger.warning("JIRA_BASE_URL not configured — skipping JIRA collection")
            return []

        query = jql or self._build_jql(labels or [], updated_days)

        tickets: list[JiraTicket] = []
        start_at = 0

        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                resp = await client.get(
                    f"{self._base_url}/rest/api/3/search",
                    headers=self._headers,
                    params={
                        "jql": query,
                        "startAt": start_at,
                        "maxResults": min(50, max_results - len(tickets)),
                        "fields": "summary,status,assignee,updated,labels,description",
                    },
                )

                if resp.status_code == 401:
                    logger.error("JIRA authentication failed")
                    return tickets

                if resp.status_code == 429:
                    logger.warning("JIRA rate limit hit — returning partial results")
                    return tickets

                resp.raise_for_status()
                data = resp.json()

                issues = data.get("issues", [])
                for issue in issues:
                    fields = issue.get("fields", {})
                    assignee = fields.get("assignee")
                    description_doc = fields.get("description")

                    # Extract plain text from Atlassian Document Format
                    description_text = _extract_adf_text(description_doc)

                    tickets.append(
                        JiraTicket(
                            id=issue["id"],
                            key=issue["key"],
                            summary=fields.get("summary", ""),
                            status=fields.get("status", {}).get("name", "Unknown"),
                            assignee=(
                                assignee.get("displayName") if assignee else None
                            ),
                            updated=datetime.fromisoformat(
                                fields["updated"].replace("Z", "+00:00")
                            ),
                            labels=fields.get("labels", []),
                            description=description_text,
                        )
                    )

                total = data.get("total", 0)
                start_at += len(issues)

                if start_at >= total or len(tickets) >= max_results or not issues:
                    break

        logger.info("JiraAdapter: fetched %d tickets", len(tickets))
        return tickets

    async def create_ticket(
        self,
        project_key: str,
        summary: str,
        description: str,
        labels: list[str] | None = None,
        issue_type: str = "Task",
    ) -> str | None:
        """Create a JIRA ticket — used by the Executor Agent. Returns issue key."""
        if not self._base_url:
            return None

        payload = {
            "fields": {
                "project": {"key": project_key},
                "summary": summary,
                "description": {
                    "type": "doc",
                    "version": 1,
                    "content": [
                        {
                            "type": "paragraph",
                            "content": [{"type": "text", "text": description}],
                        }
                    ],
                },
                "issuetype": {"name": issue_type},
                "labels": labels or [],
            }
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{self._base_url}/rest/api/3/issue",
                headers=self._headers,
                json=payload,
            )
            if resp.status_code in (200, 201):
                return resp.json().get("key")
            logger.error("JIRA create_ticket failed: %s", resp.text)
            return None


def _extract_adf_text(adf: dict | None) -> str | None:
    """Recursively extract plain text from Atlassian Document Format."""
    if not adf:
        return None
    texts = []
    if isinstance(adf, dict):
        if adf.get("type") == "text":
            texts.append(adf.get("text", ""))
        for child in adf.get("content", []):
            result = _extract_adf_text(child)
            if result:
                texts.append(result)
    return " ".join(texts).strip() or None
