"""
nexusflow/adapters/github.py
GitHub MCP Adapter — L1 Collector Agent tool.
Uses GitHub REST API v3. Pure httpx, no SDK.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import httpx

from nexusflow.core.models import GitHubPR
from nexusflow.core.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

GITHUB_API = "https://api.github.com"


class GitHubAdapter:
    """
    Fetches recently updated pull requests from a GitHub repository.
    Returns typed GitHubPR objects.
    """

    def __init__(self, token: str | None = None):
        _token = token or settings.github_token
        self._headers = {
            "Authorization": f"Bearer {_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        self._enabled = bool(_token)

    async def fetch_pull_requests(
        self,
        owner: str,
        repo: str,
        state: str = "all",
        updated_days: int = 7,
        max_results: int = 50,
    ) -> list[GitHubPR]:
        """
        Fetch PRs from `owner/repo` updated within the last `updated_days`.
        """
        if not self._enabled:
            logger.warning("GITHUB_TOKEN not configured — skipping GitHub collection")
            return []

        since = (
            datetime.now(timezone.utc) - timedelta(days=updated_days)
        ).isoformat()

        prs: list[GitHubPR] = []
        page = 1

        async with httpx.AsyncClient(timeout=30.0) as client:
            while len(prs) < max_results:
                resp = await client.get(
                    f"{GITHUB_API}/repos/{owner}/{repo}/pulls",
                    headers=self._headers,
                    params={
                        "state": state,
                        "sort": "updated",
                        "direction": "desc",
                        "per_page": 50,
                        "page": page,
                    },
                )

                if resp.status_code == 401:
                    logger.error("GitHub authentication failed")
                    return prs

                if resp.status_code == 403:
                    logger.warning("GitHub rate limit or permission error")
                    return prs

                if resp.status_code == 404:
                    logger.error("GitHub repo %s/%s not found", owner, repo)
                    return prs

                resp.raise_for_status()
                items = resp.json()

                if not items:
                    break

                for pr in items:
                    updated_at = datetime.fromisoformat(
                        pr["updated_at"].replace("Z", "+00:00")
                    )
                    # Stop if we've passed our lookback window
                    if updated_at < datetime.fromisoformat(since):
                        return prs

                    prs.append(
                        GitHubPR(
                            id=pr["id"],
                            number=pr["number"],
                            title=pr["title"],
                            state=pr["state"],
                            author=pr.get("user", {}).get("login", "unknown"),
                            created_at=datetime.fromisoformat(
                                pr["created_at"].replace("Z", "+00:00")
                            ),
                            updated_at=updated_at,
                            body=pr.get("body"),
                            labels=[lbl["name"] for lbl in pr.get("labels", [])],
                        )
                    )

                page += 1

        logger.info("GitHubAdapter: fetched %d PRs from %s/%s", len(prs), owner, repo)
        return prs
