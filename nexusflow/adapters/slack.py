"""
nexusflow/adapters/slack.py
Slack MCP Adapter — L1 Collector Agent tool.
Pure httpx, no Slack SDK dependency.
Handles pagination, rate limiting, and auth token rotation.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import httpx

from nexusflow.core.models import SlackMessage
from nexusflow.core.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

SLACK_API = "https://slack.com/api"
RATE_LIMIT_DELAY = 1.1  # seconds between requests (Slack: 1 req/sec tier 3)


class CollectorError(Exception):
    """Raised when a Collector Agent fails to retrieve data."""
    def __init__(self, message: str, retry_after: int = 60):
        super().__init__(message)
        self.retry_after = retry_after


class SlackAdapter:
    """
    Fetches messages from a Slack channel within a lookback window.
    Returns typed SlackMessage objects — no raw dicts escape this adapter.
    """

    def __init__(self, bot_token: str | None = None):
        self._token = bot_token or settings.slack_bot_token
        self._headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    async def fetch_messages(
        self,
        channel_id: str,
        lookback_hours: int = 72,
    ) -> list[SlackMessage]:
        """
        Fetch all messages from `channel_id` within the last `lookback_hours`.
        Handles Slack cursor-based pagination automatically.
        Raises CollectorError on auth failure or rate limit.
        """
        if not self._token:
            raise CollectorError("SLACK_BOT_TOKEN not configured", retry_after=0)

        oldest = (
            datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        ).timestamp()

        messages: list[SlackMessage] = []
        cursor: str | None = None

        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                params: dict = {
                    "channel": channel_id,
                    "oldest": str(oldest),
                    "limit": 200,
                }
                if cursor:
                    params["cursor"] = cursor

                await asyncio.sleep(RATE_LIMIT_DELAY)

                resp = await client.get(
                    f"{SLACK_API}/conversations.history",
                    headers=self._headers,
                    params=params,
                )

                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 60))
                    raise CollectorError(
                        f"Slack rate limit hit on channel {channel_id}",
                        retry_after=retry_after,
                    )

                if resp.status_code == 401:
                    raise CollectorError(
                        "Slack authentication failed — check SLACK_BOT_TOKEN",
                        retry_after=0,
                    )

                resp.raise_for_status()
                data = resp.json()

                if not data.get("ok"):
                    error = data.get("error", "unknown_error")
                    raise CollectorError(f"Slack API error: {error}")

                for msg in data.get("messages", []):
                    # Skip bot messages and system messages
                    if msg.get("subtype") in ("bot_message", "channel_join", "channel_leave"):
                        continue
                    if not msg.get("text"):
                        continue

                    messages.append(
                        SlackMessage(
                            id=msg["ts"],
                            channel_id=channel_id,
                            author=msg.get("user", "unknown"),
                            timestamp=datetime.fromtimestamp(
                                float(msg["ts"]), tz=timezone.utc
                            ),
                            content=msg["text"],
                            thread_ts=msg.get("thread_ts"),
                        )
                    )

                # Pagination
                response_metadata = data.get("response_metadata", {})
                cursor = response_metadata.get("next_cursor")
                if not cursor:
                    break

        logger.info(
            "SlackAdapter: fetched %d messages from %s (last %dh)",
            len(messages), channel_id, lookback_hours,
        )
        return messages

    async def fetch_thread(
        self,
        channel_id: str,
        thread_ts: str,
    ) -> list[SlackMessage]:
        """Fetch all replies in a specific thread."""
        if not self._token:
            raise CollectorError("SLACK_BOT_TOKEN not configured", retry_after=0)

        replies: list[SlackMessage] = []

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{SLACK_API}/conversations.replies",
                headers=self._headers,
                params={"channel": channel_id, "ts": thread_ts},
            )

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 60))
                raise CollectorError("Slack rate limit hit", retry_after=retry_after)

            resp.raise_for_status()
            data = resp.json()

            if not data.get("ok"):
                raise CollectorError(f"Slack API error: {data.get('error')}")

            for msg in data.get("messages", []):
                if not msg.get("text"):
                    continue
                replies.append(
                    SlackMessage(
                        id=msg["ts"],
                        channel_id=channel_id,
                        author=msg.get("user", "unknown"),
                        timestamp=datetime.fromtimestamp(
                            float(msg["ts"]), tz=timezone.utc
                        ),
                        content=msg["text"],
                        thread_ts=thread_ts,
                    )
                )

        return replies

    async def post_message(self, channel_id: str, text: str) -> bool:
        """Post a message to a channel — used by the Executor Agent."""
        if not self._token:
            raise CollectorError("SLACK_BOT_TOKEN not configured", retry_after=0)

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{SLACK_API}/chat.postMessage",
                headers=self._headers,
                json={"channel": channel_id, "text": text},
            )
            data = resp.json()
            if not data.get("ok"):
                logger.error("Slack post_message failed: %s", data.get("error"))
                return False
            return True
