"""
nexusflow/tests/adapters/test_slack.py
Test suite for the Slack MCP adapter.
Uses respx to mock HTTP responses — no real Slack API calls.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
import respx
from httpx import Response

from nexusflow.adapters.slack import CollectorError, SlackAdapter


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _slack_message(ts: str, user: str, text: str, subtype: str | None = None) -> dict:
    msg = {"ts": ts, "user": user, "text": text}
    if subtype:
        msg["subtype"] = subtype
    return msg


def _slack_history_response(messages: list[dict], has_more: bool = False) -> dict:
    return {
        "ok": True,
        "messages": messages,
        "has_more": has_more,
        "response_metadata": {"next_cursor": "cursor123" if has_more else ""},
    }


# ── Tests ─────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
@respx.mock
async def test_fetch_messages_success():
    """Adapter returns typed SlackMessage objects on successful API call."""
    messages = [
        _slack_message("1718000000.000001", "U001", "Budget variance detected in Q3"),
        _slack_message("1718000001.000002", "U002", "This is serious, costs are 18% over"),
        _slack_message("1718000002.000003", "U001", "I've raised a JIRA ticket"),
    ]

    respx.get("https://slack.com/api/conversations.history").mock(
        return_value=Response(200, json=_slack_history_response(messages))
    )

    adapter = SlackAdapter(bot_token="xoxb-test-token")
    result = await adapter.fetch_messages(channel_id="C123456", lookback_hours=72)

    assert len(result) == 3
    assert result[0].author == "U001"
    assert "Budget variance" in result[0].content
    assert result[0].channel_id == "C123456"
    assert isinstance(result[0].timestamp, datetime)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_messages_skips_bot_messages():
    """Adapter filters out bot messages and system subtypes."""
    messages = [
        _slack_message("1718000000.000001", "U001", "Real user message"),
        _slack_message("1718000001.000002", "B001", "Bot message", subtype="bot_message"),
        _slack_message("1718000002.000003", "U002", "", subtype="channel_join"),
    ]

    respx.get("https://slack.com/api/conversations.history").mock(
        return_value=Response(200, json=_slack_history_response(messages))
    )

    adapter = SlackAdapter(bot_token="xoxb-test-token")
    result = await adapter.fetch_messages(channel_id="C123456", lookback_hours=72)

    assert len(result) == 1
    assert result[0].author == "U001"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_messages_rate_limit_raises():
    """Adapter raises CollectorError with retry_after on 429."""
    respx.get("https://slack.com/api/conversations.history").mock(
        return_value=Response(429, headers={"Retry-After": "45"})
    )

    adapter = SlackAdapter(bot_token="xoxb-test-token")

    with pytest.raises(CollectorError) as exc_info:
        await adapter.fetch_messages(channel_id="C123456", lookback_hours=72)

    assert exc_info.value.retry_after == 45


@pytest.mark.asyncio
@respx.mock
async def test_fetch_messages_auth_failure():
    """Adapter raises CollectorError with retry_after=0 on 401."""
    respx.get("https://slack.com/api/conversations.history").mock(
        return_value=Response(401)
    )

    adapter = SlackAdapter(bot_token="xoxb-invalid")

    with pytest.raises(CollectorError) as exc_info:
        await adapter.fetch_messages(channel_id="C123456", lookback_hours=72)

    assert exc_info.value.retry_after == 0


@pytest.mark.asyncio
async def test_no_token_raises():
    """Adapter raises CollectorError immediately if no token configured."""
    adapter = SlackAdapter(bot_token="")

    with pytest.raises(CollectorError):
        await adapter.fetch_messages(channel_id="C123456")


@pytest.mark.asyncio
@respx.mock
async def test_post_message_success():
    """post_message returns True on successful API response."""
    respx.post("https://slack.com/api/chat.postMessage").mock(
        return_value=Response(200, json={"ok": True, "ts": "1718000099.000001"})
    )

    adapter = SlackAdapter(bot_token="xoxb-test-token")
    result = await adapter.post_message(channel_id="C123456", text="Decision executed ✅")

    assert result is True


@pytest.mark.asyncio
@respx.mock
async def test_post_message_failure():
    """post_message returns False on API error response."""
    respx.post("https://slack.com/api/chat.postMessage").mock(
        return_value=Response(200, json={"ok": False, "error": "channel_not_found"})
    )

    adapter = SlackAdapter(bot_token="xoxb-test-token")
    result = await adapter.post_message(channel_id="CINVALID", text="Test")

    assert result is False


@pytest.mark.asyncio
@respx.mock
async def test_slack_api_error_response():
    """Adapter raises CollectorError on Slack API ok=False response."""
    respx.get("https://slack.com/api/conversations.history").mock(
        return_value=Response(200, json={"ok": False, "error": "not_in_channel"})
    )

    adapter = SlackAdapter(bot_token="xoxb-test-token")

    with pytest.raises(CollectorError) as exc_info:
        await adapter.fetch_messages(channel_id="C123456", lookback_hours=72)

    assert "not_in_channel" in str(exc_info.value)
