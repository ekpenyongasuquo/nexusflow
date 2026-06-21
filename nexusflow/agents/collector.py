"""
nexusflow/agents/collector.py
L1 Collector Agent — orchestrates all MCP adapters concurrently.
Aggregates signals into a typed CollectedCorpus.
Isolated input/output contract: receives PipelineState, returns PipelineState.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from nexusflow.adapters.github import GitHubAdapter
from nexusflow.adapters.jira import JiraAdapter
from nexusflow.adapters.slack import CollectorError, SlackAdapter
from nexusflow.core.models import CollectedCorpus, PipelineState, PipelineStatus
from nexusflow.core.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


async def run_collector_agent(state: PipelineState) -> PipelineState:
    """
    L1 Collector Agent entry point.
    Runs all MCP adapters concurrently. Partial failures are tolerated
    and logged — the pipeline continues with whatever was collected.

    Input:  PipelineState with trigger_metadata
    Output: PipelineState with corpus populated
    """
    logger.info("[L1-COLLECTOR] Pipeline %s — starting collection", state.pipeline_id)
    state.status = PipelineStatus.COLLECTING

    meta = state.trigger_metadata
    errors: list[str] = []

    # ── Run all adapters concurrently ─────────────────────────────────────────
    slack_task = _safe_slack_collect(meta, errors)
    jira_task = _safe_jira_collect(meta, errors)
    github_task = _safe_github_collect(meta, errors)

    slack_messages, jira_tickets, github_prs = await asyncio.gather(
        slack_task, jira_task, github_task
    )

    corpus = CollectedCorpus(
        pipeline_id=state.pipeline_id,
        collected_at=datetime.now(timezone.utc),
        slack_messages=slack_messages,
        jira_tickets=jira_tickets,
        github_prs=github_prs,
        collection_errors=errors,
    )

    state.corpus = corpus

    logger.info(
        "[L1-COLLECTOR] Pipeline %s — collected %d items (%d Slack, %d JIRA, %d GitHub). Errors: %d",
        state.pipeline_id,
        corpus.total_items,
        len(slack_messages),
        len(jira_tickets),
        len(github_prs),
        len(errors),
    )

    # Fail the pipeline only if we collected nothing at all
    if corpus.total_items == 0:
        state.status = PipelineStatus.FAILED
        state.error_stage = "COLLECTOR"
        state.error_message = (
            "All adapters returned zero items. "
            f"Errors: {'; '.join(errors) if errors else 'No adapters configured'}"
        )
        logger.error("[L1-COLLECTOR] Pipeline %s — zero items collected. Halting.", state.pipeline_id)

    return state


# ── Safe wrappers — catch errors, log, return empty list ─────────────────────

async def _safe_slack_collect(meta: dict, errors: list[str]):
    try:
        adapter = SlackAdapter()
        channel_id = meta.get("slack_channel_id", "")
        lookback_hours = meta.get("lookback_hours", 72)
        if not channel_id:
            return []
        return await adapter.fetch_messages(channel_id, lookback_hours)
    except CollectorError as e:
        errors.append(f"Slack: {e}")
        logger.warning("[L1-COLLECTOR] Slack error: %s", e)
        return []
    except Exception as e:
        errors.append(f"Slack: unexpected error — {e}")
        logger.exception("[L1-COLLECTOR] Slack unexpected error")
        return []


async def _safe_jira_collect(meta: dict, errors: list[str]):
    try:
        adapter = JiraAdapter()
        labels = meta.get("jira_labels", [])
        updated_days = meta.get("updated_days", 7)
        jql = meta.get("jira_jql")
        return await adapter.fetch_tickets(labels=labels, jql=jql, updated_days=updated_days)
    except Exception as e:
        errors.append(f"JIRA: {e}")
        logger.warning("[L1-COLLECTOR] JIRA error: %s", e)
        return []


async def _safe_github_collect(meta: dict, errors: list[str]):
    try:
        adapter = GitHubAdapter()
        owner = meta.get("github_owner", "")
        repo = meta.get("github_repo", "")
        if not owner or not repo:
            return []
        updated_days = meta.get("updated_days", 7)
        return await adapter.fetch_pull_requests(owner, repo, updated_days=updated_days)
    except Exception as e:
        errors.append(f"GitHub: {e}")
        logger.warning("[L1-COLLECTOR] GitHub error: %s", e)
        return []
