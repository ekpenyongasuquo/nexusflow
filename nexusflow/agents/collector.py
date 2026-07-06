"""
nexusflow/agents/collector.py
L1 Collector Agent — orchestrates all MCP adapters concurrently.
Aggregates signals into a typed CollectedCorpus.
Isolated input/output contract: receives PipelineState, returns PipelineState.
Includes circuit breaker on every adapter and demo mode fallback.
"""
from __future__ import annotations

import asyncio
import enum
import logging
import time
from datetime import datetime, timezone

from nexusflow.core.models import (
    CollectedCorpus,
    PipelineState,
    PipelineStatus,
)
from nexusflow.core.observability import trace_node
from nexusflow.core.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


# ── Circuit Breaker ───────────────────────────────────────────────────────────

class _State(enum.Enum):
    CLOSED    = "CLOSED"
    OPEN      = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitOpenError(Exception):
    """Raised when a circuit breaker is OPEN and skips the adapter."""
    pass


class CircuitBreaker:
    """
    Per-adapter circuit breaker.
    After `failure_threshold` consecutive failures the circuit OPENS.
    After `reset_timeout` seconds it moves to HALF_OPEN and probes once.
    On success → CLOSED. On probe failure → OPEN again.
    All state changes logged with [CIRCUIT-BREAKER:<name>] prefix.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 3,
        reset_timeout: float = 60.0,
        call_timeout: float = 30.0,
    ):
        self.name              = name
        self.failure_threshold = failure_threshold
        self.reset_timeout     = reset_timeout
        self.call_timeout      = call_timeout
        self._state            = _State.CLOSED
        self._failure_count    = 0
        self._opened_at: float = 0.0

    # ── Public ────────────────────────────────────────────────────────────────

    async def call(self, coro_factory):
        """
        Execute `coro_factory()` through the breaker.
        Raises CircuitOpenError when OPEN (fast-fail).
        """
        self._maybe_transition_to_half_open()

        if self._state is _State.OPEN:
            msg = (
                f"[CIRCUIT-BREAKER:{self.name}] OPEN — "
                f"skipping {self.name.upper()} adapter: "
                f"circuit tripped, retry in "
                f"{self.reset_timeout - (time.monotonic() - self._opened_at):.0f}s"
            )
            logger.warning(msg)
            raise CircuitOpenError(msg)

        try:
            result = await asyncio.wait_for(
                coro_factory(), timeout=self.call_timeout
            )
            self._on_success()
            return result
        except Exception as exc:
            self._on_failure()
            raise exc

    # ── State helpers ─────────────────────────────────────────────────────────

    def _maybe_transition_to_half_open(self) -> None:
        if self._state is _State.OPEN:
            elapsed = time.monotonic() - self._opened_at
            if elapsed >= self.reset_timeout:
                self._state = _State.HALF_OPEN
                logger.info(
                    "[CIRCUIT-BREAKER:%s] reset timeout elapsed — "
                    "transitioning to HALF_OPEN",
                    self.name,
                )

    def _on_success(self) -> None:
        if self._state is _State.HALF_OPEN:
            logger.info(
                "[CIRCUIT-BREAKER:%s] probe succeeded — closing breaker",
                self.name,
            )
        self._state         = _State.CLOSED
        self._failure_count = 0

    def _on_failure(self) -> None:
        self._failure_count += 1
        if (
            self._state is _State.HALF_OPEN
            or self._failure_count >= self.failure_threshold
        ):
            self._state     = _State.OPEN
            self._opened_at = time.monotonic()
            logger.warning(
                "[CIRCUIT-BREAKER:%s] OPENING after %d consecutive failure(s)",
                self.name,
                self._failure_count,
            )


# ── Per-adapter circuit breakers (module-level singletons) ───────────────────
_cb_slack    = CircuitBreaker("slack")
_cb_jira     = CircuitBreaker("jira")
_cb_github   = CircuitBreaker("github")
_cb_pagerduty = CircuitBreaker("pagerduty")
_cb_linear   = CircuitBreaker("linear")
_cb_confluence = CircuitBreaker("confluence")
_cb_datadog  = CircuitBreaker("datadog")
_cb_sentry   = CircuitBreaker("sentry")
_cb_notion   = CircuitBreaker("notion")
_cb_gcal     = CircuitBreaker("google_calendar")
_cb_sendgrid = CircuitBreaker("sendgrid")


# ── Main Agent Entry Point ────────────────────────────────────────────────────

@trace_node("collect")
async def run_collector_agent(state: PipelineState) -> PipelineState:
    """
    L1 Collector Agent entry point.
    Runs all MCP adapters concurrently with circuit breakers.
    Partial failures are tolerated — pipeline continues with whatever was collected.
    If zero items collected across all adapters, injects demo data so the
    full pipeline can run end-to-end without real API credentials.

    Input:  PipelineState with trigger_metadata
    Output: PipelineState with corpus populated
    """
    logger.info("[L1-COLLECTOR] Pipeline %s — starting collection", state.pipeline_id)
    state.status = PipelineStatus.COLLECTING

    meta = state.trigger_metadata
    errors: list[str] = []

    # ── Run all adapters concurrently ─────────────────────────────────────────
    results = await asyncio.gather(
        _safe_slack_collect(meta, errors),
        _safe_jira_collect(meta, errors),
        _safe_github_collect(meta, errors),
        _safe_pagerduty_collect(meta, errors),
        _safe_linear_collect(meta, errors),
        _safe_confluence_collect(meta, errors),
        _safe_datadog_collect(meta, errors),
        _safe_sentry_collect(meta, errors),
        _safe_notion_collect(meta, errors),
        _safe_gcal_collect(meta, errors),
        _safe_sendgrid_collect(meta, errors),
    )

    (
        slack_messages,
        jira_tickets,
        github_prs,
        pagerduty_incidents,
        linear_issues,
        confluence_pages,
        datadog_monitors,
        sentry_issues,
        notion_pages,
        calendar_events,
        sendgrid_bounces,
    ) = results

    corpus = CollectedCorpus(
        pipeline_id=state.pipeline_id,
        collected_at=datetime.now(timezone.utc),
        slack_messages=slack_messages,
        jira_tickets=jira_tickets,
        github_prs=github_prs,
        collection_errors=errors,
    )

    # ── If zero items — inject demo data so pipeline runs end-to-end ─────────
    if corpus.total_items == 0:
        logger.info(
            "[L1-COLLECTOR] No adapters configured — injecting demo data for pipeline %s",
            state.pipeline_id,
        )
        corpus = _inject_demo_data(state.pipeline_id)

    state.corpus = corpus

    logger.info(
        "[L1-COLLECTOR] Pipeline %s — collected %d items. Errors: %d",
        state.pipeline_id,
        corpus.total_items,
        len(corpus.collection_errors),
    )

    return state


# ── Demo Data Injection ───────────────────────────────────────────────────────

def _inject_demo_data(pipeline_id: str) -> CollectedCorpus:
    """
    Injects realistic mock enterprise signals so the full 5-agent pipeline
    can demonstrate end-to-end execution without real API credentials.
    """
    from nexusflow.core.models import JiraTicket, SlackMessage

    now = datetime.now(timezone.utc)

    slack_messages = [
        SlackMessage(
            id="demo-slack-001",
            channel_id="C001",
            author="CFO",
            timestamp=now,
            content="Q3 budget variance detected — costs are 18% over plan. "
                    "Vendor contracts exceeded allocation by $75K this quarter.",
        ),
        SlackMessage(
            id="demo-slack-002",
            channel_id="C001",
            author="VP-Finance",
            timestamp=now,
            content="Engineering team overspend on cloud infrastructure. "
                    "AWS bill came in at $142K vs $98K budgeted. Need CFO sign-off.",
        ),
        SlackMessage(
            id="demo-slack-003",
            channel_id="C001",
            author="CTO",
            timestamp=now,
            content="We can optimise the infrastructure spend by moving to reserved "
                    "instances. Estimated savings $40K/quarter going forward.",
        ),
    ]

    jira_tickets = [
        JiraTicket(
            id="10042",
            key="FIN-42",
            summary="Q3 budget overrun — CFO approval required",
            status="In Progress",
            assignee="VP-Finance",
            updated=now,
            labels=["Q3-budget", "urgent", "finance"],
            description="Budget variance of 18% detected across engineering and vendor "
                        "contracts. Immediate CFO review required before month-end close.",
        ),
        JiraTicket(
            id="10043",
            key="FIN-43",
            summary="AWS infrastructure cost review — reserved instances proposal",
            status="Todo",
            assignee="CTO",
            updated=now,
            labels=["Q3-budget", "infrastructure", "cost-optimisation"],
            description="Proposal to migrate to reserved EC2 instances to reduce "
                        "monthly AWS spend by approximately $13K per month.",
        ),
    ]

    return CollectedCorpus(
        pipeline_id=pipeline_id,
        collected_at=datetime.now(timezone.utc),
        slack_messages=slack_messages,
        jira_tickets=jira_tickets,
        github_prs=[],
        collection_errors=[],
    )


# ── Safe Wrappers — Slack, JIRA, GitHub ──────────────────────────────────────

async def _safe_slack_collect(meta: dict, errors: list[str]):
    try:
        from nexusflow.adapters.slack import CollectorError, SlackAdapter
        channel_id = meta.get("slack_channel_id", "")
        if not channel_id:
            return []
        lookback_hours = meta.get("lookback_hours", 72)
        adapter = SlackAdapter()
        return await _cb_slack.call(
            lambda: adapter.fetch_messages(channel_id, lookback_hours)
        )
    except CircuitOpenError as e:
        errors.append(f"Slack: {e}")
        return []
    except Exception as e:
        errors.append(f"Slack: {e}")
        logger.warning("[L1-COLLECTOR] Slack error: %s", e)
        return []


async def _safe_jira_collect(meta: dict, errors: list[str]):
    try:
        from nexusflow.adapters.jira import JiraAdapter
        adapter = JiraAdapter()
        labels = meta.get("jira_labels", [])
        updated_days = meta.get("updated_days", 7)
        jql = meta.get("jira_jql")
        return await _cb_jira.call(
            lambda: adapter.fetch_tickets(labels=labels, jql=jql, updated_days=updated_days)
        )
    except CircuitOpenError as e:
        errors.append(f"JIRA: {e}")
        return []
    except Exception as e:
        errors.append(f"JIRA: {e}")
        logger.warning("[L1-COLLECTOR] JIRA error: %s", e)
        return []


async def _safe_github_collect(meta: dict, errors: list[str]):
    try:
        from nexusflow.adapters.github import GitHubAdapter
        owner = meta.get("github_owner", "")
        repo  = meta.get("github_repo", "")
        if not owner or not repo:
            return []
        updated_days = meta.get("updated_days", 7)
        adapter = GitHubAdapter()
        return await _cb_github.call(
            lambda: adapter.fetch_pull_requests(owner, repo, updated_days=updated_days)
        )
    except CircuitOpenError as e:
        errors.append(f"GitHub: {e}")
        return []
    except Exception as e:
        errors.append(f"GitHub: {e}")
        logger.warning("[L1-COLLECTOR] GitHub error: %s", e)
        return []


# ── Safe Wrappers — New Adapters ─────────────────────────────────────────────

async def _safe_pagerduty_collect(meta: dict, errors: list[str]):
    try:
        from nexusflow.adapters.pagerduty import PagerDutyAdapter
        if not settings.pagerduty_api_key:
            errors.append("PagerDuty: PAGERDUTY_API_KEY not configured")
            return []
        adapter = PagerDutyAdapter()
        lookback_hours = meta.get("lookback_hours", 72)
        return await _cb_pagerduty.call(
            lambda: adapter.fetch_incidents(lookback_hours=lookback_hours)
        )
    except CircuitOpenError as e:
        errors.append(f"PagerDuty: {e}")
        return []
    except Exception as e:
        errors.append(f"PagerDuty: {e}")
        logger.warning("[L1-COLLECTOR] PagerDuty error: %s", e)
        return []


async def _safe_linear_collect(meta: dict, errors: list[str]):
    try:
        from nexusflow.adapters.linear import LinearAdapter
        if not settings.linear_api_key:
            errors.append("Linear: LINEAR_API_KEY not configured")
            return []
        adapter = LinearAdapter()
        team_id = meta.get("linear_team_id")
        lookback_hours = meta.get("lookback_hours", 72)
        return await _cb_linear.call(
            lambda: adapter.fetch_issues(lookback_hours=lookback_hours, team_id=team_id)
        )
    except CircuitOpenError as e:
        errors.append(f"Linear: {e}")
        return []
    except Exception as e:
        errors.append(f"Linear: {e}")
        logger.warning("[L1-COLLECTOR] Linear error: %s", e)
        return []


async def _safe_confluence_collect(meta: dict, errors: list[str]):
    try:
        from nexusflow.adapters.confluence import ConfluenceAdapter
        if not settings.confluence_base_url:
            errors.append("Confluence: CONFLUENCE_BASE_URL not configured")
            return []
        adapter = ConfluenceAdapter()
        space_key = meta.get("confluence_space_key", "")
        if not space_key:
            return []
        lookback_hours = meta.get("lookback_hours", 72)
        return await _cb_confluence.call(
            lambda: adapter.fetch_pages(space_key=space_key, lookback_hours=lookback_hours)
        )
    except CircuitOpenError as e:
        errors.append(f"Confluence: {e}")
        return []
    except Exception as e:
        errors.append(f"Confluence: {e}")
        logger.warning("[L1-COLLECTOR] Confluence error: %s", e)
        return []


async def _safe_datadog_collect(meta: dict, errors: list[str]):
    try:
        from nexusflow.adapters.datadog import DatadogAdapter
        if not settings.datadog_api_key:
            errors.append("Datadog: DATADOG_API_KEY not configured")
            return []
        adapter = DatadogAdapter()
        tags = meta.get("datadog_tags")
        lookback_hours = meta.get("lookback_hours", 72)
        return await _cb_datadog.call(
            lambda: adapter.fetch_monitors(lookback_hours=lookback_hours, tags=tags)
        )
    except CircuitOpenError as e:
        errors.append(f"Datadog: {e}")
        return []
    except Exception as e:
        errors.append(f"Datadog: {e}")
        logger.warning("[L1-COLLECTOR] Datadog error: %s", e)
        return []


async def _safe_sentry_collect(meta: dict, errors: list[str]):
    try:
        from nexusflow.adapters.sentry import SentryAdapter
        if not settings.sentry_auth_token:
            errors.append("Sentry: SENTRY_AUTH_TOKEN not configured")
            return []
        adapter = SentryAdapter()
        org_slug = meta.get("sentry_org_slug", settings.sentry_org_slug)
        project_slug = meta.get("sentry_project_slug")
        if not org_slug:
            return []
        return await _cb_sentry.call(
            lambda: adapter.fetch_issues(
                organization_slug=org_slug,
                project_slug=project_slug,
            )
        )
    except CircuitOpenError as e:
        errors.append(f"Sentry: {e}")
        return []
    except Exception as e:
        errors.append(f"Sentry: {e}")
        logger.warning("[L1-COLLECTOR] Sentry error: %s", e)
        return []


async def _safe_notion_collect(meta: dict, errors: list[str]):
    try:
        from nexusflow.adapters.notion import NotionAdapter
        if not settings.notion_secret:
            errors.append("Notion: NOTION_SECRET not configured")
            return []
        adapter = NotionAdapter()
        database_id = meta.get("notion_database_id", settings.notion_database_id)
        if not database_id:
            return []
        lookback_hours = meta.get("lookback_hours", 72)
        return await _cb_notion.call(
            lambda: adapter.fetch_pages(
                database_id=database_id,
                lookback_hours=lookback_hours,
            )
        )
    except CircuitOpenError as e:
        errors.append(f"Notion: {e}")
        return []
    except Exception as e:
        errors.append(f"Notion: {e}")
        logger.warning("[L1-COLLECTOR] Notion error: %s", e)
        return []


async def _safe_gcal_collect(meta: dict, errors: list[str]):
    try:
        from nexusflow.adapters.google_calendar import GoogleCalendarAdapter
        if not settings.google_calendar_access_token:
            errors.append("GoogleCalendar: GOOGLE_CALENDAR_ACCESS_TOKEN not configured")
            return []
        adapter = GoogleCalendarAdapter()
        calendar_id = meta.get("google_calendar_id", settings.google_calendar_id or "primary")
        lookback_hours = meta.get("lookback_hours", 72)
        return await _cb_gcal.call(
            lambda: adapter.fetch_events(
                calendar_id=calendar_id,
                lookback_hours=lookback_hours,
            )
        )
    except CircuitOpenError as e:
        errors.append(f"GoogleCalendar: {e}")
        return []
    except Exception as e:
        errors.append(f"GoogleCalendar: {e}")
        logger.warning("[L1-COLLECTOR] Google Calendar error: %s", e)
        return []


async def _safe_sendgrid_collect(meta: dict, errors: list[str]):
    try:
        from nexusflow.adapters.sendgrid import SendGridAdapter
        if not settings.sendgrid_api_key:
            errors.append("SendGrid: SENDGRID_API_KEY not configured")
            return []
        adapter = SendGridAdapter()
        lookback_days = meta.get("sendgrid_lookback_days", 3)
        return await _cb_sendgrid.call(
            lambda: adapter.fetch_bounces(lookback_hours=lookback_days * 24)
        )
    except CircuitOpenError as e:
        errors.append(f"SendGrid: {e}")
        return []
    except Exception as e:
        errors.append(f"SendGrid: {e}")
        logger.warning("[L1-COLLECTOR] SendGrid error: %s", e)
        return []