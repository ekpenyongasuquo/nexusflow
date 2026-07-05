"""
nexusflow/agents/collector.py
L1 Collector Agent — orchestrates all MCP adapters concurrently.
Aggregates signals into a typed CollectedCorpus.
Isolated input/output contract: receives PipelineState, returns PipelineState.

Circuit-breaker protection
--------------------------
Each adapter has its own CircuitBreaker instance (module-level, shared across
pipeline runs in the same process).  Three states:

  CLOSED    — normal operation; failures increment a counter.
  OPEN      — tripped after FAILURE_THRESHOLD consecutive failures; all calls
              fast-fail immediately for RESET_TIMEOUT seconds.
  HALF_OPEN — one probe call allowed after the reset window expires; success
              re-closes the breaker, failure re-opens it.

A per-call asyncio.wait_for deadline (ADAPTER_TIMEOUT_SECS) is applied on top
of the circuit breaker so a hung TCP connection or a stalled 429-retry loop
inside the adapter can never block the asyncio.gather indefinitely.

Adapters
--------
  Existing : Slack, JIRA, GitHub
  New      : PagerDuty, Linear, Confluence, Datadog, Sentry,
             Notion, Google Calendar, SendGrid
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Awaitable, Callable, TypeVar

from nexusflow.adapters.confluence import ConfluenceAdapter
from nexusflow.adapters.datadog import DatadogAdapter
from nexusflow.adapters.github import GitHubAdapter
from nexusflow.adapters.google_calendar import GoogleCalendarAdapter
from nexusflow.adapters.jira import JiraAdapter
from nexusflow.adapters.linear import LinearAdapter
from nexusflow.adapters.notion import NotionAdapter
from nexusflow.adapters.pagerduty import PagerDutyAdapter
from nexusflow.adapters.sentry import SentryAdapter
from nexusflow.adapters.sendgrid import SendGridAdapter
from nexusflow.adapters.slack import CollectorError, SlackAdapter
from nexusflow.core.models import CollectedCorpus, PipelineState, PipelineStatus
from nexusflow.core.observability import trace_node
from nexusflow.core.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

T = TypeVar("T")

# ── Circuit-breaker tuning constants ─────────────────────────────────────────
FAILURE_THRESHOLD = 3        # consecutive failures before opening the breaker
RESET_TIMEOUT     = 60.0     # seconds the breaker stays OPEN before probing
ADAPTER_TIMEOUT   = 15.0     # hard wall-clock deadline per adapter call (seconds)


# ── Circuit-breaker implementation ───────────────────────────────────────────

class _State(Enum):
    CLOSED    = auto()
    OPEN      = auto()
    HALF_OPEN = auto()


@dataclass
class CircuitBreaker:
    """
    Lightweight async circuit breaker.  Thread-safe enough for a single-process
    asyncio application — all state mutations happen inside coroutines that are
    never run concurrently for the same breaker instance.
    """
    name: str
    failure_threshold: int = FAILURE_THRESHOLD
    reset_timeout: float   = RESET_TIMEOUT

    _state:            _State = field(default=_State.CLOSED,  init=False, repr=False)
    _failure_count:    int    = field(default=0,              init=False, repr=False)
    _opened_at:        float  = field(default=0.0,            init=False, repr=False)

    # ── public read-only property ─────────────────────────────────────────────

    @property
    def state(self) -> _State:
        return self._state

    # ── main entry point ──────────────────────────────────────────────────────

    async def call(
        self,
        coro_factory: Callable[[], Awaitable[T]],
        *,
        timeout: float = ADAPTER_TIMEOUT,
    ) -> T:
        """
        Execute *coro_factory()* under the circuit breaker + timeout guard.

        Raises:
            CircuitOpenError  — breaker is OPEN; caller should treat as a
                                transient failure and record the error.
            asyncio.TimeoutError — adapter exceeded *timeout* seconds.
            Any exception the underlying coroutine raises.
        """
        self._maybe_transition_to_half_open()

        if self._state is _State.OPEN:
            raise CircuitOpenError(
                f"[circuit-breaker:{self.name}] OPEN — fast-failing "
                f"(resets in {self._seconds_until_reset():.0f}s)"
            )

        try:
            result: T = await asyncio.wait_for(coro_factory(), timeout=timeout)
            self._on_success()
            return result
        except (CircuitOpenError, asyncio.TimeoutError):
            self._on_failure()
            raise
        except Exception:
            self._on_failure()
            raise

    # ── state-machine helpers ─────────────────────────────────────────────────

    def _maybe_transition_to_half_open(self) -> None:
        if (
            self._state is _State.OPEN
            and time.monotonic() - self._opened_at >= self.reset_timeout
        ):
            logger.info(
                "[CIRCUIT-BREAKER:%s] reset timeout elapsed — transitioning to HALF_OPEN",
                self.name,
            )
            self._state = _State.HALF_OPEN

    def _on_success(self) -> None:
        if self._state is not _State.CLOSED:
            logger.info(
                "[CIRCUIT-BREAKER:%s] probe succeeded — closing breaker", self.name
            )
        self._state = _State.CLOSED
        self._failure_count = 0

    def _on_failure(self) -> None:
        self._failure_count += 1
        if (
            self._state is _State.HALF_OPEN
            or self._failure_count >= self.failure_threshold
        ):
            if self._state is not _State.OPEN:
                logger.warning(
                    "[CIRCUIT-BREAKER:%s] OPENING after %d consecutive failure(s)",
                    self.name,
                    self._failure_count,
                )
            self._state   = _State.OPEN
            self._opened_at = time.monotonic()

    def _seconds_until_reset(self) -> float:
        return max(0.0, self.reset_timeout - (time.monotonic() - self._opened_at))


class CircuitOpenError(Exception):
    """Raised by CircuitBreaker.call() when the breaker is in the OPEN state."""


# ── Module-level breaker instances (persist across pipeline runs) ─────────────
_slack_breaker    = CircuitBreaker("slack")
_jira_breaker     = CircuitBreaker("jira")
_github_breaker   = CircuitBreaker("github")
_pagerduty_breaker   = CircuitBreaker("pagerduty")
_linear_breaker      = CircuitBreaker("linear")
_confluence_breaker  = CircuitBreaker("confluence")
_datadog_breaker     = CircuitBreaker("datadog")
_sentry_breaker      = CircuitBreaker("sentry")
_notion_breaker      = CircuitBreaker("notion")
_gcal_breaker        = CircuitBreaker("google_calendar")
_sendgrid_breaker    = CircuitBreaker("sendgrid")


# ── Agent entry point ─────────────────────────────────────────────────────────

@trace_node("collect")
async def run_collector_agent(state: PipelineState) -> PipelineState:
    """
    L1 Collector Agent entry point.
    Runs all MCP adapters concurrently under individual circuit breakers and a
    per-adapter timeout.  Partial failures are tolerated and logged — the
    pipeline continues with whatever was collected.

    Input:  PipelineState with trigger_metadata
    Output: PipelineState with corpus populated

    Adapters and trigger_metadata keys
    -----------------------------------
    Slack          : slack_channel_id, lookback_hours
    JIRA           : jira_labels, jira_jql, updated_days
    GitHub         : github_owner, github_repo, updated_days
    PagerDuty      : pagerduty_org_slug, lookback_hours
    Linear         : linear_team_id, lookback_hours
    Confluence     : confluence_space_key, lookback_hours
    Datadog        : datadog_tags (list), lookback_hours
    Sentry         : sentry_org_slug, sentry_project_slug
    Notion         : notion_database_id, lookback_hours
    Google Calendar: google_calendar_id, lookback_hours
    SendGrid       : sendgrid_lookback_days
    """
    logger.info("[L1-COLLECTOR] Pipeline %s — starting collection", state.pipeline_id)
    state.status = PipelineStatus.COLLECTING

    meta   = state.trigger_metadata
    errors: list[str] = []

    # ── Run all 11 adapters concurrently ──────────────────────────────────────
    (
        slack_messages,
        jira_tickets,
        github_prs,
        pd_incidents,
        linear_issues,
        confluence_pages,
        dd_monitors,
        sentry_issues,
        notion_pages,
        calendar_events,
        sg_bounces,
    ) = await asyncio.gather(
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

    corpus = CollectedCorpus(
        pipeline_id=state.pipeline_id,
        collected_at=datetime.now(timezone.utc),
        slack_messages=slack_messages,
        jira_tickets=jira_tickets,
        github_prs=github_prs,
        pagerduty_incidents=pd_incidents,
        linear_issues=linear_issues,
        confluence_pages=confluence_pages,
        datadog_monitors=dd_monitors,
        sentry_issues=sentry_issues,
        notion_pages=notion_pages,
        calendar_events=calendar_events,
        sendgrid_bounces=sg_bounces,
        collection_errors=errors,
    )

    state.corpus = corpus

    logger.info(
        "[L1-COLLECTOR] Pipeline %s — collected %d items "
        "(%d Slack, %d JIRA, %d GitHub, %d PagerDuty, %d Linear, "
        "%d Confluence, %d Datadog, %d Sentry, %d Notion, "
        "%d Calendar, %d SendGrid). Errors: %d",
        state.pipeline_id,
        corpus.total_items,
        len(slack_messages),
        len(jira_tickets),
        len(github_prs),
        len(pd_incidents),
        len(linear_issues),
        len(confluence_pages),
        len(dd_monitors),
        len(sentry_issues),
        len(notion_pages),
        len(calendar_events),
        len(sg_bounces),
        len(errors),
    )

    # Fail the pipeline only if we collected nothing at all
    if corpus.total_items == 0:
        state.status      = PipelineStatus.FAILED
        state.error_stage = "COLLECTOR"
        state.error_message = (
            "All adapters returned zero items. "
            f"Errors: {'; '.join(errors) if errors else 'No adapters configured'}"
        )
        logger.error(
            "[L1-COLLECTOR] Pipeline %s — zero items collected. Halting.",
            state.pipeline_id,
        )

    return state


# ── Safe wrappers — circuit-breaker + timeout + catch-all ────────────────────
# Pattern per wrapper:
#   1. Extract relevant keys from trigger_metadata; return [] when the key
#      required to identify the target resource is absent or empty.
#   2. Delegate to the adapter inside circuit_breaker.call(), which wraps
#      the coroutine in asyncio.wait_for(timeout=ADAPTER_TIMEOUT).
#   3. Catch CircuitOpenError, asyncio.TimeoutError, the adapter's own
#      CollectorError, and a bare Exception catch-all.  All failures append
#      to the shared errors list and return [].


async def _safe_slack_collect(meta: dict, errors: list[str]) -> list:
    channel_id     = meta.get("slack_channel_id", "")
    lookback_hours = int(meta.get("lookback_hours", 72))
    if not channel_id:
        return []
    try:
        return await _slack_breaker.call(
            lambda: SlackAdapter().fetch_messages(channel_id, lookback_hours)
        )
    except CircuitOpenError as e:
        errors.append(f"Slack: {e}")
        logger.warning("[CIRCUIT-BREAKER:slack] OPEN — skipping Slack adapter: %s", e)
        return []
    except asyncio.TimeoutError:
        errors.append(f"Slack: adapter timed out after {ADAPTER_TIMEOUT:.0f}s")
        logger.warning("[L1-COLLECTOR] Slack adapter timed out")
        return []
    except CollectorError as e:
        errors.append(f"Slack: {e}")
        logger.warning("[L1-COLLECTOR] Slack error: %s", e)
        return []
    except Exception as e:
        errors.append(f"Slack: unexpected error — {e}")
        logger.exception("[L1-COLLECTOR] Slack unexpected error")
        return []


async def _safe_jira_collect(meta: dict, errors: list[str]) -> list:
    labels       = meta.get("jira_labels", [])
    updated_days = int(meta.get("updated_days", 7))
    jql          = meta.get("jira_jql")
    try:
        return await _jira_breaker.call(
            lambda: JiraAdapter().fetch_tickets(
                labels=labels, jql=jql, updated_days=updated_days
            )
        )
    except CircuitOpenError as e:
        errors.append(f"JIRA: {e}")
        logger.warning("[CIRCUIT-BREAKER:jira] OPEN — skipping JIRA adapter: %s", e)
        return []
    except asyncio.TimeoutError:
        errors.append(f"JIRA: adapter timed out after {ADAPTER_TIMEOUT:.0f}s")
        logger.warning("[L1-COLLECTOR] JIRA adapter timed out")
        return []
    except Exception as e:
        errors.append(f"JIRA: {e}")
        logger.warning("[L1-COLLECTOR] JIRA error: %s", e)
        return []


async def _safe_github_collect(meta: dict, errors: list[str]) -> list:
    owner        = meta.get("github_owner", "")
    repo         = meta.get("github_repo", "")
    updated_days = int(meta.get("updated_days", 7))
    if not owner or not repo:
        return []
    try:
        return await _github_breaker.call(
            lambda: GitHubAdapter().fetch_pull_requests(
                owner, repo, updated_days=updated_days
            )
        )
    except CircuitOpenError as e:
        errors.append(f"GitHub: {e}")
        logger.warning("[CIRCUIT-BREAKER:github] OPEN — skipping GitHub adapter: %s", e)
        return []
    except asyncio.TimeoutError:
        errors.append(f"GitHub: adapter timed out after {ADAPTER_TIMEOUT:.0f}s")
        logger.warning("[L1-COLLECTOR] GitHub adapter timed out")
        return []
    except Exception as e:
        errors.append(f"GitHub: {e}")
        logger.warning("[L1-COLLECTOR] GitHub error: %s", e)
        return []


async def _safe_pagerduty_collect(meta: dict, errors: list[str]) -> list:
    """
    Collects PagerDuty incidents.

    trigger_metadata keys
    ---------------------
    pagerduty_org_slug : str  — PagerDuty subdomain (e.g. "acme").
                                When absent the adapter uses settings.pagerduty_api_key
                                unconditionally, so we always attempt collection when
                                the key is configured.
    lookback_hours     : int  — defaults to 72.
    """
    lookback_hours = int(meta.get("lookback_hours", 72))
    try:
        return await _pagerduty_breaker.call(
            lambda: PagerDutyAdapter().fetch_incidents(lookback_hours=lookback_hours)
        )
    except CircuitOpenError as e:
        errors.append(f"PagerDuty: {e}")
        logger.warning("[CIRCUIT-BREAKER:pagerduty] OPEN — skipping PagerDuty adapter: %s", e)
        return []
    except asyncio.TimeoutError:
        errors.append(f"PagerDuty: adapter timed out after {ADAPTER_TIMEOUT:.0f}s")
        logger.warning("[L1-COLLECTOR] PagerDuty adapter timed out")
        return []
    except Exception as e:
        errors.append(f"PagerDuty: {e}")
        logger.warning("[L1-COLLECTOR] PagerDuty error: %s", e)
        return []


async def _safe_linear_collect(meta: dict, errors: list[str]) -> list:
    """
    Collects Linear issues.

    trigger_metadata keys
    ---------------------
    linear_team_id : str  — Linear team ID to scope the query.
                            When absent, all teams the token can see are queried.
    lookback_hours : int  — defaults to 72.
    """
    team_id        = meta.get("linear_team_id") or None
    lookback_hours = int(meta.get("lookback_hours", 72))
    try:
        return await _linear_breaker.call(
            lambda: LinearAdapter().fetch_issues(
                lookback_hours=lookback_hours, team_id=team_id
            )
        )
    except CircuitOpenError as e:
        errors.append(f"Linear: {e}")
        logger.warning("[CIRCUIT-BREAKER:linear] OPEN — skipping Linear adapter: %s", e)
        return []
    except asyncio.TimeoutError:
        errors.append(f"Linear: adapter timed out after {ADAPTER_TIMEOUT:.0f}s")
        logger.warning("[L1-COLLECTOR] Linear adapter timed out")
        return []
    except Exception as e:
        errors.append(f"Linear: {e}")
        logger.warning("[L1-COLLECTOR] Linear error: %s", e)
        return []


async def _safe_confluence_collect(meta: dict, errors: list[str]) -> list:
    """
    Collects Confluence pages from a given space.

    trigger_metadata keys
    ---------------------
    confluence_space_key : str  — Confluence space key (e.g. "ENG").
                                  Required; returns [] when absent.
    lookback_hours       : int  — defaults to 72.
    """
    space_key      = meta.get("confluence_space_key", "")
    lookback_hours = int(meta.get("lookback_hours", 72))
    if not space_key:
        return []
    try:
        return await _confluence_breaker.call(
            lambda: ConfluenceAdapter().fetch_pages(
                space_key=space_key, lookback_hours=lookback_hours
            )
        )
    except CircuitOpenError as e:
        errors.append(f"Confluence: {e}")
        logger.warning("[CIRCUIT-BREAKER:confluence] OPEN — skipping Confluence adapter: %s", e)
        return []
    except asyncio.TimeoutError:
        errors.append(f"Confluence: adapter timed out after {ADAPTER_TIMEOUT:.0f}s")
        logger.warning("[L1-COLLECTOR] Confluence adapter timed out")
        return []
    except Exception as e:
        errors.append(f"Confluence: {e}")
        logger.warning("[L1-COLLECTOR] Confluence error: %s", e)
        return []


async def _safe_datadog_collect(meta: dict, errors: list[str]) -> list:
    """
    Collects Datadog monitors.

    trigger_metadata keys
    ---------------------
    datadog_tags   : list[str]  — Datadog tag filters (e.g. ["env:prod"]).
                                  Defaults to no tag filter.
    lookback_hours : int        — passed as the monitor lookback window;
                                  defaults to 72.
    """
    tags           = meta.get("datadog_tags") or None
    lookback_hours = int(meta.get("lookback_hours", 72))
    try:
        return await _datadog_breaker.call(
            lambda: DatadogAdapter().fetch_monitors(
                lookback_hours=lookback_hours, tags=tags
            )
        )
    except CircuitOpenError as e:
        errors.append(f"Datadog: {e}")
        logger.warning("[CIRCUIT-BREAKER:datadog] OPEN — skipping Datadog adapter: %s", e)
        return []
    except asyncio.TimeoutError:
        errors.append(f"Datadog: adapter timed out after {ADAPTER_TIMEOUT:.0f}s")
        logger.warning("[L1-COLLECTOR] Datadog adapter timed out")
        return []
    except Exception as e:
        errors.append(f"Datadog: {e}")
        logger.warning("[L1-COLLECTOR] Datadog error: %s", e)
        return []


async def _safe_sentry_collect(meta: dict, errors: list[str]) -> list:
    """
    Collects Sentry issues for a given org / project.

    trigger_metadata keys
    ---------------------
    sentry_org_slug     : str  — Sentry organisation slug. Required.
    sentry_project_slug : str  — Sentry project slug. Optional; when absent
                                 all projects in the org are queried.
    """
    org_slug     = meta.get("sentry_org_slug", "")
    project_slug = meta.get("sentry_project_slug") or None
    if not org_slug:
        return []
    try:
        return await _sentry_breaker.call(
            lambda: SentryAdapter().fetch_issues(
                organization_slug=org_slug, project_slug=project_slug
            )
        )
    except CircuitOpenError as e:
        errors.append(f"Sentry: {e}")
        logger.warning("[CIRCUIT-BREAKER:sentry] OPEN — skipping Sentry adapter: %s", e)
        return []
    except asyncio.TimeoutError:
        errors.append(f"Sentry: adapter timed out after {ADAPTER_TIMEOUT:.0f}s")
        logger.warning("[L1-COLLECTOR] Sentry adapter timed out")
        return []
    except Exception as e:
        errors.append(f"Sentry: {e}")
        logger.warning("[L1-COLLECTOR] Sentry error: %s", e)
        return []


async def _safe_notion_collect(meta: dict, errors: list[str]) -> list:
    """
    Collects Notion pages from a database.

    trigger_metadata keys
    ---------------------
    notion_database_id : str  — Notion database ID to query. Required.
    lookback_hours     : int  — defaults to 72.
    """
    database_id    = meta.get("notion_database_id", "")
    lookback_hours = int(meta.get("lookback_hours", 72))
    if not database_id:
        return []
    try:
        return await _notion_breaker.call(
            lambda: NotionAdapter().fetch_pages(
                database_id=database_id, lookback_hours=lookback_hours
            )
        )
    except CircuitOpenError as e:
        errors.append(f"Notion: {e}")
        logger.warning("[CIRCUIT-BREAKER:notion] OPEN — skipping Notion adapter: %s", e)
        return []
    except asyncio.TimeoutError:
        errors.append(f"Notion: adapter timed out after {ADAPTER_TIMEOUT:.0f}s")
        logger.warning("[L1-COLLECTOR] Notion adapter timed out")
        return []
    except Exception as e:
        errors.append(f"Notion: {e}")
        logger.warning("[L1-COLLECTOR] Notion error: %s", e)
        return []


async def _safe_gcal_collect(meta: dict, errors: list[str]) -> list:
    """
    Collects Google Calendar events.

    trigger_metadata keys
    ---------------------
    google_calendar_id : str  — Calendar ID to query.
                                Defaults to "primary" (falls back to
                                settings.google_calendar_id).
    lookback_hours     : int  — defaults to 72.
    """
    calendar_id    = meta.get("google_calendar_id", "primary")
    lookback_hours = int(meta.get("lookback_hours", 72))
    try:
        return await _gcal_breaker.call(
            lambda: GoogleCalendarAdapter().fetch_events(
                calendar_id=calendar_id, lookback_hours=lookback_hours
            )
        )
    except CircuitOpenError as e:
        errors.append(f"GoogleCalendar: {e}")
        logger.warning("[CIRCUIT-BREAKER:google_calendar] OPEN — skipping Google Calendar adapter: %s", e)
        return []
    except asyncio.TimeoutError:
        errors.append(f"GoogleCalendar: adapter timed out after {ADAPTER_TIMEOUT:.0f}s")
        logger.warning("[L1-COLLECTOR] Google Calendar adapter timed out")
        return []
    except Exception as e:
        errors.append(f"GoogleCalendar: {e}")
        logger.warning("[L1-COLLECTOR] Google Calendar error: %s", e)
        return []


async def _safe_sendgrid_collect(meta: dict, errors: list[str]) -> list:
    """
    Collects SendGrid bounce records.

    trigger_metadata keys
    ---------------------
    sendgrid_lookback_days : int  — number of days to look back for bounces,
                                    converted to hours for the adapter call.
                                    Defaults to 7 days (168 hours).
    """
    lookback_days  = int(meta.get("sendgrid_lookback_days", 7))
    lookback_hours = lookback_days * 24
    try:
        return await _sendgrid_breaker.call(
            lambda: SendGridAdapter().fetch_bounces(lookback_hours=lookback_hours)
        )
    except CircuitOpenError as e:
        errors.append(f"SendGrid: {e}")
        logger.warning("[CIRCUIT-BREAKER:sendgrid] OPEN — skipping SendGrid adapter: %s", e)
        return []
    except asyncio.TimeoutError:
        errors.append(f"SendGrid: adapter timed out after {ADAPTER_TIMEOUT:.0f}s")
        logger.warning("[L1-COLLECTOR] SendGrid adapter timed out")
        return []
    except Exception as e:
        errors.append(f"SendGrid: {e}")
        logger.warning("[L1-COLLECTOR] SendGrid error: %s", e)
        return []
