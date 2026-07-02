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
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Awaitable, Callable, TypeVar

from nexusflow.adapters.github import GitHubAdapter
from nexusflow.adapters.jira import JiraAdapter
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
_slack_breaker  = CircuitBreaker("slack")
_jira_breaker   = CircuitBreaker("jira")
_github_breaker = CircuitBreaker("github")


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
    """
    logger.info("[L1-COLLECTOR] Pipeline %s — starting collection", state.pipeline_id)
    state.status = PipelineStatus.COLLECTING

    meta   = state.trigger_metadata
    errors: list[str] = []

    # ── Run all adapters concurrently ─────────────────────────────────────────
    slack_messages, jira_tickets, github_prs = await asyncio.gather(
        _safe_slack_collect(meta, errors),
        _safe_jira_collect(meta, errors),
        _safe_github_collect(meta, errors),
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
        "[L1-COLLECTOR] Pipeline %s — collected %d items "
        "(%d Slack, %d JIRA, %d GitHub). Errors: %d",
        state.pipeline_id,
        corpus.total_items,
        len(slack_messages),
        len(jira_tickets),
        len(github_prs),
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

async def _safe_slack_collect(meta: dict, errors: list[str]) -> list:
    channel_id     = meta.get("slack_channel_id", "")
    lookback_hours = meta.get("lookback_hours", 72)
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
    updated_days = meta.get("updated_days", 7)
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
    updated_days = meta.get("updated_days", 7)
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
