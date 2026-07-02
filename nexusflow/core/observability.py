"""
nexusflow/core/observability.py
Lightweight observability layer for the NexusFlow agent pipeline.

Public surface
--------------
  make_run_id(pipeline_id)         → deterministic run_id string
  trace_node(node_name)            → async decorator factory
  PipelineMetrics                  → dataclass for one completed run
  MetricsCollector                 → in-memory singleton (last 100 runs)
  get_metrics_summary()            → JSON-safe dict for the /metrics endpoint

Usage in pipeline nodes
-----------------------
  from nexusflow.core.observability import trace_node

  @trace_node("collect")
  async def run_collector_agent(state: PipelineState) -> PipelineState:
      ...

Usage in API
------------
  from nexusflow.core.observability import get_metrics_summary

  @router.get("/metrics")
  async def metrics():
      return get_metrics_summary()
"""
from __future__ import annotations

import functools
import logging
import time
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Callable, Deque

from nexusflow.core.models import PipelineState

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
_MAX_STORED_RUNS = 100   # MetricsCollector ring-buffer capacity


# ── run_id ────────────────────────────────────────────────────────────────────

def make_run_id(pipeline_id: str) -> str:
    """
    Build a deterministic run_id that correlates every log line for one
    pipeline invocation across all agent nodes.

    Format: nf-{pipeline_id[:8]}-{epoch_ms}

    The epoch_ms suffix is computed once per trace_node entry so that the
    run_id is unique even when the same pipeline_id is re-run (e.g. retry).

    Example: nf-a3f7c201-1718123456789
    """
    epoch_ms = int(time.time() * 1000)
    return f"nf-{pipeline_id[:8]}-{epoch_ms}"


# ── @trace_node decorator ─────────────────────────────────────────────────────

def trace_node(node_name: str) -> Callable:
    """
    Parametrised async decorator factory.

    Wraps any async agent function with signature:
        async (state: PipelineState) -> PipelineState

    Emits three structured log lines per call:
        [TRACE]       entry  — node_name, run_id, utc timestamp
        [TRACE]       exit   — node_name, run_id, duration_ms
        [TRACE-ERROR] error  — node_name, run_id, exc type+message (then re-raises)

    Usage:
        @trace_node("collect")
        async def run_collector_agent(state: PipelineState) -> PipelineState:
            ...

    The run_id is derived from state.pipeline_id so it is present on the very
    first log line — before any agent logic executes.
    """
    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        async def wrapper(state: PipelineState) -> PipelineState:
            run_id   = make_run_id(state.pipeline_id)
            entered  = time.perf_counter()
            utc_now  = datetime.now(timezone.utc).isoformat()

            logger.info(
                "[TRACE] enter node=%s run_id=%s pipeline_id=%s at=%s",
                node_name,
                run_id,
                state.pipeline_id,
                utc_now,
                extra={"run_id": run_id, "node": node_name},
            )

            try:
                result: PipelineState = await fn(state)
            except Exception as exc:
                duration_ms = (time.perf_counter() - entered) * 1000
                logger.error(
                    "[TRACE-ERROR] node=%s run_id=%s pipeline_id=%s "
                    "duration_ms=%.1f exc_type=%s exc=%s",
                    node_name,
                    run_id,
                    state.pipeline_id,
                    duration_ms,
                    type(exc).__name__,
                    exc,
                    extra={"run_id": run_id, "node": node_name},
                )
                raise

            duration_ms = (time.perf_counter() - entered) * 1000
            logger.info(
                "[TRACE] exit  node=%s run_id=%s pipeline_id=%s duration_ms=%.1f",
                node_name,
                run_id,
                state.pipeline_id,
                duration_ms,
                extra={"run_id": run_id, "node": node_name},
            )

            # Attach timing to the result state so MetricsCollector can read it
            # without coupling to the decorator internals.  We store it in a
            # transient attribute — PipelineState.model_config allows extras via
            # Config.extra or we stash on __dict__ directly since Pydantic v2
            # BaseModel instances support arbitrary attribute assignment when
            # model_config extra != "forbid" (PipelineState uses default "ignore").
            # We use a private sidecar dict attached to the state object instead
            # of polluting model fields.
            if not hasattr(result, "_trace"):
                object.__setattr__(result, "_trace", {})
            result._trace[node_name] = duration_ms  # type: ignore[attr-defined]

            return result

        return wrapper
    return decorator


# ── PipelineMetrics dataclass ─────────────────────────────────────────────────

@dataclass
class PipelineMetrics:
    """
    Snapshot of one completed pipeline run, captured after the final node.

    Fields
    ------
    pipeline_id       Pipeline UUID (first 8 chars used in run_id)
    run_id            Correlation ID: nf-{pid8}-{epoch_ms}
    trigger_type      TriggerType enum value as string
    started_at        ISO-8601 UTC timestamp of pipeline creation
    completed_at      ISO-8601 UTC timestamp when metrics were captured
    node_durations    Wall-clock milliseconds per named node
    total_duration_ms Sum of all node_durations values
    items_collected   corpus.total_items at collection time (0 if collection failed)
    confidence_score  brief.confidence_score (0.0 if brief is absent)
    outcome           Human decision outcome string, or final pipeline status
    error_message     Set if the pipeline ended in FAILED or HALTED_COMPLIANCE
    """
    pipeline_id:      str
    run_id:           str
    trigger_type:     str
    started_at:       str
    completed_at:     str
    node_durations:   dict[str, float]    = field(default_factory=dict)
    total_duration_ms: float              = 0.0
    items_collected:  int                 = 0
    confidence_score: float               = 0.0
    outcome:          str                 = ""
    error_message:    str | None          = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe dict (all values are primitives or dicts/lists)."""
        return asdict(self)

    @classmethod
    def from_state(cls, state: PipelineState) -> "PipelineMetrics":
        """
        Build a PipelineMetrics from a completed PipelineState.
        Reads _trace side-car if present (populated by @trace_node wrappers).
        """
        pid = state.pipeline_id

        # ── Node durations (from @trace_node side-car) ────────────────────────
        trace: dict[str, float] = getattr(state, "_trace", {})
        total_ms = sum(trace.values())

        # ── Items collected ───────────────────────────────────────────────────
        items = 0
        if state.corpus:
            items = state.corpus.total_items

        # ── Confidence score ──────────────────────────────────────────────────
        confidence = 0.0
        if state.brief:
            confidence = float(state.brief.confidence_score)

        # ── Outcome ───────────────────────────────────────────────────────────
        # Prefer the human decision outcome; fall back to pipeline status.
        if state.human_decision:
            outcome = str(state.human_decision.outcome)
        else:
            outcome = str(state.status)

        run_id = make_run_id(pid)

        return cls(
            pipeline_id      = pid,
            run_id           = run_id,
            trigger_type     = str(state.trigger_type),
            started_at       = state.created_at.isoformat()
                               if hasattr(state.created_at, "isoformat")
                               else str(state.created_at),
            completed_at     = datetime.now(timezone.utc).isoformat(),
            node_durations   = dict(trace),
            total_duration_ms= round(total_ms, 2),
            items_collected  = items,
            confidence_score = round(confidence, 4),
            outcome          = outcome,
            error_message    = state.error_message,
        )


# ── MetricsCollector singleton ────────────────────────────────────────────────

class MetricsCollector:
    """
    In-memory ring-buffer of the last _MAX_STORED_RUNS pipeline runs.

    Thread / coroutine safety
    -------------------------
    All mutations are synchronous appends to a collections.deque, which is
    GIL-protected in CPython.  In a single-process asyncio app this is safe
    without an explicit lock — no two coroutines mutate the deque concurrently
    because the event loop executes them cooperatively.

    Usage
    -----
    collector = MetricsCollector.instance()
    collector.record(PipelineMetrics.from_state(final_state))
    """

    _singleton: MetricsCollector | None = None

    def __init__(self) -> None:
        self._runs: Deque[PipelineMetrics] = deque(maxlen=_MAX_STORED_RUNS)

    # ── Singleton accessor ────────────────────────────────────────────────────

    @classmethod
    def instance(cls) -> "MetricsCollector":
        """Return the process-wide singleton, creating it on first call."""
        if cls._singleton is None:
            cls._singleton = cls()
        return cls._singleton

    # ── Write ─────────────────────────────────────────────────────────────────

    def record(self, metrics: PipelineMetrics) -> None:
        """
        Append one completed run to the ring-buffer.
        When the buffer is full the oldest entry is automatically evicted.
        """
        self._runs.append(metrics)
        logger.debug(
            "[METRICS] recorded run_id=%s pipeline_id=%s outcome=%s "
            "total_ms=%.1f stored_runs=%d",
            metrics.run_id,
            metrics.pipeline_id,
            metrics.outcome,
            metrics.total_duration_ms,
            len(self._runs),
        )

    # ── Read ──────────────────────────────────────────────────────────────────

    def all_runs(self) -> list[PipelineMetrics]:
        """Return all stored runs, oldest first."""
        return list(self._runs)

    def last(self, n: int = 10) -> list[PipelineMetrics]:
        """Return up to the *n* most-recent runs, newest first."""
        runs = list(self._runs)
        return list(reversed(runs))[:n]

    def __len__(self) -> int:
        return len(self._runs)


# ── /metrics summary dict ─────────────────────────────────────────────────────

def get_metrics_summary() -> dict[str, Any]:
    """
    Return a JSON-safe summary dict suitable for a FastAPI /metrics endpoint.

    Shape
    -----
    {
        "stored_runs": int,          # number of runs currently in memory
        "capacity":    int,          # ring-buffer maximum (100)
        "recent_runs": [             # last 10 runs, newest first
            {
                "pipeline_id":       str,
                "run_id":            str,
                "trigger_type":      str,
                "started_at":        str,   # ISO-8601
                "completed_at":      str,   # ISO-8601
                "node_durations":    { node_name: ms, ... },
                "total_duration_ms": float,
                "items_collected":   int,
                "confidence_score":  float,
                "outcome":           str,
                "error_message":     str | null,
            },
            ...
        ],
        "aggregates": {
            "total_pipelines_run":   int,
            "success_rate_pct":      float,   # % with non-error outcome
            "avg_total_duration_ms": float,
            "avg_items_collected":   float,
            "avg_confidence_score":  float,
            "outcomes": {                     # count per outcome string
                "AWAITING_HUMAN": int,
                "COMPLETE":       int,
                "FAILED":         int,
                ...
            },
            "avg_node_durations": {           # mean ms per node across all runs
                "collect":     float,
                "synthesise":  float,
                ...
            },
        },
    }

    All numeric values are rounded to 2 d.p. for readability.
    """
    collector = MetricsCollector.instance()
    all_runs  = collector.all_runs()
    n         = len(all_runs)

    # ── Aggregates ────────────────────────────────────────────────────────────
    outcomes: dict[str, int] = {}
    total_dur   = 0.0
    total_items = 0
    total_conf  = 0.0
    node_totals: dict[str, float] = {}
    node_counts: dict[str, int]   = {}

    _error_outcomes = {"FAILED", "HALTED_COMPLIANCE"}

    for run in all_runs:
        outcomes[run.outcome] = outcomes.get(run.outcome, 0) + 1
        total_dur   += run.total_duration_ms
        total_items += run.items_collected
        total_conf  += run.confidence_score
        for node, ms in run.node_durations.items():
            node_totals[node] = node_totals.get(node, 0.0) + ms
            node_counts[node] = node_counts.get(node, 0) + 1

    success_count = sum(
        cnt for outcome, cnt in outcomes.items()
        if outcome not in _error_outcomes
    )
    success_rate = round(success_count / n * 100, 2) if n else 0.0

    avg_node_durations = {
        node: round(node_totals[node] / node_counts[node], 2)
        for node in node_totals
    }

    return {
        "stored_runs": n,
        "capacity":    _MAX_STORED_RUNS,
        "recent_runs": [r.to_dict() for r in collector.last(10)],
        "aggregates": {
            "total_pipelines_run":   n,
            "success_rate_pct":      success_rate,
            "avg_total_duration_ms": round(total_dur   / n, 2) if n else 0.0,
            "avg_items_collected":   round(total_items / n, 2) if n else 0.0,
            "avg_confidence_score":  round(total_conf  / n, 4) if n else 0.0,
            "outcomes":              outcomes,
            "avg_node_durations":    avg_node_durations,
        },
    }
