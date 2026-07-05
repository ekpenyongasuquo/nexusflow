# NexusFlow — Bob Shell Audit Trail

**Generated:** 2026-07-05 03:13:19 UTC  
**Project root:** `C:\Users\DELL\.bob\playground\nexusflow`  
**Deployment:** https://nexusflow-api-e6u8.onrender.com  
**Audit tool:** IBM Bob (Agent mode)

---

## 1. All Python Files with Line Counts

> Sorted alphabetically. `__init__.py` stubs included.

| File | Lines |
|------|------:|
| `__init__.py` | 1 |
| `adapters\__init__.py` | 0 |
| `adapters\confluence.py` | 301 |
| `adapters\datadog.py` | 234 |
| `adapters\github.py` | 97 |
| `adapters\google_calendar.py` | 258 |
| `adapters\jira.py` | 163 |
| `adapters\linear.py` | 278 |
| `adapters\notion.py` | 273 |
| `adapters\pagerduty.py` | 202 |
| `adapters\sendgrid.py` | 286 |
| `adapters\sentry.py` | 240 |
| `adapters\slack.py` | 162 |
| `agents\__init__.py` | 0 |
| `agents\collector.py` | 573 |
| `agents\executor.py` | 230 |
| `agents\recommender.py` | 194 |
| `agents\synthesiser.py` | 275 |
| `agents\validator.py` | 196 |
| `api\__init__.py` | 0 |
| `api\main.py` | 79 |
| `api\middleware\__init__.py` | 0 |
| `api\middleware\auth.py` | 71 |
| `api\routes\__init__.py` | 0 |
| `api\routes\admin.py` | 109 |
| `api\routes\auth.py` | 100 |
| `api\routes\pipelines.py` | 310 |
| `core\__init__.py` | 0 |
| `core\approval_gateway.py` | 328 |
| `core\graph\__init__.py` | 1 |
| `core\indexer.py` | 105 |
| `core\memory.py` | 399 |
| `core\models.py` | 336 |
| `core\observability.py` | 329 |
| `core\policy\__init__.py` | 1 |
| `core\settings.py` | 70 |
| `core\state\__init__.py` | 0 |
| `core\state\pipeline.py` | 322 |
| `db\__init__.py` | 0 |
| `db\audit.py` | 92 |
| `db\models.py` | 122 |
| `db\session.py` | 68 |
| `tests\__init__.py` | 1 |
| `tests\adapters\__init__.py` | 1 |
| `tests\adapters\test_slack.py` | 121 |
| `tests\agents\__init__.py` | 1 |
| `tests\agents\test_collector.py` | 266 |
| `tests\agents\test_executor.py` | 147 |
| `tests\agents\test_recommender.py` | 116 |
| `tests\agents\test_validator.py` | 135 |
| `tests\api\__init__.py` | 1 |
| `tests\api\test_routes.py` | 161 |
| `tests\test_audit.py` | 96 |
| `tests\test_indexer.py` | 81 |
| `tests\test_models.py` | 104 |

---

## 2. Total Lines of Code

```
Total Python lines of code: 8,036
```

---

## 3. MCP Adapters — `nexusflow/adapters/`

| # | Adapter File | Lines | Integration |
|---|---|---:|---|
| 1 | `confluence.py` | 301 | Atlassian Confluence REST API v2 |
| 2 | `datadog.py` | 234 | Datadog API v1 — monitors & events |
| 3 | `github.py` | 97 | GitHub REST API v3 — pull requests |
| 4 | `google_calendar.py` | 258 | Google Calendar API v3 — events |
| 5 | `jira.py` | 163 | Atlassian JIRA REST API v3 — tickets |
| 6 | `linear.py` | 278 | Linear GraphQL API — issues |
| 7 | `notion.py` | 273 | Notion REST API v1 — database pages |
| 8 | `pagerduty.py` | 202 | PagerDuty REST API v2 — incidents |
| 9 | `sendgrid.py` | 286 | SendGrid REST API v3 — bounces & stats |
| 10 | `sentry.py` | 240 | Sentry REST API v0 — issues & events |
| 11 | `slack.py` | 162 | Slack Web API — channel messages |

**Total: 11 MCP adapters · 2,494 lines**

---

## 4. Agent Files — `nexusflow/agents/`

| # | Agent File | Lines | Role |
|---|---|---:|---|
| 1 | `collector.py` | 573 | L1 — concurrent MCP signal collection (11 adapters, circuit breakers) |
| 2 | `executor.py` | 230 | L5 — decision execution, Slack/JIRA dispatch, audit receipt write |
| 3 | `recommender.py` | 194 | L4 — LLM-driven option generation with ROI projections |
| 4 | `synthesiser.py` | 275 | L2 — hybrid FAISS + BM25 retrieval, LLM brief generation, episodic memory |
| 5 | `validator.py` | 196 | L3 — PII detection, policy rule evaluation, compliance halting |

**Total: 5 agents · 1,468 lines**

---

## 5. Core Modules — `nexusflow/core/`

| # | Module | Lines | Purpose |
|---|---|---:|---|
| 1 | `approval_gateway.py` | 328 | Pre-execution gate — 4 checks: presence, option validity, staleness, role authority |
| 2 | `indexer.py` | 105 | Hybrid FAISS + BM25 document index for Synthesiser retrieval |
| 3 | `memory.py` | 399 | Episodic memory store — BM25 recall, SQLite persistence, deque cache |
| 4 | `models.py` | 336 | All domain types — 30+ Pydantic models, `CollectedCorpus` (11 adapters) |
| 5 | `observability.py` | 329 | OpenTelemetry tracing, Prometheus metrics, structured logging |
| 6 | `settings.py` | 70 | Pydantic-settings central config — 30+ env vars |
| 7 | `state\pipeline.py` | 322 | LangGraph state machine — collect→synthesise→validate→recommend→execute |

**Total: 7 core modules · 1,889 lines**

---

## 6. Project Summary

| Metric | Value |
|--------|-------|
| **Total Python files** | 55 |
| **Total lines of code** | 8,036 |
| **MCP adapters** | 11 |
| **LangGraph agents** | 5 |
| **Test files** | 9 |
| **DB models** | 2 databases (PostgreSQL main + SQLite audit) |
| **API routes** | 3 routers (auth, pipelines, admin) |
| **Deployment URL** | https://nexusflow-api-e6u8.onrender.com |

---

## 7. Architecture Overview

```
TRIGGER (API / webhook)
        │
        ▼
┌───────────────────────────────────────────────────────────┐
│                    LangGraph Pipeline                     │
│                                                           │
│  [L1 Collector] ──────────────────────────────────────►  │
│   11 MCP adapters in asyncio.gather()                     │
│   Per-adapter circuit breaker + 15s timeout               │
│        │                                                  │
│        ▼                                                  │
│  [L2 Synthesiser] ─────────────────────────────────────►  │
│   FAISS + BM25 hybrid retrieval                           │
│   OpenRouter LLM (Llama 3.3 70B)                          │
│   Episodic memory recall (BM25, SQLite)                   │
│        │                                                  │
│        ▼                                                  │
│  [confidence < 0.7?] ──yes──► second collect pass         │
│        │ no                                               │
│        ▼                                                  │
│  [L3 Validator] ───────────────────────────────────────►  │
│   PII detection, policy rules, compliance halt            │
│        │                                                  │
│        ▼                                                  │
│  [L4 Recommender] ─────────────────────────────────────►  │
│   3 decision options with ROI projections                 │
│        │                                                  │
│        ▼                                                  │
│  ── AWAITING_HUMAN ──  (pipeline pauses)                  │
└───────────────────────────────────────────────────────────┘
        │
        │  POST /pipelines/{id}/approve
        ▼
┌───────────────────────────────────────────────────────────┐
│  [Approval Gateway]                                       │
│   ✓ decision present                                      │
│   ✓ option ID resolves to known option (anti-hallucination)│
│   ✓ decision age ≤ 30 min (anti-stale-replay)             │
│   ✓ approver role ≥ required role (policy enforcement)    │
│        │                                                  │
│        ▼                                                  │
│  [L5 Executor]                                            │
│   Slack notification, JIRA ticket, audit receipt          │
│   SHA-256 hash chain (append-only SQLite)                 │
│   Episodic memory store (for future synthesis)            │
└───────────────────────────────────────────────────────────┘
```

---

## 8. Git Commit History

```
b671574  Update live demo URL to deployed Render instance
4754a42  Fix: remove torch/sentence-transformers - too large for free tier
a9cd1c6  Fix: exclude pool_size/max_overflow for SQLite engine
879158c  Fix: resolve langgraph/langchain-core version conflict
0537bb2  Fix: resolve langgraph/langchain-core version conflict
19f7082  Fix: pin Python 3.12, update faiss-cpu to 1.13.0
8ba6c43  Initial commit - NexusFlow autonomous decision intelligence platform
```

---

## 9. Package Layout

```
nexusflow/
├── nexusflow/
│   ├── adapters/          # 11 MCP adapters (pure httpx, no SDKs)
│   │   ├── confluence.py
│   │   ├── datadog.py
│   │   ├── github.py
│   │   ├── google_calendar.py
│   │   ├── jira.py
│   │   ├── linear.py
│   │   ├── notion.py
│   │   ├── pagerduty.py
│   │   ├── sendgrid.py
│   │   ├── sentry.py
│   │   └── slack.py
│   ├── agents/            # 5 LangGraph agent entry points
│   │   ├── collector.py
│   │   ├── executor.py
│   │   ├── recommender.py
│   │   ├── synthesiser.py
│   │   └── validator.py
│   ├── api/               # FastAPI application
│   │   ├── main.py
│   │   ├── middleware/
│   │   │   └── auth.py
│   │   └── routes/
│   │       ├── admin.py
│   │       ├── auth.py
│   │       └── pipelines.py
│   ├── core/              # Domain logic — no I/O dependencies
│   │   ├── approval_gateway.py
│   │   ├── indexer.py
│   │   ├── memory.py
│   │   ├── models.py
│   │   ├── observability.py
│   │   ├── settings.py
│   │   ├── graph/
│   │   ├── policy/
│   │   └── state/
│   │       └── pipeline.py
│   ├── db/                # SQLAlchemy ORM + audit chain
│   │   ├── audit.py
│   │   ├── models.py
│   │   └── session.py
│   └── tests/             # pytest test suite
│       ├── adapters/
│       ├── agents/
│       ├── api/
│       ├── test_audit.py
│       ├── test_indexer.py
│       └── test_models.py
├── BOB_SHELL_AUDIT_TRAIL.md
└── ...
```

---

*Audit trail generated by IBM Bob — Agent mode.*  
*All file counts and line counts reflect live filesystem state at time of generation.*
