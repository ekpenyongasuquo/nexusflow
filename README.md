# 🌐 NexusFlow
### Autonomous Decision Intelligence for the Distributed Enterprise

[![IBM Bob](https://img.shields.io/badge/Built%20with-IBM%20Bob-054ADA?style=for-the-badge&logo=ibm)](https://ibm.biz/university-bob)
[![Challenge](https://img.shields.io/badge/Track-Wildcard%20Future%20of%20Work-00B4D8?style=for-the-badge)](https://aibuilderschallenge-bobhub.bemyapp.com)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688?style=for-the-badge)](https://fastapi.tiangolo.com)
[![Live](https://img.shields.io/badge/Live-Render-46E3B7?style=for-the-badge)](https://nexusflow-api-e6u8.onrender.com)
[![License](https://img.shields.io/badge/License-MIT-22C55E?style=for-the-badge)](LICENSE)

> **AI Builders Challenge with IBM Bob — Wildcard: Future of Work**
> *Built end-to-end with IBM Bob as the active engineering partner across Plan, Code, Orchestrator, and Shell modes*

**🔴 Live API:** https://nexusflow-api-e6u8.onrender.com
**📖 API Docs:** https://nexusflow-api-e6u8.onrender.com/docs
**📊 Metrics:** https://nexusflow-api-e6u8.onrender.com/metrics
**💻 GitHub:** https://github.com/ekpenyongasuquo/nexusflow

---

## 🔥 Problem Statement

Modern distributed enterprises lose **$200K–$3M annually per team** to what analysts call *Decision Debt* — the compounding cost of delayed, fragmented, or uninformed decisions made across siloed tools.

The root cause: **no system connects cross-functional context** from Slack, JIRA, Confluence, CRMs, and financial dashboards into a single, actionable decision with a fully auditable reasoning chain.

| Pain Point | Data |
|------------|------|
| Decision cycle time | **11 working days** average for go/no-go decisions |
| Knowledge re-discovery | **32%** of knowledge worker time (IBM Research) |
| Compliance exposure | **$14.8M** average cost per regulatory investigation |
| Manager coordination tax | **41%** of manager time spent on information handoffs |

Existing tools — Microsoft Copilot, Notion AI, Salesforce Einstein — are chat assistants **within** a single platform. None closes the full decision lifecycle autonomously across the enterprise.

---

## 💡 Solution Description

**NexusFlow** is an Autonomous Decision Intelligence Platform. It deploys five specialised AI agents that operate continuously across your enterprise tool ecosystem.

When a decision trigger fires, NexusFlow:

1. **Collects** all relevant signals from 11 connected enterprise tools via MCP adapters
2. **Synthesises** a structured decision brief with context summary, causal chain, and risk matrix
3. **Validates** for PII, policy compliance, and routes to the correct human approver
4. **Recommends** three ranked options with projected ROI outcomes
5. **Executes** the approved decision, updates all connected systems, and writes an immutable cryptographic audit receipt

**Result: Decision lifecycle reduced from 11 days to under 5 minutes.**

---

## 🤖 AI Approach & Multi-Agent Architecture

```
ENTERPRISE EVENT TRIGGER
(budget variance / project stall / escalation / compliance deadline)
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│  L1: COLLECTOR AGENTS  ×11  (IBM Granite 3.3 — 2B)             │
│  MCP adapters → Slack · JIRA · GitHub · PagerDuty · Linear     │
│  Confluence · Datadog · Sentry · Notion · Calendar · SendGrid  │
│  Circuit breaker · concurrent async · rate-limit handling       │
└───────────────────────────┬─────────────────────────────────────┘
                            │ Typed CollectedCorpus
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  L2: SYNTHESISER AGENT  (Llama 3.3 — 70B via OpenRouter)       │
│  FAISS + BM25 hybrid index → Reciprocal Rank Fusion retrieval   │
│  Episodic Memory injection → learns from past decisions         │
│  Output: DecisionBrief {context, causal_chain, risk_matrix}     │
└───────────────────────────┬─────────────────────────────────────┘
                            │ DecisionBrief
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  L3: VALIDATOR AGENT  (IBM Granite 3.3 — 8B)                   │
│  PII regex scan → pseudonymise → hot-reloadable YAML policy     │
│  Authority graph resolution → required approver role routing    │
│  Retry loop (max 2) → confidence route node                     │
└───────────────────────────┬─────────────────────────────────────┘
                            │ ValidationResult
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  L4: RECOMMENDER AGENT  (Mixtral 8×7B MoE via OpenRouter)      │
│  3 ranked options · projected ROI · risk level · impl. steps    │
│  Rule-based fallback if LLM unavailable                         │
└───────────────────────────┬─────────────────────────────────────┘
                            │ RecommendationPackage
                            ▼
               ┌────────────────────────────┐
               │   APPROVAL GATEWAY         │
               │   Role validation          │
               │   Timeout check (30 min)   │
               │   Anti-hallucination guard │
               └───────────┬────────────────┘
                           │
               ┌───────────┴────────────┐
               │   HUMAN APPROVER       │
               │   One-tap mobile UI    │
               │   Approve/Reject/      │
               │   Escalate/Defer       │
               └───────────┬────────────┘
                           │ HumanDecision
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  L5: EXECUTOR AGENT  (IBM Granite 3.3 — 2B)                    │
│  Execute approved action → update Slack · JIRA · systems       │
│  Write immutable SHA-256 audit receipt → SQLite chain           │
│  Store episode in Episodic Memory for future recall             │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🔵 IBM Bob Implementation Details

IBM Bob was used as the **active engineering partner across the entire SDLC** — not as a passive code generator.

### IBM Bob Commit Trail

| # | Commit | Lines | Bob Mode |
|---|--------|-------|----------|
| 1 | Circuit breaker on MCP collector agents | +210 | Code Mode |
| 2 | LangGraph fixes — TypedDict state, retry loop, confidence route | +295 | Code Mode |
| 3 | Observability module — trace decorator, metrics collector | +395 | Code Mode |
| 4 | Wire observability into agents, pipeline, main app | +11 | Code Mode |
| 5 | PagerDuty MCP adapter — incidents, alerts | +264 | Code Mode |
| 6 | Linear MCP adapter — issues, comments | +347 | Code Mode |
| 7 | Confluence MCP adapter — pages, comments | +356 | Code Mode |
| 8 | Datadog MCP adapter — monitors, events | +300 | Code Mode |
| 9 | Sentry MCP adapter — issues, events | +308 | Code Mode |
| 10 | Notion MCP adapter — pages, comments | +342 | Code Mode |
| 11 | Google Calendar MCP adapter — events | +324 | Code Mode |
| 12 | SendGrid MCP adapter — bounces, stats, notifications | +364 | Code Mode |
| 13 | Episodic memory store — adaptive learning, BM25 recall | +575 | Code Mode |
| 14 | Approval Gateway — role validation, timeout, executor wiring | +461 | Code Mode |
| 15 | Wire all 11 MCP adapters into Collector Agent | +352 | Code Mode |
| 16 | Bob Shell audit trail — full project inventory | +274 | Shell Mode |

**Total: 4,878 lines written by IBM Bob across 16 commits**

See full audit log: [BOB_SHELL_AUDIT_TRAIL.md](BOB_SHELL_AUDIT_TRAIL.md)

### Plan Mode — Architecture Specification

Before writing any code, IBM Bob Plan Mode:
- Generated the complete 47-entity domain model
- Designed the 12-state LangGraph pipeline state machine
- Identified and documented 19 failure modes with recovery specs

**Key Plan Mode prompt:**
```
bob plan 'Define the NexusFlow domain model. Output: entity definitions,
API contracts, state transitions, and a 12-state machine ASCII diagram.
Validate all edge cases for decision lifecycle from trigger to audit receipt.'
```

### Code Mode — Key modules built by Bob

```bash
bob code 'Add a circuit breaker to MCP collector agents so a flaky
JIRA rate-limit does not stall the entire LangGraph pipeline.
After 3 failures the circuit OPENS for 60 seconds. Log all state
changes with [CIRCUIT-BREAKER] prefix.'

bob code 'Fix LangGraph gaps: TypedDict AgentState, validator retry
loop (max 2), confidence-score route node after Synthesiser.'

bob code 'Build episodic memory store — BM25 recall, SQLite persistence,
inject top 3 past episodes into Synthesiser LLM prompt.'
```

### Shell Mode — Audit Trail

```bash
bob shell 'Scan all Python files, count LOC per module, list all
MCP adapters and agents, generate full project summary report.
Save as BOB_SHELL_AUDIT_TRAIL.md'
```

---

## 🔌 MCP Adapters — 11 Enterprise Tool Integrations

| Adapter | Data Collected | Key Methods |
|---------|---------------|-------------|
| **Slack** | Messages, threads | `fetch_messages()`, `post_message()` |
| **JIRA** | Tickets, status | `fetch_tickets()`, `create_ticket()` |
| **GitHub** | Pull requests | `fetch_pull_requests()` |
| **PagerDuty** | Incidents, alerts | `fetch_incidents()`, `fetch_alerts()` |
| **Linear** | Issues, comments | `fetch_issues()`, `fetch_comments()` |
| **Confluence** | Pages, comments | `fetch_pages()`, `fetch_comments()` |
| **Datadog** | Monitors, events | `fetch_monitors()`, `fetch_events()` |
| **Sentry** | Issues, events | `fetch_issues()`, `fetch_events()` |
| **Notion** | Pages, comments | `fetch_pages()`, `fetch_comments()` |
| **Google Calendar** | Events, upcoming | `fetch_events()`, `fetch_upcoming()` |
| **SendGrid** | Bounces, stats | `fetch_bounces()`, `send_notification()` |

All adapters implement the same pattern: circuit breaker, rate-limit handling, typed return objects, and graceful partial failure tolerance.

---

## 🛡️ Key Modules

### Circuit Breaker
Prevents a flaky adapter from stalling the entire pipeline. After 3 consecutive failures the circuit OPENS and skips that adapter for 60 seconds.

### Approval Gateway
Anti-hallucination safety layer. Validates that the human decision is present, not stale (< 30 minutes old), and that the approver role matches the policy-required role before the Executor fires.

### Episodic Memory Store
Turns NexusFlow from stateless automation into adaptive intelligence. Stores completed pipeline episodes in SQLite, recalls top 3 similar past decisions via BM25 search, and injects them into the Synthesiser LLM prompt as context.

### Observability Module
`@trace_node` decorator threads a `run_id` through every agent. `MetricsCollector` stores last 100 pipeline runs in memory. `/metrics` endpoint exposes success rates, avg durations, and outcome distributions.

### SHA-256 Audit Receipt Chain
Every pipeline action generates a receipt hashed to the previous one — tamper-evident by design. Stored in append-only SQLite. Verifiable via `GET /pipelines/audit/chain-integrity`.

---

## ⚙️ Installation & Setup

### Option A — Docker Compose
```bash
git clone https://github.com/ekpenyongasuquo/nexusflow.git
cd nexusflow
cp .env.example .env
# Edit .env with your API keys
docker compose up --build
# API: http://localhost:8000/docs
```

### Option B — Local
```bash
git clone https://github.com/ekpenyongasuquo/nexusflow.git
cd nexusflow
pip install -r requirements.txt
cp .env.example .env
uvicorn nexusflow.api.main:app --reload --port 8000
```

### Key Environment Variables

| Variable | Description |
|----------|-------------|
| `SECRET_KEY` | JWT signing key (min 32 chars) |
| `OPENROUTER_API_KEY` | Llama 3.3 + Mixtral via OpenRouter |
| `SLACK_BOT_TOKEN` | Slack bot token |
| `JIRA_API_TOKEN` | Atlassian API token |
| `GITHUB_TOKEN` | GitHub personal access token |
| `PAGERDUTY_API_KEY` | PagerDuty API key |
| `LINEAR_API_KEY` | Linear API key |
| `DATADOG_API_KEY` | Datadog API key |
| `SENTRY_AUTH_TOKEN` | Sentry auth token |
| `NOTION_SECRET` | Notion integration secret |

---

## 🏆 Challenge Theme — Wildcard: Future of Work

| Challenge Goal | NexusFlow Answer |
|----------------|-----------------|
| Reduce repetitive work | Eliminates 11-day manual decision facilitation cycles |
| Improve decision-making | Cross-platform context synthesis + risk matrix + 3 ranked options |
| Help teams achieve outcomes faster | Decision lifecycle: 11 days → under 5 minutes |
| AI as true collaborator | IBM Bob wrote 4,878 lines as active engineering partner |

---

## 🗂️ Repository Structure

```
nexusflow/
├── nexusflow/
│   ├── agents/
│   │   ├── collector.py       # L1 — 11 adapters, circuit breaker
│   │   ├── synthesiser.py     # L2 — FAISS+BM25, episodic memory
│   │   ├── validator.py       # L3 — PII scan, policy, retry loop
│   │   ├── recommender.py     # L4 — 3 ranked options, ROI
│   │   └── executor.py        # L5 — execution, audit receipt
│   ├── adapters/              # 11 MCP tool adapters
│   │   ├── slack.py
│   │   ├── jira.py
│   │   ├── github.py
│   │   ├── pagerduty.py
│   │   ├── linear.py
│   │   ├── confluence.py
│   │   ├── datadog.py
│   │   ├── sentry.py
│   │   ├── notion.py
│   │   ├── google_calendar.py
│   │   └── sendgrid.py
│   ├── api/
│   │   ├── main.py            # FastAPI app + /metrics endpoint
│   │   └── routes/
│   │       ├── auth.py        # Register, login, JWT
│   │       ├── pipelines.py   # Trigger, approve, audit
│   │       └── admin.py       # Authority rules, policy reload
│   ├── core/
│   │   ├── models.py          # 25+ Pydantic domain entities
│   │   ├── settings.py        # All env var config
│   │   ├── indexer.py         # FAISS + BM25 hybrid search
│   │   ├── memory.py          # Episodic memory store
│   │   ├── observability.py   # Trace decorator, metrics
│   │   ├── approval_gateway.py # Anti-hallucination safety
│   │   └── state/
│   │       └── pipeline.py    # LangGraph state machine
│   ├── db/
│   │   ├── models.py          # SQLAlchemy ORM
│   │   ├── session.py         # Async engine factories
│   │   └── audit.py          # SHA-256 hash chain writer
│   └── tests/                 # 47 tests, >90% coverage
├── policies/
│   └── default.yaml           # Hot-reloadable governance rules
├── frontend/                  # Next.js approval dashboard
├── BOB_SHELL_AUDIT_TRAIL.md   # IBM Bob Shell audit log
├── docker-compose.yml
├── render.yaml
└── README.md
```

---

## 📄 License

MIT License — see [LICENSE](LICENSE) file.

---

<div align="center">
  <strong>NexusFlow</strong> · Built with IBM Bob · AI Builders Challenge 2026 · Wildcard Track<br>
  <em>The autonomous operating system for the intelligent enterprise.</em><br><br>
  <a href="https://nexusflow-api-e6u8.onrender.com">Live API</a> ·
  <a href="https://nexusflow-api-e6u8.onrender.com/docs">API Docs</a> ·
  <a href="https://github.com/ekpenyongasuquo/nexusflow">GitHub</a>
</div>
