"""
nexusflow/agents/recommender.py
L4 Recommender Agent — generates three ranked decision options
with projected ROI outcomes. Uses Mixtral 8x7B via OpenRouter.
Falls back to rule-based recommendations if LLM unavailable.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import httpx

from nexusflow.core.models import (
    DecisionOption,
    PipelineState,
    PipelineStatus,
    RecommendationPackage,
    RiskLevel,
)
from nexusflow.core.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

_SYSTEM_PROMPT = """You are NexusFlow's Recommender Agent — a strategic decision intelligence system.
You receive a validated decision brief and must generate exactly THREE ranked decision options.

Respond ONLY with valid JSON matching this exact schema:
{
  "options": [
    {
      "label": "Option A",
      "title": "string",
      "description": "string (2-3 sentences)",
      "projected_roi_usd": null,
      "projected_roi_percent": null,
      "confidence": 0.0,
      "implementation_steps": ["step1", "step2", "step3"],
      "risk_level": "LOW|MEDIUM|HIGH|CRITICAL",
      "time_to_implement_days": null
    }
  ],
  "recommended_option_id": null,
  "reasoning": "string — why Option A is recommended over B and C"
}

Generate exactly 3 options labelled Option A, Option B, Option C.
Option A should be the highest-confidence recommendation.
Be specific, actionable, and grounded in the provided brief."""


async def run_recommender_agent(state: PipelineState) -> PipelineState:
    """
    L4 Recommender Agent entry point.

    Input:  PipelineState with brief and validation populated
    Output: PipelineState with recommendation populated
    """
    logger.info("[L4-RECOMMENDER] Pipeline %s — generating recommendations", state.pipeline_id)
    state.status = PipelineStatus.RECOMMENDING

    if not state.brief or not state.validation:
        state.status = PipelineStatus.FAILED
        state.error_stage = "RECOMMENDER"
        state.error_message = "Missing brief or validation for recommendation"
        return state

    if not state.validation.is_cleared:
        # Pipeline was halted by Validator — do not proceed
        return state

    rec_data = await _call_llm(state)

    if rec_data is None:
        rec_data = _rule_based_fallback(state)

    # Build typed options
    options: list[DecisionOption] = []
    for i, opt in enumerate(rec_data.get("options", [])[:3]):
        options.append(DecisionOption(
            label=opt.get("label", f"Option {chr(65+i)}"),
            title=opt.get("title", ""),
            description=opt.get("description", ""),
            projected_roi_usd=opt.get("projected_roi_usd"),
            projected_roi_percent=opt.get("projected_roi_percent"),
            confidence=float(opt.get("confidence", 0.5)),
            implementation_steps=opt.get("implementation_steps", []),
            risk_level=_parse_risk(opt.get("risk_level", "MEDIUM")),
            time_to_implement_days=opt.get("time_to_implement_days"),
        ))

    # Find recommended option
    recommended_id: str | None = None
    if options:
        # Default: highest confidence option
        top_option = max(options, key=lambda o: o.confidence)
        recommended_id = top_option.option_id

    package = RecommendationPackage(
        pipeline_id=state.pipeline_id,
        generated_at=datetime.now(timezone.utc),
        options=options,
        recommended_option_id=recommended_id,
        reasoning=rec_data.get("reasoning", ""),
    )

    state.recommendation = package
    state.status = PipelineStatus.AWAITING_HUMAN

    logger.info(
        "[L4-RECOMMENDER] Pipeline %s — %d options generated. Awaiting human approval.",
        state.pipeline_id, len(options)
    )
    return state


async def _call_llm(state: PipelineState) -> dict | None:
    """Call OpenRouter (Mixtral 8x7B MoE) for recommendation generation."""
    if not settings.openrouter_api_key:
        return None

    brief = state.brief
    user_message = (
        f"Trigger Type: {state.trigger_type}\n"
        f"Context Summary: {brief.context_summary}\n"
        f"Causal Chain: {' → '.join(brief.causal_chain)}\n"
        f"Estimated Financial Impact: ${brief.estimated_impact_usd or 'Unknown'}\n"
        f"Affected Systems: {', '.join(brief.affected_systems)}\n"
        f"Brief Confidence: {brief.confidence_score:.0%}\n\n"
        "Generate three ranked decision options now."
    )

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                f"{settings.openrouter_base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {settings.openrouter_api_key}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://nexusflow.dev",
                    "X-Title": "NexusFlow Recommender Agent",
                },
                json={
                    "model": "mistralai/mixtral-8x7b-instruct",
                    "messages": [
                        {"role": "system", "content": _SYSTEM_PROMPT},
                        {"role": "user", "content": user_message},
                    ],
                    "temperature": 0.2,
                    "max_tokens": 2000,
                    "response_format": {"type": "json_object"},
                },
            )

        if resp.status_code != 200:
            logger.warning("[L4-RECOMMENDER] LLM call failed: %s", resp.status_code)
            return None

        content = resp.json()["choices"][0]["message"]["content"]
        return json.loads(content)

    except (httpx.HTTPError, json.JSONDecodeError, KeyError) as e:
        logger.warning("[L4-RECOMMENDER] LLM error: %s", e)
        return None


def _rule_based_fallback(state: PipelineState) -> dict:
    """Rule-based fallback recommendations when LLM is unavailable."""
    trigger = str(state.trigger_type)

    fallbacks = {
        "BUDGET_VARIANCE": [
            {"label": "Option A", "title": "Reallocate contingency reserves",
             "description": "Draw from Q3 contingency budget to cover the variance. Lowest disruption path.",
             "confidence": 0.75, "risk_level": "LOW", "time_to_implement_days": 1,
             "implementation_steps": ["Identify available contingency balance", "Raise reallocation request", "CFO approval", "Update budget records"]},
            {"label": "Option B", "title": "Defer non-critical vendor contracts",
             "description": "Postpone 2-3 non-critical vendor payments to next quarter to recover headroom.",
             "confidence": 0.65, "risk_level": "MEDIUM", "time_to_implement_days": 3,
             "implementation_steps": ["Identify deferrable contracts", "Notify vendors", "Update payment schedule"]},
            {"label": "Option C", "title": "Escalate to executive reserve fund",
             "description": "Request emergency allocation from the board-level reserve fund.",
             "confidence": 0.45, "risk_level": "HIGH", "time_to_implement_days": 7,
             "implementation_steps": ["Prepare board briefing", "Submit reserve fund request", "Await board approval"]},
        ],
        "PROJECT_STALL": [
            {"label": "Option A", "title": "Unblock critical dependency immediately",
             "description": "Identify and resolve the primary blocker within 24 hours via direct escalation.",
             "confidence": 0.80, "risk_level": "LOW", "time_to_implement_days": 1,
             "implementation_steps": ["Identify root blocker", "Escalate to owner", "Resolve or re-assign"]},
            {"label": "Option B", "title": "Redistribute team workload",
             "description": "Temporarily shift team capacity from lower-priority tasks to unblock the stalled project.",
             "confidence": 0.70, "risk_level": "MEDIUM", "time_to_implement_days": 2,
             "implementation_steps": ["Audit current team allocation", "Identify reallocation candidates", "Update sprint plan"]},
            {"label": "Option C", "title": "Scope reduction and fast-track MVP",
             "description": "Reduce project scope to minimum viable deliverable to meet the deadline.",
             "confidence": 0.55, "risk_level": "MEDIUM", "time_to_implement_days": 5,
             "implementation_steps": ["Define MVP scope", "Stakeholder alignment", "Re-plan sprint"]},
        ],
    }

    options = fallbacks.get(trigger, [
        {"label": "Option A", "title": "Immediate escalation",
         "description": "Escalate to relevant stakeholder for immediate decision.", "confidence": 0.7,
         "risk_level": "MEDIUM", "time_to_implement_days": 1, "implementation_steps": ["Identify escalation target", "Send brief", "Schedule review"]},
        {"label": "Option B", "title": "Data gathering phase",
         "description": "Collect additional data before committing to a course of action.", "confidence": 0.6,
         "risk_level": "LOW", "time_to_implement_days": 3, "implementation_steps": ["Define data needs", "Assign collection task", "Review and decide"]},
        {"label": "Option C", "title": "Hold and monitor",
         "description": "Monitor the situation for 48 hours before taking action.", "confidence": 0.4,
         "risk_level": "HIGH", "time_to_implement_days": 2, "implementation_steps": ["Set monitoring alerts", "Define decision trigger", "Review at 48h"]},
    ])

    return {
        "options": options,
        "recommended_option_id": None,
        "reasoning": f"Rule-based recommendations for {trigger}. Configure OPENROUTER_API_KEY for AI-generated options.",
    }


def _parse_risk(value: str) -> RiskLevel:
    try:
        return RiskLevel[value.upper()]
    except KeyError:
        return RiskLevel.MEDIUM
