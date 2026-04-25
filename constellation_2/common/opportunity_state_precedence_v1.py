from __future__ import annotations

from typing import Any, Callable, Dict, Mapping


class OpportunityStatePrecedenceError(RuntimeError):
    pass


RulePredicate = Callable[[Mapping[str, Any]], bool]


def _historical_visibility_flags(visibility_state: str) -> Dict[str, bool]:
    return {
        "is_current": visibility_state in {"current", "current_downgraded"},
        "is_historical_only": visibility_state == "historical_only",
        "is_suppressed": visibility_state == "suppressed",
    }


RULES: tuple[dict[str, Any], ...] = (
    {
        "rule_id": "SUPERSEDED_OPPORTUNITY",
        "match": lambda c: str(c.get("freshness_state") or "") == "superseded",
        "opportunity_state": "historical_only",
        "actionability_state": "inspect_only",
        "review_priority": "historical_only",
        "visibility_state": "historical_only",
    },
    {
        "rule_id": "STALE_OPPORTUNITY",
        "match": lambda c: str(c.get("freshness_state") or "") in {"stale", "historical_only"},
        "opportunity_state": "monitor_only",
        "actionability_state": "inspect_only",
        "review_priority": "review_soon",
        "visibility_state": "historical_only",
        "inject_blocker": "stale_opportunity_state",
        "advisory_effect_state": "downgrade",
    },
    {
        "rule_id": "DEGRADED_UPSTREAM_TRUTH",
        "match": lambda c: bool(c.get("degraded_upstream_truth")),
        "opportunity_state": "blocked",
        "actionability_state": "blocked",
        "review_priority": "review_now",
        "visibility_state": "current_downgraded",
        "inject_blocker": "degraded_upstream_truth",
        "advisory_effect_state": "block",
    },
    {
        "rule_id": "READINESS_BLOCKED",
        "match": lambda c: bool(c.get("release_blocked")),
        "opportunity_state": "blocked",
        "actionability_state": "blocked",
        "review_priority": "review_now",
        "visibility_state": "current_downgraded",
        "inject_blocker": "release_readiness_blocked",
        "advisory_effect_state": "block",
    },
    {
        "rule_id": "TAX_BLOCKED",
        "match": lambda c: str(c.get("tax_effect_state") or "") == "block",
        "opportunity_state": "blocked",
        "actionability_state": "blocked",
        "review_priority": "review_now",
        "visibility_state": "current_downgraded",
        "inject_blocker": "tax_blocked_opportunity",
        "advisory_effect_state": "block",
    },
    {
        "rule_id": "ADVISORY_BLOCKED_IMPORTANT",
        "match": lambda c: bool(c.get("advisory_blocked")),
        "opportunity_state": "blocked",
        "actionability_state": "blocked",
        "review_priority": "review_now",
        "visibility_state": "current_downgraded",
        "inject_blocker": "advisory_blocked",
        "advisory_effect_state": "block",
    },
    {
        "rule_id": "BLOCKED_BUT_IMPORTANT",
        "match": lambda c: str(c.get("delta_state") or "") == "blocked_but_still_important",
        "opportunity_state": "blocked",
        "actionability_state": "inspect_only",
        "review_priority": "review_now",
        "visibility_state": "current_downgraded",
        "advisory_effect_state": "downgrade",
    },
    {
        "rule_id": "SCENARIO_REVIEW_NOW",
        "match": lambda c: str(c.get("scenario_significance_state") or "") == "review_now",
        "opportunity_state": "actionable",
        "actionability_state": "inspect_only",
        "review_priority": "review_now",
        "visibility_state": "current",
    },
    {
        "rule_id": "ACTIONABLE_REVIEW_NOW",
        "match": lambda c: str(c.get("base_review_priority") or "") == "review_now",
        "opportunity_state": "actionable",
        "actionability_state": "actionable",
        "review_priority": "review_now",
        "visibility_state": "current",
    },
    {
        "rule_id": "MONITOR_ONLY",
        "match": lambda c: str(c.get("base_review_priority") or "") in {"monitor_only", "review_soon"},
        "opportunity_state": "monitor_only",
        "actionability_state": "monitor_only",
        "review_priority": "monitor_only",
        "visibility_state": "current",
        "advisory_effect_state": "downgrade",
    },
    {
        "rule_id": "NO_VISIBLE_OPPORTUNITY",
        "match": lambda c: True,
        "opportunity_state": "monitor_only",
        "actionability_state": "inspect_only",
        "review_priority": "historical_only",
        "visibility_state": "suppressed",
        "advisory_effect_state": "downgrade",
    },
)


def evaluate_opportunity_state_precedence_v1(
    *,
    freshness_state: str,
    degraded_upstream_truth: bool,
    release_blocked: bool,
    tax_effect_state: str,
    advisory_blocked: bool,
    base_review_priority: str,
    blocker_states: list[str],
    delta_state: str,
    scenario_significance_state: str,
) -> Dict[str, Any]:
    context = {
        "freshness_state": str(freshness_state or "").strip(),
        "degraded_upstream_truth": bool(degraded_upstream_truth),
        "release_blocked": bool(release_blocked),
        "tax_effect_state": str(tax_effect_state or "").strip(),
        "advisory_blocked": bool(advisory_blocked),
        "base_review_priority": str(base_review_priority or "").strip(),
        "blocker_states": [str(item).strip() for item in blocker_states if str(item).strip()],
        "delta_state": str(delta_state or "").strip(),
        "scenario_significance_state": str(scenario_significance_state or "").strip(),
    }
    for row in RULES:
        predicate: RulePredicate = row["match"]
        if not predicate(context):
            continue
        effective_blockers = list(context["blocker_states"])
        injected = str(row.get("inject_blocker") or "").strip()
        if injected and injected not in effective_blockers:
            effective_blockers.append(injected)
        visibility_state = str(row["visibility_state"])
        review_priority = str(row["review_priority"])
        if context["delta_state"] in {"new", "changed"} and visibility_state == "current" and review_priority != "historical_only":
            review_priority = "review_now"
        return {
            "primary_rule_id": str(row["rule_id"]),
            "opportunity_state": str(row["opportunity_state"]),
            "actionability_state": str(row["actionability_state"]),
            "review_priority": review_priority,
            "visibility_state": visibility_state,
            "historical_visibility": _historical_visibility_flags(visibility_state),
            "effective_blocker_states": effective_blockers,
            "advisory_effect_state": str(row.get("advisory_effect_state") or "clear"),
        }
    raise OpportunityStatePrecedenceError("OPPORTUNITY_STATE_PRECEDENCE_NO_RULE_MATCHED")
