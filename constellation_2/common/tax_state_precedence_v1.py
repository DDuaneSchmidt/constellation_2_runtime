from __future__ import annotations

from typing import Any, Callable, Dict, Mapping


class TaxStatePrecedenceError(RuntimeError):
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
        "rule_id": "SUPERSEDED_TAX_STATE",
        "match": lambda c: str(c.get("freshness_state") or "") == "superseded",
        "visibility_state": "historical_only",
        "opportunity_visibility": "historical_only",
        "advisory_effect_state": "block",
        "inject_blocker": "superseded_tax_state",
    },
    {
        "rule_id": "STALE_TAX_STATE",
        "match": lambda c: str(c.get("freshness_state") or "") == "stale",
        "visibility_state": "current_downgraded",
        "opportunity_visibility": "suppressed",
        "advisory_effect_state": "downgrade",
        "inject_blocker": "stale_tax_state",
    },
    {
        "rule_id": "INCOMPLETE_TAX_BASIS",
        "match": lambda c: str(c.get("completeness_state") or "") == "incomplete_basis",
        "visibility_state": "current_downgraded",
        "opportunity_visibility": "suppressed",
        "advisory_effect_state": "block",
    },
    {
        "rule_id": "DEGRADED_TAX_RUNTIME",
        "match": lambda c: str(c.get("completeness_state") or "") == "degraded_runtime",
        "visibility_state": "current_downgraded",
        "opportunity_visibility": "suppressed",
        "advisory_effect_state": "downgrade",
    },
    {
        "rule_id": "WASH_SALE_CONFLICT_BLOCK",
        "match": lambda c: "wash_sale_conflict" in set(c.get("blocker_states") or ()),
        "visibility_state": "current_downgraded",
        "opportunity_visibility": "suppressed",
        "advisory_effect_state": "block",
    },
    {
        "rule_id": "TAX_BLOCKER_PRESENT",
        "match": lambda c: bool(c.get("blocker_states")),
        "visibility_state": "current_downgraded",
        "opportunity_visibility": "suppressed",
        "advisory_effect_state": "block",
    },
    {
        "rule_id": "HISTORICAL_VISIBILITY_ONLY",
        "match": lambda c: str(c.get("freshness_state") or "") == "historical_only",
        "visibility_state": "historical_only",
        "opportunity_visibility": "historical_only",
        "advisory_effect_state": "downgrade",
    },
    {
        "rule_id": "TAX_OPPORTUNITY_VISIBLE",
        "match": lambda c: bool(c.get("opportunity_states")),
        "visibility_state": "current",
        "opportunity_visibility": "visible",
        "advisory_effect_state": "clear",
    },
    {
        "rule_id": "TAX_CURRENT_NO_OPPORTUNITY",
        "match": lambda c: True,
        "visibility_state": "current",
        "opportunity_visibility": "unavailable",
        "advisory_effect_state": "clear",
    },
)


def evaluate_tax_state_precedence_v1(
    *,
    completeness_state: str,
    freshness_state: str,
    blocker_states: list[str],
    opportunity_states: list[str],
) -> Dict[str, Any]:
    context = {
        "completeness_state": str(completeness_state or "").strip(),
        "freshness_state": str(freshness_state or "").strip(),
        "blocker_states": [str(item).strip() for item in blocker_states if str(item).strip()],
        "opportunity_states": [str(item).strip() for item in opportunity_states if str(item).strip()],
    }
    for row in RULES:
        predicate: RulePredicate = row["match"]
        if not predicate(context):
            continue
        effective_blockers = list(context["blocker_states"])
        injected = str(row.get("inject_blocker") or "").strip()
        if injected and injected not in effective_blockers:
            effective_blockers.append(injected)
        effective_opportunities = (
            list(context["opportunity_states"])
            if str(row["opportunity_visibility"]) == "visible"
            else []
        )
        visibility_state = str(row["visibility_state"])
        return {
            "primary_rule_id": str(row["rule_id"]),
            "visibility_state": visibility_state,
            "opportunity_visibility": str(row["opportunity_visibility"]),
            "historical_visibility": _historical_visibility_flags(visibility_state),
            "effective_blocker_states": effective_blockers,
            "effective_opportunity_states": effective_opportunities,
            "advisory_effect_state": str(row["advisory_effect_state"]),
        }
    raise TaxStatePrecedenceError("TAX_STATE_PRECEDENCE_NO_RULE_MATCHED")
