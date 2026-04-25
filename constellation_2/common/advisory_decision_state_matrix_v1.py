from __future__ import annotations

from typing import Any, Callable, Dict, Mapping, Sequence


DECISION_STATE_PRECEDENCE: tuple[str, ...] = (
    "uncertified",
    "incomplete_basis",
    "superseded",
    "blocked",
    "stale",
    "historical_only",
    "promotion_eligible",
    "actionable",
)


class AdvisoryDecisionMatrixError(RuntimeError):
    pass


def _is_release_relevant(advisory_authority_class: str) -> bool:
    return advisory_authority_class in {"recommendation", "promotion_eligible"}


def _promotion_state(advisory_authority_class: str, decision_state: str) -> str:
    if advisory_authority_class != "promotion_eligible":
        return "not_applicable"
    return "eligible" if decision_state == "promotion_eligible" else "revoked"


def _historical_visibility_flags(visibility_state: str) -> Dict[str, bool]:
    return {
        "is_current": visibility_state in {"current", "current_downgraded"},
        "is_historical_only": visibility_state == "historical_only",
        "is_suppressed": visibility_state == "suppressed",
    }


def _visibility_for_state(
    *,
    advisory_authority_class: str,
    decision_state: str,
    base_visibility: str,
) -> str:
    if decision_state in {"actionable", "promotion_eligible"}:
        return "current"
    if decision_state in {"blocked", "incomplete_basis"}:
        return "current_downgraded"
    if decision_state in {"stale", "superseded", "uncertified"}:
        return base_visibility
    if decision_state == "historical_only":
        return "historical_only"
    raise AdvisoryDecisionMatrixError(f"UNKNOWN_DECISION_STATE_VISIBILITY:{decision_state}:{advisory_authority_class}")


def _actionability_for_state(decision_state: str) -> str:
    if decision_state in {"blocked", "uncertified", "incomplete_basis"}:
        return "blocked"
    if decision_state in {"stale", "superseded", "historical_only"}:
        return "inspect_only"
    if decision_state == "promotion_eligible":
        return "promotion_eligible"
    if decision_state == "actionable":
        return "actionable"
    raise AdvisoryDecisionMatrixError(f"UNKNOWN_DECISION_STATE_ACTIONABILITY:{decision_state}")


def _base_reason_codes(context: Mapping[str, Any]) -> list[str]:
    codes: list[str] = []
    if str(context.get("freshness_state") or "") == "uncertified":
        codes.append("UNCERTIFIED_BASIS")
    if str(context.get("opportunity_effect_state") or "") == "block":
        codes.append("OPPORTUNITY_BINDING_BLOCKED")
    elif str(context.get("opportunity_effect_state") or "") == "downgrade":
        codes.append("OPPORTUNITY_BINDING_DOWNGRADED")
    if str(context.get("tax_effect_state") or "") == "block":
        codes.append("TAX_BINDING_BLOCKED")
    elif str(context.get("tax_effect_state") or "") == "downgrade":
        codes.append("TAX_BINDING_DOWNGRADED")
    if bool(context.get("release_basis_missing")):
        codes.append("INCOMPLETE_RELEASE_BASIS")
    if bool(context.get("transition_superseded")):
        codes.append("SUPERSEDED_CERTIFIED_BASIS")
    if bool(context.get("release_blocked")):
        codes.append("RELEASE_BASELINE_BLOCKED")
    if bool(context.get("stale_relative_to_latest_certified_basis")):
        codes.append("STALE_CERTIFIED_BASIS")
    if str(context.get("base_visibility") or "") == "historical_only":
        codes.append("HISTORICAL_VISIBILITY_ONLY")
    if bool(context.get("base_promotion_eligibility")):
        codes.append("PROMOTION_ELIGIBLE_CERTIFIED")
    else:
        codes.append("ACTIONABLE_CERTIFIED")
    # Preserve precedence order deterministically.
    seen: set[str] = set()
    ordered: list[str] = []
    for code in codes:
        if code in seen:
            continue
        seen.add(code)
        ordered.append(code)
    return ordered


RulePredicate = Callable[[Mapping[str, Any]], bool]


RULES: tuple[dict[str, Any], ...] = (
    {
        "rule_id": "UNCERTIFIED_BASIS",
        "decision_state": "uncertified",
        "match": lambda c: str(c.get("freshness_state") or "") == "uncertified",
    },
    {
        "rule_id": "OPPORTUNITY_BINDING_BLOCKED",
        "decision_state": "blocked",
        "match": lambda c: str(c.get("opportunity_effect_state") or "") == "block",
    },
    {
        "rule_id": "OPPORTUNITY_BINDING_DOWNGRADED",
        "decision_state": "stale",
        "match": lambda c: str(c.get("opportunity_effect_state") or "") == "downgrade",
    },
    {
        "rule_id": "TAX_BINDING_BLOCKED",
        "decision_state": "blocked",
        "match": lambda c: str(c.get("tax_effect_state") or "") == "block",
    },
    {
        "rule_id": "TAX_BINDING_DOWNGRADED",
        "decision_state": "stale",
        "match": lambda c: str(c.get("tax_effect_state") or "") == "downgrade",
    },
    {
        "rule_id": "INCOMPLETE_RELEASE_BASIS",
        "decision_state": "incomplete_basis",
        "match": lambda c: _is_release_relevant(str(c.get("advisory_authority_class") or ""))
        and bool(c.get("release_basis_missing")),
    },
    {
        "rule_id": "SUPERSEDED_CERTIFIED_BASIS",
        "decision_state": "superseded",
        "match": lambda c: str(c.get("freshness_state") or "") == "superseded",
    },
    {
        "rule_id": "RELEASE_BASELINE_BLOCKED",
        "decision_state": "blocked",
        "match": lambda c: _is_release_relevant(str(c.get("advisory_authority_class") or ""))
        and bool(c.get("release_blocked")),
    },
    {
        "rule_id": "STALE_CERTIFIED_BASIS",
        "decision_state": "stale",
        "match": lambda c: str(c.get("freshness_state") or "") == "stale",
    },
    {
        "rule_id": "HISTORICAL_VISIBILITY_ONLY",
        "decision_state": "historical_only",
        "match": lambda c: str(c.get("base_visibility") or "") == "historical_only",
    },
    {
        "rule_id": "PROMOTION_ELIGIBLE_CERTIFIED",
        "decision_state": "promotion_eligible",
        "match": lambda c: bool(c.get("base_promotion_eligibility")) and not bool(c.get("release_blocked")),
    },
    {
        "rule_id": "ACTIONABLE_CERTIFIED",
        "decision_state": "actionable",
        "match": lambda c: True,
    },
)


def evaluate_advisory_decision_matrix_v1(
    *,
    advisory_authority_class: str,
    freshness_state: str,
    base_visibility: str,
    base_promotion_eligibility: bool,
    release_basis_missing: bool,
    release_blocked: bool,
    transition_superseded: bool,
    stale_relative_to_latest_certified_basis: bool,
    tax_effect_state: str = "clear",
    opportunity_effect_state: str = "clear",
) -> Dict[str, Any]:
    context = {
        "advisory_authority_class": str(advisory_authority_class or "").strip(),
        "freshness_state": str(freshness_state or "").strip(),
        "base_visibility": str(base_visibility or "").strip(),
        "base_promotion_eligibility": bool(base_promotion_eligibility),
        "release_basis_missing": bool(release_basis_missing),
        "release_blocked": bool(release_blocked),
        "transition_superseded": bool(transition_superseded),
        "stale_relative_to_latest_certified_basis": bool(stale_relative_to_latest_certified_basis),
        "tax_effect_state": str(tax_effect_state or "").strip(),
        "opportunity_effect_state": str(opportunity_effect_state or "").strip(),
    }
    for row in RULES:
        predicate = row["match"]
        if predicate(context):
            decision_state = str(row["decision_state"])
            visibility_state = _visibility_for_state(
                advisory_authority_class=context["advisory_authority_class"],
                decision_state=decision_state,
                base_visibility=context["base_visibility"],
            )
            return {
                "rule_id": str(row["rule_id"]),
                "decision_state": decision_state,
                "actionability_state": _actionability_for_state(decision_state),
                "visibility_state": visibility_state,
                "promotion_eligibility_state": _promotion_state(
                    context["advisory_authority_class"], decision_state
                ),
                "ordered_reason_codes": _base_reason_codes(context),
                "historical_visibility": _historical_visibility_flags(visibility_state),
            }
    raise AdvisoryDecisionMatrixError("ADVISORY_DECISION_MATRIX_NO_RULE_MATCHED")
