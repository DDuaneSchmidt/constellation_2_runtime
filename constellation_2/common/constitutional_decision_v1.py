from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/constitutional_decision.v1.schema.json"
DECISION_ENUMS_V1 = (
    "AUTO_EXECUTE",
    "AUTO_EXECUTE_PROTECTIVE",
    "REQUIRE_HUMAN_REVIEW",
    "ADVISORY_ONLY",
    "DEFER",
    "BLOCK",
    "FREEZE_SCOPE",
)
GENERAL_ADMISSIBILITY_VALUES_V1 = (
    "VERIFIED_COMPLETE",
    "VERIFIED_PARTIAL",
    "ESTIMATED",
    "STALE",
    "CONFLICTED",
    "UNAVAILABLE",
    "UNKNOWN",
)
TAX_ADMISSIBILITY_VALUES_V1 = (
    "EXACT_LOT_LEVEL",
    "ESTIMATED_LOT_LEVEL",
    "ESTIMATED_POSITION_LEVEL",
    "INCOMPLETE",
    "UNKNOWN",
)
DEPENDENCY_HEALTH_VALUES_V1 = (
    "HEALTHY",
    "DEGRADED_NON_BLOCKING",
    "DEGRADED_BLOCKING",
    "UNAVAILABLE",
)
STATE_COHERENCE_VALUES_V1 = (
    "COHERENT",
    "PARTIAL",
    "CONFLICTED",
    "STALE",
    "UNKNOWN",
)
SCOPE_LEVELS_V1 = ("global", "domain", "account", "sleeve", "action_class")
GATE_NAMES_V1 = (
    "proposal_contract_valid",
    "required_fact_set_present",
    "state_coherence_acceptable",
    "dependency_health_acceptable",
    "action_class_allowed_in_scope",
    "required_admissibility_states_met",
    "hard_envelopes_passed",
    "policy_blockers_absent",
    "decision_persisted_successfully",
    "authorization_artifact_issuable",
)
_RESTRICTIVENESS_V1 = {
    "AUTO_EXECUTE": 0,
    "AUTO_EXECUTE_PROTECTIVE": 1,
    "ADVISORY_ONLY": 2,
    "DEFER": 3,
    "REQUIRE_HUMAN_REVIEW": 4,
    "FREEZE_SCOPE": 5,
    "BLOCK": 6,
}
_DEFAULT_GENERAL_ADMISSIBILITY_ALLOW_V1 = {
    "PROTECTIVE": ("VERIFIED_COMPLETE", "VERIFIED_PARTIAL", "ESTIMATED"),
    "CONSTRUCTIVE": ("VERIFIED_COMPLETE", "VERIFIED_PARTIAL"),
}
_DEFAULT_TAX_ADMISSIBILITY_ALLOW_V1 = {
    "PROTECTIVE": ("EXACT_LOT_LEVEL", "ESTIMATED_LOT_LEVEL", "ESTIMATED_POSITION_LEVEL", "INCOMPLETE"),
    "CONSTRUCTIVE": ("EXACT_LOT_LEVEL", "ESTIMATED_LOT_LEVEL", "ESTIMATED_POSITION_LEVEL"),
}
_DEFAULT_DEPENDENCY_ALLOW_V1 = {
    "PROTECTIVE": ("HEALTHY", "DEGRADED_NON_BLOCKING"),
    "CONSTRUCTIVE": ("HEALTHY", "DEGRADED_NON_BLOCKING"),
}
_DEFAULT_STATE_COHERENCE_ALLOW_V1 = {
    "PROTECTIVE": ("COHERENT", "PARTIAL"),
    "CONSTRUCTIVE": ("COHERENT",),
}


def _sorted_strings(values: list[Any] | tuple[Any, ...]) -> list[str]:
    return sorted({str(value).strip() for value in values if str(value).strip()})


def _gate_row(*, gate_index: int, gate_name: str, outcome: str, detail: str) -> dict[str, Any]:
    return {
        "gate_index": int(gate_index),
        "gate_name": str(gate_name),
        "outcome": str(outcome),
        "detail": str(detail),
    }


def _rule_row(*, rule_id: str, gate_index: int, outcome: str, detail: str) -> dict[str, Any]:
    return {
        "rule_id": str(rule_id),
        "gate_index": int(gate_index),
        "outcome": str(outcome),
        "detail": str(detail),
    }


def _negative_evidence_row(*, evidence_type: str, fact: str, severity: str, detail: str) -> dict[str, str]:
    return {
        "type": str(evidence_type).strip(),
        "fact": str(fact).strip(),
        "severity": str(severity).strip().upper(),
        "detail": str(detail).strip(),
    }


def is_protective_action_v1(action_class: str) -> bool:
    return str(action_class or "").strip().upper() == "PROTECTIVE"


def decision_restrictiveness_rank_v1(decision: str) -> int:
    normalized = str(decision or "").strip().upper()
    if normalized not in _RESTRICTIVENESS_V1:
        raise ValueError(f"CONSTITUTIONAL_DECISION_INVALID:{decision!r}")
    return int(_RESTRICTIVENESS_V1[normalized])


def most_restrictive_decision_v1(decisions: list[str] | tuple[str, ...]) -> str:
    normalized = [str(value).strip().upper() for value in decisions if str(value).strip()]
    if not normalized:
        raise ValueError("CONSTITUTIONAL_SCOPE_EMPTY")
    invalid = [value for value in normalized if value not in _RESTRICTIVENESS_V1]
    if invalid:
        raise ValueError(f"CONSTITUTIONAL_SCOPE_INVALID:{invalid!r}")
    return max(normalized, key=lambda value: _RESTRICTIVENESS_V1[value])


def _normalized_policy_value_list_v1(value: Any, fallback: tuple[str, ...]) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        return tuple(fallback)
    normalized = tuple(sorted({str(item).strip().upper() for item in value if str(item).strip()}))
    return normalized or tuple(fallback)


def _policy_action_key_v1(action_class: str) -> str:
    return "PROTECTIVE" if is_protective_action_v1(action_class) else "CONSTRUCTIVE"


def _policy_allow_values_v1(
    *,
    policy_profile: Mapping[str, Any] | None,
    action_class: str,
    section_name: str,
    defaults: Mapping[str, tuple[str, ...]],
) -> tuple[str, ...]:
    action_key = _policy_action_key_v1(action_class)
    if not isinstance(policy_profile, Mapping):
        return tuple(defaults[action_key])
    admissibility_rules = policy_profile.get("admissibility_rules")
    if not isinstance(admissibility_rules, Mapping):
        return tuple(defaults[action_key])
    section = admissibility_rules.get(section_name)
    if not isinstance(section, Mapping):
        return tuple(defaults[action_key])
    return _normalized_policy_value_list_v1(section.get(action_key), defaults[action_key])


def resolve_effective_scope_v1(
    *,
    scope_authorities: Mapping[str, Any] | None,
    proposal_scope: Mapping[str, Any] | None,
    action_class: str,
    dependency_health: str,
) -> dict[str, str]:
    authorities = dict(scope_authorities or {})
    scope = dict(proposal_scope or {})
    normalized: dict[str, str] = {}
    for level in SCOPE_LEVELS_V1:
        fallback = "AUTO_EXECUTE_PROTECTIVE" if level == "action_class" and is_protective_action_v1(action_class) else "REQUIRE_HUMAN_REVIEW"
        normalized[level] = str(authorities.get(level) or fallback).strip().upper()
    effective_authority = most_restrictive_decision_v1(list(normalized.values()))
    dependency = str(dependency_health or "").strip().upper()
    if dependency == "DEGRADED_NON_BLOCKING":
        if is_protective_action_v1(action_class) and effective_authority == "AUTO_EXECUTE":
            effective_authority = "AUTO_EXECUTE_PROTECTIVE"
        elif not is_protective_action_v1(action_class) and effective_authority in {"AUTO_EXECUTE", "AUTO_EXECUTE_PROTECTIVE"}:
            effective_authority = "REQUIRE_HUMAN_REVIEW"
    return {
        "global": str(scope.get("global") or "").strip(),
        "domain": str(scope.get("domain") or "").strip(),
        "account": str(scope.get("account") or "").strip(),
        "sleeve": str(scope.get("sleeve") or "").strip(),
        "action_class": str(scope.get("action_class") or action_class).strip(),
        "effective_authority": effective_authority,
    }


def _admissibility_allowed_v1(
    *,
    action_class: str,
    general_admissibility: str,
    tax_admissibility: str,
    policy_profile: Mapping[str, Any] | None = None,
) -> tuple[bool, str]:
    general = str(general_admissibility or "").strip().upper()
    tax = str(tax_admissibility or "").strip().upper()
    general_allowed = _policy_allow_values_v1(
        policy_profile=policy_profile,
        action_class=action_class,
        section_name="general_admissibility_allow_by_action_class",
        defaults=_DEFAULT_GENERAL_ADMISSIBILITY_ALLOW_V1,
    )
    tax_allowed = _policy_allow_values_v1(
        policy_profile=policy_profile,
        action_class=action_class,
        section_name="tax_admissibility_allow_by_action_class",
        defaults=_DEFAULT_TAX_ADMISSIBILITY_ALLOW_V1,
    )
    if general not in set(general_allowed):
        return False, f"GENERAL_ADMISSIBILITY_BLOCKED:{general}"
    if tax not in set(tax_allowed):
        return False, f"TAX_ADMISSIBILITY_BLOCKED:{tax}"
    return True, f"{_policy_action_key_v1(action_class)}_ADMISSIBILITY_OK"


def evaluate_constitutional_decision_v1(
    *,
    proposal: Mapping[str, Any],
    proposal_hash: str,
    fact_bundle: Mapping[str, Any],
    fact_bundle_hash: str,
    policy_version: str,
    scope_authorities: Mapping[str, Any] | None,
    hard_envelope_ok: bool,
    policy_blockers: list[Any] | tuple[Any, ...],
    persistence_ok: bool,
    evaluated_at: str,
    policy_profile: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    proposal_obj = dict(proposal)
    validate_against_repo_schema_v1(proposal_obj, REPO_ROOT, "governance/04_DATA/SCHEMAS/C2/REPORTS/constitutional_proposal.v1.schema.json")
    fact = dict(fact_bundle)
    required_fact_types = _sorted_strings(list(proposal_obj.get("required_fact_types") or []))
    present_fact_types = _sorted_strings(list(fact.get("fact_types_present") or []))
    missing_fact_types = sorted(set(required_fact_types) - set(present_fact_types))
    negative_evidence: list[dict[str, str]] = []
    for row in list(fact.get("negative_evidence") or []):
        if isinstance(row, Mapping):
            negative_evidence.append(
                _negative_evidence_row(
                    evidence_type=str(row.get("type") or "FACT_EVIDENCE"),
                    fact=str(row.get("fact") or "unknown_fact"),
                    severity=str(row.get("severity") or "NON_BLOCKING"),
                    detail=str(row.get("detail") or ""),
                )
            )
        elif str(row).strip():
            text = str(row).strip()
            negative_evidence.append(
                _negative_evidence_row(
                    evidence_type="FACT_EVIDENCE",
                    fact=text.split(":", 1)[0],
                    severity="NON_BLOCKING",
                    detail=text,
                )
            )
    for item in missing_fact_types:
        negative_evidence.append(
            _negative_evidence_row(
                evidence_type="MISSING_FACT",
                fact=item,
                severity="BLOCKING",
                detail=f"required fact type missing: {item}",
            )
        )
    general_admissibility = str(fact.get("general_admissibility") or "UNKNOWN").strip().upper()
    tax_admissibility = str(fact.get("tax_admissibility") or "UNKNOWN").strip().upper()
    dependency_health = str(fact.get("dependency_health") or "UNKNOWN").strip().upper()
    state_coherence = str(fact.get("state_coherence") or "UNKNOWN").strip().upper()
    action_class = str(proposal_obj.get("action_class") or "").strip().upper()
    effective_scope = resolve_effective_scope_v1(
        scope_authorities=scope_authorities,
        proposal_scope=dict(proposal_obj.get("target_scope") or {}),
        action_class=action_class,
        dependency_health=dependency_health,
    )
    gate_results: list[dict[str, Any]] = []
    rule_provenance: list[dict[str, Any]] = []
    blocker_rules: list[str] = []

    proposal_valid = True
    gate_results.append(_gate_row(gate_index=1, gate_name=GATE_NAMES_V1[0], outcome="PASS", detail="PROPOSAL_SCHEMA_VALID"))
    rule_provenance.append(_rule_row(rule_id="constitutional_proposal_schema_v1", gate_index=1, outcome="PASS", detail="proposal validated"))

    facts_present = not missing_fact_types
    gate_results.append(
        _gate_row(
            gate_index=2,
            gate_name=GATE_NAMES_V1[1],
            outcome="PASS" if facts_present else "FAIL",
            detail="REQUIRED_FACTS_PRESENT" if facts_present else ",".join(f"MISSING_FACT_TYPE:{item}" for item in missing_fact_types),
        )
    )
    rule_provenance.append(
        _rule_row(
            rule_id="constitutional_required_fact_set_v1",
            gate_index=2,
            outcome="PASS" if facts_present else "FAIL",
            detail="all required facts present" if facts_present else "required facts missing",
        )
    )
    if not facts_present:
        blocker_rules.append("CONSTITUTIONAL_REQUIRED_FACTS_MISSING")

    allowed_state_values = set(
        _policy_allow_values_v1(
            policy_profile=policy_profile,
            action_class=action_class,
            section_name="state_coherence_allow_by_action_class",
            defaults=_DEFAULT_STATE_COHERENCE_ALLOW_V1,
        )
    )
    state_ok = state_coherence in allowed_state_values
    gate_results.append(
        _gate_row(
            gate_index=3,
            gate_name=GATE_NAMES_V1[2],
            outcome="PASS" if state_ok else "FAIL",
            detail=f"STATE_COHERENCE:{state_coherence}",
        )
    )
    rule_provenance.append(
        _rule_row(
            rule_id="constitutional_state_coherence_v1",
            gate_index=3,
            outcome="PASS" if state_ok else "FAIL",
            detail=f"state_coherence={state_coherence}",
        )
    )
    if not state_ok:
        blocker_rules.append("CONSTITUTIONAL_STATE_COHERENCE_BLOCK")

    allowed_dependency_values = set(
        _policy_allow_values_v1(
            policy_profile=policy_profile,
            action_class=action_class,
            section_name="dependency_health_allow_by_action_class",
            defaults=_DEFAULT_DEPENDENCY_ALLOW_V1,
        )
    )
    dependency_ok = dependency_health in allowed_dependency_values
    gate_results.append(
        _gate_row(
            gate_index=4,
            gate_name=GATE_NAMES_V1[3],
            outcome="PASS" if dependency_ok else "FAIL",
            detail=f"DEPENDENCY_HEALTH:{dependency_health}",
        )
    )
    rule_provenance.append(
        _rule_row(
            rule_id="constitutional_dependency_health_v1",
            gate_index=4,
            outcome="PASS" if dependency_ok else "FAIL",
            detail=f"dependency_health={dependency_health}",
        )
    )
    if not dependency_ok:
        blocker_rules.append("CONSTITUTIONAL_DEPENDENCY_HEALTH_BLOCK")

    scope_authority = effective_scope["effective_authority"]
    scope_ok = scope_authority not in {"BLOCK", "FREEZE_SCOPE", "ADVISORY_ONLY", "DEFER"}
    gate_results.append(
        _gate_row(
            gate_index=5,
            gate_name=GATE_NAMES_V1[4],
            outcome="PASS" if scope_ok else "FAIL",
            detail=f"EFFECTIVE_SCOPE_AUTHORITY:{scope_authority}",
        )
    )
    rule_provenance.append(
        _rule_row(
            rule_id="constitutional_scope_intersection_v1",
            gate_index=5,
            outcome="PASS" if scope_ok else "FAIL",
            detail=f"effective_authority={scope_authority}",
        )
    )
    if not scope_ok:
        blocker_rules.append("CONSTITUTIONAL_SCOPE_BLOCK")

    admissibility_ok, admissibility_detail = _admissibility_allowed_v1(
        action_class=action_class,
        general_admissibility=general_admissibility,
        tax_admissibility=tax_admissibility,
        policy_profile=policy_profile,
    )
    gate_results.append(
        _gate_row(
            gate_index=6,
            gate_name=GATE_NAMES_V1[5],
            outcome="PASS" if admissibility_ok else "FAIL",
            detail=admissibility_detail,
        )
    )
    rule_provenance.append(
        _rule_row(
            rule_id="constitutional_admissibility_v1",
            gate_index=6,
            outcome="PASS" if admissibility_ok else "FAIL",
            detail=admissibility_detail,
        )
    )
    if not admissibility_ok:
        blocker_rules.append("CONSTITUTIONAL_ADMISSIBILITY_BLOCK")

    envelope_ok = bool(hard_envelope_ok)
    gate_results.append(
        _gate_row(
            gate_index=7,
            gate_name=GATE_NAMES_V1[6],
            outcome="PASS" if envelope_ok else "FAIL",
            detail="HARD_ENVELOPES_OK" if envelope_ok else "HARD_ENVELOPES_FAILED",
        )
    )
    rule_provenance.append(
        _rule_row(
            rule_id="constitutional_hard_envelopes_v1",
            gate_index=7,
            outcome="PASS" if envelope_ok else "FAIL",
            detail="hard envelopes evaluated",
        )
    )
    if not envelope_ok:
        blocker_rules.append("CONSTITUTIONAL_HARD_ENVELOPE_BLOCK")

    policy_blocker_rows = _sorted_strings(list(policy_blockers))
    policy_ok = not policy_blocker_rows
    gate_results.append(
        _gate_row(
            gate_index=8,
            gate_name=GATE_NAMES_V1[7],
            outcome="PASS" if policy_ok else "FAIL",
            detail="POLICY_BLOCKERS_ABSENT" if policy_ok else ",".join(policy_blocker_rows),
        )
    )
    rule_provenance.append(
        _rule_row(
            rule_id="constitutional_policy_blockers_v1",
            gate_index=8,
            outcome="PASS" if policy_ok else "FAIL",
            detail="policy blockers absent" if policy_ok else "policy blockers present",
        )
    )
    if not policy_ok:
        blocker_rules.extend(policy_blocker_rows)

    gate_results.append(
        _gate_row(
            gate_index=9,
            gate_name=GATE_NAMES_V1[8],
            outcome="PASS" if bool(persistence_ok) else "FAIL",
            detail="DECISION_PERSISTENCE_OK" if persistence_ok else "DECISION_PERSISTENCE_FAILED",
        )
    )
    rule_provenance.append(
        _rule_row(
            rule_id="constitutional_persistence_v1",
            gate_index=9,
            outcome="PASS" if bool(persistence_ok) else "FAIL",
            detail="persistence evaluation complete",
        )
    )
    if not bool(persistence_ok):
        blocker_rules.append("CONSTITUTIONAL_PERSISTENCE_BLOCK")

    authorizing = (
        proposal_valid
        and facts_present
        and state_ok
        and dependency_ok
        and scope_ok
        and admissibility_ok
        and envelope_ok
        and policy_ok
        and bool(persistence_ok)
    )
    decision_enum = scope_authority
    if not authorizing:
        if scope_authority == "FREEZE_SCOPE":
            decision_enum = "FREEZE_SCOPE"
        elif scope_authority in {"ADVISORY_ONLY", "DEFER"}:
            decision_enum = scope_authority
        else:
            decision_enum = "BLOCK"
    elif scope_authority == "REQUIRE_HUMAN_REVIEW":
        decision_enum = "REQUIRE_HUMAN_REVIEW"
    elif is_protective_action_v1(action_class) and dependency_health == "DEGRADED_NON_BLOCKING":
        decision_enum = "AUTO_EXECUTE_PROTECTIVE"
    elif is_protective_action_v1(action_class) and scope_authority == "AUTO_EXECUTE_PROTECTIVE":
        decision_enum = "AUTO_EXECUTE_PROTECTIVE"
    else:
        decision_enum = "AUTO_EXECUTE"
    authorization_issuable = decision_enum in {"AUTO_EXECUTE", "AUTO_EXECUTE_PROTECTIVE"} and authorizing
    approval_required = decision_enum == "REQUIRE_HUMAN_REVIEW"

    gate_results.append(
        _gate_row(
            gate_index=10,
            gate_name=GATE_NAMES_V1[9],
            outcome="PASS" if authorization_issuable else "FAIL",
            detail="AUTHORIZATION_ISSUABLE" if authorization_issuable else f"DECISION_ENUM:{decision_enum}",
        )
    )
    rule_provenance.append(
        _rule_row(
            rule_id="constitutional_authorization_issuance_v1",
            gate_index=10,
            outcome="PASS" if authorization_issuable else "FAIL",
            detail=f"decision_enum={decision_enum}",
        )
    )

    payload = {
        "schema_id": "constitutional_decision",
        "schema_version": "v1",
        "decision_id": f"constitutional-decision:{proposal_hash[:16]}:{fact_bundle_hash[:16]}",
        "proposal_hash": str(proposal_hash).strip().lower(),
        "fact_bundle_hash": str(fact_bundle_hash).strip().lower(),
        "policy_version": str(policy_version).strip(),
        "decision_enum": decision_enum,
        "effective_scope": effective_scope,
        "authorization_issuable": bool(authorization_issuable),
        "approval_required": bool(approval_required),
        "required_fact_types": required_fact_types,
        "gate_results": gate_results,
        "blocker_rules": _sorted_strings(blocker_rules),
        "rule_provenance": sorted(
            rule_provenance,
            key=lambda row: (int(row["gate_index"]), str(row["rule_id"]), str(row["detail"])),
        ),
        "negative_evidence": negative_evidence,
        "evaluated_at": str(evaluated_at).strip(),
        "decision_time_utc": str(evaluated_at).strip(),
    }
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH_V1)
    return payload


def decision_hash_v1(decision: Mapping[str, Any]) -> str:
    payload = dict(decision)
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH_V1)
    return hashlib.sha256(canonical_json_bytes_v1(payload)).hexdigest()
