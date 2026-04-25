from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.common.constitutional_authorization_v1 import build_constitutional_authorization_v1
from constellation_2.common.paper_session_fact_plane_v1 import atomic_write_validated_json_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
REVIEW_PACKET_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/constitutional_review_packet.v1.schema.json"
)
OPERATOR_DECISION_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/constitutional_operator_decision.v1.schema.json"
)
REVIEW_OPTIONS_V1 = ("APPROVE", "REJECT", "DEFER")
APPROVED_DECISIONS_BY_ACTION_CLASS_V1 = {
    "PROTECTIVE": "AUTO_EXECUTE_PROTECTIVE",
    "CONSTRUCTIVE": "AUTO_EXECUTE",
}


def _sorted_strings(values: list[Any] | tuple[Any, ...]) -> list[str]:
    return sorted({str(value).strip() for value in values if str(value).strip()})


def _stable_negative_evidence_v1(rows: list[Any] | tuple[Any, ...]) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        normalized.append(
            {
                "type": str(row.get("type") or "").strip(),
                "fact": str(row.get("fact") or "").strip(),
                "severity": str(row.get("severity") or "").strip().upper(),
                "detail": str(row.get("detail") or "").strip(),
            }
        )
    normalized.sort(key=lambda item: (item["type"], item["fact"], item["severity"], item["detail"]))
    return normalized


def _packet_hash_v1(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes_v1(dict(payload))).hexdigest()


def _decision_record_hash_v1(payload: Mapping[str, Any], field_name: str) -> str:
    clone = dict(payload)
    clone[field_name] = None
    return hashlib.sha256(canonical_json_bytes_v1(clone)).hexdigest()


def _default_consequence_of_no_action_v1(action_class: str) -> str:
    normalized = str(action_class or "").strip().upper()
    if normalized == "PROTECTIVE":
        return "Protective action remains blocked until an operator records an explicit decision."
    return "No autonomous execution will occur until an operator records an explicit decision."


def _missing_fact_rows_v1(negative_evidence: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...]) -> list[str]:
    return sorted(
        {
            str(row.get("fact") or "").strip()
            for row in negative_evidence
            if isinstance(row, Mapping) and str(row.get("type") or "").strip() == "MISSING_FACT"
        }
    )


def _dependency_issue_rows_v1(
    *,
    fact_records: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...],
    negative_evidence: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...],
) -> list[str]:
    issues = {
        str(row.get("fact") or "").strip()
        for row in negative_evidence
        if isinstance(row, Mapping) and str(row.get("type") or "").strip() == "DEPENDENCY_HEALTH"
    }
    for row in fact_records:
        if not isinstance(row, Mapping):
            continue
        dependency_health = str(row.get("dependency_health") or "").strip().upper()
        if dependency_health in {"DEGRADED_NON_BLOCKING", "DEGRADED_BLOCKING", "UNAVAILABLE"}:
            logical_name = str(row.get("logical_name") or row.get("fact_type") or "").strip()
            if logical_name:
                issues.add(logical_name)
    return sorted(issues)


def _visible_facts_v1(rows: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...]) -> list[dict[str, str]]:
    visible_facts: list[dict[str, str]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        visible_facts.append(
            {
                "fact_id": str(row.get("fact_id") or "").strip(),
                "fact_type": str(row.get("fact_type") or "").strip(),
                "logical_name": str(row.get("logical_name") or row.get("scope_keys", {}).get("logical_name") or "").strip(),
                "observed_at": str(row.get("observed_at") or "").strip(),
                "captured_at": str(row.get("captured_at") or "").strip(),
                "freshness_class": str(row.get("freshness_class") or "").strip().upper(),
                "provenance_class": str(row.get("provenance_class") or "").strip().upper(),
                "content_hash": str(row.get("content_hash") or "").strip().lower(),
                "general_admissibility": str(row.get("general_admissibility") or "").strip().upper(),
                "tax_admissibility": str(row.get("tax_admissibility") or "").strip().upper(),
                "dependency_health": str(row.get("dependency_health") or "").strip().upper(),
                "state_coherence": str(row.get("state_coherence") or "").strip().upper(),
                "artifact_path": str(row.get("artifact_path") or "").strip(),
            }
        )
    visible_facts.sort(
        key=lambda item: (
            item["fact_id"],
            item["fact_type"],
            item["logical_name"],
            item["content_hash"],
        )
    )
    return visible_facts


def build_constitutional_review_packet_v1(
    *,
    proposal_hash: str,
    fact_bundle_hash: str,
    policy_version: str,
    created_at: str,
    action_type: str,
    action_class: str,
    target_entities: list[Any] | tuple[Any, ...],
    expected_economic_effect: Mapping[str, Any] | None,
    expected_tax_effect: Mapping[str, Any] | None,
    expected_risk_effect: Mapping[str, Any] | None,
    admissibility_summary: Mapping[str, Any],
    missing_facts: list[Any] | tuple[Any, ...],
    dependency_issues: list[Any] | tuple[Any, ...],
    decision_enum: str,
    blocker_rules: list[Any] | tuple[Any, ...],
    negative_evidence: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...],
    consequence_of_no_action: str,
    effective_scope: Mapping[str, Any] | None,
    authorization_expires_at: str | None,
    visible_fact_summary: Mapping[str, Any] | None,
    visible_facts: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...] = (),
    suggested_options: list[Any] | tuple[Any, ...] = REVIEW_OPTIONS_V1,
) -> dict[str, Any]:
    payload = {
        "schema_id": "constitutional_review_packet",
        "schema_version": "v1",
        "packet_hash": None,
        "proposal_hash": str(proposal_hash).strip().lower(),
        "fact_bundle_hash": str(fact_bundle_hash).strip().lower(),
        "policy_version": str(policy_version).strip(),
        "created_at": str(created_at).strip(),
        "action_type": str(action_type).strip(),
        "action_class": str(action_class).strip().upper(),
        "target_entities": _sorted_strings(list(target_entities)),
        "expected_economic_effect": dict(expected_economic_effect or {}),
        "expected_tax_effect": dict(expected_tax_effect or {}),
        "expected_risk_effect": dict(expected_risk_effect or {}),
        "admissibility_summary": {
            "general_admissibility": str(admissibility_summary.get("general_admissibility") or "").strip().upper(),
            "tax_admissibility": str(admissibility_summary.get("tax_admissibility") or "").strip().upper(),
            "dependency_health": str(admissibility_summary.get("dependency_health") or "").strip().upper(),
            "state_coherence": str(admissibility_summary.get("state_coherence") or "").strip().upper(),
        },
        "missing_facts": _sorted_strings(list(missing_facts)),
        "dependency_issues": _sorted_strings(list(dependency_issues)),
        "decision_enum": str(decision_enum).strip().upper(),
        "blocker_rules": _sorted_strings(list(blocker_rules)),
        "negative_evidence": _stable_negative_evidence_v1(list(negative_evidence)),
        "consequence_of_no_action": str(consequence_of_no_action).strip(),
        "suggested_options": [
            option for option in REVIEW_OPTIONS_V1
            if option in {str(item).strip().upper() for item in suggested_options if str(item).strip()}
        ],
        "effective_scope": {
            "global": str((effective_scope or {}).get("global") or "").strip(),
            "domain": str((effective_scope or {}).get("domain") or "").strip(),
            "account": str((effective_scope or {}).get("account") or "").strip(),
            "sleeve": str((effective_scope or {}).get("sleeve") or "").strip(),
            "action_class": str((effective_scope or {}).get("action_class") or "").strip(),
            "effective_authority": str((effective_scope or {}).get("effective_authority") or "").strip(),
        },
        "authorization_expires_at": None if authorization_expires_at is None else str(authorization_expires_at).strip(),
        "visible_fact_summary": {
            "required_fact_types": _sorted_strings(list((visible_fact_summary or {}).get("required_fact_types") or [])),
            "fact_types_present": _sorted_strings(list((visible_fact_summary or {}).get("fact_types_present") or [])),
        },
        "visible_facts": _visible_facts_v1(list(visible_facts)),
    }
    if not payload["consequence_of_no_action"]:
        payload["consequence_of_no_action"] = _default_consequence_of_no_action_v1(payload["action_class"])
    if payload["suggested_options"] != list(REVIEW_OPTIONS_V1):
        payload["suggested_options"] = list(REVIEW_OPTIONS_V1)
    payload["packet_hash"] = _packet_hash_v1(payload)
    validate_against_repo_schema_v1(payload, REPO_ROOT, REVIEW_PACKET_SCHEMA_RELPATH_V1)
    return payload


def build_review_packet_from_authorization_artifact_v1(payload: Mapping[str, Any]) -> dict[str, Any]:
    obj = dict(payload)
    constitutional_shadow = dict(obj.get("constitutional_shadow") or {})
    proposal = dict(constitutional_shadow.get("proposal") or {})
    fact_bundle = dict(constitutional_shadow.get("fact_bundle") or {})
    decision = dict(constitutional_shadow.get("decision") or {})
    constitutional_authorization = dict(obj.get("constitutional_authorization") or {})
    negative_evidence = list(decision.get("negative_evidence") or [])
    fact_records = list(fact_bundle.get("fact_records") or [])
    return build_constitutional_review_packet_v1(
        proposal_hash=str(obj.get("proposal_hash") or ""),
        fact_bundle_hash=str(obj.get("fact_bundle_hash") or ""),
        policy_version=str(constitutional_shadow.get("policy_version") or ""),
        created_at=str(obj.get("produced_utc") or ""),
        action_type=str(proposal.get("action_type") or ""),
        action_class=str(proposal.get("action_class") or ""),
        target_entities=list(proposal.get("target_entities") or []),
        expected_economic_effect=dict(proposal.get("expected_economic_effect") or {}),
        expected_tax_effect=dict(proposal.get("expected_tax_effect") or {}),
        expected_risk_effect=dict(proposal.get("expected_risk_effect") or {}),
        admissibility_summary={
            "general_admissibility": str(fact_bundle.get("general_admissibility") or ""),
            "tax_admissibility": str(fact_bundle.get("tax_admissibility") or ""),
            "dependency_health": str(fact_bundle.get("dependency_health") or ""),
            "state_coherence": str(fact_bundle.get("state_coherence") or ""),
        },
        missing_facts=_missing_fact_rows_v1(list(negative_evidence)),
        dependency_issues=_dependency_issue_rows_v1(
            fact_records=list(fact_bundle.get("fact_records") or []),
            negative_evidence=list(negative_evidence),
        ),
        decision_enum=str(decision.get("decision_enum") or ""),
        blocker_rules=list(decision.get("blocker_rules") or []),
        negative_evidence=list(negative_evidence),
        consequence_of_no_action="",
        effective_scope=dict(decision.get("effective_scope") or {}),
        authorization_expires_at=constitutional_authorization.get("expires_at"),
        visible_fact_summary={
            "required_fact_types": list(decision.get("required_fact_types") or []),
            "fact_types_present": list(fact_bundle.get("fact_types_present") or []),
        },
        visible_facts=fact_records,
    )


def build_review_packet_from_post_entry_boundary_v1(payload: Mapping[str, Any]) -> dict[str, Any]:
    obj = dict(payload)
    constitutional_shadow = dict(obj.get("constitutional_shadow") or {})
    proposal = dict(constitutional_shadow.get("proposal") or {})
    fact_bundle = dict(constitutional_shadow.get("fact_bundle") or {})
    decision = dict(constitutional_shadow.get("decision") or {})
    constitutional_authorization = dict(constitutional_shadow.get("constitutional_authorization") or {})
    negative_evidence = list(decision.get("negative_evidence") or [])
    fact_records = list(fact_bundle.get("fact_records") or [])
    return build_constitutional_review_packet_v1(
        proposal_hash=str(constitutional_shadow.get("proposal_hash") or ""),
        fact_bundle_hash=str(constitutional_shadow.get("fact_bundle_hash") or ""),
        policy_version=str(constitutional_shadow.get("policy_version") or ""),
        created_at=str(obj.get("evaluated_at_utc") or ""),
        action_type=str(proposal.get("action_type") or ""),
        action_class=str(proposal.get("action_class") or ""),
        target_entities=list(proposal.get("target_entities") or []),
        expected_economic_effect=dict(proposal.get("expected_economic_effect") or {}),
        expected_tax_effect=dict(proposal.get("expected_tax_effect") or {}),
        expected_risk_effect=dict(proposal.get("expected_risk_effect") or {}),
        admissibility_summary={
            "general_admissibility": str(fact_bundle.get("general_admissibility") or ""),
            "tax_admissibility": str(fact_bundle.get("tax_admissibility") or ""),
            "dependency_health": str(fact_bundle.get("dependency_health") or ""),
            "state_coherence": str(fact_bundle.get("state_coherence") or ""),
        },
        missing_facts=_missing_fact_rows_v1(list(negative_evidence)),
        dependency_issues=_dependency_issue_rows_v1(
            fact_records=[],
            negative_evidence=list(negative_evidence),
        ),
        decision_enum=str(decision.get("decision_enum") or ""),
        blocker_rules=list(decision.get("blocker_rules") or []),
        negative_evidence=list(negative_evidence),
        consequence_of_no_action="",
        effective_scope=dict(decision.get("effective_scope") or {}),
        authorization_expires_at=constitutional_authorization.get("expires_at"),
        visible_fact_summary={
            "required_fact_types": list(decision.get("required_fact_types") or []),
            "fact_types_present": list(fact_bundle.get("fact_types_present") or []),
        },
        visible_facts=fact_records,
    )


def _final_decision_for_operator_action_v1(*, action_class: str, operator_action: str) -> str:
    normalized_action_class = str(action_class or "").strip().upper()
    normalized_operator_action = str(operator_action or "").strip().upper()
    if normalized_operator_action == "APPROVE":
        return APPROVED_DECISIONS_BY_ACTION_CLASS_V1.get(normalized_action_class, "AUTO_EXECUTE")
    if normalized_operator_action == "REJECT":
        return "BLOCK"
    if normalized_operator_action == "DEFER":
        return "DEFER"
    raise ValueError(f"CONSTITUTIONAL_OPERATOR_ACTION_INVALID:{operator_action!r}")


def build_constitutional_operator_decision_v1(
    *,
    review_packet: Mapping[str, Any],
    operator_action: str,
    operator_id: str | None,
    decided_at: str,
    source_artifact_type: str,
    source_artifact_path: str,
    source_artifact_hash: str,
    operator_note: str = "",
    issuer_identity: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    packet = dict(review_packet)
    validate_against_repo_schema_v1(packet, REPO_ROOT, REVIEW_PACKET_SCHEMA_RELPATH_V1)
    if str(packet.get("decision_enum") or "").strip().upper() != "REQUIRE_HUMAN_REVIEW":
        raise ValueError("CONSTITUTIONAL_REVIEW_PACKET_NOT_HUMAN_REQUIRED")
    action = str(operator_action or "").strip().upper()
    final_decision_applied = _final_decision_for_operator_action_v1(
        action_class=str(packet.get("action_class") or ""),
        operator_action=action,
    )
    effective_scope = dict(packet.get("effective_scope") or {})
    resolved_authorization = None
    if action == "APPROVE":
        resolved_effective_scope = {
            "global": str(effective_scope.get("global") or "").strip(),
            "domain": str(effective_scope.get("domain") or "").strip(),
            "account": str(effective_scope.get("account") or "").strip(),
            "sleeve": str(effective_scope.get("sleeve") or "").strip(),
            "action_class": str(effective_scope.get("action_class") or "").strip(),
            "effective_authority": final_decision_applied,
        }
        resolved_authorization = build_constitutional_authorization_v1(
            proposal_hash=str(packet.get("proposal_hash") or ""),
            fact_bundle_hash=str(packet.get("fact_bundle_hash") or ""),
            policy_version=str(packet.get("policy_version") or ""),
            effective_scope=resolved_effective_scope,
            decision_enum=final_decision_applied,
            issued_at=str(decided_at).strip(),
            expires_at=packet.get("authorization_expires_at"),
            issuer_identity=dict(issuer_identity or {
                "issuer": "constitutional_operator_decision_v1",
                "producer_module": "constellation_2/common/constitutional_review_resolution_v1.py",
                "git_sha": "UNKNOWN",
            }),
            authorization_source="HUMAN_OVERRIDE",
        )
    payload = {
        "schema_id": "constitutional_operator_decision",
        "schema_version": "v1",
        "decision_record_hash": None,
        "review_packet_hash": str(packet.get("packet_hash") or "").strip().lower(),
        "proposal_hash": str(packet.get("proposal_hash") or "").strip().lower(),
        "fact_bundle_hash": str(packet.get("fact_bundle_hash") or "").strip().lower(),
        "policy_version": str(packet.get("policy_version") or "").strip(),
        "original_decision_enum": str(packet.get("decision_enum") or "").strip().upper(),
        "options_shown": list(packet.get("suggested_options") or []),
        "operator_action": action,
        "operator_id": None if operator_id is None or not str(operator_id).strip() else str(operator_id).strip(),
        "decided_at": str(decided_at).strip(),
        "final_decision_applied": final_decision_applied,
        "authorization_source": "HUMAN_OVERRIDE" if action == "APPROVE" else "NONE",
        "source_artifact_ref": {
            "artifact_type": str(source_artifact_type).strip(),
            "artifact_path": str(source_artifact_path).strip(),
            "artifact_hash": str(source_artifact_hash).strip().lower(),
        },
        "operator_note": str(operator_note or "").strip(),
        "resolved_constitutional_authorization": resolved_authorization,
    }
    payload["decision_record_hash"] = _decision_record_hash_v1(payload, "decision_record_hash")
    validate_against_repo_schema_v1(payload, REPO_ROOT, OPERATOR_DECISION_SCHEMA_RELPATH_V1)
    return payload


def resolve_constitutional_operator_decision_path_v1(
    *,
    truth_root: str | Path,
    day_utc: str,
    proposal_hash: str,
    decision_record_hash: str,
) -> Path:
    return (
        Path(str(truth_root)).resolve()
        / "reports"
        / "constitutional_operator_decision_v1"
        / str(day_utc).strip()
        / (
            f"{str(proposal_hash).strip().lower()}."
            f"{str(decision_record_hash).strip().lower()}."
            "constitutional_operator_decision.v1.json"
        )
    ).resolve()


def iter_constitutional_operator_decision_paths_v1(
    *,
    truth_root: str | Path,
    day_utc: str,
    proposal_hash: str,
) -> list[Path]:
    root = (
        Path(str(truth_root)).resolve()
        / "reports"
        / "constitutional_operator_decision_v1"
        / str(day_utc).strip()
    ).resolve()
    if not root.exists() or not root.is_dir():
        return []
    return sorted(
        path.resolve()
        for path in root.glob(f"{str(proposal_hash).strip().lower()}.*.constitutional_operator_decision.v1.json")
        if path.is_file()
    )


def read_latest_constitutional_operator_decision_v1(
    *,
    truth_root: str | Path,
    day_utc: str,
    proposal_hash: str,
) -> dict[str, Any] | None:
    latest_payload: dict[str, Any] | None = None
    latest_path: Path | None = None
    latest_key = ("", "")
    for path in iter_constitutional_operator_decision_paths_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        proposal_hash=proposal_hash,
    ):
        obj = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(obj, dict):
            continue
        validate_against_repo_schema_v1(obj, REPO_ROOT, OPERATOR_DECISION_SCHEMA_RELPATH_V1)
        decided_at = str(obj.get("decided_at") or "").strip()
        decision_record_hash = str(obj.get("decision_record_hash") or "").strip().lower()
        sort_key = (decided_at, decision_record_hash)
        if sort_key >= latest_key:
            latest_key = sort_key
            latest_payload = dict(obj)
            latest_path = path.resolve()
    if latest_payload is None or latest_path is None:
        return None
    return {
        "path": str(latest_path),
        "payload": latest_payload,
    }


def write_constitutional_operator_decision_v1(
    *,
    truth_root: str | Path,
    day_utc: str,
    decision_record: Mapping[str, Any],
) -> Path:
    payload = dict(decision_record)
    validate_against_repo_schema_v1(payload, REPO_ROOT, OPERATOR_DECISION_SCHEMA_RELPATH_V1)
    path = resolve_constitutional_operator_decision_path_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        proposal_hash=str(payload.get("proposal_hash") or ""),
        decision_record_hash=str(payload.get("decision_record_hash") or ""),
    )
    atomic_write_validated_json_v1(
        path=path,
        payload=payload,
        schema_relpath=OPERATOR_DECISION_SCHEMA_RELPATH_V1,
    )
    return path
