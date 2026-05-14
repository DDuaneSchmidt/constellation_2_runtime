from __future__ import annotations

from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_RELPATHS = {
    "research_evidence_packet": "governance/04_DATA/SCHEMAS/C2/REPORTS/research_evidence_packet.v1.schema.json",
    "research_to_lite_promotion": "governance/04_DATA/SCHEMAS/C2/REPORTS/research_to_lite_promotion.v1.schema.json",
    "research_lab_index": "governance/04_DATA/SCHEMAS/C2/REPORTS/research_lab_index.v1.schema.json",
    "edge_taxonomy": "governance/04_DATA/SCHEMAS/C2/REPORTS/edge_taxonomy.v1.schema.json",
}
RESEARCH_TYPES = {
    "SLEEVE",
    "EDGE",
    "REGIME",
    "GOVERNANCE",
    "OVERLAP",
    "STOP_LOGIC",
    "RISK_SIZING",
    "BEHAVIORAL_STATE",
}
RESEARCH_STATUSES = {"DRAFT", "UNDER_REVIEW", "VALIDATED_RESEARCH", "REJECTED", "ARCHIVED"}
PROMOTION_COMPONENT_TYPES = {
    "SLEEVE",
    "EDGE_CLUSTER_RULE",
    "REGIME_SIGNAL",
    "GOVERNANCE_RULE",
    "STOP_LOGIC",
    "RISK_RULE",
    "REPORT_FIELD",
}
PROMOTION_STATUSES = {
    "NOT_READY",
    "CANDIDATE",
    "APPROVED_FOR_LITE_REVIEW",
    "APPROVED_FOR_LITE_IMPLEMENTATION",
    "REJECTED",
    "DEFERRED",
}


def research_artifact_path_v1(*, truth_root: Path, artifact_id: str, day_utc: str, research_id: str) -> Path:
    filename = f"{artifact_id.replace('_v1', '')}.v1.json"
    return Path(truth_root).resolve() / "research_lab" / artifact_id / day_utc / _safe_id(research_id) / filename


def validate_research_lab_artifact_v1(payload: dict[str, Any]) -> None:
    schema_id = str(payload.get("schema_id") or "")
    relpath = SCHEMA_RELPATHS.get(schema_id)
    if not relpath:
        raise ValueError(f"UNSUPPORTED_AEGIS_RESEARCH_LAB_SCHEMA:{schema_id}")
    validate_against_repo_schema_v1(payload, REPO_ROOT, relpath)


def write_research_lab_artifact_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    validate_research_lab_artifact_v1(payload)
    identifier = str(payload.get("research_id") or payload.get("promotion_id") or payload.get("taxonomy_id") or "index")
    path = research_artifact_path_v1(
        truth_root=truth_root,
        artifact_id=str(payload["artifact_id"]),
        day_utc=day_utc,
        research_id=identifier,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def build_research_evidence_packet_v1(
    *,
    research_id: str,
    hypothesis_id: str,
    title: str,
    hypothesis_description: str,
    research_type: str,
    created_at_utc: str,
    source_data_summary: str,
    replay_window: str,
    instruments_tested: list[str],
    regimes_tested: list[str],
    edge_family: str,
    expected_holding_period: str,
    methodology_summary: str,
    metrics_summary: dict[str, Any],
    expectancy_summary: str,
    drawdown_summary: str,
    MAE_MFE_summary: str,
    failure_modes: list[str],
    known_limitations: list[str],
    reproducibility_notes: str,
    artifact_lineage: list[dict[str, Any]],
    research_status: str,
) -> dict[str, Any]:
    payload = {
        "schema_id": "research_evidence_packet",
        "schema_version": "v1",
        "artifact_id": "research_evidence_packet_v1",
        "research_id": research_id,
        "hypothesis_id": hypothesis_id,
        "title": title,
        "hypothesis_description": hypothesis_description,
        "research_type": _enum(research_type, RESEARCH_TYPES, "research_type"),
        "created_at_utc": created_at_utc,
        "source_data_summary": source_data_summary,
        "replay_window": replay_window,
        "instruments_tested": _strings(instruments_tested),
        "regimes_tested": _strings(regimes_tested),
        "edge_family": edge_family,
        "expected_holding_period": expected_holding_period,
        "methodology_summary": methodology_summary,
        "metrics_summary": metrics_summary,
        "expectancy_summary": expectancy_summary,
        "drawdown_summary": drawdown_summary,
        "MAE_MFE_summary": MAE_MFE_summary,
        "failure_modes": _strings(failure_modes),
        "known_limitations": _strings(known_limitations),
        "reproducibility_notes": reproducibility_notes,
        "artifact_lineage": artifact_lineage,
        "research_status": _enum(research_status, RESEARCH_STATUSES, "research_status"),
        "research_lab_only": True,
        "execution_authority_granted": False,
        "runtime_authorized": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "automatic_lite_promotion_allowed": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_research_to_lite_promotion_v1(
    *,
    promotion_id: str,
    research_id: str,
    hypothesis_id: str,
    proposed_lite_component_type: str,
    promotion_status: str,
    evidence_packet_refs: list[dict[str, Any]],
    validation_summary: str,
    regime_evidence: str,
    expectancy_evidence: str,
    failure_mode_review: str,
    governance_compatibility_review: str,
    risk_contract_review: str,
    stop_logic_review: str,
    manual_execution_compatibility: str,
    operator_clarity_review: str,
    implementation_notes: str,
    required_tests: list[str],
    approval_reason_codes: list[str],
    rejection_reason_codes: list[str],
    approved_by_human: bool,
    created_at_utc: str,
    reviewed_at_utc: str = "",
    source_research_status: str = "VALIDATED_RESEARCH",
) -> dict[str, Any]:
    status = _enum(promotion_status, PROMOTION_STATUSES, "promotion_status")
    source_status = _enum(source_research_status, RESEARCH_STATUSES, "source_research_status")
    if not evidence_packet_refs:
        raise ValueError("PROMOTION_REQUIRES_EVIDENCE_PACKET_REFS")
    if status in {"APPROVED_FOR_LITE_REVIEW", "APPROVED_FOR_LITE_IMPLEMENTATION"} and source_status != "VALIDATED_RESEARCH":
        raise ValueError(f"PROMOTION_REQUIRES_VALIDATED_RESEARCH:source_status={source_status}")
    if status == "APPROVED_FOR_LITE_IMPLEMENTATION" and not approved_by_human:
        raise ValueError("APPROVED_FOR_LITE_IMPLEMENTATION_REQUIRES_APPROVED_BY_HUMAN")
    payload = {
        "schema_id": "research_to_lite_promotion",
        "schema_version": "v1",
        "artifact_id": "research_to_lite_promotion_v1",
        "promotion_id": promotion_id,
        "research_id": research_id,
        "hypothesis_id": hypothesis_id,
        "proposed_lite_component_type": _enum(
            proposed_lite_component_type,
            PROMOTION_COMPONENT_TYPES,
            "proposed_lite_component_type",
        ),
        "promotion_status": status,
        "evidence_packet_refs": evidence_packet_refs,
        "validation_summary": validation_summary,
        "regime_evidence": regime_evidence,
        "expectancy_evidence": expectancy_evidence,
        "failure_mode_review": failure_mode_review,
        "governance_compatibility_review": governance_compatibility_review,
        "risk_contract_review": risk_contract_review,
        "stop_logic_review": stop_logic_review,
        "manual_execution_compatibility": manual_execution_compatibility,
        "operator_clarity_review": operator_clarity_review,
        "implementation_notes": implementation_notes,
        "required_tests": _strings(required_tests),
        "approval_reason_codes": _strings(approval_reason_codes),
        "rejection_reason_codes": _strings(rejection_reason_codes),
        "approved_by_human": bool(approved_by_human),
        "created_at_utc": created_at_utc,
        "reviewed_at_utc": reviewed_at_utc,
        "source_research_status": source_status,
        "eligible_for_lite_implementation": status == "APPROVED_FOR_LITE_IMPLEMENTATION" and bool(approved_by_human),
        "advisory_governance_evidence_only": True,
        "runtime_mutation_allowed": False,
        "automatic_lite_promotion_allowed": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_research_lab_index_v1(
    *,
    generated_at_utc: str,
    research_items: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = {
        "schema_id": "research_lab_index",
        "schema_version": "v1",
        "artifact_id": "research_lab_index_v1",
        "generated_at_utc": generated_at_utc,
        "research_items": [_research_index_item(row) for row in research_items],
        "research_lab_only": True,
        "runtime_mutation_allowed": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_edge_taxonomy_v1(
    *,
    taxonomy_id: str,
    generated_at_utc: str,
    market_theses: list[str],
    edge_families: list[str],
    edge_clusters: list[str],
    trade_expressions: list[str],
    deprecated_terms: list[str],
    naming_rules: list[str],
) -> dict[str, Any]:
    duplicate_warnings = _duplicate_term_warnings(
        market_theses=market_theses,
        edge_families=edge_families,
        edge_clusters=edge_clusters,
        trade_expressions=trade_expressions,
        deprecated_terms=deprecated_terms,
    )
    payload = {
        "schema_id": "edge_taxonomy",
        "schema_version": "v1",
        "artifact_id": "edge_taxonomy_v1",
        "taxonomy_id": taxonomy_id,
        "generated_at_utc": generated_at_utc,
        "market_theses": _strings(market_theses),
        "edge_families": _strings(edge_families),
        "edge_clusters": _strings(edge_clusters),
        "trade_expressions": _strings(trade_expressions),
        "deprecated_terms": _strings(deprecated_terms),
        "naming_rules": _strings(naming_rules),
        "duplicate_term_warnings": duplicate_warnings,
        "research_lab_only": True,
        "runtime_mutation_allowed": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def _research_index_item(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "research_id": str(row.get("research_id") or ""),
        "title": str(row.get("title") or ""),
        "research_type": _enum(str(row.get("research_type") or "EDGE"), RESEARCH_TYPES, "research_type"),
        "research_status": _enum(str(row.get("research_status") or "DRAFT"), RESEARCH_STATUSES, "research_status"),
        "latest_evidence_packet": str(row.get("latest_evidence_packet") or ""),
        "latest_promotion_status": _enum(
            str(row.get("latest_promotion_status") or "NOT_READY"),
            PROMOTION_STATUSES,
            "latest_promotion_status",
        ),
        "related_edge_family": str(row.get("related_edge_family") or ""),
        "related_sleeves": _strings(row.get("related_sleeves")),
        "archived": bool(row.get("archived", False)),
        "operator_notes": str(row.get("operator_notes") or ""),
    }


def _duplicate_term_warnings(
    *,
    market_theses: list[str],
    edge_families: list[str],
    edge_clusters: list[str],
    trade_expressions: list[str],
    deprecated_terms: list[str],
) -> list[str]:
    sections = {
        "market_theses": _strings(market_theses),
        "edge_families": _strings(edge_families),
        "edge_clusters": _strings(edge_clusters),
        "trade_expressions": _strings(trade_expressions),
    }
    deprecated = {_norm(term) for term in deprecated_terms}
    seen: dict[str, list[str]] = {}
    for section, values in sections.items():
        for value in values:
            seen.setdefault(_norm(value), []).append(section)
    warnings: list[str] = []
    for term, locations in sorted(seen.items()):
        if len(locations) > 1:
            warnings.append(f"DUPLICATE_TERM:{term}:{','.join(sorted(locations))}")
        if term in deprecated:
            warnings.append(f"DEPRECATED_TERM_USED:{term}")
    return warnings


def _enum(value: str, allowed: set[str], field_name: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in allowed:
        raise ValueError(f"INVALID_{field_name.upper()}:{normalized}")
    return normalized


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    item = str(value).strip()
    return [item] if item else []


def _norm(value: str) -> str:
    return "_".join(str(value).strip().upper().replace("-", "_").split())


def _safe_id(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(value or "").strip())
    return cleaned or "research_lab"
