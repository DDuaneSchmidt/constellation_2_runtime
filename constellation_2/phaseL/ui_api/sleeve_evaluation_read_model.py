from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from .common import (
    REPO_ROOT,
    SLEEVE_TRUTH_ROOT,
    evidence_ref,
    freshness_state,
    latest_day_from_roots,
    latest_timestamp,
    read_json_dict,
)
from .dto import evidence_refs, markers, view_envelope
from .metadata_contract import validate_projection_envelope


def _string_or_none(value: Any) -> Optional[str]:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _float_from_text(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _usd_from_cents(value: Any) -> Optional[float]:
    if isinstance(value, int):
        return value / 100.0
    return None


def _read_json_object(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _resolve_sleeve_day(day: Optional[str]) -> Optional[str]:
    text = _string_or_none(day)
    if text:
        return text
    evaluation_root = (SLEEVE_TRUTH_ROOT / "reports" / "evaluation_input_manifest_v1").resolve()
    measurement_root = (SLEEVE_TRUTH_ROOT / "reports" / "sleeve_edge_measurement_snapshot_v1").resolve()
    governance_root = (SLEEVE_TRUTH_ROOT / "reports" / "allocation_governance_snapshot_v1").resolve()
    latest_evaluation_day = latest_day_from_roots([evaluation_root, measurement_root, governance_root])
    if latest_evaluation_day:
        return latest_evaluation_day
    return latest_day_from_roots([SLEEVE_TRUTH_ROOT / "allocation_v1" / "capital_authority_allocation_v1"])


def _load_capital_policy() -> tuple[List[Dict[str, Any]], Optional[Path], List[str]]:
    policy_path = (REPO_ROOT / "governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json").resolve()
    policy_doc, policy_err = read_json_dict(policy_path)
    warnings: List[str] = []
    if policy_doc is None:
        return [], None, [f"CAPITAL_AUTHORITY_POLICY_{policy_err or 'UNREADABLE'}"]
    sleeves = policy_doc.get("sleeves")
    if not isinstance(sleeves, list):
        return [], policy_path, ["CAPITAL_AUTHORITY_POLICY_SLEEVES_INVALID"]
    rows = [row for row in sleeves if isinstance(row, dict)]
    if not rows:
        warnings.append("CAPITAL_AUTHORITY_POLICY_SLEEVES_EMPTY")
    return rows, policy_path, warnings


def build_sleeve_evaluation_view(day: Optional[str] = None) -> Dict[str, Any]:
    resolved_day = _resolve_sleeve_day(day)
    policy_rows, policy_path, policy_warnings = _load_capital_policy()

    allocation_path = (
        SLEEVE_TRUTH_ROOT / "allocation_v1" / "capital_authority_allocation_v1" / (resolved_day or "UNKNOWN") / "capital_authority_allocation.v1.json"
    ).resolve()
    allocation_doc, allocation_err = read_json_dict(allocation_path)
    allocation_rows = allocation_doc.get("per_sleeve") if isinstance(allocation_doc, dict) and isinstance(allocation_doc.get("per_sleeve"), list) else []
    allocation_by_sleeve = {
        _string_or_none(row.get("sleeve_id")): row
        for row in allocation_rows
        if isinstance(row, dict) and _string_or_none(row.get("sleeve_id"))
    }

    policy_ref = evidence_ref(policy_path if policy_path else None, label="Capital Authority Policy", artifact_type="C2_CAPITAL_AUTHORITY_POLICY_V1")
    allocation_ref = evidence_ref(
        allocation_path if allocation_doc else None,
        label="Capital Authority Allocation",
        artifact_type="capital_authority_allocation_v1",
    )

    rows: List[Dict[str, Any]] = []
    sleeve_warnings: List[str] = list(policy_warnings)
    source_refs: List[Dict[str, Any]] = evidence_refs(policy_ref, allocation_ref)
    top_as_of_candidates: List[str] = []

    for policy_row in policy_rows:
        sleeve_id = _string_or_none(policy_row.get("sleeve_id"))
        if not sleeve_id:
            continue
        display_name = _string_or_none(policy_row.get("display_name")) or sleeve_id
        engine_ids = [str(item).strip() for item in policy_row.get("engine_ids") or [] if str(item).strip()]
        limits = policy_row.get("limits") if isinstance(policy_row.get("limits"), dict) else {}
        allocation_row = allocation_by_sleeve.get(sleeve_id, {})

        evaluation_manifest_path = (
            SLEEVE_TRUTH_ROOT / "reports" / "evaluation_input_manifest_v1" / (resolved_day or "UNKNOWN") / sleeve_id / "evaluation_input_manifest.v1.json"
        ).resolve()
        measurement_path = (
            SLEEVE_TRUTH_ROOT
            / "reports"
            / "sleeve_edge_measurement_snapshot_v1"
            / (resolved_day or "UNKNOWN")
            / sleeve_id
            / "sleeve_edge_measurement_snapshot.v1.json"
        ).resolve()
        governance_path = (
            SLEEVE_TRUTH_ROOT / "reports" / "allocation_governance_snapshot_v1" / (resolved_day or "UNKNOWN") / sleeve_id / "allocation_governance_snapshot.v1.json"
        ).resolve()

        evaluation_manifest_doc, evaluation_manifest_err = read_json_dict(evaluation_manifest_path)
        measurement_doc, measurement_err = read_json_dict(measurement_path)
        governance_doc, governance_err = read_json_dict(governance_path)

        evaluation_manifest_ref = evidence_ref(
            evaluation_manifest_path if evaluation_manifest_doc else None,
            label=f"{sleeve_id} Evaluation Input",
            artifact_type="evaluation_input_manifest_v1",
        )
        measurement_ref = evidence_ref(
            measurement_path if measurement_doc else None,
            label=f"{sleeve_id} Sleeve Edge Measurement",
            artifact_type="sleeve_edge_measurement_snapshot_v1",
        )
        governance_ref = evidence_ref(
            governance_path if governance_doc else None,
            label=f"{sleeve_id} Allocation Governance",
            artifact_type="allocation_governance_snapshot_v1",
        )
        legacy_snapshot_path_text = _string_or_none(allocation_row.get("qualification_snapshot_path")) if isinstance(allocation_row, dict) else None
        legacy_snapshot_path = Path(legacy_snapshot_path_text).resolve() if legacy_snapshot_path_text else None
        legacy_snapshot_ref = evidence_ref(
            legacy_snapshot_path if legacy_snapshot_path and legacy_snapshot_path.exists() else None,
            label=f"{sleeve_id} Legacy Sleeve Edge Snapshot",
            artifact_type="sleeve_edge_snapshot_v1",
        )

        row_warnings: List[str] = []
        if evaluation_manifest_doc is None:
            row_warnings.append(f"EVALUATION_INPUT_MANIFEST_{evaluation_manifest_err or 'UNREADABLE'}")
        if measurement_doc is None:
            row_warnings.append(f"SLEEVE_EDGE_MEASUREMENT_{measurement_err or 'UNREADABLE'}")
        if governance_doc is None:
            row_warnings.append(f"ALLOCATION_GOVERNANCE_{governance_err or 'UNREADABLE'}")
        if not isinstance(allocation_row, dict) or not allocation_row:
            row_warnings.append("CAPITAL_AUTHORITY_ALLOCATION_ROW_MISSING")
        row_warnings.append("TARGET_ALLOCATION_POLICY_UNPROVEN")
        row_warnings.append("TAX_EFFICIENCY_SUMMARY_UNAVAILABLE")

        measurement_window = measurement_doc.get("measurement_window") if isinstance(measurement_doc, dict) and isinstance(measurement_doc.get("measurement_window"), dict) else {}
        evidence_summary = (
            evaluation_manifest_doc.get("evidence_summary")
            if isinstance(evaluation_manifest_doc, dict) and isinstance(evaluation_manifest_doc.get("evidence_summary"), dict)
            else {}
        )
        edge_measurement = (
            measurement_doc.get("edge_measurement")
            if isinstance(measurement_doc, dict) and isinstance(measurement_doc.get("edge_measurement"), dict)
            else {}
        )
        stability_measurement = (
            measurement_doc.get("stability_measurement")
            if isinstance(measurement_doc, dict) and isinstance(measurement_doc.get("stability_measurement"), dict)
            else {}
        )
        confidence_measurement = (
            measurement_doc.get("confidence_measurement")
            if isinstance(measurement_doc, dict) and isinstance(measurement_doc.get("confidence_measurement"), dict)
            else {}
        )
        current_control_context = (
            governance_doc.get("current_control_context")
            if isinstance(governance_doc, dict) and isinstance(governance_doc.get("current_control_context"), dict)
            else {}
        )

        row_as_of_utc = latest_timestamp(
            _string_or_none(evaluation_manifest_doc.get("produced_utc") if isinstance(evaluation_manifest_doc, dict) else None),
            _string_or_none(measurement_doc.get("produced_utc") if isinstance(measurement_doc, dict) else None),
            _string_or_none(governance_doc.get("produced_utc") if isinstance(governance_doc, dict) else None),
            _string_or_none(allocation_doc.get("produced_utc") if isinstance(allocation_doc, dict) else None),
        )
        if row_as_of_utc:
            top_as_of_candidates.append(row_as_of_utc)

        row_refs = evidence_refs(policy_ref, allocation_ref, evaluation_manifest_ref, measurement_ref, governance_ref, legacy_snapshot_ref)
        source_refs.extend(row_refs)

        rows.append(
            {
                "entity_id": sleeve_id,
                "sleeve_id": sleeve_id,
                "display_name": display_name,
                "engine_ids": engine_ids,
                "priority_rank": policy_row.get("priority_rank"),
                "execution_scope_id": _string_or_none(evaluation_manifest_doc.get("execution_sleeve_id") if isinstance(evaluation_manifest_doc, dict) else None),
                "mode": _string_or_none(evaluation_manifest_doc.get("mode") if isinstance(evaluation_manifest_doc, dict) else None),
                "target_allocation_pct": None,
                "target_allocation_status": "UNAVAILABLE_NOT_PROVEN",
                "actual_allocation_pct": _float_from_text(allocation_row.get("actual_notional_pct")) if isinstance(allocation_row, dict) else None,
                "actual_value_usd": _usd_from_cents(allocation_row.get("actual_value_cents")) if isinstance(allocation_row, dict) else None,
                "effective_budget_usd": None,
                "effective_budget_status": "UNAVAILABLE_NON_RISK_BUDGET",
                "effective_risk_budget_usd": _usd_from_cents(allocation_row.get("allowed_capital_at_risk_cents")) if isinstance(allocation_row, dict) else None,
                "used_risk_budget_usd": _usd_from_cents(allocation_row.get("used_capital_at_risk_cents")) if isinstance(allocation_row, dict) else None,
                "headroom_usd": _usd_from_cents(allocation_row.get("headroom_cents")) if isinstance(allocation_row, dict) else None,
                "performance_metrics": {
                    "lookback_window": measurement_window,
                    "sample_trade_count": measurement_doc.get("sample_trade_count") if isinstance(measurement_doc, dict) else None,
                    "included_trade_count": evidence_summary.get("included_trade_count"),
                    "excluded_trade_count": evidence_summary.get("excluded_trade_count"),
                    "realized_return_pct": None,
                    "annualized_return_pct": None,
                    "warnings": ["REALIZED_PERFORMANCE_METRICS_UNAVAILABLE"],
                },
                "drawdown_metrics": {
                    "max_drawdown_pct": None,
                    "drawdown_status": "UNAVAILABLE",
                    "warnings": ["DRAWDOWN_METRICS_UNAVAILABLE"],
                },
                "stability_metrics": {
                    "drift_band": _string_or_none(stability_measurement.get("drift_band"))
                    or _string_or_none(allocation_row.get("drift_band") if isinstance(allocation_row, dict) else None)
                    or "UNKNOWN",
                    "stability_state": _string_or_none(stability_measurement.get("stability_state")) or "unavailable",
                    "sample_sufficiency_band": _string_or_none(confidence_measurement.get("sample_sufficiency_band"))
                    or _string_or_none(allocation_row.get("sample_sufficiency_band") if isinstance(allocation_row, dict) else None)
                    or "UNKNOWN",
                    "confidence_state": _string_or_none(confidence_measurement.get("confidence_state")) or "UNAVAILABLE",
                    "execution_health_band": _string_or_none(confidence_measurement.get("execution_health_band"))
                    or _string_or_none(allocation_row.get("execution_health_band") if isinstance(allocation_row, dict) else None)
                    or "UNKNOWN",
                },
                "qualification_state": _string_or_none(edge_measurement.get("qualification_state"))
                or _string_or_none(allocation_row.get("qualification_state") if isinstance(allocation_row, dict) else None)
                or "UNAVAILABLE",
                "edge_band": _string_or_none(edge_measurement.get("edge_band"))
                or _string_or_none(allocation_row.get("edge_band") if isinstance(allocation_row, dict) else None)
                or "UNKNOWN",
                "tax_efficiency_summary": {
                    "status": "UNAVAILABLE",
                    "warnings": ["TAX_EFFICIENCY_SUMMARY_UNAVAILABLE"],
                },
                "recommendation": {
                    "recommendation_state": _string_or_none(governance_doc.get("recommended_action_state") if isinstance(governance_doc, dict) else None)
                    or "unavailable",
                    "state_class": _string_or_none(governance_doc.get("recommended_state_class") if isinstance(governance_doc, dict) else None)
                    or "UNAVAILABLE",
                    "alignment_state": _string_or_none(governance_doc.get("allocation_alignment_state") if isinstance(governance_doc, dict) else None)
                    or "UNAVAILABLE",
                    "control_state": _string_or_none(current_control_context.get("control_state")) or "unavailable",
                    "backend_reason_codes": (
                        governance_doc.get("recommendation_reason_codes")
                        if isinstance(governance_doc, dict) and isinstance(governance_doc.get("recommendation_reason_codes"), list)
                        else []
                    ),
                },
                "evidence_summary": {
                    "source_artifact_count": evidence_summary.get("source_artifact_count"),
                    "sample_trade_count": evidence_summary.get("sample_trade_count"),
                    "closure_state": _string_or_none(evaluation_manifest_doc.get("closure_state") if isinstance(evaluation_manifest_doc, dict) else None)
                    or _string_or_none(measurement_doc.get("closure_state") if isinstance(measurement_doc, dict) else None)
                    or _string_or_none(governance_doc.get("closure_state") if isinstance(governance_doc, dict) else None)
                    or "UNKNOWN",
                    "first_blocker_code": _string_or_none(evaluation_manifest_doc.get("first_blocker_code") if isinstance(evaluation_manifest_doc, dict) else None)
                    or _string_or_none(measurement_doc.get("first_blocker_code") if isinstance(measurement_doc, dict) else None)
                    or _string_or_none(governance_doc.get("first_blocker_code") if isinstance(governance_doc, dict) else None)
                    or "",
                },
                "policy_limits": {
                    "max_capital_at_risk_usd": _usd_from_cents(limits.get("max_capital_at_risk_cents")),
                    "max_symbols": limits.get("max_symbols"),
                    "max_single_name_notional_pct": _float_from_text(limits.get("max_single_name_notional_pct")),
                    "max_sector_concentration_pct": _float_from_text(limits.get("max_sector_concentration_pct")),
                },
                "truth_state": "derived" if row_warnings else "canonical",
                "as_of_utc": row_as_of_utc,
                "freshness_state": freshness_state(row_as_of_utc),
                "source_authority": [
                    "C2_CAPITAL_AUTHORITY_POLICY_V1",
                    "capital_authority_allocation_v1",
                    "evaluation_input_manifest_v1",
                    "sleeve_edge_measurement_snapshot_v1",
                    "allocation_governance_snapshot_v1",
                ],
                "provenance_refs": row_refs,
                "degradation_codes": row_warnings,
            }
        )

    if allocation_doc is None:
        sleeve_warnings.append(f"CAPITAL_AUTHORITY_ALLOCATION_{allocation_err or 'UNREADABLE'}")

    as_of_utc = latest_timestamp(*top_as_of_candidates)
    payload = view_envelope(
        view_name="sleeve_evaluation_state",
        as_of_utc=as_of_utc,
        freshness_state=freshness_state(as_of_utc),
        provenance_markers=markers("canonical", "derived"),
        source_refs=source_refs,
        surface_kind="projection",
        entity_scope="sleeves",
        truth_state="derived" if sleeve_warnings else "canonical",
        data_condition="degraded" if sleeve_warnings else "fresh",
        source_authority=[
            "C2_CAPITAL_AUTHORITY_POLICY_V1",
            "capital_authority_allocation_v1",
            "evaluation_input_manifest_v1",
            "sleeve_edge_measurement_snapshot_v1",
            "allocation_governance_snapshot_v1",
        ],
        contract_id="sleeve_evaluation_projection",
        contract_version="v1",
        provenance_refs=source_refs,
        degradation_codes=sleeve_warnings,
        current_day=resolved_day,
        sleeve_status="DEGRADED" if sleeve_warnings else "OK",
        sleeve_warnings=sleeve_warnings,
        sleeves=rows,
        sleeve_registry_summary={
            "total_sleeves": len(rows),
            "evaluated_sleeves": sum(1 for row in rows if row["recommendation"]["recommendation_state"] != "unavailable"),
            "source_refs": evidence_refs(policy_ref, allocation_ref),
            "warnings": ["TARGET_ALLOCATION_POLICY_UNPROVEN"],
        },
    )
    validation = validate_projection_envelope(payload)
    payload["_metadata_validation"] = validation
    if not validation.get("ok"):
        payload["_metadata_errors"] = validation
    return payload
