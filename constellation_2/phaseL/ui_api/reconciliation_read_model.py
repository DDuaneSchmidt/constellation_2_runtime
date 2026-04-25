from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from .common import SLEEVE_TRUTH_ROOT, evidence_ref, freshness_state, read_json_dict, resolve_ui_day
from .dto import evidence_refs, view_envelope
from .metadata_contract import validate_projection_envelope
from .shared_status import classify_health


def _truth_state_from_status(status: str, has_inputs: bool) -> str:
    normalized = str(status or "UNKNOWN").strip().upper()
    if not has_inputs or normalized == "UNKNOWN":
        return "UNKNOWN"
    if normalized in {"OK", "SKIPPED_SAFE_IDLE", "MATCHED"}:
        return "canonical"
    return "derived"


def _data_condition_from_freshness(value: Optional[str]) -> str:
    normalized = str(value or "").strip().lower()
    if normalized == "healthy":
        return "fresh"
    if normalized in {"fresh", "stale", "degraded", "fail_closed", "unknown"}:
        return normalized
    return "UNKNOWN"


def _envelope_truth_state(values: List[str]) -> str:
    states = {str(value or "UNKNOWN") for value in values}
    if len(states) == 1:
        return next(iter(states))
    if len(states) > 1:
        return "derived"
    return "UNKNOWN"


def build_reconciliation_view(day: Optional[str] = None) -> Dict[str, Any]:
    resolved_day = resolve_ui_day(day)
    report_path = (SLEEVE_TRUTH_ROOT / "reports" / "reconciliation_report_v3" / (resolved_day or "UNKNOWN") / "reconciliation_report.v3.json").resolve()
    report_doc, _ = read_json_dict(report_path)
    execution_path = (SLEEVE_TRUTH_ROOT / "reports" / "execution_reconciliation_v1" / (resolved_day or "UNKNOWN") / "execution_reconciliation.v1.json").resolve()
    execution_doc, _ = read_json_dict(execution_path)
    source_authority = ["reconciliation_report_v3", "execution_reconciliation_v1"]

    comparisons = report_doc.get("comparisons") if isinstance(report_doc, dict) and isinstance(report_doc.get("comparisons"), dict) else {}
    mismatches: List[Dict[str, Any]] = []
    comparison_truth_states: List[str] = []
    for key, value in comparisons.items():
        if not isinstance(value, dict):
            continue
        status = str(value.get("status") or "UNKNOWN")
        has_inputs = bool(report_doc or execution_doc or value)
        truth_state = _truth_state_from_status(status, has_inputs)
        comparison_truth_states.append(truth_state)
        if status not in {"OK", "SKIPPED_SAFE_IDLE"}:
            refs = evidence_refs(
                evidence_ref(report_path if report_doc else None, label="Reconciliation Report v3", artifact_type="reconciliation_report_v3"),
                evidence_ref(execution_path if execution_doc else None, label="Execution Reconciliation", artifact_type="execution_reconciliation"),
            )
            mismatches.append(
                {
                    "entity_id": key,
                    "comparison_id": key,
                    "status": status,
                    "semantic": classify_health(status),
                    "reason": value.get("reason"),
                    "truth_state": truth_state,
                    "source_authority": list(source_authority),
                    "provenance_refs": refs,
                    "degradation_codes": [] if truth_state != "UNKNOWN" else ["missing_inputs"],
                }
            )

    fill_root = (SLEEVE_TRUTH_ROOT / "fill_ledger_v1" / (resolved_day or "UNKNOWN")).resolve()
    fill_status = "UNKNOWN"
    fill_refs: List[Dict[str, Any]] = []
    if fill_root.exists() and fill_root.is_dir():
        fill_paths = sorted([path for path in fill_root.iterdir() if path.is_file()])
        fill_status = "OK" if fill_paths else "UNKNOWN"
        fill_refs = evidence_refs(*[evidence_ref(path, artifact_type="fill_ledger") for path in fill_paths[:5]])

    as_of_utc = str((report_doc or {}).get("produced_utc") or (execution_doc or {}).get("produced_utc") or "") or None
    envelope_freshness_state = freshness_state(as_of_utc)
    payload = view_envelope(
        view_name="reconciliation",
        as_of_utc=as_of_utc,
        freshness_state=envelope_freshness_state,
        provenance_markers=["canonical", "derived"],
        source_refs=evidence_refs(
            evidence_ref(report_path if report_doc else None, label="Reconciliation Report v3", artifact_type="reconciliation_report_v3"),
            evidence_ref(execution_path if execution_doc else None, label="Execution Reconciliation", artifact_type="execution_reconciliation"),
            *fill_refs,
        ),
        surface_kind="projection",
        entity_scope="reconciliation",
        truth_state=_envelope_truth_state(comparison_truth_states),
        data_condition=_data_condition_from_freshness(envelope_freshness_state),
        source_authority=list(source_authority),
        contract_id="reconciliation_status_projection",
        contract_version="v1",
        provenance_refs=evidence_refs(
            evidence_ref(report_path if report_doc else None, label="Reconciliation Report v3", artifact_type="reconciliation_report_v3"),
            evidence_ref(execution_path if execution_doc else None, label="Execution Reconciliation", artifact_type="execution_reconciliation"),
            *fill_refs,
        ),
        degradation_codes=[],
        current_day=resolved_day,
        execution_reconciliation_status=str((execution_doc or {}).get("status") or comparisons.get("truth_submissions_vs_broker_execdetails", {}).get("status") or "UNKNOWN"),
        fill_ledger_status=fill_status,
        canonical_vs_sleeve_agreement_summary={
            "status": "UNKNOWN",
            "semantic": "unknown",
            "reason": "No dedicated canonical-vs-sleeve agreement artifact was proven in repo for this workspace; preserving UNKNOWN explicitly.",
        },
        orphan_reconstructed_lineage_summary={
            "status": "UNKNOWN",
            "semantic": "unknown",
            "reason": "No dedicated orphan/reconstructed lineage report was proven in the canonical runtime-data roots for this day.",
        },
        mismatches=mismatches,
        affected_submission_ids=((report_doc or {}).get("truth_side") or {}).get("submission_ids") if isinstance(((report_doc or {}).get("truth_side") or {}).get("submission_ids"), list) else [],
        broker_summary=(report_doc or {}).get("broker_side"),
        evidence_refs=evidence_refs(evidence_ref(report_path if report_doc else None, label="Reconciliation Report v3", artifact_type="reconciliation_report_v3")),
    )
    validation = validate_projection_envelope(payload)
    payload["_metadata_validation"] = validation
    if not validation.get("ok"):
        payload["_metadata_errors"] = validation
    return payload
