from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from .common import GLOBAL_TRUTH_ROOT, SLEEVE_TRUTH_ROOT, evidence_ref, freshness_state, latest_timestamp, read_json_dict, resolve_ui_day
from .dto import evidence_refs, markers, view_envelope
from .metadata_contract import validate_projection_envelope
from .shared_status import classify_health


def _order_terms(order_plan: Dict[str, Any]) -> Dict[str, Any]:
    terms = order_plan.get("order_terms")
    return terms if isinstance(terms, dict) else {}


def _truth_state_from_markers(marker_values: List[str]) -> str:
    if "reconstructed" in marker_values:
        return "reconstructed"
    if "derived" in marker_values:
        return "derived"
    if "canonical" in marker_values and "unknown" not in marker_values:
        return "canonical"
    return "UNKNOWN"


def _data_condition_from_freshness(value: Optional[str]) -> str:
    normalized = str(value or "").strip().lower()
    if normalized == "healthy":
        return "fresh"
    if normalized in {"fresh", "stale", "degraded", "fail_closed", "unknown"}:
        return normalized
    return "UNKNOWN"


def _envelope_truth_state(rows: List[Dict[str, Any]]) -> str:
    states = {str(row.get("truth_state") or "UNKNOWN") for row in rows}
    if len(states) == 1:
        return next(iter(states))
    if len(states) > 1:
        return "derived"
    return "UNKNOWN"


def build_orders_view(day: Optional[str] = None) -> Dict[str, Any]:
    resolved_day = resolve_ui_day(day)
    submissions_root = (SLEEVE_TRUTH_ROOT / "execution_evidence_v1" / "submissions" / (resolved_day or "UNKNOWN")).resolve()
    fill_root = (SLEEVE_TRUTH_ROOT / "fill_ledger_v1" / (resolved_day or "UNKNOWN")).resolve()
    lifecycle_authority_path = (
        SLEEVE_TRUTH_ROOT
        / "reports"
        / "execution_lifecycle_authority_v1"
        / (resolved_day or "UNKNOWN")
        / "execution_lifecycle_authority.v1.json"
    ).resolve()
    lifecycle_authority_doc, _ = read_json_dict(lifecycle_authority_path)
    lifecycle_rows = {
        str(row.get("submission_id") or "").strip(): row
        for row in ((lifecycle_authority_doc or {}).get("submissions") or [])
        if isinstance(row, dict) and str(row.get("submission_id") or "").strip()
    }
    lineage_authority_path = (
        GLOBAL_TRUTH_ROOT
        / "reports"
        / "trade_lineage_graph_v1"
        / (resolved_day or "UNKNOWN")
        / "trade_lineage_graph.v1.json"
    ).resolve()
    lineage_authority_doc, _ = read_json_dict(lineage_authority_path)
    lineage_rows = {
        str(row.get("submission_id") or "").strip(): row
        for row in ((lineage_authority_doc or {}).get("lineages") or [])
        if isinstance(row, dict) and str(row.get("submission_id") or "").strip()
    }
    execution_mode_authority_path = (
        GLOBAL_TRUTH_ROOT
        / "reports"
        / "execution_mode_authority_v1"
        / (resolved_day or "UNKNOWN")
        / "execution_mode_authority.v1.json"
    ).resolve()
    execution_mode_authority_doc, _ = read_json_dict(execution_mode_authority_path)
    mode_submission_rows = {
        str(row.get("submission_id") or "").strip(): row
        for row in ((execution_mode_authority_doc or {}).get("submissions") or [])
        if isinstance(row, dict) and str(row.get("submission_id") or "").strip()
    }
    risk_sizing_authority_path = (
        GLOBAL_TRUTH_ROOT
        / "reports"
        / "risk_sizing_authority_v1"
        / (resolved_day or "UNKNOWN")
        / "risk_sizing_authority.v1.json"
    ).resolve()
    risk_sizing_authority_doc, _ = read_json_dict(risk_sizing_authority_path)
    sizing_rows = {
        str(row.get("intent_hash") or "").strip(): row
        for row in ((risk_sizing_authority_doc or {}).get("sizing_decisions") or [])
        if isinstance(row, dict) and str(row.get("intent_hash") or "").strip()
    }
    source_authority = [
        "execution_lifecycle_authority_v1",
        "trade_lineage_graph_v1",
        "execution_mode_authority_v1",
        "risk_sizing_authority_v1",
    ]

    rows: List[Dict[str, Any]] = []
    source_refs: List[Dict[str, Any]] = []
    as_of_candidates: List[str] = []

    if submissions_root.exists() and submissions_root.is_dir():
        for submission_dir in sorted([path for path in submissions_root.iterdir() if path.is_dir()], key=lambda item: item.name):
            submission_id = submission_dir.name
            order_plan_path = (submission_dir / "equity_order_plan.v1.json").resolve()
            broker_path = (submission_dir / "broker_submission_record.v2.json").resolve()
            execution_path = (submission_dir / "execution_event_record.v1.json").resolve()
            mapping_path = (submission_dir / "mapping_ledger_record.v2.json").resolve()
            fill_path = (fill_root / f"{submission_id}.fill_ledger.v1.json").resolve()

            order_plan, _ = read_json_dict(order_plan_path)
            broker_doc, _ = read_json_dict(broker_path)
            execution_doc, _ = read_json_dict(execution_path)
            mapping_doc, _ = read_json_dict(mapping_path)
            fill_doc, _ = read_json_dict(fill_path)
            authority_row = lifecycle_rows.get(submission_id) or {}
            lineage_row = lineage_rows.get(submission_id) or {}
            mode_row = mode_submission_rows.get(submission_id) or {}
            intent_hash = str(lineage_row.get("intent_hash") or (order_plan or {}).get("intent_hash") or "").strip()
            sizing_row = sizing_rows.get(intent_hash) or {}

            terms = _order_terms(order_plan or {})
            broker_ids = (broker_doc or {}).get("broker_ids") if isinstance((broker_doc or {}).get("broker_ids"), dict) else {}
            lifecycle_status = (
                authority_row.get("current_lifecycle_state")
                or (execution_doc or {}).get("status")
                or (fill_doc or {}).get("lifecycle_status")
                or (broker_doc or {}).get("status")
                or "UNKNOWN"
            )
            broker_status = (broker_doc or {}).get("status") or "UNKNOWN"
            last_update = latest_timestamp(
                str((execution_doc or {}).get("event_time_utc") or ""),
                str((fill_doc or {}).get("produced_utc") or ""),
                str((broker_doc or {}).get("submitted_at_utc") or ""),
                str((order_plan or {}).get("created_at_utc") or ""),
            )
            if last_update:
                as_of_candidates.append(last_update)

            marker_values = ["canonical"]
            if execution_doc is None and fill_doc is None and broker_doc is not None:
                marker_values.append("derived")
            if broker_doc is None:
                marker_values.append("unknown")

            refs = evidence_refs(
                evidence_ref(order_plan_path if order_plan else None, label="Order Plan", artifact_type="equity_order_plan"),
                evidence_ref(broker_path if broker_doc else None, label="Broker Submission", artifact_type="broker_submission_record"),
                evidence_ref(execution_path if execution_doc else None, label="Execution Event", artifact_type="execution_event_record"),
                evidence_ref(fill_path if fill_doc else None, label="Fill Ledger", artifact_type="fill_ledger"),
                evidence_ref(mapping_path if mapping_doc else None, label="Mapping Ledger", artifact_type="mapping_ledger_record"),
                evidence_ref(lifecycle_authority_path if lifecycle_authority_doc else None, label="Execution Lifecycle Authority", artifact_type="execution_lifecycle_authority"),
                evidence_ref(lineage_authority_path if lineage_authority_doc else None, label="Trade Lineage Graph", artifact_type="trade_lineage_graph"),
                evidence_ref(execution_mode_authority_path if execution_mode_authority_doc else None, label="Execution Mode Authority", artifact_type="execution_mode_authority"),
                evidence_ref(risk_sizing_authority_path if risk_sizing_authority_doc else None, label="Risk Sizing Authority", artifact_type="risk_sizing_authority"),
            )
            truth_state = _truth_state_from_markers(marker_values)
            row_freshness_state = freshness_state(last_update)
            source_refs.extend(refs)
            rows.append(
                {
                    "entity_id": submission_id,
                    "submission_id": submission_id,
                    "symbol": (order_plan or {}).get("symbol"),
                    "side": (order_plan or {}).get("action"),
                    "qty": (order_plan or {}).get("qty_shares"),
                    "order_type": terms.get("order_type"),
                    "tif": terms.get("time_in_force"),
                    "broker_status": broker_status,
                    "broker_ids": {
                        "order_id": authority_row.get("broker_order_id") if authority_row else broker_ids.get("order_id"),
                        "perm_id": authority_row.get("broker_perm_id") if authority_row else broker_ids.get("perm_id"),
                    },
                    "lifecycle_status": lifecycle_status,
                    "identity_state": lineage_row.get("identity_state") if lineage_row else "UNKNOWN",
                    "intent_hash": intent_hash or None,
                    "trade_lineage_id": lineage_row.get("trade_lineage_id") if lineage_row else None,
                    "risk_sizing_state": sizing_row.get("sizing_state") if sizing_row else ((risk_sizing_authority_doc or {}).get("risk_sizing_state") if risk_sizing_authority_doc else "UNKNOWN"),
                    "risk_final_quantity": sizing_row.get("final_quantity") if sizing_row else None,
                    "risk_final_risk_cents": sizing_row.get("final_risk_cents") if sizing_row else None,
                    "risk_sizing_reason_code": sizing_row.get("reason_code") if sizing_row else None,
                    "execution_mode": (execution_mode_authority_doc or {}).get("mode_state") if execution_mode_authority_doc else "UNKNOWN",
                    "broker_transmit_enabled": (execution_mode_authority_doc or {}).get("broker_transmit_enabled") if execution_mode_authority_doc else None,
                    "broker_ids_expected": (execution_mode_authority_doc or {}).get("broker_ids_expected") if execution_mode_authority_doc else None,
                    "broker_ids_present": mode_row.get("broker_ids_present") if mode_row else None,
                    "lifecycle_semantic": classify_health(lifecycle_status),
                    "first_blocker_or_gap": authority_row.get("first_blocker_or_gap") if authority_row else "",
                    "fills_status": authority_row.get("fills_status") if authority_row else None,
                    "reconciliation_complete": authority_row.get("reconciliation_complete") if authority_row else None,
                    "last_update": last_update,
                    "as_of_utc": last_update,
                    "freshness_state": row_freshness_state,
                    "data_condition": _data_condition_from_freshness(row_freshness_state),
                    "truth_state": truth_state,
                    "source_authority": list(source_authority),
                    "provenance_markers": markers(*marker_values),
                    "provenance_refs": refs,
                    "degradation_codes": [],
                    "canonical_marker": "canonical" in marker_values,
                    "derived_marker": "derived" in marker_values,
                    "reconstructed_marker": False,
                    "evidence_refs": refs,
                }
            )

    rows.sort(key=lambda item: (str(item.get("last_update") or ""), str(item.get("submission_id") or "")), reverse=True)
    as_of_utc = max(as_of_candidates) if as_of_candidates else None
    envelope_freshness_state = freshness_state(as_of_utc)
    payload = view_envelope(
        view_name="orders",
        as_of_utc=as_of_utc,
        freshness_state=envelope_freshness_state,
        provenance_markers=["canonical", "derived"],
        source_refs=source_refs,
        surface_kind="projection",
        entity_scope="orders",
        truth_state=_envelope_truth_state(rows),
        data_condition=_data_condition_from_freshness(envelope_freshness_state),
        source_authority=list(source_authority),
        contract_id="order_lifecycle_projection",
        contract_version="v1",
        provenance_refs=source_refs,
        degradation_codes=[],
        current_day=resolved_day,
        orders=rows,
        total_orders=len(rows),
        active_orders=sum(1 for row in rows if str(row.get("lifecycle_status") or "").upper() not in {"FILLED", "CANCELLED", "REJECTED"}),
    )
    validation_errors = validate_projection_envelope(payload)
    payload["_metadata_validation"] = validation_errors
    if not validation_errors.get("ok"):
        payload["_metadata_errors"] = validation_errors
    return payload
