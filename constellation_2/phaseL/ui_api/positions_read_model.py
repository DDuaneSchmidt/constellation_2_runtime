from __future__ import annotations

from typing import Any, Dict, List, Optional

from .common import SLEEVE_TRUTH_ROOT, evidence_ref, freshness_state, normalize_symbol, provenance_markers, read_json_dict, resolve_ui_day
from .dto import evidence_refs, markers, view_envelope
from .orders_read_model import build_orders_view


def _data_condition_from_freshness(value: Optional[str]) -> str:
    normalized = str(value or "").strip().lower()
    if normalized == "healthy":
        return "fresh"
    if normalized in {"fresh", "stale", "degraded", "fail_closed", "unknown"}:
        return normalized
    return "UNKNOWN"


def position_state_projection(day: Optional[str] = None) -> Dict[str, Any]:
    resolved_day = resolve_ui_day(day)
    snapshot_path = (SLEEVE_TRUTH_ROOT / "positions_v1" / "snapshots" / (resolved_day or "UNKNOWN") / "positions_snapshot.v2.json").resolve()
    snapshot_doc, _ = read_json_dict(snapshot_path)

    positions_doc = snapshot_doc.get("positions") if isinstance(snapshot_doc, dict) and isinstance(snapshot_doc.get("positions"), dict) else {}
    items = positions_doc.get("items") if isinstance(positions_doc.get("items"), list) else []
    notes = snapshot_doc.get("notes") if isinstance(snapshot_doc.get("notes"), list) else []
    as_of_utc = str(positions_doc.get("asof_utc") or snapshot_doc.get("produced_utc") or "") or None
    projection_truth_state = "canonical" if snapshot_doc else "UNKNOWN"
    projection_freshness_state = freshness_state(as_of_utc)
    projection_refs = evidence_refs(
        evidence_ref(snapshot_path if snapshot_doc else None, label="Positions Snapshot", artifact_type="positions_snapshot")
    )

    rows: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        symbol = normalize_symbol(item)
        marker_values = ["canonical"]
        if any("CARRY_FORWARD" in str(note).upper() for note in notes):
            marker_values.append("reconstructed")
        row_as_of_utc = positions_doc.get("asof_utc") or snapshot_doc.get("produced_utc")
        row_freshness_state = freshness_state(row_as_of_utc)
        rows.append(
            {
                "position_id": item.get("position_id"),
                "symbol": symbol,
                "engine_id": item.get("engine_id"),
                "qty": item.get("qty"),
                "status": item.get("status"),
                "avg_price": (int(item.get("avg_cost_cents")) / 100.0) if isinstance(item.get("avg_cost_cents"), int) else None,
                "unrealized_pnl": None,
                "realized_pnl": None,
                "last_update": row_as_of_utc,
                "as_of_utc": row_as_of_utc,
                "freshness_state": row_freshness_state,
                "data_condition": _data_condition_from_freshness(row_freshness_state),
                "truth_state": projection_truth_state,
                "source_authority": ["positions_snapshot_v2"],
                "provenance_markers": markers(*marker_values),
                "provenance_refs": projection_refs,
                "degradation_codes": [],
                "evidence_refs": projection_refs,
            }
        )

    return view_envelope(
        view_name="positions",
        as_of_utc=as_of_utc,
        freshness_state=projection_freshness_state,
        provenance_markers=provenance_markers("canonical", "reconstructed" if any("reconstructed" in row.get("provenance_markers", []) for row in rows) else ""),
        source_refs=projection_refs,
        surface_kind="projection",
        entity_scope="positions",
        truth_state=projection_truth_state,
        data_condition=_data_condition_from_freshness(projection_freshness_state),
        source_authority=["positions_snapshot_v2"],
        contract_id="position_state_projection",
        contract_version="v1",
        provenance_refs=projection_refs,
        degradation_codes=[],
        current_day=resolved_day,
        positions=rows,
        total_positions=len(rows),
        notes=notes,
    )


def positions_workspace_view(day: Optional[str] = None) -> Dict[str, Any]:
    projection = position_state_projection(day)
    resolved_day = projection.get("current_day")
    orders_view = build_orders_view(resolved_day)
    order_rows = orders_view.get("orders") if isinstance(orders_view.get("orders"), list) else []

    composed_rows: List[Dict[str, Any]] = []
    for row in projection.get("positions", []):
        if not isinstance(row, dict):
            continue
        symbol = row.get("symbol")
        linked_orders = [
            {
                "submission_id": order_row.get("submission_id"),
                "lifecycle_status": order_row.get("lifecycle_status"),
            }
            for order_row in order_rows
            if order_row.get("symbol") == symbol and order_row.get("lifecycle_status") not in {"FILLED", "CANCELLED", "REJECTED"}
        ]
        composed_row = dict(row)
        composed_row["open_order_linkage"] = linked_orders
        composed_rows.append(composed_row)

    payload = dict(projection)
    payload["surface_kind"] = "composition"
    payload["contract_id"] = "positions_workspace_view"
    payload["contract_version"] = "v1"
    payload["_projection_metadata"] = {
        "contract_id": "position_state_projection",
        "contract_version": "v1",
        "source_authority": ["positions_snapshot_v2"],
    }
    payload["positions"] = composed_rows
    return payload


def build_positions_view(day: Optional[str] = None) -> Dict[str, Any]:
    return positions_workspace_view(day)
