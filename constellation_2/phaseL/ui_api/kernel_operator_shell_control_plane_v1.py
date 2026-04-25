from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from constellation_2.common.control_plane_read_gateway_v1 import read_control_plane_surface_v1


def _surface_payload(
    *,
    domain: str,
    surface: str,
    truth_root: Path,
) -> tuple[Optional[Path], Optional[Dict[str, Any]]]:
    try:
        ref = read_control_plane_surface_v1(
            domain=domain,
            surface=surface,
            truth_root=truth_root,
        )
    except Exception:
        return None, None
    return ref.path, dict(ref.payload)


def _scope_from_record(record: Optional[Dict[str, Any]]) -> dict[str, str] | None:
    if not isinstance(record, dict):
        return None
    return {
        "day_utc": str(record.get("day_utc") or ""),
        "environment": str(record.get("environment") or ""),
        "ib_account": str(record.get("ib_account") or ""),
        "sleeve_id": str(record.get("sleeve_id") or "PRIMARY"),
        "capability_scope": str(record.get("capability_scope") or "paper_trade_submit_entry_v1"),
        "expected_runtime_control_record_id": str(record.get("runtime_control_record_id") or ""),
    }


def load_runtime_control_workspace_bundle_v1(*, truth_root: Path) -> Dict[str, Any]:
    record_path, record = _surface_payload(
        domain="execution",
        surface="runtime_control_record",
        truth_root=truth_root,
    )
    decision_path, decision = _surface_payload(
        domain="execution",
        surface="runtime_control_decision",
        truth_root=truth_root,
    )
    envelope_path, envelope = _surface_payload(
        domain="execution",
        surface="runtime_control_run_envelope",
        truth_root=truth_root,
    )
    return {
        "record_path": record_path,
        "record": record,
        "decision_path": decision_path,
        "decision": decision,
        "envelope_path": envelope_path,
        "envelope": envelope,
        "scope": _scope_from_record(record),
    }
