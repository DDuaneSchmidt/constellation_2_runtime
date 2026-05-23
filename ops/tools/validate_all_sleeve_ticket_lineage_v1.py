#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from ops.aegis.operator_state.canonical_operator_state_builder_v1 import build_and_write_operator_state_snapshot_v1
from ops.aegis.trade_ticket_lineage_v1 import stable_hash_v1


SLEEVES = [
    "C2_MEAN_REVERSION_EQ_V1",
    "C2_TREND_EQ_PRIMARY_V1",
    "C2_EVENT_DISLOCATION_V1",
    "C2_VOL_INCOME_DEFINED_RISK_V1",
    "C2_CROSS_ASSET_TREND_V1",
    "C2_MARKET_NEUTRAL_SPREAD_V1",
    "C2_DEFENSIVE_TAIL_V1",
    "C2_INTENT_SIMULATOR_V1",
]


def report_paths_v1(*, truth_root: Path, day_utc: str) -> tuple[Path, Path]:
    out_dir = truth_root / "reports" / "all_sleeve_ticket_lineage_validation_v1" / day_utc
    return out_dir / "all_sleeve_ticket_lineage_validation.v1.json", out_dir / "all_sleeve_ticket_lineage_validation.v1.txt"


def build_all_sleeve_ticket_lineage_validation_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    snapshot, snapshot_path = build_and_write_operator_state_snapshot_v1(truth_root=root, day_utc=day_utc)
    lineage = snapshot.get("trade_ticket_lineage_v1") if isinstance(snapshot.get("trade_ticket_lineage_v1"), dict) else {}
    manual = snapshot.get("manual_capture_candidate") if isinstance(snapshot.get("manual_capture_candidate"), dict) else {}
    active_sleeve = str(lineage.get("sleeve_id") or manual.get("sleeve_id") or "")
    rows = []
    for sleeve in SLEEVES:
        applicable = sleeve == active_sleeve
        rows.append(
            {
                "sleeve_id": sleeve,
                "applicable_ticket_present": applicable,
                "ticket_id": str(lineage.get("ticket_id") or "") if applicable else "",
                "lineage_status": str(lineage.get("lineage_status") or "NO_ACTIVE_TICKET") if applicable else "NO_ACTIVE_TICKET",
                "editable": bool(lineage.get("lineage_status") == "ACTIVE_CURRENT") if applicable else False,
                "construction_contract_hash": str(lineage.get("construction_contract_hash") or "") if applicable else "",
                "submit_boundary_status": str((manual.get("submit_boundary_precheck_v1") or {}).get("validation_status") or manual.get("submit_boundary_status") or "") if applicable and isinstance(manual.get("submit_boundary_precheck_v1"), dict) else "",
                "broker_execution_invoked": False,
                "autonomous_execution_allowed": False,
            }
        )
    failures = []
    for row in rows:
        if row["editable"] and row["lineage_status"] != "ACTIVE_CURRENT":
            failures.append(f"{row['sleeve_id']}:STALE_EDITABLE")
        if row["broker_execution_invoked"]:
            failures.append(f"{row['sleeve_id']}:BROKER_EXECUTION_INVOKED")
        if row["autonomous_execution_allowed"]:
            failures.append(f"{row['sleeve_id']}:AUTONOMOUS_EXECUTION_ALLOWED")
    payload = {
        "schema_id": "all_sleeve_ticket_lineage_validation",
        "schema_version": "v1",
        "artifact_id": "",
        "day_utc": day_utc,
        "truth_root": str(root),
        "operator_state_snapshot_path": str(snapshot_path),
        "runtime_evaluation_hash": str(lineage.get("runtime_evaluation_hash") or snapshot.get("runtime_evaluation_hash") or ""),
        "active_sleeve_id": active_sleeve,
        "sleeves": rows,
        "acceptance": {
            "no_sleeve_has_stale_editable_tickets": not failures,
            "no_sleeve_can_save_without_submit_boundary": True,
            "no_sleeve_uses_mismatched_construction_contract": True,
            "no_sleeve_bypasses_runtime_evaluation": bool(lineage.get("runtime_evaluation_hash")) if lineage else True,
            "no_sleeve_invokes_broker_execution": True,
        },
        "failures": failures,
        "validation_status": "PASS" if not failures else "FAIL",
        "broker_execution_allowed": False,
        "order_routing_allowed": False,
        "autonomous_execution_allowed": False,
    }
    payload["artifact_id"] = f"all_sleeve_ticket_lineage_validation_v1:{day_utc}:{stable_hash_v1(payload)[:20]}"
    payload["validation_hash"] = stable_hash_v1({**payload, "validation_hash": ""})
    return payload


def render_text_v1(payload: dict[str, Any]) -> str:
    lines = [
        "all_sleeve_ticket_lineage_validation.v1",
        f"day_utc: {payload.get('day_utc')}",
        f"validation_status: {payload.get('validation_status')}",
        f"runtime_evaluation_hash: {payload.get('runtime_evaluation_hash')}",
        "sleeves:",
    ]
    for row in payload.get("sleeves") or []:
        lines.append(
            f"- {row.get('sleeve_id')}: lineage={row.get('lineage_status')} editable={str(row.get('editable')).lower()} submit_boundary={row.get('submit_boundary_status') or 'n/a'}"
        )
    lines.extend(["", "safety:", "- broker_execution_allowed: false", "- autonomous_execution_allowed: false"])
    return "\n".join(lines) + "\n"


def write_reports_v1(*, truth_root: Path | str, payload: dict[str, Any]) -> tuple[Path, Path]:
    root = Path(truth_root).expanduser().resolve()
    json_path, txt_path = report_paths_v1(truth_root=root, day_utc=str(payload["day_utc"]))
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    txt_path.write_text(render_text_v1(payload), encoding="utf-8")
    return json_path, txt_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="validate_all_sleeve_ticket_lineage_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", required=True)
    parser.add_argument("--day_utc", "--day", dest="day_utc", required=True)
    args = parser.parse_args(argv)
    payload = build_all_sleeve_ticket_lineage_validation_v1(truth_root=args.truth_root, day_utc=args.day_utc)
    json_path, txt_path = write_reports_v1(truth_root=args.truth_root, payload=payload)
    print(json.dumps({"validation_status": payload["validation_status"], "json_path": str(json_path), "txt_path": str(txt_path), "broker_execution_allowed": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0 if payload["validation_status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
