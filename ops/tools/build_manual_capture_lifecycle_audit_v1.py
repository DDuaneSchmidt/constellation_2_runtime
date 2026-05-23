#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.operator_state.current_operator_truth_resolver_v1 import resolve_current_operator_truth_v1
from ops.aegis.operator_state.manual_capture_record_v1 import list_manual_capture_records_v1
from ops.aegis.submit_boundary_precheck_v1 import build_submit_boundary_precheck_v1
from ops.aegis.trade_lifecycle.captured_ticket_history_v1 import (
    build_captured_ticket_history_v1,
    captured_ticket_projection_v1,
)
from ops.aegis.trade_lifecycle.paper_trade_construction_v1 import build_paper_trade_construction_v1
from ops.aegis.trade_lifecycle.trade_lifecycle_case_v1 import build_trade_lifecycle_case_v1
from ops.aegis.trade_lifecycle.trade_ticket_projection_v1 import trade_ticket_projection_v1
from ops.aegis.trade_ticket_lineage_v1 import build_trade_ticket_lineage_v1


STAGES = [
    "candidate_generation",
    "active_current_promotion",
    "conversion_package",
    "submit_boundary",
    "ticket_lineage",
    "operator_projection",
    "manual_capture_append",
    "post_capture_history",
    "replay_rebuild",
]


def _status(value: str) -> str:
    return value or "MISSING"


def build_manual_capture_lifecycle_audit_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    truth = resolve_current_operator_truth_v1(truth_root=root, day_utc=day_utc)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=day_utc, current_operator_truth=truth)
    lineage = build_trade_ticket_lineage_v1(truth_root=root, construction=construction)
    precheck = build_submit_boundary_precheck_v1(truth_root=root, construction=construction)
    case = build_trade_lifecycle_case_v1(truth_root=root, day_utc=day_utc, current_operator_truth=truth, paper_trade_construction=construction)
    projection = trade_ticket_projection_v1({**case, "trade_ticket_lineage_v1": lineage, "submit_boundary_precheck_v1": precheck})
    selected_id = str(construction.get("selected_exposure_intent_id") or truth.get("selected_exposure_intent_id") or "")
    records = list_manual_capture_records_v1(truth_root=root, day_utc=day_utc, selected_exposure_intent_id=selected_id)
    latest = records.get("latest_record") if isinstance(records.get("latest_record"), Mapping) else {}
    history = build_captured_ticket_history_v1(truth_root=root, day_utc=day_utc, capture_record=latest) if latest else {}
    captured_projection = captured_ticket_projection_v1(history) if history else {}
    post_capture = bool(history)
    nodes = [
        {
            "node_id": "candidate_generation",
            "artifact_name": "portfolio_gate_candidate_report_v1 / selected exposure intent",
            "state": "CANDIDATE" if selected_id else "MISSING",
            "editable": False,
            "historical": False,
            "hash": str(truth.get("source_run_id") or ""),
            "authority_boundary": "candidate discovery only",
            "downstream": ["active_current_promotion"],
        },
        {
            "node_id": "active_current_promotion",
            "artifact_name": "paper_trade_construction_v1",
            "state": "ACTIVE_CURRENT" if construction.get("trade_construction_status") == "complete" and not post_capture else "SUPERSEDED",
            "editable": bool(construction.get("trade_construction_status") == "complete" and not post_capture),
            "historical": False,
            "hash": str(construction.get("construction_contract_hash") or ""),
            "authority_boundary": "pre-capture construction",
            "downstream": ["conversion_package", "submit_boundary"],
        },
        {
            "node_id": "submit_boundary",
            "artifact_name": "submit_boundary_precheck_v1",
            "state": "POST_CAPTURE_NOT_APPLICABLE" if post_capture else _status(str(precheck.get("validation_status") or "")),
            "editable": False,
            "historical": post_capture,
            "hash": str(precheck.get("submit_boundary_hash") or ""),
            "authority_boundary": "pre-capture only" if not post_capture else "frozen at capture time",
            "downstream": ["manual_capture_append"] if not post_capture else ["post_capture_history"],
        },
        {
            "node_id": "ticket_lineage",
            "artifact_name": "trade_ticket_lineage_v1",
            "state": str(lineage.get("lineage_status") or ""),
            "editable": bool(lineage.get("editable") is True),
            "historical": bool(lineage.get("lineage_status") == "CAPTURED_HISTORICAL"),
            "hash": str(lineage.get("lineage_hash") or ""),
            "authority_boundary": "pre-capture lineage" if not post_capture else "capture-time lineage reference",
            "downstream": ["operator_projection"],
        },
        {
            "node_id": "manual_capture_append",
            "artifact_name": "manual_capture_record_v1",
            "state": "CAPTURED_MANUALLY" if latest else "NOT_RECORDED",
            "editable": False,
            "historical": bool(latest),
            "hash": str(latest.get("content_hash") or latest.get("immutable_hash") or ""),
            "authority_boundary": "append-only manual evidence",
            "downstream": ["post_capture_history"],
        },
        {
            "node_id": "post_capture_history",
            "artifact_name": "captured_ticket_history_v1 / captured_ticket_projection_v1",
            "state": str(captured_projection.get("historical_state") or "NOT_APPLICABLE"),
            "editable": False,
            "historical": bool(captured_projection),
            "hash": str(history.get("history_hash") or ""),
            "authority_boundary": "post-capture historical replay",
            "downstream": ["replay_rebuild", "operator_projection"],
        },
        {
            "node_id": "operator_projection",
            "artifact_name": "trade_ticket_projection_v1",
            "state": str(projection.get("lifecycle_state") or projection.get("current_state") or ""),
            "editable": bool(projection.get("editable") is True),
            "historical": bool(projection.get("historical_read_only") is True),
            "hash": str(projection.get("immutable_hash") or projection.get("projection_hash") or ""),
            "authority_boundary": "operator display projection",
            "downstream": [],
        },
    ]
    return {
        "schema_id": "manual_capture_lifecycle_audit",
        "schema_version": "v1",
        "day_utc": day_utc,
        "truth_root": str(root),
        "selected_exposure_intent_id": selected_id,
        "ticket_id": str(lineage.get("ticket_id") or ""),
        "lifecycle_state": str(projection.get("lifecycle_state") or projection.get("current_state") or ""),
        "captured_record_id": str(latest.get("record_id") or ""),
        "event_ids": [str(item) for item in latest.get("event_ids") or [] if str(item)],
        "post_capture_authority": "IMMUTABLE_CAPTURE_EVIDENCE" if post_capture else "PRE_CAPTURE_RUNTIME_EVALUATION",
        "pre_capture_revalidation_applies": not post_capture,
        "submit_boundary_revalidation_required": False if post_capture else True,
        "runtime_revalidation_required": False if post_capture else True,
        "nodes": nodes,
        "state_machine": {
            "allowed_states": ["CANDIDATE", "FILTERED_OUT", "ACTIVE_CURRENT", "CAPTURE_READY", "CAPTURE_IN_PROGRESS", "CAPTURED_MANUALLY", "CAPTURED_HISTORICAL", "INVALIDATED", "STALE_PRE_CAPTURE", "SUPERSEDED"],
            "terminal_historical_states": ["CAPTURED_HISTORICAL", "INVALIDATED", "FILTERED_OUT"],
            "forbidden_post_capture_transitions": ["CAPTURED_HISTORICAL->ACTIVE_CURRENT", "CAPTURED_HISTORICAL->STALE_PRE_CAPTURE"],
        },
        "safety": {
            "broker_execution_allowed": False,
            "order_routing_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
        },
    }


def _render_text(payload: Mapping[str, Any]) -> str:
    lines = [
        "Manual Capture Lifecycle Audit",
        f"day_utc: {payload.get('day_utc')}",
        f"ticket_id: {payload.get('ticket_id')}",
        f"lifecycle_state: {payload.get('lifecycle_state')}",
        f"post_capture_authority: {payload.get('post_capture_authority')}",
        "",
        "Nodes:",
    ]
    for node in payload.get("nodes") or []:
        lines.append(f"- {node['node_id']}: {node['state']} editable={node['editable']} historical={node['historical']} hash={node['hash']}")
    return "\n".join(lines) + "\n"


def _render_dot(payload: Mapping[str, Any]) -> str:
    edges = [
        ("CANDIDATE", "ACTIVE_CURRENT"),
        ("ACTIVE_CURRENT", "CAPTURE_READY"),
        ("CAPTURE_READY", "CAPTURE_IN_PROGRESS"),
        ("CAPTURE_IN_PROGRESS", "CAPTURED_MANUALLY"),
        ("CAPTURED_MANUALLY", "CAPTURED_HISTORICAL"),
        ("ACTIVE_CURRENT", "STALE_PRE_CAPTURE"),
        ("ACTIVE_CURRENT", "INVALIDATED"),
        ("ACTIVE_CURRENT", "SUPERSEDED"),
    ]
    lines = ["digraph manual_capture_lifecycle_v1 {", "  rankdir=LR;"]
    for left, right in edges:
        lines.append(f'  "{left}" -> "{right}";')
    lines.append('  "CAPTURED_HISTORICAL" [shape=doublecircle];')
    lines.append("}")
    return "\n".join(lines) + "\n"


def write_reports(payload: Mapping[str, Any], *, truth_root: Path | str, day_utc: str) -> dict[str, str]:
    out = Path(truth_root).expanduser().resolve() / "reports" / "manual_capture_lifecycle_audit_v1" / day_utc
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "manual_capture_lifecycle_audit_v1.json"
    txt_path = out / "manual_capture_lifecycle_audit_v1.txt"
    dot_path = out / "lifecycle_state_machine_v1.dot"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    txt_path.write_text(_render_text(payload), encoding="utf-8")
    dot_path.write_text(_render_dot(payload), encoding="utf-8")
    return {"json_path": str(json_path), "txt_path": str(txt_path), "dot_path": str(dot_path)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_manual_capture_lifecycle_audit_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", required=True)
    parser.add_argument("--day-utc", "--day_utc", "--day", dest="day_utc", required=True)
    args = parser.parse_args(argv)
    payload = build_manual_capture_lifecycle_audit_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc))
    paths = write_reports(payload, truth_root=Path(args.truth_root), day_utc=str(args.day_utc))
    print(json.dumps({"ok": True, **paths, "lifecycle_state": payload.get("lifecycle_state"), "ticket_id": payload.get("ticket_id")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
