#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _latest(root: Path, filename: str) -> Path | None:
    paths = sorted(root.rglob(filename))
    return paths[-1] if paths else None


def _latest_matching(root: Path, filename: str, day_utc: str, day_field: str) -> Path | None:
    paths = []
    for path in sorted(root.rglob(filename)):
        payload = _read(path)
        if str(payload.get(day_field) or "") == day_utc or day_utc in path.parts:
            paths.append(path)
    return paths[-1] if paths else None


def _read(path: Path | None) -> dict:
    if path is None:
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_operator_status_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", required=True)
    args = parser.parse_args(argv)

    root = Path(args.truth_root).expanduser().resolve()
    eod = _read(_latest(root / "reports" / "aegis_lite_eod_report_v1" / args.day_utc, "aegis_lite_eod_report.v1.json"))
    packet = _read(_latest_matching(root, "manual_trade_packet.v1.json", args.day_utc, "date"))
    perf = _read(_latest_matching(root, "sleeve_performance_report.v1.json", args.day_utc, "day_utc"))
    event = _read(_latest_matching(root, "event_monitoring_status.v1.json", args.day_utc, "day_utc"))
    dataset = _read(_latest_matching(root, "research_dataset_gap.v1.json", args.day_utc, "day_utc"))
    candidates = [row for row in packet.get("trade_candidates", []) if isinstance(row, dict)]
    receipts_missing = int((perf.get("portfolio_summary") or {}).get("missing_receipt_count") or 0)
    outcomes_missing = int((perf.get("portfolio_summary") or {}).get("missing_outcome_count") or 0)
    dataset_blockers = [row["blocker"] for row in dataset.get("dataset_gaps", []) if isinstance(row, dict) and row.get("blocker")]
    if receipts_missing:
        next_action = "Record missing manual execution receipts."
    elif outcomes_missing:
        next_action = "Record missing trade outcomes."
    elif candidates and any(row.get("actionable") for row in candidates):
        next_action = "Review manual trade packet and enter at most one supervised IB paper trade if still valid."
    elif dataset_blockers:
        next_action = "Resolve Research dataset blockers before relying on Research tests."
    else:
        next_action = "Run Aegis Lite EOD."
    payload = {
        "schema_id": "aegis_operator_status",
        "schema_version": "v1",
        "artifact_id": "aegis_operator_status_v1",
        "day_utc": args.day_utc,
        "generated_at_utc": _now(),
        "last_eod_run": eod.get("run_id", ""),
        "current_promoted_candidates": len(candidates),
        "actionable_manual_packets": sum(1 for row in candidates if row.get("actionable")),
        "manual_packets_needing_action": [row.get("recommended_trade_id") for row in candidates if row.get("actionable")],
        "receipts_missing": receipts_missing,
        "outcomes_missing": outcomes_missing,
        "sleeve_performance_status": "PRESENT" if perf else "MISSING",
        "event_monitor_status": "PRESENT" if event else "MISSING",
        "research_lab_open_tasks": "UNPROVEN_UI_JSON_ONLY",
        "dataset_blockers": dataset_blockers,
        "next_operator_action": next_action,
        "manual_execution_only": True,
        "broker_submit_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    path = root / "reports" / "aegis_operator_status_v1" / args.day_utc / "aegis_operator_status.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    print(json.dumps({"path": str(path), "next_operator_action": next_action, "broker_submit_required": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
