#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.intelligence_common_v1 import write_json_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402

REPORT_FAMILY = "aegis_runtime_readiness_repair_v1"


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _run(cmd: list[str], *, env: dict[str, str] | None = None) -> dict[str, Any]:
    started = _now()
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, env=env)
    return {
        "command": " ".join(cmd),
        "exit_code": int(proc.returncode),
        "stdout": proc.stdout.strip()[-4000:],
        "stderr": proc.stderr.strip()[-4000:],
        "started_at_utc": started,
        "completed_at_utc": _now(),
    }


def _artifact_path(root: Path, day_utc: str, artifact_id: str) -> str:
    mapping = {
        "aegis_lite_operating_status": root / "reports" / "aegis_lite_operating_status_v1" / day_utc / "aegis_lite_operating_status.v1.json",
        "aegis_lite_eod_report": root / "reports" / "aegis_lite_eod_report_v1" / day_utc,
        "operator_execution_queue": root / "reports" / "operator_execution_queue_v1" / day_utc,
        "manual_trade_packet": root / "reports" / "manual_trade_packet_v1" / day_utc,
        "manual_execution_receipt": root / "reports" / "manual_execution_receipt_v1" / day_utc / "index" / "manual_execution_receipt.v1.json",
        "event_monitoring_status": root / "reports" / "event_monitoring_status_v1" / day_utc,
        "event_rules_registry": root / "reports" / "event_rules_registry_v1" / day_utc,
        "market_context_provider_health": root / "reports" / "aegis_market_context_provider_health_v1" / day_utc / "provider_health.v1.json",
        "market_context_demand": root / "reports" / "aegis_market_context_demand_v1" / day_utc / "market_context_demand.v1.json",
        "event_market_snapshot": root / "reports" / "event_market_snapshot_v1" / day_utc / "event_market_snapshot.v1.json",
        "event_validity_gate": root / "reports" / "event_validity_gate_v1" / day_utc / "index" / "event_validity_gate.v1.json",
        "alert_transport_proof": root / "reports" / "alert_transport_proof_v1" / day_utc,
        "ai_feedback_review": root / "reports" / "ai_feedback_review_v1" / "EOD" / day_utc / "ai_feedback_review.v1.json",
        "research_dataset_binding": root / "reports" / "research_dataset_gap_v1" / day_utc / "research_dataset_gap.v1.json",
        "research_task_queue": root / "research_lab" / "research_task_queue_v1" / day_utc / "index" / "research_task_queue.v1.json",
    }
    return str(mapping[artifact_id])


def _artifact_present(path_text: str) -> bool:
    path = Path(path_text)
    if path.is_file():
        return True
    if path.is_dir():
        return any(path.rglob("*.json"))
    return False


def _commands(root: Path, day_utc: str) -> list[tuple[str, list[str]]]:
    py = sys.executable
    return [
        ("repair_candidate_readiness", [py, "ops/tools/repair_aegis_candidate_readiness_v1.py", "--truth_root", str(root), "--day", day_utc]),
        ("market_context_provider_health", [py, "ops/tools/build_aegis_market_context_provider_health_v1.py", "--truth_root", str(root), "--day_utc", day_utc]),
        ("market_context_demand", [py, "ops/tools/build_aegis_market_context_demand_v1.py", "--truth_root", str(root), "--day_utc", day_utc]),
        ("event_market_snapshot", [py, "ops/tools/build_event_market_snapshot_v1.py", "--truth_root", str(root), "--day_utc", day_utc]),
        ("event_monitoring_status", [py, "ops/tools/run_aegis_event_monitor_v1.py", "--truth_root", str(root), "--day_utc", day_utc]),
        ("event_validity_gate", [py, "ops/tools/write_event_validity_evidence_v1.py", "--truth_root", str(root), "--day_utc", day_utc]),
        ("alert_transport_proof", [py, "ops/tools/write_alert_transport_proof_v1.py", "--truth_root", str(root), "--day_utc", day_utc, "--transport_mode", "GATE_ONLY"]),
        ("research_task_queue", [py, "ops/tools/run_research_test_queue_v1.py", "--truth_root", str(root), "--day_utc", day_utc, "--generated_at_utc", _now()]),
        ("research_dataset_binding", [py, "ops/tools/audit_research_dataset_bindings_v1.py", "--truth_root", str(root), "--day_utc", day_utc]),
        ("ai_feedback_review", [py, "ops/tools/build_ai_eod_feedback_review_v1.py", "--truth_root", str(root), "--day", day_utc]),
        ("aegis_lite_runtime", [py, "ops/tools/run_aegis_lite_eod_pipeline_v1.py", "--truth_root", str(root), "--day_utc", day_utc, "--manual-only", "--allow-not-ready-exit-zero"]),
        ("aegis_audit", ["npm", "run", "aegis:audit"]),
    ]


def build_runtime_readiness_repair_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    env = {**os.environ, "TARGET_DAY": day_utc, "AEGIS_TRUTH_ROOT": str(root)}
    attempts: list[dict[str, Any]] = []
    for artifact_id, cmd in _commands(root, day_utc):
        attempt = _run(cmd, env=env)
        attempt["artifact_id"] = artifact_id
        attempts.append(attempt)
    artifact_rows = []
    for artifact_id in [
        "aegis_lite_operating_status",
        "aegis_lite_eod_report",
        "operator_execution_queue",
        "manual_trade_packet",
        "manual_execution_receipt",
        "event_monitoring_status",
        "event_rules_registry",
        "market_context_provider_health",
        "market_context_demand",
        "event_market_snapshot",
        "event_validity_gate",
        "alert_transport_proof",
        "ai_feedback_review",
        "research_dataset_binding",
        "research_task_queue",
    ]:
        path_text = _artifact_path(root, day_utc, artifact_id)
        artifact_rows.append({
            "artifact_id": artifact_id,
            "path": path_text,
            "status": "PRESENT" if _artifact_present(path_text) else "MISSING",
            "recovery_command": "npm run aegis:repair-runtime-readiness",
        })
    runtime_blockers = [row for row in artifact_rows if row["status"] != "PRESENT"]
    status = "REPAIRED" if not runtime_blockers else "PARTIAL"
    payload = {
        "schema_id": "aegis_runtime_readiness_repair",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day_utc,
        "generated_at_utc": _now(),
        "status": status,
        "attempts": attempts,
        "artifact_statuses": artifact_rows,
        "runtime_blockers": runtime_blockers,
        "recommended_recovery_commands": sorted({row["recovery_command"] for row in runtime_blockers}),
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
        "manual_capture_enabled": False,
    }
    return payload


def write_runtime_readiness_repair_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "runtime_readiness_repair.v1.json", payload)
    summary_path = out_dir / "runtime_readiness_repair.summary.txt"
    lines = [
        "AEGIS RUNTIME READINESS REPAIR v1",
        f"day_utc: {day_utc}",
        f"status: {payload.get('status')}",
        "artifact_statuses:",
    ]
    for row in payload.get("artifact_statuses") or []:
        lines.append(f"- {row.get('artifact_id')}: {row.get('status')} path={row.get('path')}")
    lines.append("recommended_recovery_commands:")
    for command in payload.get("recommended_recovery_commands") or []:
        lines.append(f"- {command}")
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="repair_aegis_runtime_readiness_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day_utc", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    payload = build_runtime_readiness_repair_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc))
    paths = write_runtime_readiness_repair_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), payload=payload)
    print(json.dumps({**paths, "status": payload["status"], "runtime_blocker_count": len(payload.get("runtime_blockers") or []), "trade_advice_allowed": False, "broker_execution_allowed": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
