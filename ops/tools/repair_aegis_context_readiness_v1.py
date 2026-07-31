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

from ops.aegis.intelligence_common_v1 import latest_json_v1, write_json_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402

REPORT_FAMILY = "aegis_context_readiness_repair_v1"


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _run(cmd: list[str], *, env: dict[str, str]) -> dict[str, Any]:
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


def build_context_readiness_repair_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    env = {**os.environ, "TARGET_DAY": day_utc, "AEGIS_TRUTH_ROOT": str(root)}
    py = sys.executable
    commands = [
        ("validate_breadth_drop", [py, "ops/tools/validate_aegis_breadth_drop_v1.py", "--truth_root", str(root), "--day", day_utc]),
        ("validate_vix_drop", [py, "ops/tools/validate_aegis_vix_drop_v1.py", "--truth_root", str(root), "--day", day_utc]),
        ("verify_vix_source", [py, "ops/tools/verify_aegis_vix_source_v1.py", "--truth_root", str(root), "--day", day_utc]),
        ("provider_health", [py, "ops/tools/build_aegis_market_context_provider_health_v1.py", "--truth_root", str(root), "--day_utc", day_utc]),
        ("market_context_demand", [py, "ops/tools/build_aegis_market_context_demand_v1.py", "--truth_root", str(root), "--day_utc", day_utc]),
        ("event_market_snapshot", [py, "ops/tools/build_event_market_snapshot_v1.py", "--truth_root", str(root), "--day_utc", day_utc]),
        ("runtime_truth_kernel", [py, "ops/tools/run_aegis_runtime_truth_kernel_v1.py", "--truth_root", str(root), "--day", day_utc]),
        ("verified_graph", ["npm", "run", "aegis:verified-graph", "--", "--strict"]),
        ("hydrate", ["npm", "run", "aegis:chatgpt:hydrate"]),
        ("audit_handoff", [py, "ops/tools/build_aegis_audit_handoff_v1.py", "--truth_root", str(root), "--day", day_utc]),
    ]
    attempts = []
    for step_id, cmd in commands:
        attempt = _run(cmd, env=env)
        attempt["step_id"] = step_id
        attempts.append(attempt)

    breadth_path, breadth_payload = latest_json_v1(root, "aegis_breadth_drop_validation_v1", day_utc, "breadth_drop_validation.v1.json")
    vix_path, vix_payload = latest_json_v1(root, "aegis_vix_source_verification_v1", day_utc, "vix_source_verification.v1.json")
    provider_health_path, provider_health_payload = latest_json_v1(root, "aegis_market_context_provider_health_v1", day_utc, "provider_health.v1.json")
    event_path, event_payload = latest_json_v1(root, "event_market_snapshot_v1", day_utc, "event_market_snapshot.v1.json")
    runtime_path, runtime_payload = latest_json_v1(root, "aegis_runtime_truth_kernel_v1", day_utc, "runtime_truth_kernel.v1.json")
    graph_path, graph_payload = latest_json_v1(root, "aegis_verified_runtime_graph_v1", day_utc, "verified_runtime_graph.v1.json")

    provider_items = provider_health_payload.get("provider_items") if isinstance(provider_health_payload, dict) and isinstance(provider_health_payload.get("provider_items"), list) else []
    breadth_health_rows = [row for row in provider_items if isinstance(row, dict) and str(row.get("context_item_id") or "") in {"advance_decline_delta", "breadth_down_pct"}]
    breadth_status = "CERTIFIED" if breadth_health_rows and all(str(row.get("health_status") or "") == "CONTEXT_CERTIFIED" for row in breadth_health_rows) else (breadth_payload.get("certification_status") if isinstance(breadth_payload, dict) else "MISSING")
    market_context_items = event_payload.get("market_context_items") if isinstance(event_payload.get("market_context_items"), list) else []
    blockers = [
        f"{row.get('context_item_id')}: {row.get('fulfillment_status')} - {row.get('failure_reason')}"
        for row in market_context_items
        if isinstance(row, dict) and str(row.get("fulfillment_status") or "") != "CONTEXT_CERTIFIED"
    ]
    next_action = "Run npm run aegis:audit to confirm full readiness." if not blockers else "Resolve the listed context blockers, then rerun npm run aegis:repair-context-readiness."
    payload = {
        "schema_id": "aegis_context_readiness_repair",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day_utc,
        "generated_at_utc": _now(),
        "attempts": attempts,
        "breadth_status": breadth_status,
        "breadth_report_path": str(breadth_path or ""),
        "vix_status": vix_payload.get("status") if isinstance(vix_payload, dict) else "MISSING",
        "vix_report_path": str(vix_path or ""),
        "event_market_snapshot_status": event_payload.get("market_context_overall_status") if isinstance(event_payload, dict) else "MISSING",
        "event_market_snapshot_path": str(event_path or ""),
        "runtime_truth_classification": runtime_payload.get("runtime_truth_classification") if isinstance(runtime_payload, dict) else "UNKNOWN",
        "trade_advice_allowed": bool(runtime_payload.get("trade_advice_allowed", False)) if isinstance(runtime_payload, dict) else False,
        "verified_runtime_graph_path": str(graph_path or ""),
        "remaining_blockers": blockers,
        "next_operator_action": next_action,
        "closeout_summary": {
            "breadth_status": breadth_status,
            "vix_status": vix_payload.get("status") if isinstance(vix_payload, dict) else "MISSING",
            "event_market_snapshot_status": event_payload.get("market_context_overall_status") if isinstance(event_payload, dict) else "MISSING",
            "runtime_truth_classification": runtime_payload.get("runtime_truth_classification") if isinstance(runtime_payload, dict) else "UNKNOWN",
            "trade_advice_allowed": bool(runtime_payload.get("trade_advice_allowed", False)) if isinstance(runtime_payload, dict) else False,
            "remaining_blockers": blockers,
            "next_operator_action": next_action,
        },
        "safety": {
            "trade_advice_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "manual_capture_enabled": False,
        },
    }
    return payload


def write_context_readiness_repair_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "context_readiness_repair.v1.json", payload)
    summary_path = out_dir / "context_readiness_repair.summary.txt"
    lines = [
        f"breadth status: {payload.get('breadth_status')}",
        f"VIX status: {payload.get('vix_status')}",
        f"event_market_snapshot status: {payload.get('event_market_snapshot_status')}",
        f"runtime_truth_classification: {payload.get('runtime_truth_classification')}",
        f"trade_advice_allowed: {str(payload.get('trade_advice_allowed')).lower()}",
        "remaining blockers:",
    ]
    blockers = payload.get("remaining_blockers") or []
    lines.extend([f"- {item}" for item in blockers] or ["- none"])
    lines.append(f"next operator action: {payload.get('next_operator_action')}")
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="repair_aegis_context_readiness_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day_utc", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    payload = build_context_readiness_repair_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc))
    paths = write_context_readiness_repair_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), payload=payload)
    print(json.dumps({
        **paths,
        "breadth_status": payload["breadth_status"],
        "vix_status": payload["vix_status"],
        "event_market_snapshot_status": payload["event_market_snapshot_status"],
        "runtime_truth_classification": payload["runtime_truth_classification"],
        "trade_advice_allowed": payload["trade_advice_allowed"],
        "remaining_blockers": payload["remaining_blockers"],
        "next_operator_action": payload["next_operator_action"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
