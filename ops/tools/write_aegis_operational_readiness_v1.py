#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.intelligence_common_v1 import intelligence_summaries_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT, build_runtime_truth_kernel_v1, write_runtime_truth_kernel_reports_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="write_aegis_operational_readiness_v1")
    parser.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)

    root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day)
    kernel = build_runtime_truth_kernel_v1(truth_root=root, day_utc=day)
    write_runtime_truth_kernel_reports_v1(truth_root=root, payload=kernel)
    payload = build_operational_readiness_v1(kernel)
    out_dir = root / "reports" / "aegis_operational_readiness_v1" / day
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "operational_readiness.v1.json"
    summary_path = out_dir / "operational_readiness.summary.txt"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(render_summary(payload), encoding="utf-8")
    print(json.dumps({"path": str(json_path), "summary_path": str(summary_path), "broker_submit_required": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


def build_operational_readiness_v1(kernel: dict[str, Any]) -> dict[str, Any]:
    statuses = kernel.get("artifact_statuses") if isinstance(kernel.get("artifact_statuses"), list) else []
    graph = kernel.get("dependency_graph") if isinstance(kernel.get("dependency_graph"), dict) else {}
    evidence_classes = {
        "evaluated_absence": [],
        "simulated_proofs": [],
        "dry_run_proofs": [],
        "live_proofs": [],
    }
    for row in statuses:
        if not isinstance(row, dict) or row.get("status") != "OK":
            continue
        payload = _read_json(Path(str(row.get("path") or "")))
        artifact_id = str(row.get("artifact_id") or "")
        if payload.get("validity_status") == "NO_EVENT_PACKET" or payload.get("result") == "NO_MANUAL_EXECUTION_DECLARED":
            evidence_classes["evaluated_absence"].append(artifact_id)
        if payload.get("lifecycle_mode") == "SIMULATED":
            evidence_classes["simulated_proofs"].append(artifact_id)
        if payload.get("transport_mode") == "DRY_RUN":
            evidence_classes["dry_run_proofs"].append(artifact_id)
        if payload.get("transport_mode") == "LIVE" or payload.get("lifecycle_mode") == "LIVE":
            evidence_classes["live_proofs"].append(artifact_id)
    target_graph = {cap: row for cap, row in graph.items() if isinstance(row, dict) and bool(row.get("readiness_relevant", True))}
    total = len(target_graph) or 1
    allowed = [cap for cap, row in graph.items() if isinstance(row, dict) and bool(row.get("allowed", False))]
    target_allowed = [cap for cap, row in target_graph.items() if bool(row.get("allowed", False))]
    return {
        "schema_id": "aegis_operational_readiness",
        "schema_version": "v1",
        "day_utc": kernel.get("day_utc"),
        "generated_at_utc": kernel.get("generated_at_utc"),
        "runtime_truth_classification": kernel.get("runtime_truth_classification"),
        "target_operating_mode": kernel.get("target_operating_mode"),
        "live_broker_trading_policy": kernel.get("live_broker_trading_policy"),
        "autonomous_execution_policy": kernel.get("autonomous_execution_policy"),
        "broker_submit_transmit_policy": kernel.get("broker_submit_transmit_policy"),
        "highest_readiness_layer": kernel.get("highest_readiness_layer"),
        "human_approved_advisory_runtime_ready": bool(kernel.get("human_approved_advisory_runtime_ready")),
        "advisory_status": kernel.get("advisory_status"),
        "operator_action_required": bool(kernel.get("operator_action_required")),
        "operator_action_reason": kernel.get("operator_action_reason") or "",
        "manual_capture_status": kernel.get("manual_capture_status") or {},
        "manual_receipts_today": int(((kernel.get("manual_capture_status") or {}).get("manual_trade_receipt_count") or 0) if isinstance(kernel.get("manual_capture_status"), dict) else 0),
        "readiness_layers": kernel.get("layers") or {},
        "disabled_or_optional_layers": kernel.get("disabled_or_optional_layers") or {},
        "all_capabilities": graph,
        "policy_disabled_capabilities": kernel.get("policy_disabled_capabilities") or [],
        "optional_not_required_capabilities": kernel.get("optional_not_required_capabilities") or [],
        "out_of_scope_capabilities": kernel.get("out_of_scope_capabilities") or {},
        "all_remaining_blockers": kernel.get("missing_or_stale_sources") or [],
        "evidence_classes_achieved": evidence_classes,
        "simulated_proofs_achieved": evidence_classes["simulated_proofs"],
        "dry_run_proofs_achieved": evidence_classes["dry_run_proofs"],
        "live_proofs_achieved": evidence_classes["live_proofs"],
        "remaining_forbidden_claims": kernel.get("do_not_claim") or [],
        "safety_guarantees_preserved": {
            "broker_submit_required": False,
            "autonomous_execution_allowed": False,
            "live_trade_ready_forced_false": not bool(graph.get("LIVE_TRADE_READY", {}).get("allowed")),
            "trade_advice_allowed": bool(graph.get("TRADE_ADVICE_ALLOWED", {}).get("allowed")),
            "manual_trade_capture_allowed": bool(graph.get("MANUAL_TRADE_CAPTURE_ALLOWED", {}).get("allowed")),
        },
        "truth_drift_risk": (kernel.get("kernel_authority") or {}).get("truth_drift_risk") or "UNKNOWN",
        "operational_completeness_percentage": round(len(target_allowed) / total * 100, 2),
        "intelligence_summaries": intelligence_summaries_v1(Path(str(kernel.get("truth_root") or DEFAULT_TRUTH_ROOT)), str(kernel.get("day_utc") or "")),
    }


def render_summary(payload: dict[str, Any]) -> str:
    safety = payload.get("safety_guarantees_preserved") if isinstance(payload.get("safety_guarantees_preserved"), dict) else {}
    return "\n".join(
        [
            "AEGIS OPERATIONAL READINESS v1",
            f"day_utc: {payload.get('day_utc')}",
            f"runtime_truth_classification: {payload.get('runtime_truth_classification')}",
            f"target_operating_mode: {payload.get('target_operating_mode')}",
            f"live_broker_trading_policy: {payload.get('live_broker_trading_policy')}",
            f"autonomous_execution_policy: {payload.get('autonomous_execution_policy')}",
            f"highest_readiness_layer: {payload.get('highest_readiness_layer')}",
            f"human_approved_advisory_runtime_ready: {str(payload.get('human_approved_advisory_runtime_ready')).lower()}",
            f"advisory_status: {payload.get('advisory_status')}",
            f"manual_receipts_today: {payload.get('manual_receipts_today')}",
            f"remaining_blockers: {len(payload.get('all_remaining_blockers') or [])}",
            f"operational_completeness_percentage: {payload.get('operational_completeness_percentage')}",
            f"simulated_proofs: {', '.join(payload.get('simulated_proofs_achieved') or []) or 'NONE'}",
            f"dry_run_proofs: {', '.join(payload.get('dry_run_proofs_achieved') or []) or 'NONE'}",
            f"live_proofs: {', '.join(payload.get('live_proofs_achieved') or []) or 'NONE'}",
            f"trade_advice_allowed: {str(safety.get('trade_advice_allowed')).lower()}",
            f"manual_trade_capture_allowed: {str(safety.get('manual_trade_capture_allowed')).lower()}",
            f"broker_submit_required: {str(safety.get('broker_submit_required')).lower()}",
            f"intelligence_reports_available: {sum(1 for row in (payload.get('intelligence_summaries') or {}).values() if isinstance(row, dict) and row.get('status') == 'AVAILABLE')}",
            "",
        ]
    )


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


if __name__ == "__main__":
    raise SystemExit(main())
