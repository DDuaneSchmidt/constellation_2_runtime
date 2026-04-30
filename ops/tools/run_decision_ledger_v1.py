#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1, read_json_object_v1, resolve_fact_plane_truth_root_v1
from ops.tools.run_intent_arbitration_v1 import selected_intent_pointer_path
from ops.tools.run_portfolio_activation_gate_v1 import portfolio_activation_gate_path
from ops.tools.run_portfolio_state_v1 import portfolio_state_path

PAPER_MODE = "PAPER"


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_canonical_bytes(payload) + b"\n")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return read_json_object_v1(path)
    except Exception:
        return {}


def decision_ledger_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "decision_ledger_v1" / day_utc / "decision_ledger.v1.json"


def _git_commit() -> str:
    proc = subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    return str(proc.stdout or "").strip() if proc.returncode == 0 else "UNKNOWN"


def _source_reproducibility_status() -> str:
    proc = subprocess.run(["git", "status", "--short"], cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    return "SOURCE_REPRODUCIBILITY_BLOCKED" if str(proc.stdout or "").strip() else "PASSED"


def _latest_scan_paths(truth_root: Path, day_utc: str) -> dict[str, str]:
    pointer_path = Path(truth_root).resolve() / "pointers" / "latest_scan_cycle_pointer.v1.json"
    pointer = _read_json(pointer_path)
    artifact_root = str(pointer.get("artifact_root") or "").strip() if str(pointer.get("day_utc") or "") == day_utc else ""
    return {
        "latest_scan_cycle_pointer_path": str(pointer_path),
        "raw_sleeve_scan_path": str(Path(artifact_root) / "scan_rollup.v1.json") if artifact_root else "",
        "arbitration_result_path": str(Path(artifact_root) / "arbitration_result.v1.json") if artifact_root else "",
    }


def build_decision_ledger_v1(
    *,
    day_utc: str,
    truth_root: Path,
    environment: str = PAPER_MODE,
    aegis_day_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    day_run_path = truth_root / "reports" / "aegis_day_run_v1" / day_utc / "day_run.v1.json"
    day_run = aegis_day_payload if isinstance(aegis_day_payload, dict) else _read_json(day_run_path)
    selected_pointer_path = selected_intent_pointer_path(truth_root=truth_root, day_utc=day_utc)
    selected_pointer = _read_json(selected_pointer_path)
    selected = selected_pointer.get("selected_intent") if isinstance(selected_pointer.get("selected_intent"), dict) else {}
    scan_paths = _latest_scan_paths(truth_root, day_utc)
    phase_results = day_run.get("phase_results") if isinstance(day_run.get("phase_results"), dict) else {}
    authorization = phase_results.get("AUTHORIZATION_FINAL") if isinstance(phase_results.get("AUTHORIZATION_FINAL"), dict) else {}
    execution = phase_results.get("EXECUTION") if isinstance(phase_results.get("EXECUTION"), dict) else {}
    reason_codes = []
    if str(day_run.get("canonical_blocker") or ""):
        reason_codes.append(str(day_run.get("canonical_blocker")))
    for row in day_run.get("root_cause_chain") if isinstance(day_run.get("root_cause_chain"), list) else []:
        if isinstance(row, dict) and str(row.get("canonical_blocker") or ""):
            reason_codes.append(str(row.get("canonical_blocker")))
    out_path = decision_ledger_path(truth_root=truth_root, day_utc=day_utc)
    registry = REPO_ROOT / "governance" / "02_REGISTRIES" / "ENGINE_MODEL_REGISTRY_V1.json"
    market_manifest = truth_root / "market_data_snapshot_v1" / "dataset_manifest.json"
    payload = {
        "schema_id": "decision_ledger",
        "schema_version": "v1",
        "day_utc": day_utc,
        "environment": environment,
        "git_commit": _git_commit(),
        "source_reproducibility_status": _source_reproducibility_status(),
        "registry_version": "ENGINE_MODEL_REGISTRY_V1",
        "policy_paths": [str(registry)],
        "market_data_artifact_paths": [str(market_manifest)],
        "portfolio_state_path": str(portfolio_state_path(truth_root=truth_root, day_utc=day_utc)),
        "raw_sleeve_scan_path": scan_paths["raw_sleeve_scan_path"],
        "portfolio_activation_gate_path": str(portfolio_activation_gate_path(truth_root=truth_root, day_utc=day_utc)),
        "arbitration_result_path": scan_paths["arbitration_result_path"],
        "authorization_result_path": (authorization.get("outputs") or [""])[0] if isinstance(authorization.get("outputs"), list) and authorization.get("outputs") else "",
        "execution_result_path": (execution.get("outputs") or [""])[0] if isinstance(execution.get("outputs"), list) and execution.get("outputs") else "",
        "selected_intent_pointer_path": str(selected_pointer_path),
        "selected_intent_id": str(selected.get("intent_id") or ""),
        "final_status": str(day_run.get("final_status") or "UNKNOWN"),
        "canonical_phase": str(day_run.get("canonical_phase") or ""),
        "canonical_blocker": str(day_run.get("canonical_blocker") or ""),
        "reason_codes": sorted(set(reason_codes)),
        "operator_next_action": str(day_run.get("operator_next_action") or ""),
        "aegis_day_run_path": str(day_run_path),
        "latest_scan_cycle_pointer_path": scan_paths["latest_scan_cycle_pointer_path"],
        "produced_at_utc": _now_iso(),
        "producer": "ops/tools/run_decision_ledger_v1.py",
        "artifact_path": str(out_path),
    }
    _write_json(out_path, payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_decision_ledger_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default=PAPER_MODE, choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    payload = build_decision_ledger_v1(day_utc=day_utc, truth_root=truth_root, environment=str(args.environment).strip().upper())
    print(json.dumps({"status": payload["final_status"], "canonical_blocker": payload["canonical_blocker"], "path": payload["artifact_path"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
