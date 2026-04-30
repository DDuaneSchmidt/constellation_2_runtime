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
from ops.tools.run_intent_lifecycle_state_v1 import intent_lifecycle_state_path
from ops.tools.run_intent_arbitration_v1 import selected_intent_pointer_path
from ops.tools.run_portfolio_activation_gate_v1 import portfolio_activation_gate_path
from ops.tools.run_portfolio_scoring_v1 import portfolio_scoring_path
from ops.tools.run_portfolio_state_v1 import portfolio_state_path
from ops.tools.run_position_lifecycle_state_v1 import position_lifecycle_state_path

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


def _scoring_summary(truth_root: Path, day_utc: str) -> dict[str, Any]:
    path = portfolio_scoring_path(truth_root=truth_root, day_utc=day_utc)
    scoring = _read_json(path)
    rankings = scoring.get("rankings") if isinstance(scoring.get("rankings"), list) else scoring.get("ranked_intents")
    rows = rankings if isinstance(rankings, list) else []
    return {
        "portfolio_scoring_path": str(path),
        "scored_intents_count": int(scoring.get("intents_scored_count") or len([row for row in rows if isinstance(row, dict) and row.get("executable_eligible")])),
        "top_rejected_or_suppressed_reasons": sorted(
            {
                str(reason)
                for row in rows
                if isinstance(row, dict) and not row.get("executable_eligible")
                for reason in (row.get("reason_codes") if isinstance(row.get("reason_codes"), list) else [])
                if str(reason)
            }
        )[:10],
    }


def _arbitration_summary(path: str) -> dict[str, Any]:
    arbitration = _read_json(Path(path)) if str(path or "").strip() else {}
    selected = arbitration.get("selected_intent") if isinstance(arbitration.get("selected_intent"), dict) else {}
    return {
        "selected_intent_score": float(arbitration.get("selected_intent_score") or selected.get("portfolio_score_total") or 0.0),
        "selected_intent_rank": int(arbitration.get("selected_intent_rank") or selected.get("portfolio_score_rank") or 0),
    }


def _lifecycle_summary(truth_root: Path, day_utc: str) -> dict[str, Any]:
    path = intent_lifecycle_state_path(truth_root=truth_root, day_utc=day_utc)
    payload = _read_json(path)
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    counts = payload.get("counts") if isinstance(payload.get("counts"), dict) else {}
    decision_counts: dict[str, int] = {}
    selected_codes = sorted(
        {
            str(code)
            for row in rows
            if isinstance(row, dict)
            for code in (row.get("lifecycle_reason_codes") if isinstance(row.get("lifecycle_reason_codes"), list) else [])
            if str(code)
        }
    )
    for row in rows:
        if not isinstance(row, dict):
            continue
        decision = str(row.get("lifecycle_decision") or "UNKNOWN")
        decision_counts[decision] = decision_counts.get(decision, 0) + 1
    return {
        "intent_lifecycle_state_path": str(path),
        "lifecycle_decision_counts": decision_counts,
        "reentry_intent_count": int(counts.get("reentry_intent_count") or 0),
        "suppressed_position_count": int(counts.get("suppressed_position_count") or 0),
        "suppressed_order_count": int(counts.get("suppressed_order_count") or 0),
        "uncertain_position_count": int(counts.get("uncertain_position_count") or 0),
        "selected_intent_lifecycle_reason_codes": selected_codes[:20],
    }


def _position_lifecycle_summary(truth_root: Path, day_utc: str, selected_intent_id: str) -> dict[str, Any]:
    path = position_lifecycle_state_path(truth_root=truth_root, day_utc=day_utc)
    payload = _read_json(path)
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    counts = payload.get("counts") if isinstance(payload.get("counts"), dict) else {}
    selected_row: dict[str, Any] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        if selected_intent_id and str(row.get("intent_id") or "") == selected_intent_id:
            selected_row = row
            break
    if not selected_row and rows:
        first = rows[0]
        selected_row = first if isinstance(first, dict) else {}
    reason_sets = [
        {str(reason).strip().upper() for reason in row.get("reason_codes", [])}
        for row in rows
        if isinstance(row, dict) and isinstance(row.get("reason_codes"), list)
    ]
    uncertain = (
        int(counts.get("uncertain_position_count"))
        if counts.get("uncertain_position_count") is not None
        else len([reasons for reasons in reason_sets if "POSITION_MATCH_UNCERTAIN" in reasons])
    )
    chain = {
        "intent_id": str(selected_row.get("intent_id") or selected_intent_id or ""),
        "submission_id": str(selected_row.get("submission_id") or ""),
        "fill_id": str(selected_row.get("fill_id") or ""),
        "position_id": str(selected_row.get("position_id") or ""),
    }
    return {
        "position_lifecycle_state_path": str(path),
        "selected_position_id": chain["position_id"],
        "lifecycle_chain": chain,
        "open_position_count": int(counts.get("open_position_count") or len([row for row in rows if isinstance(row, dict) and row.get("lifecycle_state") == "POSITION_OPEN"])),
        "pending_order_count": int(counts.get("pending_order_count") or len([row for row in rows if isinstance(row, dict) and row.get("lifecycle_state") == "ORDER_PENDING"])),
        "closed_position_count": int(counts.get("closed_position_count") or len([row for row in rows if isinstance(row, dict) and row.get("lifecycle_state") == "POSITION_CLOSED"])),
        "uncertain_position_count": uncertain,
        "final_exposure_state": str(selected_row.get("lifecycle_state") or "UNKNOWN"),
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
    scoring_summary = _scoring_summary(truth_root, day_utc)
    arbitration_summary = _arbitration_summary(scan_paths["arbitration_result_path"])
    lifecycle_summary = _lifecycle_summary(truth_root, day_utc)
    selected_intent_id = str(selected.get("intent_id") or "")
    position_lifecycle_summary = _position_lifecycle_summary(truth_root, day_utc, selected_intent_id)
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
        "intent_lifecycle_state_path": lifecycle_summary["intent_lifecycle_state_path"],
        "position_lifecycle_state_path": position_lifecycle_summary["position_lifecycle_state_path"],
        "portfolio_activation_gate_path": str(portfolio_activation_gate_path(truth_root=truth_root, day_utc=day_utc)),
        "portfolio_scoring_path": scoring_summary["portfolio_scoring_path"],
        "arbitration_result_path": scan_paths["arbitration_result_path"],
        "authorization_result_path": (authorization.get("outputs") or [""])[0] if isinstance(authorization.get("outputs"), list) and authorization.get("outputs") else "",
        "execution_result_path": (execution.get("outputs") or [""])[0] if isinstance(execution.get("outputs"), list) and execution.get("outputs") else "",
        "selected_intent_pointer_path": str(selected_pointer_path),
        "selected_intent_id": selected_intent_id,
        "selected_position_id": position_lifecycle_summary["selected_position_id"],
        "lifecycle_chain": position_lifecycle_summary["lifecycle_chain"],
        "open_position_count": position_lifecycle_summary["open_position_count"],
        "pending_order_count": position_lifecycle_summary["pending_order_count"],
        "closed_position_count": position_lifecycle_summary["closed_position_count"],
        "final_exposure_state": position_lifecycle_summary["final_exposure_state"],
        "selected_intent_score": arbitration_summary["selected_intent_score"],
        "selected_intent_rank": arbitration_summary["selected_intent_rank"],
        "scored_intents_count": scoring_summary["scored_intents_count"],
        "lifecycle_decision_counts": lifecycle_summary["lifecycle_decision_counts"],
        "reentry_intent_count": lifecycle_summary["reentry_intent_count"],
        "suppressed_position_count": lifecycle_summary["suppressed_position_count"],
        "suppressed_order_count": lifecycle_summary["suppressed_order_count"],
        "uncertain_position_count": max(lifecycle_summary["uncertain_position_count"], position_lifecycle_summary["uncertain_position_count"]),
        "selected_intent_lifecycle_reason_codes": lifecycle_summary["selected_intent_lifecycle_reason_codes"],
        "top_rejected_or_suppressed_reasons": scoring_summary["top_rejected_or_suppressed_reasons"],
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
