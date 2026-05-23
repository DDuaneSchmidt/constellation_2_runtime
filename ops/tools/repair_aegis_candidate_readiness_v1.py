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

from ops.aegis.candidate_generation_diagnostics_v1 import (  # noqa: E402
    SIMULATOR_ENGINE_ID,
    _authoritative_sleeve_inventory,
    build_candidate_generation_diagnostics_v1,
    write_candidate_generation_diagnostics_v1,
)
from ops.aegis.candidate_lifecycle_v1 import write_candidate_lifecycle_reports_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402
from ops.aegis.intelligence_common_v1 import write_json_v1  # noqa: E402


REPORT_FAMILY = "aegis_candidate_readiness_repair_v1"


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _run(cmd: list[str]) -> dict[str, Any]:
    started = _now()
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True)
    return {
        "command": " ".join(cmd),
        "exit_code": int(proc.returncode),
        "stdout": proc.stdout.strip()[-4000:],
        "stderr": proc.stderr.strip()[-4000:],
        "started_at_utc": started,
        "completed_at_utc": _now(),
    }


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _event_market_snapshot_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "event_market_snapshot_v1" / day_utc / "event_market_snapshot.v1.json"


def _sleeve_readiness_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "aegis_sleeve_readiness_v1" / day_utc / "sleeve_readiness.v1.json"


def build_candidate_readiness_repair_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    inventory = _authoritative_sleeve_inventory(repo_root=REPO_ROOT)
    expected_ids = [row["sleeve_id"] for row in inventory["expected_sleeves"] if row.get("sleeve_id") != SIMULATOR_ENGINE_ID]
    commands: list[dict[str, Any]] = []
    data_required: list[dict[str, str]] = []

    commands.append(_run([sys.executable, "ops/tools/build_aegis_symbol_map_v1.py", "--truth_root", str(root), "--day", day_utc]))
    commands.append(_run([sys.executable, "ops/tools/build_aegis_sleeve_input_contracts_v1.py", "--truth_root", str(root), "--day", day_utc]))
    commands.append(_run([sys.executable, "ops/tools/refresh_aegis_market_data_v1.py", "--truth_root", str(root), "--day", day_utc]))
    commands.append(_run([sys.executable, "ops/tools/build_aegis_market_data_inputs_v1.py", "--truth-root", str(root), "--day-utc", day_utc, "--emit-events"]))
    market_data_path = root / "reports" / "aegis_market_data_v1" / day_utc / "market_data.v1.json"
    market_data_payload = _read_json(market_data_path)
    commands.append(_run([sys.executable, "ops/tools/build_aegis_data_registry_v1.py", "--truth_root", str(root), "--day", day_utc]))
    commands.append(_run([sys.executable, "ops/tools/build_aegis_sleeve_readiness_v1.py", "--truth_root", str(root), "--day", day_utc]))
    readiness_path = _sleeve_readiness_path(truth_root=root, day_utc=day_utc)
    readiness_payload = _read_json(readiness_path)
    ready_count = int(readiness_payload.get("ready_count") or 0) + int(readiness_payload.get("ready_with_warnings_count") or 0)
    if market_data_payload and not bool(market_data_payload.get("usable_for_candidate_generation")) and ready_count == 0:
        data_required.append(
            {
                "artifact_id": "aegis_market_data_v1",
                "expected_path": str(market_data_path),
                "required_dataset": "Current session market prices for all sleeve-required symbols, plus required breadth if enabled",
                "producer_command": "npm run aegis:refresh-market-data",
                "status": str(market_data_payload.get("status") or "UNKNOWN"),
                "failure_reason": str(market_data_payload.get("failure_reason") or ""),
                "missing_symbols": ",".join(str(symbol) for symbol in market_data_payload.get("missing_symbols") or []),
                "stale_symbols": ",".join(str(symbol) for symbol in market_data_payload.get("stale_symbols") or []),
            }
        )

    snapshot_path = _event_market_snapshot_path(truth_root=root, day_utc=day_utc)
    market_context_json = os.environ.get("AEGIS_EVENT_MARKET_DATA_JSON", "").strip()
    if market_context_json:
        commands.append(
            _run(
                [
                    sys.executable,
                    "ops/tools/build_event_market_snapshot_v1.py",
                    "--truth_root",
                    str(root),
                    "--day_utc",
                    day_utc,
                    "--market_data_json",
                    market_context_json,
                ]
            )
        )
    else:
        commands.append(
            _run(
                [
                    sys.executable,
                    "ops/tools/build_event_market_snapshot_v1.py",
                    "--truth_root",
                    str(root),
                    "--day_utc",
                    day_utc,
                ]
            )
        )
    snapshot = _read_json(snapshot_path)
    stale_status = str(snapshot.get("stale_data_status") or ("MISSING" if not snapshot else "UNKNOWN")).strip().upper()
    if stale_status != "FRESH" and ready_count == 0:
        data_required.append(
            {
                "artifact_id": "event_market_snapshot_v1",
                "expected_path": str(snapshot_path),
                "required_dataset": "Current SPY, QQQ, VIX, breadth, and trend market/context inputs for the target day",
                "producer_command": "python3 ops/tools/build_event_market_snapshot_v1.py --truth_root /home/node/constellation_runtime_data/truth --day_utc <DAY_UTC> --market_data_json <path>",
                "stale_data_status": stale_status,
                "reason_codes": ",".join(str(code) for code in snapshot.get("reason_codes") or []),
            }
        )

    commands.append(_run([sys.executable, "ops/tools/run_aegis_runtime_truth_kernel_v1.py", "--truth_root", str(root), "--day", day_utc]))
    commands.append(_run([sys.executable, "ops/tools/run_sleeve_evaluation_kernel_v1.py", "--day_utc", day_utc, "--truth_root", str(root), "--environment", "PAPER", "--readiness-path", str(readiness_path)]))
    sleeve_rollup_path = root / "reports" / "sleeve_evaluation_kernel_v1" / day_utc / "sleeve_evaluation_rollup.v1.json"
    portfolio_gate_path = root / "reports" / "portfolio_activation_gate_v1" / day_utc / "portfolio_activation_gate.v1.json"
    commands.append(
        _run(
            [
                sys.executable,
                "ops/tools/run_portfolio_activation_gate_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(root),
                "--environment",
                "PAPER",
                "--source_rollup_path",
                str(sleeve_rollup_path),
            ]
        )
    )
    commands.append(
        _run(
            [
                sys.executable,
                "ops/tools/run_portfolio_scoring_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(root),
                "--environment",
                "PAPER",
                "--source_rollup_path",
                str(sleeve_rollup_path),
                "--portfolio_gate_path",
                str(portfolio_gate_path),
            ]
        )
    )
    commands.append(_run([sys.executable, "ops/tools/run_intent_arbitration_v1.py", "--day_utc", day_utc, "--truth_root", str(root), "--environment", "PAPER"]))
    commands.append(_run([sys.executable, "ops/tools/promote_aegis_selected_intent_v1.py", "--truth_root", str(root), "--day", day_utc]))
    lifecycle_paths = write_candidate_lifecycle_reports_v1(truth_root=root, day_utc=day_utc)
    commands.append({"command": "write_candidate_lifecycle_reports_v1", "exit_code": 0, "paths": lifecycle_paths})
    commands.append(_run([sys.executable, "ops/tools/write_aegis_candidate_review_ledger_v1.py", "--truth_root", str(root), "--day", day_utc]))
    commands.append(_run([sys.executable, "ops/tools/run_aegis_candidate_ranking_v1.py", "--truth_root", str(root), "--day", day_utc]))

    diagnostics = build_candidate_generation_diagnostics_v1(truth_root=root, repo_root=REPO_ROOT, day_utc=day_utc)
    diagnostics_paths = write_candidate_generation_diagnostics_v1(truth_root=root, day_utc=day_utc, payload=diagnostics)
    commands.append({"command": "write_aegis_candidate_generation_diagnostics_v1", "exit_code": 0, "paths": diagnostics_paths})

    commands.append(_run([sys.executable, "ops/tools/build_aegis_canonical_operator_state_v1.py", "--truth_root", str(root), "--day", day_utc]))
    commands.append(_run([sys.executable, "ops/tools/write_aegis_operator_brief_v1.py", "--truth_root", str(root), "--day", day_utc]))

    sleeve_rows = diagnostics.get("sleeves") if isinstance(diagnostics.get("sleeves"), list) else []
    missing_expected = sorted(set(expected_ids) - {str(row.get("sleeve_id") or "") for row in sleeve_rows if isinstance(row, dict)})
    status = "REPAIRED"
    if data_required:
        status = "DATA_REQUIRED"
    if any(row.get("run_status") in {"BLOCKED", "FAILED", "NOT_RUN", "UNKNOWN"} for row in sleeve_rows if isinstance(row, dict)):
        status = "PARTIAL" if status == "REPAIRED" else status
    if missing_expected:
        status = "FAILED"
    return {
        "schema_id": "aegis_candidate_readiness_repair",
        "schema_version": "v1",
        "artifact_id": "aegis_candidate_readiness_repair_v1",
        "generated_at": _now(),
        "day_utc": day_utc,
        "status": status,
        "advisory_only": True,
        "expected_sleeve_ids": expected_ids,
        "attempted_sleeve_ids": [str(row.get("sleeve_id") or "") for row in sleeve_rows if isinstance(row, dict)],
        "missing_expected_sleeve_ids": missing_expected,
        "data_required": data_required,
        "diagnostics_path": diagnostics_paths["json"],
        "canonical_operator_state_attempted": True,
        "operator_brief_attempted": True,
        "candidate_generation_status": diagnostics.get("candidate_generation_status"),
        "operator_interpretation": diagnostics.get("operator_interpretation"),
        "total_sleeves_expected": diagnostics.get("total_sleeves_expected"),
        "total_sleeves_run": diagnostics.get("total_sleeves_run"),
        "total_sleeves_blocked": diagnostics.get("total_sleeves_blocked"),
        "total_candidates_generated": diagnostics.get("total_candidates_generated"),
        "sleeves": sleeve_rows,
        "commands": commands,
        "safety": {
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "live_trading_allowed": False,
            "automatic_approval_allowed": False,
            "automatic_sleeve_mutation_allowed": False,
            "fabricated_candidates": False,
            "missing_data_bypassed": False,
        },
    }


def write_candidate_readiness_repair_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "candidate_readiness_repair.v1.json", payload)
    summary_path = out_dir / "candidate_readiness_repair.summary.txt"
    lines = [
        "AEGIS CANDIDATE READINESS REPAIR v1",
        f"day_utc: {payload.get('day_utc')}",
        f"status: {payload.get('status')}",
        f"expected_sleeves: {payload.get('total_sleeves_expected')}",
        f"sleeves_run: {payload.get('total_sleeves_run')}",
        f"sleeves_blocked: {payload.get('total_sleeves_blocked')}",
        f"candidates_generated: {payload.get('total_candidates_generated')}",
        "data_required:",
    ]
    for row in payload.get("data_required") or []:
        lines.append(f"- {row.get('artifact_id')}: {row.get('required_dataset')}")
    lines.append("sleeves:")
    for row in payload.get("sleeves") or []:
        lines.append(f"- {row.get('sleeve_id')}: {row.get('run_status')} {row.get('canonical_blocker') or row.get('reason_no_candidate')}")
    lines.append("broker_execution_allowed: false")
    lines.append("autonomous_execution_allowed: false")
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="repair_aegis_candidate_readiness_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    payload = build_candidate_readiness_repair_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc))
    paths = write_candidate_readiness_repair_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), payload=payload)
    print(json.dumps({**paths, "status": payload["status"], "expected_sleeves": payload["total_sleeves_expected"], "sleeves_run": payload["total_sleeves_run"], "candidates_generated": payload["total_candidates_generated"], "broker_execution_allowed": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
