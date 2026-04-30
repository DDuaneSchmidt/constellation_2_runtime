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

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1, read_json_object_v1, resolve_fact_plane_truth_root_v1, resolve_paper_intent_truth_root_v1
from ops.tools.run_intent_arbitration_v1 import build_intent_arbitration, selected_intent_pointer_path
from ops.tools import run_sleeve_evaluation_kernel_v1 as sleeve_kernel

PAPER_MODE = "PAPER"
SCAN_SCHEMA_ID = "sleeve_scan_session"
SCAN_SCHEMA_VERSION = "v1"


def _now_dt() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _now_iso() -> str:
    return _now_dt().isoformat().replace("+00:00", "Z")


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


def _duration_ms(started: datetime, completed: datetime) -> int:
    return max(0, int((completed - started).total_seconds() * 1000))


def _default_cycle_id() -> str:
    return "scan_" + _now_dt().strftime("%Y%m%dT%H%M%SZ")


def sleeve_scan_cycle_root(*, truth_root: Path, day_utc: str, cycle_id: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "sleeve_scan_session_v1" / day_utc / cycle_id


def sleeve_scan_rollup_path(*, truth_root: Path, day_utc: str, cycle_id: str) -> Path:
    return sleeve_scan_cycle_root(truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id) / "sleeve_scan_rollup.v1.json"


def sleeve_outcome_path(*, truth_root: Path, day_utc: str, cycle_id: str, sleeve_id: str) -> Path:
    return sleeve_scan_cycle_root(truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id) / sleeve_id / "sleeve_outcome.v1.json"


def _previous_cycle_outcome(*, truth_root: Path, day_utc: str, cycle_id: str, sleeve_id: str) -> dict[str, Any]:
    day_root = Path(truth_root).resolve() / "reports" / "sleeve_scan_session_v1" / day_utc
    if not day_root.exists():
        return {}
    current = str(cycle_id)
    candidates: list[Path] = []
    for path in day_root.glob(f"*/{sleeve_id}/sleeve_outcome.v1.json"):
        if path.parts[-3] == current:
            continue
        candidates.append(path)
    if not candidates:
        return {}
    return _read_json(sorted(candidates, key=lambda path: str(path))[-1])


def _scan_registry_rows() -> list[dict[str, Any]]:
    registry = sleeve_kernel._load_engine_registry()
    rows: list[dict[str, Any]] = []
    for row in registry.get("engines") if isinstance(registry.get("engines"), list) else []:
        if not isinstance(row, dict):
            continue
        if str(row.get("engine_id") or "").strip() == sleeve_kernel.SIMULATOR_ENGINE_ID:
            continue
        rows.append(row)
    return rows


def _adapt_outcome_for_scan(outcome: dict[str, Any], *, truth_root: Path, day_utc: str, cycle_id: str) -> dict[str, Any]:
    sleeve_id = str(outcome.get("sleeve_id") or outcome.get("engine_id") or "").strip()
    adapted = dict(outcome)
    adapted["schema_id"] = "sleeve_outcome"
    adapted["schema_version"] = "v1"
    adapted["cycle_id"] = cycle_id
    adapted["artifact_path"] = str(sleeve_outcome_path(truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id, sleeve_id=sleeve_id))
    return adapted


def build_market_session_intent_engine(
    *,
    day_utc: str,
    truth_root: Path,
    environment: str = PAPER_MODE,
    cycle_id: str = "",
) -> dict[str, Any]:
    environment = str(environment or PAPER_MODE).strip().upper()
    cycle_id = str(cycle_id or "").strip() or _default_cycle_id()
    started_dt = _now_dt()
    started = started_dt.isoformat().replace("+00:00", "Z")
    intent_truth_root = resolve_paper_intent_truth_root_v1(truth_root=truth_root, repo_root=REPO_ROOT)
    existing_by_engine = sleeve_kernel._existing_intents_by_engine(intent_truth_root=intent_truth_root, day_utc=day_utc)
    outcomes: list[dict[str, Any]] = []

    for row in _scan_registry_rows():
        sleeve_id = str(row.get("engine_id") or "").strip()
        previous = _previous_cycle_outcome(truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id, sleeve_id=sleeve_id)
        if sleeve_kernel._status_from_registry(row) != "ACTIVE":
            outcome = sleeve_kernel._outcome_for_inactive(row=row, day_utc=day_utc, environment=environment, truth_root=truth_root)
        else:
            outcome = sleeve_kernel._evaluate_active_engine(
                row=row,
                day_utc=day_utc,
                environment=environment,
                truth_root=truth_root,
                intent_truth_root=intent_truth_root,
                existing_by_engine=existing_by_engine,
            )
        outcome = _adapt_outcome_for_scan(outcome, truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id)
        outcome = sleeve_kernel._apply_state_memory(outcome, previous)
        if outcome["status"] not in sleeve_kernel.OUTCOMES:
            outcome["status"] = "BLOCKED"
            outcome["current_status"] = "BLOCKED"
            outcome["canonical_blocker"] = "INVALID_SLEEVE_SCAN_STATUS"
        _write_json(Path(outcome["artifact_path"]), outcome)
        outcomes.append(outcome)

    rollup_path = sleeve_scan_rollup_path(truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id)
    completed_dt = _now_dt()
    blockers = [row for row in outcomes if row.get("status") == "BLOCKED"]
    rollup = {
        "schema_id": SCAN_SCHEMA_ID,
        "schema_version": SCAN_SCHEMA_VERSION,
        "day_utc": day_utc,
        "cycle_id": cycle_id,
        "environment": environment,
        "status": "BLOCKED" if blockers else "PASS",
        "canonical_blocker": str(blockers[0].get("canonical_blocker") or "SLEEVE_SCAN_BLOCKED") if blockers else "",
        "engine_registry_path": str(sleeve_kernel._engine_registry_path()),
        "engine_registry_sha256": sleeve_kernel._sha256_file(sleeve_kernel._engine_registry_path()),
        "intent_truth_root": str(intent_truth_root),
        "sleeve_outcomes": outcomes,
        "outcomes": outcomes,
        "summary": {
            "configured_sleeve_count": len(outcomes),
            "active_sleeve_count": len([row for row in outcomes if row.get("activation_status") == "ACTIVE"]),
            "disabled_sleeve_count": len([row for row in outcomes if row.get("status") == "DISABLED"]),
            "intent_created_count": len([row for row in outcomes if row.get("status") == "INTENT_CREATED"]),
            "no_intent_count": len([row for row in outcomes if row.get("status") == "NO_INTENT"]),
            "blocked_count": len(blockers),
            "filtered_out_count": len([row for row in outcomes if row.get("status") == "FILTERED_OUT"]),
        },
        "started_at_utc": started,
        "completed_at_utc": completed_dt.isoformat().replace("+00:00", "Z"),
        "duration_ms": _duration_ms(started_dt, completed_dt),
        "artifact_path": str(rollup_path),
    }
    _write_json(rollup_path, rollup)

    arbitration = build_intent_arbitration(
        day_utc=day_utc,
        truth_root=truth_root,
        environment=environment,
        cycle_id=cycle_id,
        source_rollup_path=rollup_path,
    )
    payload = {
        "schema_id": "market_session_intent_engine",
        "schema_version": "v1",
        "day_utc": day_utc,
        "cycle_id": cycle_id,
        "environment": environment,
        "status": "BLOCKED" if rollup["status"] == "BLOCKED" or arbitration["status"] == "BLOCKED" else "PASS",
        "canonical_blocker": rollup["canonical_blocker"] or arbitration.get("canonical_blocker", ""),
        "sleeve_scan_rollup_path": str(rollup_path),
        "intent_arbitration_path": str(arbitration.get("artifact_path") or ""),
        "selected_intent_pointer_path": str(selected_intent_pointer_path(truth_root=truth_root, day_utc=day_utc)),
        "sleeve_outcomes": outcomes,
        "arbitration": arbitration,
        "started_at_utc": started,
        "completed_at_utc": _now_iso(),
    }
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_market_session_intent_engine_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default=PAPER_MODE)
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--once", action="store_true", help="Run exactly one market-session scan cycle.")
    args = parser.parse_args(argv)
    if not args.once:
        raise SystemExit("--once is required for market_session_intent_engine_v1")
    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    payload = build_market_session_intent_engine(day_utc=day_utc, truth_root=truth_root, environment=str(args.environment or PAPER_MODE).strip().upper())
    print(
        json.dumps(
            {
                "status": payload["status"],
                "canonical_blocker": payload["canonical_blocker"],
                "cycle_id": payload["cycle_id"],
                "sleeve_scan_rollup_path": payload["sleeve_scan_rollup_path"],
                "intent_arbitration_path": payload["intent_arbitration_path"],
                "selected_intent_pointer_path": payload["selected_intent_pointer_path"],
            },
            sort_keys=True,
        )
    )
    return 0 if payload["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
