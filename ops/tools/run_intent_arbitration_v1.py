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

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1, read_json_object_v1, resolve_fact_plane_truth_root_v1
from ops.tools.run_sleeve_evaluation_kernel_v1 import sleeve_evaluation_rollup_path

PAPER_MODE = "PAPER"
DEFAULT_PRIORITY = [
    "C2_VOL_INCOME_DEFINED_RISK_V1",
    "C2_TREND_EQ_PRIMARY_V1",
    "C2_MEAN_REVERSION_EQ_V1",
    "C2_EVENT_DISLOCATION_V1",
    "C2_DEFENSIVE_TAIL_V1",
    "C2_CROSS_ASSET_TREND_V1",
    "C2_MARKET_NEUTRAL_SPREAD_V1",
]


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_canonical_bytes(payload) + b"\n")


def intent_arbitration_path(*, truth_root: Path, day_utc: str, cycle_id: str = "") -> Path:
    base = Path(truth_root).resolve() / "reports" / "intent_arbitration_v1" / day_utc
    if str(cycle_id or "").strip():
        return base / str(cycle_id).strip() / "intent_arbitration.v1.json"
    return base / "intent_arbitration.v1.json"


def selected_intent_pointer_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "pointers" / "selected_intent_pointer.v1.json"


def _candidate_rows(rollup: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    outcomes = rollup.get("outcomes") if isinstance(rollup.get("outcomes"), list) else rollup.get("sleeve_outcomes")
    for outcome in outcomes if isinstance(outcomes, list) else []:
        if not isinstance(outcome, dict) or outcome.get("status") != "INTENT_CREATED":
            continue
        if outcome.get("canonical_blocker"):
            continue
        for intent in outcome.get("output_intents") if isinstance(outcome.get("output_intents"), list) else []:
            if not isinstance(intent, dict):
                continue
            rows.append(
                {
                    "intent_id": str(intent.get("intent_id") or "").strip(),
                    "sleeve_id": str(outcome.get("sleeve_id") or outcome.get("engine_id") or "").strip(),
                    "engine_id": str(outcome.get("engine_id") or "").strip(),
                    "intent_path": str(intent.get("intent_path") or "").strip(),
                    "intent_hash": str(intent.get("intent_hash") or "").strip(),
                    "symbol": str(intent.get("symbol") or "").strip().upper(),
                    "risk_class": "",
                    "selection_reason": "",
                }
            )
    return rows


def _priority_index(engine_id: str) -> tuple[int, str]:
    try:
        return (DEFAULT_PRIORITY.index(engine_id), engine_id)
    except ValueError:
        return (len(DEFAULT_PRIORITY), engine_id)


def build_intent_arbitration(
    *,
    day_utc: str,
    truth_root: Path,
    environment: str = PAPER_MODE,
    cycle_id: str = "",
    source_rollup_path: Path | None = None,
) -> dict[str, Any]:
    cycle_id = str(cycle_id or "").strip()
    rollup_path = Path(source_rollup_path).resolve() if source_rollup_path is not None else sleeve_evaluation_rollup_path(truth_root=truth_root, day_utc=day_utc)
    if not rollup_path.exists() or not rollup_path.is_file():
        payload = {
            "schema_id": "intent_arbitration",
            "schema_version": "v1",
            "day_utc": day_utc,
            "cycle_id": cycle_id,
            "environment": environment,
            "status": "BLOCKED",
            "canonical_blocker": "SLEEVE_EVALUATION_ROLLUP_MISSING",
            "selection_policy": {"policy_id": "intent_arbitration_priority_v1", "policy_path": "embedded:v1", "tie_breakers": ["priority_order", "engine_id", "intent_hash"]},
            "candidate_intents": [],
            "rejected_or_filtered_intents": [],
            "selected_intent": {},
            "non_selected_sleeve_outcomes": [],
            "source_rollup_path": str(rollup_path),
            "operator_next_action": "Run ops/tools/run_sleeve_evaluation_kernel_v1.py for the current day.",
            "created_at_utc": _now_iso(),
            "artifact_path": str(intent_arbitration_path(truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id)),
            "selected_intent_pointer_path": "",
        }
        _write_json(Path(payload["artifact_path"]), payload)
        return payload

    rollup = read_json_object_v1(rollup_path)
    candidates = _candidate_rows(rollup)
    candidates.sort(key=lambda row: (_priority_index(str(row.get("engine_id") or "")), str(row.get("intent_hash") or "")))
    selected = dict(candidates[0]) if candidates else {}
    if selected:
        selected["cycle_id"] = cycle_id
        selected["selection_reason"] = "FIRST_BY_INTENT_ARBITRATION_PRIORITY_V1"
        selected["arbitration_reason"] = "FIRST_BY_INTENT_ARBITRATION_PRIORITY_V1"
        status = "SELECTED"
        blocker = ""
        next_action = ""
    else:
        outcomes = rollup.get("outcomes") if isinstance(rollup.get("outcomes"), list) else rollup.get("sleeve_outcomes", [])
        blocked = [row for row in outcomes if isinstance(row, dict) and row.get("status") == "BLOCKED"]
        status = "BLOCKED" if blocked else "NO_EXECUTABLE_INTENT"
        blocker = str(blocked[0].get("canonical_blocker") or "NO_EXECUTABLE_INTENT") if blocked else "NO_EXECUTABLE_INTENT"
        next_action = "Review sleeve evaluation outcomes and resolve blockers or accept no-trade day."

    rejected = [{**row, "rejection_reason": "NOT_SELECTED_BY_INTENT_ARBITRATION_PRIORITY_V1"} for row in candidates[1:]]
    non_selected = []
    outcomes = rollup.get("outcomes") if isinstance(rollup.get("outcomes"), list) else rollup.get("sleeve_outcomes")
    for outcome in outcomes if isinstance(outcomes, list) else []:
        if not isinstance(outcome, dict):
            continue
        if selected and str(outcome.get("engine_id") or "") == selected.get("engine_id"):
            continue
        non_selected.append(
            {
                "sleeve_id": outcome.get("sleeve_id", ""),
                "engine_id": outcome.get("engine_id", ""),
                "status": outcome.get("status", ""),
                "canonical_blocker": outcome.get("canonical_blocker", ""),
                "reason_codes": outcome.get("reason_codes", []),
            }
        )

    arbitration_path = intent_arbitration_path(truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id)
    pointer_path = selected_intent_pointer_path(truth_root=truth_root, day_utc=day_utc)
    payload = {
        "schema_id": "intent_arbitration",
        "schema_version": "v1",
        "day_utc": day_utc,
        "cycle_id": cycle_id,
        "environment": environment,
        "status": status,
        "canonical_blocker": blocker,
        "selection_policy": {"policy_id": "intent_arbitration_priority_v1", "policy_path": "embedded:v1", "priority_order": DEFAULT_PRIORITY, "tie_breakers": ["priority_order", "engine_id", "intent_hash"]},
        "candidate_intents": candidates,
        "rejected_or_filtered_intents": rejected,
        "selected_intent": selected,
        "non_selected_sleeve_outcomes": non_selected,
        "source_rollup_path": str(rollup_path),
        "operator_next_action": next_action,
        "created_at_utc": _now_iso(),
        "artifact_path": str(arbitration_path),
        "selected_intent_pointer_path": str(pointer_path),
    }
    _write_json(arbitration_path, payload)
    pointer = {
        "schema_id": "selected_intent_pointer",
        "schema_version": "v1",
        "day_utc": day_utc,
        "cycle_id": cycle_id,
        "environment": environment,
        "status": "SELECTED" if selected else status,
        "canonical_blocker": "" if selected else blocker,
        "selected_intent": selected,
        "source_arbitration_path": str(arbitration_path),
        "source_rollup_path": str(rollup_path),
        "created_at_utc": _now_iso(),
    }
    _write_json(pointer_path, pointer)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_intent_arbitration_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default=PAPER_MODE)
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    payload = build_intent_arbitration(day_utc=day_utc, truth_root=truth_root, environment=str(args.environment or PAPER_MODE).strip().upper())
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "path": payload["artifact_path"], "selected_intent_pointer_path": payload.get("selected_intent_pointer_path", "")}, sort_keys=True))
    return 0 if payload["status"] in {"SELECTED", "NO_EXECUTABLE_INTENT"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
