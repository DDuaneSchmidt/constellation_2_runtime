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
from ops.tools.run_intent_arbitration_v1 import selected_intent_pointer_path
from ops.tools.run_portfolio_scoring_v1 import portfolio_scoring_path
from ops.tools.run_selection_quality_v1 import selection_quality_path

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


def _float(value: Any) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return 0.0


def decision_consistency_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "decision_consistency_v1" / day_utc / "decision_consistency.v1.json"


def _ranked_rows(scoring: dict[str, Any]) -> list[dict[str, Any]]:
    rows = scoring.get("rankings") if isinstance(scoring.get("rankings"), list) else scoring.get("ranked_intents")
    out = [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
    out.sort(key=lambda row: (int(row.get("rank") or 999999), -_float(row.get("score_total")), str(row.get("intent_id") or "")))
    return out


def _selected_id(truth_root: Path, day_utc: str, scoring: dict[str, Any]) -> str:
    pointer = _read_json(selected_intent_pointer_path(truth_root=truth_root, day_utc=day_utc))
    selected = pointer.get("selected_intent") if isinstance(pointer.get("selected_intent"), dict) else {}
    explicit = str(selected.get("intent_id") or pointer.get("selected_intent_id") or "").strip()
    if explicit:
        return explicit
    rows = _ranked_rows(scoring)
    return str(rows[0].get("intent_id") or "").strip() if rows else ""


def _score_gap(rows: list[dict[str, Any]]) -> float:
    executable = [row for row in rows if row.get("executable_eligible") is True or int(row.get("rank") or 0) > 0]
    if not executable:
        return 0.0
    if len(executable) == 1:
        return _float(executable[0].get("score_total"))
    return round(_float(executable[0].get("score_total")) - _float(executable[1].get("score_total")), 6)


def _signature(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"intent_id": str(row.get("intent_id") or ""), "rank": int(row.get("rank") or 999999), "score_total": _float(row.get("score_total"))}
        for row in rows
        if str(row.get("intent_id") or "").strip()
    ]


def _previous_day_paths(truth_root: Path, day_utc: str) -> tuple[str, Path, Path]:
    root = Path(truth_root).resolve() / "reports" / "portfolio_scoring_v1"
    days = sorted([path.name for path in root.iterdir() if path.is_dir() and path.name < day_utc], reverse=True) if root.is_dir() else []
    if not days:
        return "", Path(""), Path("")
    prior = days[0]
    return (
        prior,
        portfolio_scoring_path(truth_root=truth_root, day_utc=prior),
        Path(truth_root).resolve() / "reports" / "decision_ledger_v1" / prior / "decision_ledger.v1.json",
    )


def build_decision_consistency_v1(*, day_utc: str, truth_root: Path, environment: str = PAPER_MODE) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    scoring_path = portfolio_scoring_path(truth_root=truth_root, day_utc=day_utc)
    selection_path = selection_quality_path(truth_root=truth_root, day_utc=day_utc)
    scoring = _read_json(scoring_path)
    selection = _read_json(selection_path)
    rows = _ranked_rows(scoring)
    selected_id = _selected_id(truth_root, day_utc, scoring)
    selected_row = next((row for row in rows if str(row.get("intent_id") or "") == selected_id), rows[0] if rows else {})
    prior_day, prior_scoring_path, prior_ledger_path = _previous_day_paths(truth_root, day_utc)
    prior_scoring = _read_json(prior_scoring_path) if prior_day else {}
    prior_ledger = _read_json(prior_ledger_path) if prior_day else {}
    prior_rows = _ranked_rows(prior_scoring)
    prior_selected = str(prior_ledger.get("selected_intent_id") or (prior_rows[0].get("intent_id") if prior_rows else "") or "").strip()
    current_signature = _signature(rows)
    prior_signature = _signature(prior_rows)
    candidate_set_same = {row["intent_id"] for row in current_signature} == {row["intent_id"] for row in prior_signature} and bool(current_signature)
    same_conditions_same_decision = bool(prior_day and current_signature == prior_signature and selected_id == prior_selected)
    decision_flip_detected = bool(prior_day and prior_selected and selected_id and prior_selected != selected_id and candidate_set_same)
    nondeterminism_suspected = bool(decision_flip_detected and current_signature == prior_signature)
    if not prior_day:
        ranking_stability = "UNKNOWN"
        flip_reason = "NO_PRIOR_DECISION"
    elif decision_flip_detected:
        ranking_stability = "FLIPPED"
        flip_reason = "SAME_SCORES_DIFFERENT_SELECTION" if nondeterminism_suspected else "RANK_ORDER_CHANGED"
    else:
        ranking_stability = "STABLE"
        flip_reason = "NO_FLIP"
    out_path = decision_consistency_path(truth_root=truth_root, day_utc=day_utc)
    evidence_paths = [str(path) for path in (scoring_path, selection_path, prior_scoring_path, prior_ledger_path) if str(path) and path.exists()]
    payload = {
        "schema_id": "decision_consistency",
        "schema_version": "v1",
        "day_utc": day_utc,
        "environment": environment,
        "status": "DEGRADED" if decision_flip_detected else ("UNKNOWN" if not prior_day else "PASS"),
        "selected_intent_id": selected_id,
        "selected_score": _float(selected_row.get("score_total")) if selected_row else 0.0,
        "score_gap": selection.get("score_gap") if selection else _score_gap(rows),
        "ranking_stability": ranking_stability,
        "same_conditions_same_decision": same_conditions_same_decision,
        "decision_flip_detected": decision_flip_detected,
        "flip_reason": flip_reason,
        "nondeterminism_suspected": nondeterminism_suspected,
        "prior_day_utc": prior_day,
        "prior_selected_intent_id": prior_selected,
        "current_ranking_signature": current_signature,
        "prior_ranking_signature": prior_signature,
        "evidence_paths": sorted(set(evidence_paths)),
        "produced_at_utc": _now_iso(),
        "producer": "ops/tools/run_decision_consistency_v1.py",
        "artifact_path": str(out_path),
    }
    _write_json(out_path, payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_decision_consistency_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default=PAPER_MODE, choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    payload = build_decision_consistency_v1(day_utc=day_utc, truth_root=truth_root, environment=str(args.environment).strip().upper())
    print(json.dumps({"status": payload["status"], "ranking_stability": payload["ranking_stability"], "path": payload["artifact_path"]}, sort_keys=True))
    return 0 if payload["status"] in {"PASS", "DEGRADED", "UNKNOWN"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
