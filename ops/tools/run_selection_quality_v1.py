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


def _score(value: Any) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return 0.0


def selection_quality_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "selection_quality_v1" / day_utc / "selection_quality.v1.json"


def _ranked_rows(scoring: dict[str, Any]) -> list[dict[str, Any]]:
    rows = scoring.get("rankings") if isinstance(scoring.get("rankings"), list) else scoring.get("ranked_intents")
    out = [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
    out.sort(key=lambda row: (int(row.get("rank") or 999999), -_score(row.get("score_total")), str(row.get("intent_id") or "")))
    return out


def _confidence(score_gap: float, selected: dict[str, Any]) -> str:
    if not selected:
        return "UNKNOWN"
    if score_gap >= 10.0:
        return "HIGH"
    if score_gap >= 3.0:
        return "MEDIUM"
    return "LOW"


def build_selection_quality_v1(*, day_utc: str, truth_root: Path, environment: str = PAPER_MODE) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    scoring_path = portfolio_scoring_path(truth_root=truth_root, day_utc=day_utc)
    pointer_path = selected_intent_pointer_path(truth_root=truth_root, day_utc=day_utc)
    scoring = _read_json(scoring_path)
    pointer = _read_json(pointer_path)
    selected = pointer.get("selected_intent") if isinstance(pointer.get("selected_intent"), dict) else {}
    selected_intent_id = str(selected.get("intent_id") or pointer.get("selected_intent_id") or "").strip()
    selected_sleeve_id = str(selected.get("sleeve_id") or selected.get("engine_id") or "").strip()
    ranked = _ranked_rows(scoring)
    executable = [row for row in ranked if bool(row.get("executable_eligible") is True)]
    selected_row = next((row for row in ranked if str(row.get("intent_id") or "") == selected_intent_id), {})
    if not selected_row and selected_intent_id and executable:
        selected_row = executable[0]
    top_row = selected_row or (executable[0] if executable else (ranked[0] if ranked else {}))
    alternatives = [row for row in ranked if row is not top_row and str(row.get("intent_id") or "") != selected_intent_id]
    second_row = alternatives[0] if alternatives else {}
    top_score = _score(top_row.get("score_total")) if top_row else 0.0
    second_score = _score(second_row.get("score_total")) if second_row else 0.0
    score_gap = round(top_score - second_score, 6) if top_row and second_row else top_score
    confidence_level = _confidence(score_gap, selected)
    no_trade_quality_reason = ""
    defer_recommended = False
    status = "PASS"
    if not selected_intent_id:
        status = "VALID_ZERO" if not executable else "DEGRADED"
        no_trade_quality_reason = "NO_EXECUTABLE_INTENT" if not executable else "SELECTED_INTENT_POINTER_MISSING"
        confidence_level = "UNKNOWN"
    elif confidence_level == "LOW":
        defer_recommended = True
    selected_vs_alternatives = []
    for row in alternatives[:5]:
        selected_vs_alternatives.append(
            {
                "intent_id": str(row.get("intent_id") or ""),
                "sleeve_id": str(row.get("sleeve_id") or ""),
                "score_total": _score(row.get("score_total")),
                "score_gap_vs_selected": round(top_score - _score(row.get("score_total")), 6),
                "rank": int(row.get("rank") or 0),
            }
        )
    out_path = selection_quality_path(truth_root=truth_root, day_utc=day_utc)
    payload = {
        "schema_id": "selection_quality",
        "schema_version": "v1",
        "day_utc": day_utc,
        "environment": environment,
        "status": status,
        "selected_intent_id": selected_intent_id,
        "selected_sleeve_id": selected_sleeve_id,
        "top_score": top_score,
        "second_score": second_score,
        "score_gap": score_gap,
        "confidence_level": confidence_level,
        "selected_vs_alternatives": selected_vs_alternatives,
        "no_trade_quality_reason": no_trade_quality_reason,
        "defer_recommended": defer_recommended,
        "evidence_paths": [str(path) for path in (pointer_path, scoring_path) if path.exists()],
        "produced_at_utc": _now_iso(),
        "producer": "ops/tools/run_selection_quality_v1.py",
        "artifact_path": str(out_path),
    }
    _write_json(out_path, payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_selection_quality_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default=PAPER_MODE, choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    payload = build_selection_quality_v1(day_utc=day_utc, truth_root=truth_root, environment=str(args.environment).strip().upper())
    print(json.dumps({"status": payload["status"], "confidence_level": payload["confidence_level"], "path": payload["artifact_path"]}, sort_keys=True))
    return 0 if payload["status"] in {"PASS", "VALID_ZERO", "DEGRADED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
