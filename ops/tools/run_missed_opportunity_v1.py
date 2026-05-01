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


def missed_opportunity_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "missed_opportunity_v1" / day_utc / "missed_opportunity.v1.json"


def _ranked_rows(scoring: dict[str, Any]) -> list[dict[str, Any]]:
    rows = scoring.get("rankings") if isinstance(scoring.get("rankings"), list) else scoring.get("ranked_intents")
    out = [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
    out.sort(key=lambda row: (int(row.get("rank") or 999999), -_float(row.get("score_total")), str(row.get("intent_id") or "")))
    return out


def _selected_id(selection: dict[str, Any], scoring: dict[str, Any]) -> str:
    selected = str(selection.get("selected_intent_id") or "").strip()
    if selected:
        return selected
    rows = _ranked_rows(scoring)
    return str(rows[0].get("intent_id") or "").strip() if rows else ""


def build_missed_opportunity_v1(*, day_utc: str, truth_root: Path, environment: str = PAPER_MODE) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    selection_path = selection_quality_path(truth_root=truth_root, day_utc=day_utc)
    scoring_path = portfolio_scoring_path(truth_root=truth_root, day_utc=day_utc)
    selection = _read_json(selection_path)
    scoring = _read_json(scoring_path)
    selected_intent_id = _selected_id(selection, scoring)
    selected_alts = selection.get("selected_vs_alternatives") if isinstance(selection.get("selected_vs_alternatives"), list) else []
    rows_by_id = {str(row.get("intent_id") or ""): row for row in _ranked_rows(scoring)}
    alternatives: list[dict[str, Any]] = []
    source_rows = selected_alts or [row for row in _ranked_rows(scoring) if str(row.get("intent_id") or "") != selected_intent_id][:5]
    for raw in source_rows[:10]:
        if not isinstance(raw, dict):
            continue
        intent_id = str(raw.get("intent_id") or "").strip()
        if not intent_id or intent_id == selected_intent_id:
            continue
        scoring_row = rows_by_id.get(intent_id, {})
        reasons = scoring_row.get("reason_codes") if isinstance(scoring_row.get("reason_codes"), list) else []
        alternatives.append(
            {
                "alternative_intent_id": intent_id,
                "alternative_sleeve_id": str(raw.get("sleeve_id") or scoring_row.get("sleeve_id") or ""),
                "alternative_score": _float(raw.get("score_total") if "score_total" in raw else scoring_row.get("score_total")),
                "reason_not_selected": "LOWER_SCORE_THAN_SELECTED" if not reasons else ",".join(str(reason) for reason in reasons[:5]),
                "hypothetical_outcome_status": "NOT_TRADED_NOT_OBSERVED",
                "later_realized_return_if_observable": None,
                "hypothetical_or_proxy": True,
                "created_trade": False,
                "evidence_paths": [str(path) for path in (selection_path, scoring_path) if path.exists()],
            }
        )
    out_path = missed_opportunity_path(truth_root=truth_root, day_utc=day_utc)
    payload = {
        "schema_id": "missed_opportunity",
        "schema_version": "v1",
        "day_utc": day_utc,
        "environment": environment,
        "status": "VALID_ZERO" if not alternatives else "PASS",
        "selected_intent_id": selected_intent_id,
        "alternatives": alternatives,
        "no_fabricated_trades": True,
        "trading_behavior_changed": False,
        "evidence_paths": sorted({path for alt in alternatives for path in alt["evidence_paths"]} | {str(path) for path in (selection_path, scoring_path) if path.exists()}),
        "produced_at_utc": _now_iso(),
        "producer": "ops/tools/run_missed_opportunity_v1.py",
        "artifact_path": str(out_path),
    }
    _write_json(out_path, payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_missed_opportunity_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default=PAPER_MODE, choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    payload = build_missed_opportunity_v1(day_utc=day_utc, truth_root=truth_root, environment=str(args.environment).strip().upper())
    print(json.dumps({"status": payload["status"], "alternative_count": len(payload["alternatives"]), "path": payload["artifact_path"]}, sort_keys=True))
    return 0 if payload["status"] in {"PASS", "VALID_ZERO"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
