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
from ops.tools.run_ai_advisory_review_v1 import ai_advisory_review_path

PAPER_MODE = "PAPER"
MINIMUM_EVIDENCE_WINDOW_DAYS = 30


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


def strategy_change_governance_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "strategy_change_governance_v1" / day_utc / "strategy_change_governance.v1.json"


def _history_window_met(truth_root: Path, day_utc: str) -> bool:
    root = Path(truth_root).resolve() / "reports" / "decision_ledger_v1"
    days = {path.parent.name for path in root.glob("*/decision_ledger.v1.json") if path.is_file() and path.parent.name <= day_utc}
    return len(days) >= MINIMUM_EVIDENCE_WINDOW_DAYS


def build_strategy_change_governance_v1(*, day_utc: str, truth_root: Path, environment: str = PAPER_MODE) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    review_path = ai_advisory_review_path(truth_root=truth_root, day_utc=day_utc)
    review = _read_json(review_path)
    minimum_window_met = _history_window_met(truth_root, day_utc)
    source_recommendations = review.get("recommendations") if isinstance(review.get("recommendations"), list) else []
    rows: list[dict[str, Any]] = []
    for index, rec in enumerate(source_recommendations):
        if not isinstance(rec, dict):
            continue
        recommendation_id = str(rec.get("recommendation_id") or f"{day_utc}:RECOMMENDATION_{index + 1}").strip()
        rows.append(
            {
                "recommendation_id": recommendation_id,
                "source_review_path": str(review_path),
                "affected_component": str(rec.get("affected_component") or "UNKNOWN"),
                "proposed_change": str(rec.get("recommendation") or "Observe; no automatic change proposed."),
                "hypothesis": "Performance may improve if this advisory observation survives the governed evidence window.",
                "evidence": [str(path) for path in review.get("evidence_paths", [])] if isinstance(review.get("evidence_paths"), list) else [],
                "expected_effect": "UNKNOWN_UNTIL_PAPER_VALIDATION",
                "risk": "Premature tuning can overfit or bypass deterministic governance.",
                "validation_plan": f"Collect at least {MINIMUM_EVIDENCE_WINDOW_DAYS} clean trading sessions, then review out-of-sample paper evidence.",
                "rollback_rule": "Revert any paper-approved change if safety, submit readiness, or realized edge deteriorates under governed review.",
                "minimum_evidence_window_met": minimum_window_met,
                "status": "PROPOSED" if minimum_window_met else "OBSERVE",
                "human_approval_required": True,
            }
        )
    if not rows:
        rows.append(
            {
                "recommendation_id": f"{day_utc}:NO_CHANGE",
                "source_review_path": str(review_path),
                "affected_component": "NONE",
                "proposed_change": "No strategy, scoring, risk, or submit change proposed.",
                "hypothesis": "No advisory recommendation currently meets governance intake criteria.",
                "evidence": [str(review_path)] if review_path.exists() else [],
                "expected_effect": "NO_TRADING_BEHAVIOR_CHANGE",
                "risk": "NONE",
                "validation_plan": f"Continue observation until {MINIMUM_EVIDENCE_WINDOW_DAYS} clean trading sessions are available.",
                "rollback_rule": "No rollback needed; no deployment occurs.",
                "minimum_evidence_window_met": minimum_window_met,
                "status": "OBSERVE",
                "human_approval_required": True,
            }
        )
    aggregate_status = "PROPOSED" if any(row["status"] == "PROPOSED" for row in rows) else "OBSERVE"
    first = rows[0]
    out_path = strategy_change_governance_path(truth_root=truth_root, day_utc=day_utc)
    payload = {
        "schema_id": "strategy_change_governance",
        "schema_version": "v1",
        "day_utc": day_utc,
        "environment": environment,
        "status": aggregate_status,
        "minimum_evidence_window_days": MINIMUM_EVIDENCE_WINDOW_DAYS,
        "recommendations": rows,
        "recommendation_id": first["recommendation_id"],
        "source_review_path": first["source_review_path"],
        "affected_component": first["affected_component"],
        "proposed_change": first["proposed_change"],
        "hypothesis": first["hypothesis"],
        "evidence": first["evidence"],
        "expected_effect": first["expected_effect"],
        "risk": first["risk"],
        "validation_plan": first["validation_plan"],
        "rollback_rule": first["rollback_rule"],
        "minimum_evidence_window_met": first["minimum_evidence_window_met"],
        "human_approval_required": True,
        "automatic_deployment_allowed": False,
        "producer": "ops/tools/run_strategy_change_governance_v1.py",
        "produced_at_utc": _now_iso(),
        "artifact_path": str(out_path),
    }
    _write_json(out_path, payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_strategy_change_governance_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default=PAPER_MODE, choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    payload = build_strategy_change_governance_v1(day_utc=day_utc, truth_root=truth_root, environment=str(args.environment).strip().upper())
    print(json.dumps({"status": payload["status"], "recommendation_count": len(payload["recommendations"]), "path": payload["artifact_path"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
