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
from constellation_2.common.safety_state_authority_v1 import safety_state_authority_output_path
from constellation_2.common.trading_day_readiness_authority_v1 import trading_day_readiness_authority_output_path
from ops.tools.run_decision_ledger_v1 import decision_ledger_path
from ops.tools.run_decision_consistency_v1 import decision_consistency_path
from ops.tools.run_edge_attribution_v1 import edge_attribution_path
from ops.tools.run_intent_lifecycle_state_v1 import intent_lifecycle_state_path
from ops.tools.run_missed_opportunity_v1 import missed_opportunity_path
from ops.tools.run_portfolio_activation_gate_v1 import portfolio_activation_gate_path
from ops.tools.run_portfolio_scoring_v1 import portfolio_scoring_path
from ops.tools.run_position_lifecycle_state_v1 import position_lifecycle_state_path
from ops.tools.run_regime_confidence_v1 import regime_confidence_path
from ops.tools.run_selection_quality_v1 import selection_quality_path
from ops.tools.run_trade_outcome_v1 import trade_outcome_path

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


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def ai_advisory_review_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "ai_advisory_review_v1" / day_utc / "ai_advisory_review.v1.json"


def _artifact_map(truth_root: Path, day_utc: str) -> dict[str, Path]:
    return {
        "decision_ledger_v1": decision_ledger_path(truth_root=truth_root, day_utc=day_utc),
        "selection_quality_v1": selection_quality_path(truth_root=truth_root, day_utc=day_utc),
        "edge_attribution_v1": edge_attribution_path(truth_root=truth_root, day_utc=day_utc),
        "regime_confidence_v1": regime_confidence_path(truth_root=truth_root, day_utc=day_utc),
        "trade_outcome_v1": trade_outcome_path(truth_root=truth_root, day_utc=day_utc),
        "decision_consistency_v1": decision_consistency_path(truth_root=truth_root, day_utc=day_utc),
        "missed_opportunity_v1": missed_opportunity_path(truth_root=truth_root, day_utc=day_utc),
        "portfolio_scoring_v1": portfolio_scoring_path(truth_root=truth_root, day_utc=day_utc),
        "portfolio_activation_gate_v1": portfolio_activation_gate_path(truth_root=truth_root, day_utc=day_utc),
        "intent_lifecycle_state_v1": intent_lifecycle_state_path(truth_root=truth_root, day_utc=day_utc),
        "position_lifecycle_state_v1": position_lifecycle_state_path(truth_root=truth_root, day_utc=day_utc),
        "safety_state_authority_v1": safety_state_authority_output_path(truth_root=truth_root, day_utc=day_utc),
        "trading_day_readiness_authority_v1": trading_day_readiness_authority_output_path(truth_root=truth_root, target_day=day_utc),
    }


def build_ai_advisory_review_v1(*, day_utc: str, truth_root: Path, environment: str = PAPER_MODE) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    paths = _artifact_map(truth_root, day_utc)
    payloads = {name: _read_json(path) for name, path in paths.items()}
    missing = sorted(name for name, path in paths.items() if not path.exists())
    selection = payloads["selection_quality_v1"]
    edge = payloads["edge_attribution_v1"]
    regime = payloads["regime_confidence_v1"]
    trade_outcome = payloads["trade_outcome_v1"]
    decision_consistency = payloads["decision_consistency_v1"]
    missed_opportunity = payloads["missed_opportunity_v1"]
    scoring = payloads["portfolio_scoring_v1"]
    lifecycle = payloads["intent_lifecycle_state_v1"]
    recommendations: list[dict[str, Any]] = []
    anomaly_flags: list[str] = []
    if selection.get("confidence_level") == "LOW":
        recommendations.append(
            {
                "recommendation_id": f"{day_utc}:SELECTION_CONFIDENCE_LOW",
                "affected_component": "portfolio_scoring_v1",
                "recommendation": "Observe selected-vs-alternative score gaps before proposing scoring changes.",
                "requires_governance": True,
            }
        )
    if any(row.get("edge_health") in {"NEGATIVE", "WEAKENING"} for row in edge.get("sleeves", []) if isinstance(row, dict)):
        recommendations.append(
            {
                "recommendation_id": f"{day_utc}:EDGE_HEALTH_REVIEW",
                "affected_component": "strategy_selection",
                "recommendation": "Review sleeves with weakening or negative realized edge after evidence-window requirements are met.",
                "requires_governance": True,
            }
        )
    if str(regime.get("confidence_level") or "").upper() in {"LOW", "UNKNOWN"}:
        recommendations.append(
            {
                "recommendation_id": f"{day_utc}:REGIME_CONFIDENCE_LOW",
                "affected_component": "portfolio_scoring_v1",
                "recommendation": "Keep regime-alignment impact explicitly damped until regime confidence improves.",
                "requires_governance": True,
            }
        )
    if str(trade_outcome.get("outcome_status") or "").upper() == "CLOSED" and _float(trade_outcome.get("return_pct")) < 0.0:
        recommendations.append(
            {
                "recommendation_id": f"{day_utc}:NEGATIVE_TRADE_OUTCOME_REVIEW",
                "affected_component": "strategy_selection",
                "recommendation": "Review realized losing outcome after governed evidence windows are met; do not change thresholds automatically.",
                "requires_governance": True,
            }
        )
    if decision_consistency.get("decision_flip_detected") is True:
        recommendations.append(
            {
                "recommendation_id": f"{day_utc}:DECISION_FLIP_REVIEW",
                "affected_component": "portfolio_scoring_v1",
                "recommendation": "Investigate rank flip and input stability before proposing any scoring change.",
                "requires_governance": True,
            }
        )
    if missing:
        anomaly_flags.extend([f"MISSING_INPUT:{name}" for name in missing])
    counts = lifecycle.get("counts") if isinstance(lifecycle.get("counts"), dict) else {}
    decisions_reviewed = int(counts.get("INTENT_CREATED") or 0) + int(counts.get("NO_INTENT") or 0) + int(counts.get("BLOCKED") or 0)
    out_path = ai_advisory_review_path(truth_root=truth_root, day_utc=day_utc)
    evidence_paths = [str(path) for path in paths.values() if path.exists()]
    payload = {
        "schema_id": "ai_advisory_review",
        "schema_version": "v1",
        "day_utc": day_utc,
        "environment": environment,
        "status": "DEGRADED" if missing else "PASS",
        "decisions_reviewed": decisions_reviewed,
        "selection_quality_summary": {
            "status": str(selection.get("status") or "MISSING"),
            "confidence_level": str(selection.get("confidence_level") or "UNKNOWN"),
            "score_gap": selection.get("score_gap"),
            "defer_recommended": bool(selection.get("defer_recommended") is True),
        },
        "edge_attribution_summary": {
            "status": str(edge.get("status") or "MISSING"),
            "edge_health_by_sleeve": [
                {"sleeve_id": row.get("sleeve_id"), "edge_health": row.get("edge_health"), "trades": row.get("trades")}
                for row in edge.get("sleeves", [])
                if isinstance(row, dict)
            ],
        },
        "regime_confidence_summary": {
            "status": str(regime.get("status") or "MISSING"),
            "regime": str(regime.get("regime") or "UNKNOWN"),
            "confidence_score": regime.get("confidence_score"),
            "confidence_level": str(regime.get("confidence_level") or "UNKNOWN"),
            "transition_risk": str(regime.get("transition_risk") or "UNKNOWN"),
        },
        "trade_outcome_summary": {
            "status": str(trade_outcome.get("status") or "MISSING"),
            "outcome_status": str(trade_outcome.get("outcome_status") or "UNKNOWN"),
            "intent_id": str(trade_outcome.get("intent_id") or ""),
            "return_pct": trade_outcome.get("return_pct"),
            "realized_pnl": trade_outcome.get("realized_pnl"),
            "unrealized_pnl": trade_outcome.get("unrealized_pnl"),
        },
        "decision_consistency_summary": {
            "status": str(decision_consistency.get("status") or "MISSING"),
            "ranking_stability": str(decision_consistency.get("ranking_stability") or "UNKNOWN"),
            "decision_flip_detected": bool(decision_consistency.get("decision_flip_detected") is True),
            "nondeterminism_suspected": bool(decision_consistency.get("nondeterminism_suspected") is True),
        },
        "missed_opportunity_summary": {
            "status": str(missed_opportunity.get("status") or "MISSING"),
            "alternative_count": len(missed_opportunity.get("alternatives") if isinstance(missed_opportunity.get("alternatives"), list) else []),
            "no_fabricated_trades": bool(missed_opportunity.get("no_fabricated_trades") is True),
        },
        "scoring_observations": {
            "status": str(scoring.get("status") or "MISSING"),
            "intents_scored_count": int(scoring.get("intents_scored_count") or 0),
            "selected_candidate_intent_id": str(scoring.get("selected_candidate_intent_id") or ""),
            "regime_confidence_path": str(scoring.get("regime_confidence_path") or ""),
        },
        "anomaly_flags": anomaly_flags,
        "recommendations": recommendations,
        "confidence": "LOW" if missing else "MEDIUM",
        "requires_human_review": True,
        "prohibited_actions_attempted": False,
        "evidence_paths": sorted(set(evidence_paths)),
        "produced_at_utc": _now_iso(),
        "producer": "ops/tools/run_ai_advisory_review_v1.py",
        "artifact_path": str(out_path),
    }
    _write_json(out_path, payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_ai_advisory_review_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default=PAPER_MODE, choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    payload = build_ai_advisory_review_v1(day_utc=day_utc, truth_root=truth_root, environment=str(args.environment).strip().upper())
    print(json.dumps({"status": payload["status"], "recommendation_count": len(payload["recommendations"]), "path": payload["artifact_path"]}, sort_keys=True))
    return 0 if payload["status"] in {"PASS", "DEGRADED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
