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
from ops.tools.run_decision_consistency_v1 import decision_consistency_path
from ops.tools.run_decision_ledger_v1 import decision_ledger_path
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
LOW_CONFIDENCE_SCORE_GAP_THRESHOLD = 3.0
NEAR_MISS_SCORE_DISTANCE_THRESHOLD = 5.0


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


def _text(value: Any) -> str:
    return str(value or "").strip()


def insight_engine_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "insight_engine_v1" / day_utc / "insight_engine.v1.json"


def _report_path(*, truth_root: Path, artifact_id: str, day_utc: str, filename: str) -> Path:
    return Path(truth_root).resolve() / "reports" / artifact_id / day_utc / filename


def _artifact_paths(truth_root: Path, day_utc: str) -> dict[str, Path]:
    return {
        "decision_ledger_v1": decision_ledger_path(truth_root=truth_root, day_utc=day_utc),
        "portfolio_scoring_v1": portfolio_scoring_path(truth_root=truth_root, day_utc=day_utc),
        "portfolio_activation_gate_v1": portfolio_activation_gate_path(truth_root=truth_root, day_utc=day_utc),
        "intent_lifecycle_state_v1": intent_lifecycle_state_path(truth_root=truth_root, day_utc=day_utc),
        "position_lifecycle_state_v1": position_lifecycle_state_path(truth_root=truth_root, day_utc=day_utc),
        "selection_quality_v1": selection_quality_path(truth_root=truth_root, day_utc=day_utc),
        "edge_attribution_v1": edge_attribution_path(truth_root=truth_root, day_utc=day_utc),
        "regime_confidence_v1": regime_confidence_path(truth_root=truth_root, day_utc=day_utc),
        "trade_outcome_v1": trade_outcome_path(truth_root=truth_root, day_utc=day_utc),
        "missed_opportunity_v1": missed_opportunity_path(truth_root=truth_root, day_utc=day_utc),
        "decision_consistency_v1": decision_consistency_path(truth_root=truth_root, day_utc=day_utc),
        "ai_advisory_review_v1": _report_path(truth_root=truth_root, artifact_id="ai_advisory_review_v1", day_utc=day_utc, filename="ai_advisory_review.v1.json"),
        "strategy_change_governance_v1": _report_path(truth_root=truth_root, artifact_id="strategy_change_governance_v1", day_utc=day_utc, filename="strategy_change_governance.v1.json"),
    }


def _ranked_rows(scoring: dict[str, Any]) -> list[dict[str, Any]]:
    rows = scoring.get("rankings") if isinstance(scoring.get("rankings"), list) else scoring.get("ranked_intents")
    out = [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
    out.sort(key=lambda row: (int(row.get("rank") or 999999), -_float(row.get("score_total")), _text(row.get("intent_id"))))
    return out


def _gate_rows(gate: dict[str, Any]) -> dict[str, dict[str, Any]]:
    decisions = gate.get("decisions") if isinstance(gate.get("decisions"), list) else []
    out: dict[str, dict[str, Any]] = {}
    for row in decisions:
        if not isinstance(row, dict):
            continue
        intent_id = _text(row.get("raw_intent_id") or row.get("intent_id"))
        if intent_id:
            out[intent_id] = row
    return out


def _lifecycle_rows(lifecycle: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = lifecycle.get("rows") if isinstance(lifecycle.get("rows"), list) else []
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in (_text(row.get("intent_id")), _text(row.get("sleeve_id")), _text(row.get("engine_id"))):
            if key:
                out[key] = row
    return out


def _selected_row(ranked: list[dict[str, Any]], selected_intent_id: str) -> dict[str, Any]:
    if selected_intent_id:
        found = next((row for row in ranked if _text(row.get("intent_id")) == selected_intent_id), {})
        if found:
            return found
    executable = [row for row in ranked if row.get("executable_eligible") is True]
    return executable[0] if executable else {}


def _classify_no_trade(blocker: str, ledger: dict[str, Any], lifecycle: dict[str, Any], scoring: dict[str, Any]) -> str:
    token = blocker.upper()
    if "MARKET" in token or "SESSION" in token or "PREOPEN" in token:
        return "market_session_mode"
    if "EXECUTION" in token or "SUBMIT" in token or "READINESS" in token:
        return "execution_readiness"
    if "NO_INTENT" in token or "NO_SIGNAL" in token:
        return "no_signal"
    if "LIFECYCLE" in token or "POSITION" in token or "ORDER" in token or int(ledger.get("suppressed_position_count") or 0) > 0:
        return "lifecycle_suppression"
    if "PORTFOLIO" in token or "ALLOCATION" in token:
        return "portfolio_suppression"
    if "SCOR" in token or "SELECTED_INTENT_POINTER" in token:
        return "scoring"
    if "AUTH" in token or "RISK" in token or "SAFETY" in token:
        return "authorization_safety"
    if int(scoring.get("intents_scored_count") or 0) == 0:
        return "no_signal"
    counts = lifecycle.get("counts") if isinstance(lifecycle.get("counts"), dict) else {}
    if int(counts.get("BLOCKED") or 0) > 0:
        return "lifecycle_suppression"
    return "unknown"


def _why_this_trade(
    *,
    selected_intent_id: str,
    selected_row: dict[str, Any],
    gate_by_intent: dict[str, dict[str, Any]],
    lifecycle_by_key: dict[str, dict[str, Any]],
    regime: dict[str, Any],
    selection: dict[str, Any],
    ranked: list[dict[str, Any]],
) -> dict[str, Any]:
    if not selected_intent_id:
        return {}
    sleeve_id = _text(selected_row.get("sleeve_id") or selection.get("selected_sleeve_id"))
    gate_row = gate_by_intent.get(selected_intent_id, {})
    lifecycle_row = lifecycle_by_key.get(selected_intent_id) or lifecycle_by_key.get(sleeve_id) or {}
    alternatives = [row for row in ranked if _text(row.get("intent_id")) != selected_intent_id]
    best_alt = alternatives[0] if alternatives else {}
    selected_score = _float(selected_row.get("score_total"))
    alt_score = _float(best_alt.get("score_total")) if best_alt else 0.0
    return {
        "selected_intent_id": selected_intent_id,
        "selected_sleeve_id": sleeve_id,
        "lifecycle_reason": lifecycle_row.get("lifecycle_reason_codes") or selected_row.get("lifecycle_reason_codes") or [],
        "lifecycle_decision": _text(lifecycle_row.get("lifecycle_decision") or selected_row.get("lifecycle_decision") or "UNKNOWN"),
        "portfolio_gate_decision": _text(gate_row.get("portfolio_gate_decision") or selected_row.get("portfolio_gate_decision") or "UNKNOWN"),
        "score": selected_score,
        "rank": int(selected_row.get("rank") or 0),
        "regime_fit": {
            "regime": _text(regime.get("regime") or "UNKNOWN"),
            "confidence_level": _text(regime.get("confidence_level") or selected_row.get("regime_confidence_level") or "UNKNOWN"),
            "regime_alignment_component": selected_row.get("raw_regime_alignment_component"),
        },
        "why_it_beat_alternatives": (
            f"Selected score {selected_score} beat next alternative {alt_score} by {round(selected_score - alt_score, 6)}."
            if best_alt
            else "No executable alternative outranked the selected intent."
        ),
        "evidence_paths": sorted(set(str(path) for path in selected_row.get("evidence_paths", []) if str(path))),
    }


def _why_no_trade(*, selected_intent_id: str, ledger: dict[str, Any], lifecycle: dict[str, Any], selection: dict[str, Any], scoring: dict[str, Any]) -> dict[str, Any]:
    if selected_intent_id:
        return {}
    reason_codes = ledger.get("reason_codes") if isinstance(ledger.get("reason_codes"), list) else []
    blocker = _text(ledger.get("canonical_blocker") or (reason_codes[0] if reason_codes else "") or selection.get("no_trade_quality_reason") or scoring.get("canonical_blocker"))
    return {
        "canonical_blocker": blocker or "NO_SELECTED_INTENT",
        "canonical_phase": _text(ledger.get("canonical_phase") or "UNKNOWN"),
        "final_status": _text(ledger.get("final_status") or "UNKNOWN"),
        "classification": _classify_no_trade(blocker, ledger, lifecycle, scoring),
        "reason_codes": sorted(set(str(code) for code in reason_codes if str(code))),
        "operator_next_action": _text(ledger.get("operator_next_action")),
        "selection_quality_reason": _text(selection.get("no_trade_quality_reason")),
    }


def _alternatives(ranked: list[dict[str, Any]], selected_intent_id: str, gate_by_intent: dict[str, dict[str, Any]], lifecycle_by_key: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in ranked:
        intent_id = _text(row.get("intent_id"))
        if not intent_id or intent_id == selected_intent_id:
            continue
        sleeve_id = _text(row.get("sleeve_id"))
        gate_row = gate_by_intent.get(intent_id, {})
        lifecycle_row = lifecycle_by_key.get(intent_id) or lifecycle_by_key.get(sleeve_id) or {}
        reason_codes = row.get("reason_codes") if isinstance(row.get("reason_codes"), list) else []
        rows.append(
            {
                "intent_id": intent_id,
                "sleeve_id": sleeve_id,
                "score": _float(row.get("score_total")),
                "rank": int(row.get("rank") or 0),
                "gate_decision": _text(gate_row.get("portfolio_gate_decision") or row.get("portfolio_gate_decision") or "UNKNOWN"),
                "lifecycle_decision": _text(lifecycle_row.get("lifecycle_decision") or row.get("lifecycle_decision") or "UNKNOWN"),
                "reason_not_selected": ",".join(str(code) for code in reason_codes[:5]) if reason_codes else "LOWER_SCORE_THAN_SELECTED",
            }
        )
    return rows[:10]


def _near_misses(ranked: list[dict[str, Any]], selection: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    score_gap = selection.get("score_gap")
    if isinstance(score_gap, (int, float)) and 0 <= float(score_gap) < LOW_CONFIDENCE_SCORE_GAP_THRESHOLD:
        rows.append(
            {
                "sleeve_id": _text(selection.get("selected_sleeve_id") or "SELECTION"),
                "metric": "selection_score_gap",
                "actual_value": float(score_gap),
                "threshold": LOW_CONFIDENCE_SCORE_GAP_THRESHOLD,
                "distance_to_trigger": round(LOW_CONFIDENCE_SCORE_GAP_THRESHOLD - float(score_gap), 6),
                "evidence_path": _text(selection.get("artifact_path")),
            }
        )
    executable = [row for row in ranked if row.get("executable_eligible") is True]
    top_score = _float(executable[0].get("score_total")) if executable else 0.0
    for row in executable[1:6]:
        distance = round(top_score - _float(row.get("score_total")), 6)
        if 0 <= distance <= NEAR_MISS_SCORE_DISTANCE_THRESHOLD:
            rows.append(
                {
                    "sleeve_id": _text(row.get("sleeve_id")),
                    "metric": "score_distance_to_selected",
                    "actual_value": _float(row.get("score_total")),
                    "threshold": top_score,
                    "distance_to_trigger": distance,
                    "evidence_path": _text((row.get("evidence_paths") or [""])[0] if isinstance(row.get("evidence_paths"), list) and row.get("evidence_paths") else row.get("portfolio_activation_gate_path")),
                }
            )
    return rows


def _drift_alerts(payloads: dict[str, dict[str, Any]], missing: list[str], selection: dict[str, Any], ranked: list[dict[str, Any]]) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    if missing:
        alerts.append({"metric": "artifact_availability", "status": "DEGRADED", "detail": ",".join(missing), "diagnostic_only": True})
    consistency = payloads["decision_consistency_v1"]
    if consistency.get("decision_flip_detected") is True:
        alerts.append({"metric": "selected_sleeve_mix", "status": "DRIFT", "detail": "Decision flip detected versus prior comparable day.", "diagnostic_only": True})
    regime = payloads["regime_confidence_v1"]
    if _text(regime.get("confidence_level")).upper() in {"LOW", "UNKNOWN"} or _text(regime.get("transition_risk")).upper() == "HIGH":
        alerts.append({"metric": "regime_confidence", "status": "DRIFT", "detail": f"Regime confidence={regime.get('confidence_level', 'UNKNOWN')}; transition_risk={regime.get('transition_risk', 'UNKNOWN')}.", "diagnostic_only": True})
    edge = payloads["edge_attribution_v1"]
    weak = [row for row in edge.get("sleeves", []) if isinstance(row, dict) and row.get("edge_health") in {"NEGATIVE", "WEAKENING"}]
    if weak:
        alerts.append({"metric": "score_distribution", "status": "WATCH", "detail": f"{len(weak)} sleeve edge rows are weakening or negative.", "diagnostic_only": True})
    if selection.get("defer_recommended") is True:
        alerts.append({"metric": "score_distribution", "status": "WATCH", "detail": "Selection quality marked low confidence/defer recommended.", "diagnostic_only": True})
    if ranked and not any(row.get("executable_eligible") is True for row in ranked):
        alerts.append({"metric": "signal_frequency", "status": "VALID_ZERO", "detail": "No executable scored intents available.", "diagnostic_only": True})
    return alerts


def _confidence_calibration(edge: dict[str, Any], trade_outcome: dict[str, Any], selection: dict[str, Any]) -> dict[str, Any]:
    sleeves = [row for row in edge.get("sleeves", []) if isinstance(row, dict)]
    trade_count = sum(int(row.get("trades") or 0) for row in sleeves)
    if trade_count < 20 and _text(trade_outcome.get("outcome_status")).upper() not in {"OPEN", "CLOSED"}:
        return {
            "status": "UNPROVEN",
            "confidence_level": _text(selection.get("confidence_level") or "UNKNOWN"),
            "basis": "Insufficient realized outcome history for calibration.",
            "historical_trade_count": trade_count,
        }
    return {
        "status": "PROVISIONAL",
        "confidence_level": _text(selection.get("confidence_level") or "UNKNOWN"),
        "basis": "Calibration is observational only and does not alter scoring or thresholds.",
        "historical_trade_count": trade_count,
    }


def _recommendations(*, selection: dict[str, Any], drift_alerts: list[dict[str, Any]], trade_outcome: dict[str, Any], consistency: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if _text(selection.get("confidence_level")).upper() == "LOW" or selection.get("defer_recommended") is True:
        rows.append({"recommendation_id": "INSIGHT_SELECTION_CONFIDENCE_LOW", "affected_component": "portfolio_scoring_v1", "recommendation": "Review low-confidence selections through governance; do not change scoring automatically.", "requires_governance": True})
    if _text(trade_outcome.get("outcome_status")).upper() == "CLOSED" and _float(trade_outcome.get("return_pct")) < 0.0:
        rows.append({"recommendation_id": "INSIGHT_NEGATIVE_OUTCOME_REVIEW", "affected_component": "strategy_selection", "recommendation": "Review closed negative outcome after evidence window is met; no automatic deployment allowed.", "requires_governance": True})
    if consistency.get("decision_flip_detected") is True:
        rows.append({"recommendation_id": "INSIGHT_DECISION_FLIP_REVIEW", "affected_component": "portfolio_scoring_v1", "recommendation": "Investigate decision flip stability through governance before any policy proposal.", "requires_governance": True})
    if any(alert.get("metric") == "artifact_availability" for alert in drift_alerts):
        rows.append({"recommendation_id": "INSIGHT_ARTIFACT_AVAILABILITY_REVIEW", "affected_component": "artifact_pipeline", "recommendation": "Repair missing read-model artifacts before drawing stronger conclusions.", "requires_governance": True})
    return rows


def build_insight_engine_v1(*, day_utc: str, truth_root: Path, environment: str = PAPER_MODE) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    paths = _artifact_paths(truth_root, day_utc)
    payloads = {name: _read_json(path) for name, path in paths.items()}
    required = [
        "decision_ledger_v1",
        "portfolio_scoring_v1",
        "portfolio_activation_gate_v1",
        "intent_lifecycle_state_v1",
        "position_lifecycle_state_v1",
        "selection_quality_v1",
        "edge_attribution_v1",
        "regime_confidence_v1",
        "trade_outcome_v1",
        "missed_opportunity_v1",
        "decision_consistency_v1",
    ]
    missing_required = sorted(name for name in required if not paths[name].exists())
    ledger = payloads["decision_ledger_v1"]
    scoring = payloads["portfolio_scoring_v1"]
    gate = payloads["portfolio_activation_gate_v1"]
    lifecycle = payloads["intent_lifecycle_state_v1"]
    selection = payloads["selection_quality_v1"]
    regime = payloads["regime_confidence_v1"]
    trade_outcome = payloads["trade_outcome_v1"]
    missed = payloads["missed_opportunity_v1"]
    consistency = payloads["decision_consistency_v1"]
    ranked = _ranked_rows(scoring)
    selected_intent_id = _text(selection.get("selected_intent_id") or ledger.get("selected_intent_id") or scoring.get("selected_candidate_intent_id"))
    selected = _selected_row(ranked, selected_intent_id)
    selected_sleeve_id = _text(selection.get("selected_sleeve_id") or selected.get("sleeve_id") or trade_outcome.get("sleeve_id"))
    gate_by_intent = _gate_rows(gate)
    lifecycle_by_key = _lifecycle_rows(lifecycle)
    alternatives = _alternatives(ranked, selected_intent_id, gate_by_intent, lifecycle_by_key)
    near_misses = _near_misses(ranked, selection)
    drift = _drift_alerts(payloads, missing_required, selection, ranked)
    recommendations = _recommendations(selection=selection, drift_alerts=drift, trade_outcome=trade_outcome, consistency=consistency)
    governance_required = any(row.get("requires_governance") is True for row in recommendations)
    governance_path = str(paths["strategy_change_governance_v1"])
    if governance_required:
        for row in recommendations:
            row["governance_route"] = governance_path
            row["auto_deploy_allowed"] = False
    why_this = _why_this_trade(
        selected_intent_id=selected_intent_id,
        selected_row=selected,
        gate_by_intent=gate_by_intent,
        lifecycle_by_key=lifecycle_by_key,
        regime=regime,
        selection=selection,
        ranked=ranked,
    )
    why_no = _why_no_trade(selected_intent_id=selected_intent_id, ledger=ledger, lifecycle=lifecycle, selection=selection, scoring=scoring)
    outcome_summary = {
        "status": _text(trade_outcome.get("status") or "MISSING"),
        "outcome_status": _text(trade_outcome.get("outcome_status") or "UNKNOWN"),
        "intent_id": _text(trade_outcome.get("intent_id")),
        "return_pct": trade_outcome.get("return_pct"),
        "realized_pnl": trade_outcome.get("realized_pnl"),
        "unrealized_pnl": trade_outcome.get("unrealized_pnl"),
    }
    missed_summary = {
        "status": _text(missed.get("status") or "MISSING"),
        "alternative_count": len(missed.get("alternatives") if isinstance(missed.get("alternatives"), list) else []),
        "no_fabricated_trades": bool(missed.get("no_fabricated_trades") is True),
    }
    consistency_summary = {
        "status": _text(consistency.get("status") or "MISSING"),
        "ranking_stability": _text(consistency.get("ranking_stability") or "UNKNOWN"),
        "decision_flip_detected": bool(consistency.get("decision_flip_detected") is True),
        "nondeterminism_suspected": bool(consistency.get("nondeterminism_suspected") is True),
    }
    confidence = _confidence_calibration(payloads["edge_attribution_v1"], trade_outcome, selection)
    what_happened = f"Selected {selected_intent_id} from {selected_sleeve_id}." if selected_intent_id else "No intent was selected for trading."
    why_happened = why_this.get("why_it_beat_alternatives") if why_this else f"No-trade classified as {why_no.get('classification', 'unknown')} with blocker {why_no.get('canonical_blocker', 'UNKNOWN')}."
    watch_next = "Watch near misses and drift alerts; recommendations require governance." if (near_misses or drift or recommendations) else "Continue observation; no diagnostic action required."
    operator_summary = {
        "what_happened": what_happened,
        "why_it_happened": why_happened,
        "what_to_watch_next": watch_next,
        "action_required": bool(governance_required or missing_required),
    }
    status = "BLOCKED" if not ledger else ("DEGRADED" if missing_required else "PASS")
    out_path = insight_engine_path(truth_root=truth_root, day_utc=day_utc)
    evidence_paths = sorted(set(str(path) for name, path in paths.items() if path.exists() and name != "strategy_change_governance_v1"))
    payload = {
        "schema_id": "insight_engine",
        "schema_version": "v1",
        "day_utc": day_utc,
        "environment": environment,
        "status": status,
        "selected_intent_id": selected_intent_id,
        "selected_sleeve_id": selected_sleeve_id,
        "why_this_trade": why_this,
        "why_no_trade": why_no,
        "score_gap": selection.get("score_gap"),
        "confidence_level": _text(selection.get("confidence_level") or confidence.get("confidence_level") or "UNKNOWN"),
        "alternatives_not_selected": alternatives,
        "near_misses": near_misses,
        "drift_alerts": drift,
        "confidence_calibration": confidence,
        "outcome_summary": outcome_summary,
        "missed_opportunity_summary": missed_summary,
        "decision_consistency_summary": consistency_summary,
        "operator_summary": operator_summary,
        "advisory_recommendations": recommendations,
        "governance_required": governance_required,
        "governance_route": governance_path if governance_required else "",
        "read_only_policy": {
            "thresholds_changed": False,
            "scoring_weights_changed": False,
            "trades_submitted": False,
            "risk_or_safety_gates_overridden": False,
            "strategy_logic_mutated": False,
            "recommendations_auto_deployed": False,
        },
        "prohibited_actions_attempted": False,
        "missing_required_inputs": missing_required,
        "evidence_paths": evidence_paths,
        "produced_at_utc": _now_iso(),
        "producer": "ops/tools/run_insight_engine_v1.py",
        "artifact_path": str(out_path),
    }
    _write_json(out_path, payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_insight_engine_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default=PAPER_MODE, choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    payload = build_insight_engine_v1(day_utc=day_utc, truth_root=truth_root, environment=str(args.environment).strip().upper())
    print(json.dumps({"status": payload["status"], "governance_required": payload["governance_required"], "path": payload["artifact_path"]}, sort_keys=True))
    return 0 if payload["status"] in {"PASS", "DEGRADED", "BLOCKED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
