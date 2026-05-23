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
from ops.tools.run_portfolio_activation_gate_v1 import build_portfolio_activation_gate_v1, portfolio_activation_gate_path
from ops.tools.run_portfolio_state_v1 import portfolio_state_path
from ops.tools.run_regime_confidence_v1 import regime_confidence_path

PAPER_MODE = "PAPER"
BOOTSTRAP_ACCEPTED_FOR_PAPER = "BOOTSTRAP_ACCEPTED_FOR_PAPER"
SCORING_POLICY_ID = "portfolio_scoring_v1"
SCORING_POLICY_VERSION = "2026-04-30.2"
MACRO_DIVERSIFIERS = {"DBC", "GLD", "HYG", "IEF", "LQD", "TLT", "UUP"}
EQUITY_BETA = {"SPY", "QQQ", "IWM"}
RISK_FLOOR = 0.01
PRIORITY_ORDER = [
    "C2_DEFENSIVE_TAIL_V1",
    "C2_EVENT_DISLOCATION_V1",
    "C2_MARKET_NEUTRAL_SPREAD_V1",
    "C2_MEAN_REVERSION_EQ_V1",
    "C2_CROSS_ASSET_TREND_V1",
    "C2_TREND_EQ_PRIMARY_V1",
    "C2_VOL_INCOME_DEFINED_RISK_V1",
]
COMPONENTS = [
    "signal_strength",
    "regime_alignment",
    "diversification_bonus",
    "overlap_penalty",
    "risk_penalty",
    "data_quality_penalty",
    "execution_readiness_penalty",
]
REGIME_CONFIDENCE_DAMPING_FLOOR = 0.35


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


def portfolio_scoring_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "portfolio_scoring_v1" / day_utc / "portfolio_scoring.v1.json"


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _load_gate(*, day_utc: str, truth_root: Path, environment: str, source_rollup_path: Path | None, path: Path | None) -> dict[str, Any]:
    gate_path = Path(path).resolve() if path is not None else portfolio_activation_gate_path(truth_root=truth_root, day_utc=day_utc)
    payload = _read_json(gate_path)
    if payload and str(payload.get("day_utc") or "") == day_utc:
        payload["_artifact_path_resolved"] = str(gate_path)
        return payload
    return build_portfolio_activation_gate_v1(day_utc=day_utc, truth_root=truth_root, environment=environment, source_rollup_path=source_rollup_path)


def _outcomes_by_intent(gate: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rollup_path = Path(str(gate.get("source_rollup_path") or ""))
    rollup = _read_json(rollup_path) if str(rollup_path) else {}
    outcomes = rollup.get("outcomes") if isinstance(rollup.get("outcomes"), list) else rollup.get("sleeve_outcomes")
    out: dict[str, dict[str, Any]] = {}
    for outcome in outcomes if isinstance(outcomes, list) else []:
        if not isinstance(outcome, dict):
            continue
        for intent in outcome.get("output_intents") if isinstance(outcome.get("output_intents"), list) else []:
            if isinstance(intent, dict) and str(intent.get("intent_id") or "").strip():
                out[str(intent.get("intent_id") or "").strip()] = outcome
        signature = outcome.get("intent_signature") if isinstance(outcome.get("intent_signature"), list) else []
        for intent in signature:
            if isinstance(intent, dict) and str(intent.get("intent_id") or "").strip():
                out[str(intent.get("intent_id") or "").strip()] = outcome
    return out


def _intent_payload(row: dict[str, Any]) -> dict[str, Any]:
    path = Path(str(row.get("raw_intent_path") or ""))
    return _read_json(path) if str(path) else {}


def _signal_state(outcome: dict[str, Any]) -> dict[str, Any]:
    signal = outcome.get("signal_state") if isinstance(outcome, dict) else {}
    return signal if isinstance(signal, dict) else {}


def _signal_strength(row: dict[str, Any], outcome: dict[str, Any], intent: dict[str, Any]) -> float:
    signal = _signal_state(outcome)
    explicit = (
        row.get("signal_strength")
        or intent.get("signal_strength")
        or intent.get("confidence")
        or intent.get("score")
        or signal.get("signal_strength")
        or signal.get("strength")
        or signal.get("confidence")
    )
    if explicit is not None:
        value = _as_float(explicit, 0.0)
        return round(max(0.0, min(30.0, value if value > 1 else value * 30.0)), 4)
    if str(row.get("raw_signal_status") or "").upper() != "ACTIVE":
        return 0.0
    duration = _as_float(signal.get("duration_cycles"), 0.0)
    return round(22.5 + min(7.5, duration / 2.0), 4)


def _regime_alignment(row: dict[str, Any], state: dict[str, Any]) -> float:
    decision = str(row.get("portfolio_gate_decision") or "").upper()
    if decision != "ALLOW":
        return 0.0
    sleeve_id = str(row.get("sleeve_id") or "")
    regime = str(state.get("regime") or "UNKNOWN").upper()
    trend = str(state.get("trend_strength") or "UNKNOWN").upper()
    vol = str(state.get("volatility_regime") or "UNKNOWN").upper()
    dispersion = str(state.get("dispersion_regime") or "UNKNOWN").upper()
    if str(state.get("status") or "").upper() in {"DEGRADED", "BLOCKED"} or regime == "UNKNOWN":
        return 5.0
    if regime == "CRISIS":
        return 25.0 if sleeve_id == "C2_DEFENSIVE_TAIL_V1" else 0.0
    if sleeve_id in {"C2_TREND_EQ_PRIMARY_V1", "C2_CROSS_ASSET_TREND_V1"}:
        return 25.0 if regime == "TREND" and trend in {"MEDIUM", "HIGH"} else 8.0
    if sleeve_id == "C2_VOL_INCOME_DEFINED_RISK_V1":
        return 20.0 if vol in {"LOW", "NORMAL"} and regime not in {"VOL_SHOCK", "DISPERSION"} else 0.0
    if sleeve_id == "C2_MEAN_REVERSION_EQ_V1":
        return 22.0 if regime == "CHOPPY" or trend in {"LOW", "UNKNOWN"} else 0.0
    if sleeve_id == "C2_EVENT_DISLOCATION_V1":
        return 18.0 if regime in {"DISPERSION", "VOL_SHOCK"} or dispersion == "HIGH" else 8.0
    if sleeve_id == "C2_DEFENSIVE_TAIL_V1":
        return 24.0 if regime in {"VOL_SHOCK", "DISPERSION"} or vol == "SHOCK" else 4.0
    return 0.0


def _regime_confidence_multiplier(payload: dict[str, Any]) -> float:
    try:
        score = float(payload.get("confidence_score"))
    except Exception:
        return 1.0
    return round(max(REGIME_CONFIDENCE_DAMPING_FLOOR, min(1.0, score)), 6)


def _diversification_bonus(row: dict[str, Any], allowed_rows: list[dict[str, Any]]) -> float:
    symbol = str(row.get("raw_intent_symbol") or "").upper()
    overlap = str(row.get("overlap_group") or "")
    bonus = 0.0
    if symbol in MACRO_DIVERSIFIERS and symbol not in EQUITY_BETA:
        bonus += 15.0
    if symbol and all(symbol != str(other.get("raw_intent_symbol") or "").upper() for other in allowed_rows if other is not row):
        bonus += 4.0
    if overlap and sum(1 for other in allowed_rows if str(other.get("overlap_group") or "") == overlap) == 1:
        bonus += 6.0
    return round(min(15.0, bonus), 4)


def _overlap_penalty(row: dict[str, Any], allowed_rows: list[dict[str, Any]]) -> float:
    symbol = str(row.get("raw_intent_symbol") or "").upper()
    overlap = str(row.get("overlap_group") or "")
    bucket = str(row.get("regime_bucket") or "")
    penalty = 0.0
    for other in allowed_rows:
        if other is row:
            continue
        if symbol and symbol == str(other.get("raw_intent_symbol") or "").upper():
            penalty -= 12.0
        if overlap and overlap == str(other.get("overlap_group") or ""):
            penalty -= 8.0
        if bucket and bucket == str(other.get("regime_bucket") or ""):
            penalty -= 5.0
    return round(max(-15.0, penalty), 4)


def _risk_penalty(intent: dict[str, Any]) -> float:
    target = max(0.0, _as_float(intent.get("target_notional_pct"), RISK_FLOOR))
    constraints = intent.get("constraints") if isinstance(intent.get("constraints"), dict) else {}
    max_risk = max(0.0, _as_float(constraints.get("max_risk_pct"), RISK_FLOOR))
    holding_days = max(0.0, _as_float(intent.get("expected_holding_days"), 0.0))
    penalty = (target * 20.0) + (max_risk * 80.0) + min(5.0, holding_days / 30.0)
    return round(-min(10.0, penalty), 4)


def _data_quality_penalty(state: dict[str, Any], gate: dict[str, Any]) -> float:
    penalty = 0.0
    if str(state.get("status") or "").upper() == "DEGRADED":
        penalty -= 3.0
    if str(state.get("status") or "").upper() == "BLOCKED":
        penalty -= 5.0
    degraded = state.get("degraded_inputs") if isinstance(state.get("degraded_inputs"), list) else []
    missing = state.get("missing_inputs") if isinstance(state.get("missing_inputs"), list) else []
    penalty -= min(2.0, float(len(degraded)) * 0.5)
    penalty -= min(3.0, float(len(missing)))
    if str(gate.get("status") or "").upper() == "DEGRADED":
        penalty -= 1.0
    return round(max(-5.0, penalty), 4)


def _execution_readiness_penalty(row: dict[str, Any], intent: dict[str, Any]) -> float:
    penalty = 0.0
    sleeve_id = str(row.get("sleeve_id") or "")
    exposure_type = str(intent.get("exposure_type") or "").upper()
    constraints = intent.get("constraints") if isinstance(intent.get("constraints"), dict) else {}
    if not intent:
        penalty -= 5.0
    if "VOL" in exposure_type and not isinstance(intent.get("option"), dict):
        penalty -= 5.0
    if sleeve_id == "C2_MARKET_NEUTRAL_SPREAD_V1":
        penalty -= 5.0
    if "max_risk_pct" not in constraints:
        penalty -= 2.0
    return round(max(-5.0, penalty), 4)


def _priority_index(sleeve_id: str) -> int:
    try:
        return PRIORITY_ORDER.index(sleeve_id)
    except ValueError:
        return len(PRIORITY_ORDER)


def _score_total(components: dict[str, float]) -> float:
    return round(max(0.0, min(100.0, sum(float(components.get(name) or 0.0) for name in COMPONENTS))), 4)


def _tie_break_key(row: dict[str, Any]) -> tuple[Any, ...]:
    components = row.get("score_components") if isinstance(row.get("score_components"), dict) else {}
    return (
        -float(row.get("score_total") or 0.0),
        -float(components.get("regime_alignment") or 0.0),
        -float(components.get("diversification_bonus") or 0.0),
        -float(components.get("overlap_penalty") or 0.0),
        -float(components.get("risk_penalty") or 0.0),
        _priority_index(str(row.get("sleeve_id") or "")),
        str(row.get("intent_id") or ""),
    )



def _candidate_manifest_for_rollup(*, truth_root: Path, day_utc: str, source_rollup_path: Path | None, run_id: str) -> tuple[Path | None, dict[str, Any]]:
    candidates: list[Path] = []
    if source_rollup_path is not None:
        try:
            parent = source_rollup_path.resolve().parent.name
            if parent:
                candidates.append(Path(truth_root).resolve() / "reports" / "candidate_generation_manifest_v1" / day_utc / parent / "candidate_generation_manifest.v1.json")
        except Exception:
            pass
    if run_id:
        candidates.append(Path(truth_root).resolve() / "reports" / "candidate_generation_manifest_v1" / day_utc / run_id / "candidate_generation_manifest.v1.json")
    base = Path(truth_root).resolve() / "reports" / "candidate_generation_manifest_v1" / day_utc
    if base.exists():
        candidates.extend(sorted(base.glob("*/candidate_generation_manifest.v1.json"), key=lambda item: (item.stat().st_mtime_ns, str(item)), reverse=True))
    for path in candidates:
        if path.exists() and path.is_file():
            payload = _read_json(path)
            if str(payload.get("day_utc") or "") == day_utc:
                return path.resolve(), payload
    return None, {}


def _candidate_coverage_rows(candidate_manifest: dict[str, Any], scoring_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidate_rows = candidate_manifest.get("candidate_rows") if isinstance(candidate_manifest.get("candidate_rows"), list) else []
    score_by_intent = {str(row.get("intent_id") or "").strip(): row for row in scoring_rows if isinstance(row, dict) and str(row.get("intent_id") or "").strip()}
    coverage: list[dict[str, Any]] = []
    for row in candidate_rows:
        if not isinstance(row, dict):
            continue
        raw_intent_id = str(row.get("raw_intent_id") or "").strip()
        score = score_by_intent.get(raw_intent_id, {}) if raw_intent_id else {}
        if score:
            status = "SCORED" if score.get("executable_eligible") else "NOT_EXECUTABLE"
            reason = str(score.get("score_unavailable_reason") or "")
        elif raw_intent_id:
            status = "SCORE_UNAVAILABLE"
            reason = "PORTFOLIO_SCORING_ROW_MISSING"
        else:
            status = "NOT_APPLICABLE_NO_INTENT"
            reason = "NO_INTENT_DECLARED"
        coverage.append({
            "candidate_id": str(row.get("candidate_id") or raw_intent_id or ""),
            "raw_intent_id": raw_intent_id,
            "symbol": str(row.get("symbol_or_pair") or row.get("symbol") or "").upper(),
            "sleeve_id": str(row.get("sleeve_id") or row.get("engine_id") or ""),
            "candidate_status": str(row.get("status") or ""),
            "market_data_mode": str(row.get("source_data_mode") or score.get("market_data_mode") or ""),
            "score_status": status,
            "score_unavailable_reason": reason,
            "score_total": score.get("score_total") if score else None,
            "rank": score.get("rank") if score else None,
            "final_eod_certification_status": str(row.get("final_eod_certification_status") or score.get("final_eod_certification_status") or ""),
        })
    return coverage

def build_portfolio_scoring_v1(
    *,
    day_utc: str,
    truth_root: Path,
    environment: str = PAPER_MODE,
    source_rollup_path: Path | None = None,
    portfolio_gate_path_arg: Path | None = None,
) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    state_path = portfolio_state_path(truth_root=truth_root, day_utc=day_utc)
    state = _read_json(state_path)
    regime_path = regime_confidence_path(truth_root=truth_root, day_utc=day_utc)
    regime_confidence = _read_json(regime_path)
    regime_multiplier = _regime_confidence_multiplier(regime_confidence) if regime_confidence else 1.0
    gate = _load_gate(day_utc=day_utc, truth_root=truth_root, environment=environment, source_rollup_path=source_rollup_path, path=portfolio_gate_path_arg)
    resolved_rollup_path = str(source_rollup_path.resolve()) if source_rollup_path is not None else str(gate.get("source_rollup_path") or "")
    run_id = str(gate.get("run_id") or "")
    market_data_mode = str(gate.get("market_data_mode") or gate.get("run_mode") or "")
    final_eod_certification_status = str(gate.get("final_eod_certification_status") or "")
    gate_candidate_lane = str(gate.get("candidate_lane") or gate.get("candidate_visibility_lane") or "").upper()
    gate_certification_state = str(gate.get("certification_state") or final_eod_certification_status or "").upper()
    execution_certified = gate_candidate_lane == "CERTIFIED" and gate_certification_state in {"CERTIFIED", "PASS", "VALID"}
    decisions = gate.get("decisions") if isinstance(gate.get("decisions"), list) else []
    outcome_by_intent = _outcomes_by_intent(gate)
    intent_rows = [row for row in decisions if isinstance(row, dict) and str(row.get("raw_intent_id") or "").strip()]
    allowed_rows = [row for row in intent_rows if str(row.get("portfolio_gate_decision") or "").upper() == "ALLOW" and row.get("allowed_by_portfolio_gate") is True]
    rows: list[dict[str, Any]] = []
    for row in intent_rows:
        intent_id = str(row.get("raw_intent_id") or "").strip()
        outcome = outcome_by_intent.get(intent_id, {})
        intent = _intent_payload(row)
        decision = str(row.get("portfolio_gate_decision") or "").upper()
        executable_eligible = decision == "ALLOW" and row.get("allowed_by_portfolio_gate") is True and execution_certified
        if executable_eligible:
            raw_regime_alignment = _regime_alignment(row, state)
            components = {
                "signal_strength": _signal_strength(row, outcome, intent),
                "regime_alignment": round(raw_regime_alignment * regime_multiplier, 4),
                "diversification_bonus": _diversification_bonus(row, allowed_rows),
                "overlap_penalty": _overlap_penalty(row, allowed_rows),
                "risk_penalty": _risk_penalty(intent) if intent else -5.0,
                "data_quality_penalty": _data_quality_penalty(state, gate),
                "execution_readiness_penalty": _execution_readiness_penalty(row, intent),
            }
            total = _score_total(components)
        else:
            components = {name: 0.0 for name in COMPONENTS}
            raw_regime_alignment = 0.0
            total = 0.0
        evidence_paths = [
            str(row.get("raw_intent_path") or ""),
            str(row.get("lifecycle_state_path") or ""),
            str(row.get("portfolio_state_snapshot_path") or state.get("artifact_path") or state_path),
            str(gate.get("_artifact_path_resolved") or gate.get("artifact_path") or ""),
        ]
        evidence_paths = [path for path in evidence_paths if path]
        reasons = row.get("reason_codes") if isinstance(row.get("reason_codes"), list) else []
        if executable_eligible:
            scoring_reason = "SCORING_EXECUTABLE_ELIGIBLE"
        elif decision == "ALLOW" and row.get("allowed_by_portfolio_gate") is True and not execution_certified:
            scoring_reason = "SCORING_NOT_EXECUTABLE_NON_CERTIFIED_INPUT"
        else:
            scoring_reason = f"SCORING_NOT_EXECUTABLE_{decision or 'UNKNOWN'}"
        rows.append(
            {
                "rank": 0,
                "candidate_id": str(row.get("candidate_id") or row.get("raw_intent_id") or ""),
                "intent_id": intent_id,
                "sleeve_id": str(row.get("sleeve_id") or ""),
                "symbol": str(row.get("raw_intent_symbol") or "").upper(),
                "run_id": run_id,
                "market_data_mode": market_data_mode,
                "final_eod_certification_status": final_eod_certification_status,
                "final_eod_certification_pending": final_eod_certification_status.upper() == "PENDING",
                "candidate_lane": gate_candidate_lane or str(row.get("candidate_lane") or ""),
                "certification_state": gate_certification_state or str(row.get("certification_state") or ""),
                "execution_firewall_status": "PASS" if execution_certified else "REJECTED_NON_CERTIFIED_INPUT",
                "raw_signal_status": str(row.get("raw_signal_status") or "").upper(),
                "portfolio_gate_decision": decision,
                "allowed_by_portfolio_gate": bool(row.get("allowed_by_portfolio_gate")),
                "executable_eligible": executable_eligible,
                "lifecycle_state_path": str(row.get("lifecycle_state_path") or ""),
                "lifecycle_decision": str(row.get("lifecycle_decision") or ""),
                "lifecycle_reason_codes": row.get("lifecycle_reason_codes") if isinstance(row.get("lifecycle_reason_codes"), list) else [],
                "position_match_status": str(row.get("position_match_status") or ""),
                "order_match_status": str(row.get("order_match_status") or ""),
                "reentry_eligible": bool(row.get("reentry_eligible")),
                "unchanged_signal": bool(row.get("unchanged_signal")),
                "score_total": total,
                "score_components": components,
                "regime_confidence_path": str(regime_path),
                "regime_confidence_level": str(regime_confidence.get("confidence_level") or "UNKNOWN") if regime_confidence else "UNKNOWN",
                "regime_confidence_multiplier": regime_multiplier,
                "raw_regime_alignment_component": raw_regime_alignment,
                "reason_codes": sorted(set([*reasons, scoring_reason])),
                "evidence_paths": evidence_paths,
                "portfolio_activation_gate_path": str(gate.get("_artifact_path_resolved") or gate.get("artifact_path") or ""),
                "portfolio_state_snapshot_path": str(state.get("artifact_path") or state_path),
                "source_rollup_path": resolved_rollup_path,
                "score_status": "SCORED" if executable_eligible else "NOT_EXECUTABLE",
                "score_unavailable_reason": "" if executable_eligible else ("NON_CERTIFIED_CANDIDATE_SNAPSHOT" if decision == "ALLOW" and row.get("allowed_by_portfolio_gate") is True and not execution_certified else f"PORTFOLIO_GATE_{decision or 'UNKNOWN'}"),
            }
        )
    rankable = [row for row in rows if row["executable_eligible"]]
    rankable.sort(key=_tie_break_key)
    for rank, row in enumerate(rankable, start=1):
        row["rank"] = rank
    rows.sort(key=lambda row: (row["rank"] if row["rank"] else 999999, str(row.get("sleeve_id") or ""), str(row.get("intent_id") or "")))
    candidate_manifest_path, candidate_manifest = _candidate_manifest_for_rollup(truth_root=truth_root, day_utc=day_utc, source_rollup_path=source_rollup_path, run_id=run_id)
    candidate_coverage = _candidate_coverage_rows(candidate_manifest, rows)
    out_path = portfolio_scoring_path(truth_root=truth_root, day_utc=day_utc)
    selected_candidate_intent_id = str(rankable[0].get("intent_id") or "") if rankable else ""
    if not gate:
        status = "BLOCKED"
    elif str(state.get("status") or "").upper() == BOOTSTRAP_ACCEPTED_FOR_PAPER or str(gate.get("status") or "").upper() == BOOTSTRAP_ACCEPTED_FOR_PAPER:
        status = BOOTSTRAP_ACCEPTED_FOR_PAPER
    elif str(state.get("status") or "").upper() == "DEGRADED" or str(gate.get("status") or "").upper() == "DEGRADED":
        status = "DEGRADED"
    else:
        status = "PASS"
    payload = {
        "schema_id": "portfolio_scoring",
        "schema_version": "v1",
        "day_utc": day_utc,
        "environment": environment,
        "status": status,
        "canonical_blocker": "" if gate else "PORTFOLIO_ACTIVATION_GATE_MISSING",
        "portfolio_activation_gate_path": str(gate.get("_artifact_path_resolved") or gate.get("artifact_path") or ""),
        "source_rollup_path": resolved_rollup_path,
        "run_id": run_id,
        "market_data_mode": market_data_mode,
        "final_eod_certification_status": final_eod_certification_status,
        "final_eod_certification_pending": final_eod_certification_status.upper() == "PENDING",
        "candidate_lane": gate_candidate_lane,
        "certification_state": gate_certification_state,
        "execution_firewall_status": "PASS" if execution_certified else "REJECTED_NON_CERTIFIED_INPUT",
        "portfolio_state_path": str(state.get("artifact_path") or state_path),
        "regime_confidence_path": str(regime_path),
        "regime_confidence_level": str(regime_confidence.get("confidence_level") or "UNKNOWN") if regime_confidence else "UNKNOWN",
        "regime_confidence_multiplier": regime_multiplier,
        "scoring_policy_id": SCORING_POLICY_ID,
        "scoring_policy_version": SCORING_POLICY_VERSION,
        "intents_scored_count": len(rankable),
        "selected_candidate_intent_id": selected_candidate_intent_id,
        "candidate_manifest_path": str(candidate_manifest_path or ""),
        "candidate_coverage_count": len(candidate_coverage),
        "candidate_coverage": candidate_coverage,
        "rankings": rows,
        "ranked_intents": rows,
        "policy": {
            "policy_id": SCORING_POLICY_ID,
            "policy_version": SCORING_POLICY_VERSION,
            "score_range": [0.0, 100.0],
            "components": COMPONENTS,
            "tie_breakers": [
                "score_total_desc",
                "regime_alignment_desc",
                "diversification_bonus_desc",
                "overlap_penalty_least_severe",
                "risk_penalty_least_severe",
                "priority_order",
                "intent_id_ascending",
            ],
            "priority_order": PRIORITY_ORDER,
        },
        "produced_at_utc": _now_iso(),
        "producer": "ops/tools/run_portfolio_scoring_v1.py",
        "artifact_path": str(out_path),
    }
    _write_json(out_path, payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_portfolio_scoring_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default=PAPER_MODE, choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--source_rollup_path", default="")
    parser.add_argument("--portfolio_gate_path", default="")
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    rollup = Path(args.source_rollup_path).resolve() if str(args.source_rollup_path or "").strip() else None
    gate_path = Path(args.portfolio_gate_path).resolve() if str(args.portfolio_gate_path or "").strip() else None
    payload = build_portfolio_scoring_v1(
        day_utc=day_utc,
        truth_root=truth_root,
        environment=str(args.environment).strip().upper(),
        source_rollup_path=rollup,
        portfolio_gate_path_arg=gate_path,
    )
    print(json.dumps({"status": payload["status"], "path": payload["artifact_path"], "ranked_count": len(payload["rankings"])}, sort_keys=True))
    return 0 if payload["status"] in {"PASS", "DEGRADED", BOOTSTRAP_ACCEPTED_FOR_PAPER} else 2


if __name__ == "__main__":
    raise SystemExit(main())
