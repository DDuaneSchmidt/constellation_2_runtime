from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from constellation_2.common.paper_session_fact_plane_v1 import read_json_object_v1
from ops.tools.run_portfolio_activation_gate_v1 import portfolio_activation_gate_path
from ops.tools.run_portfolio_scoring_v1 import (
    _data_quality_penalty,
    _diversification_bonus,
    _execution_readiness_penalty,
    _intent_payload,
    _outcomes_by_intent,
    _overlap_penalty,
    _regime_alignment,
    _regime_confidence_multiplier,
    _risk_penalty,
    _score_total,
    _signal_strength,
    portfolio_scoring_path,
)
from ops.tools.run_portfolio_state_v1 import portfolio_state_path
from ops.tools.run_regime_confidence_v1 import regime_confidence_path

REPORT_FAMILY = "regime_bucket_candidate_ranking_v1"
REPORT_FILENAME = "regime_bucket_candidate_ranking.v1.json"
SCHEMA_ID = "regime_bucket_candidate_ranking"
SCHEMA_VERSION = "v1"
ONE_PRIMARY = "ONE_PRIMARY_PER_REGIME_BUCKET_SUPPRESSED"


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return read_json_object_v1(path)
    except Exception:
        return {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n", encoding="utf-8")


def regime_bucket_candidate_ranking_report_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / REPORT_FAMILY / day_utc / REPORT_FILENAME


def _reason_codes(row: dict[str, Any]) -> list[str]:
    return [str(code) for code in row.get("reason_codes", []) if str(code)] if isinstance(row.get("reason_codes"), list) else []


def _pre_suppression_eligible(row: dict[str, Any]) -> bool:
    decision = str(row.get("portfolio_gate_decision") or "").upper()
    if decision == "ALLOW" and row.get("raw_intent_id"):
        return True
    reasons = set(_reason_codes(row))
    return bool(row.get("raw_intent_id")) and ONE_PRIMARY in reasons


def _gate_selected(row: dict[str, Any]) -> bool:
    return str(row.get("portfolio_gate_decision") or "").upper() == "ALLOW" and bool(row.get("allowed_by_portfolio_gate"))


def _priority_proxy(row: dict[str, Any]) -> tuple[str, str, str]:
    return (str(row.get("sleeve_id") or ""), str(row.get("raw_intent_id") or ""), str(row.get("raw_intent_hash") or ""))


def _rank_key(row: dict[str, Any]) -> tuple[Any, ...]:
    components = row.get("pre_suppression_score_components") if isinstance(row.get("pre_suppression_score_components"), dict) else {}
    return (
        -float(row.get("pre_suppression_score_total") or 0.0),
        -float(components.get("regime_alignment") or 0.0),
        -float(components.get("diversification_bonus") or 0.0),
        -float(components.get("overlap_penalty") or 0.0),
        -float(components.get("risk_penalty") or 0.0),
        _priority_proxy(row),
    )


def _score_pre_suppression_row(*, row: dict[str, Any], pre_rows: list[dict[str, Any]], state: dict[str, Any], gate: dict[str, Any], outcome_by_intent: dict[str, dict[str, Any]], regime_multiplier: float, regime_path: Path) -> dict[str, Any]:
    scoring_row = dict(row)
    scoring_row["portfolio_gate_decision"] = "ALLOW"
    scoring_row["allowed_by_portfolio_gate"] = True
    intent = _intent_payload(scoring_row)
    intent_id = str(row.get("raw_intent_id") or "")
    outcome = outcome_by_intent.get(intent_id, {})
    raw_regime_alignment = _regime_alignment(scoring_row, state)
    components = {
        "signal_strength": _signal_strength(scoring_row, outcome, intent),
        "regime_alignment": round(raw_regime_alignment * regime_multiplier, 4),
        "diversification_bonus": _diversification_bonus(scoring_row, pre_rows),
        "overlap_penalty": _overlap_penalty(scoring_row, pre_rows),
        "risk_penalty": _risk_penalty(intent) if intent else -5.0,
        "data_quality_penalty": _data_quality_penalty(state, gate),
        "execution_readiness_penalty": _execution_readiness_penalty(scoring_row, intent),
    }
    return {
        "candidate_id": intent_id,
        "intent_id": intent_id,
        "symbol": str(row.get("raw_intent_symbol") or "").upper(),
        "sleeve_id": str(row.get("sleeve_id") or ""),
        "engine_id": str(row.get("sleeve_id") or ""),
        "regime_bucket": str(row.get("regime_bucket") or "UNKNOWN"),
        "raw_signal_status": str(row.get("raw_signal_status") or ""),
        "gate_decision_after_suppression": str(row.get("portfolio_gate_decision") or ""),
        "selected_by_gate": _gate_selected(row),
        "suppressed_by_one_primary_per_bucket": ONE_PRIMARY in set(_reason_codes(row)),
        "reason_codes": _reason_codes(row),
        "pre_suppression_eligible": True,
        "pre_suppression_score_total": _score_total(components),
        "pre_suppression_score_components": components,
        "raw_regime_alignment_component": raw_regime_alignment,
        "regime_confidence_multiplier": regime_multiplier,
        "regime_confidence_path": str(regime_path),
        "raw_intent_path": str(row.get("raw_intent_path") or ""),
        "lifecycle_state_path": str(row.get("lifecycle_state_path") or ""),
        "position_match_status": str(row.get("position_match_status") or ""),
        "order_match_status": str(row.get("order_match_status") or ""),
        "input_order_index": int(row.get("_input_order_index") or 0),
    }


def build_regime_bucket_candidate_ranking_report_v1(*, day_utc: str, truth_root: Path, portfolio_gate_path_arg: Path | None = None, generated_at_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).resolve()
    gate_path = Path(portfolio_gate_path_arg).resolve() if portfolio_gate_path_arg else portfolio_activation_gate_path(truth_root=root, day_utc=day_utc)
    gate = _read_json(gate_path)
    state_path = portfolio_state_path(truth_root=root, day_utc=day_utc)
    state = _read_json(state_path)
    regime_path = regime_confidence_path(truth_root=root, day_utc=day_utc)
    regime_confidence = _read_json(regime_path)
    regime_multiplier = _regime_confidence_multiplier(regime_confidence) if regime_confidence else 1.0
    scoring_path = portfolio_scoring_path(truth_root=root, day_utc=day_utc)
    scoring = _read_json(scoring_path)
    decisions = gate.get("decisions") if isinstance(gate.get("decisions"), list) else []
    indexed = []
    for idx, row in enumerate(decisions):
        if isinstance(row, dict):
            copied = dict(row)
            copied["_input_order_index"] = idx
            indexed.append(copied)
    pre_rows = [row for row in indexed if _pre_suppression_eligible(row)]
    outcome_by_intent = _outcomes_by_intent(gate)
    scored = [
        _score_pre_suppression_row(row=row, pre_rows=pre_rows, state=state, gate=gate, outcome_by_intent=outcome_by_intent, regime_multiplier=regime_multiplier, regime_path=regime_path)
        for row in pre_rows
    ]
    by_bucket: dict[str, list[dict[str, Any]]] = {}
    for row in scored:
        by_bucket.setdefault(str(row.get("regime_bucket") or "UNKNOWN"), []).append(row)

    bucket_reports: list[dict[str, Any]] = []
    selected_candidate_id = ""
    selected_candidate_rank = 0
    top_ranked_candidate_id = ""
    order_dependency_detected = False
    selected_top_ranked = True
    missing_scoring_inputs: list[str] = []
    if not gate:
        missing_scoring_inputs.append(str(gate_path))
    if not state:
        missing_scoring_inputs.append(str(state_path))

    for bucket, rows in sorted(by_bucket.items()):
        rows.sort(key=_rank_key)
        for rank, row in enumerate(rows, start=1):
            row["pre_suppression_rank_in_bucket"] = rank
        selected = next((row for row in rows if row.get("selected_by_gate")), None)
        top = rows[0] if rows else None
        same_priority_ties = len({(row.get("sleeve_id"), row.get("engine_id")) for row in rows}) < len(rows)
        bucket_order_dependent = bool(len(rows) > 1 and same_priority_ties)
        if selected and top and selected.get("candidate_id") != top.get("candidate_id"):
            bucket_order_dependent = True
            selected_top_ranked = False
        if bucket_order_dependent:
            order_dependency_detected = True
        if selected:
            selected_candidate_id = selected_candidate_id or str(selected.get("candidate_id") or "")
            selected_candidate_rank = selected_candidate_rank or int(selected.get("pre_suppression_rank_in_bucket") or 0)
        if top:
            top_ranked_candidate_id = top_ranked_candidate_id or str(top.get("candidate_id") or "")
        bucket_reports.append(
            {
                "regime_bucket": bucket,
                "candidate_count": len(rows),
                "selected_candidate_id": str(selected.get("candidate_id") or "") if selected else "",
                "selected_candidate_rank": int(selected.get("pre_suppression_rank_in_bucket") or 0) if selected else 0,
                "top_ranked_candidate_id": str(top.get("candidate_id") or "") if top else "",
                "top_ranked_score": top.get("pre_suppression_score_total") if top else None,
                "selected_is_top_ranked": bool(selected and top and selected.get("candidate_id") == top.get("candidate_id")),
                "selection_mechanism": "portfolio_activation_gate_v1 currently suppresses by embedded sleeve priority/input order, not by these comparable scores.",
                "order_dependency_detected": bucket_order_dependent,
                "candidates": rows,
            }
        )

    report_path = regime_bucket_candidate_ranking_report_path(truth_root=root, day_utc=day_utc)
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "generated_at_utc": generated_at_utc or _now_iso(),
        "status": "DEGRADED" if missing_scoring_inputs else "PASS",
        "diagnostic_only": True,
        "selection_behavior_changed": False,
        "trading_behavior_changed": False,
        "broker_execution_allowed": False,
        "order_submission_attempted": False,
        "capital_allocation_allowed": False,
        "selected_candidate_id": selected_candidate_id,
        "selected_candidate_rank": selected_candidate_rank,
        "top_ranked_candidate_id": top_ranked_candidate_id,
        "selected_candidate_truly_top_ranked": selected_top_ranked,
        "order_dependency_detected": order_dependency_detected,
        "missing_scoring_inputs": missing_scoring_inputs,
        "recommendation": "Review whether ONE_PRIMARY_PER_REGIME_BUCKET should consume comparable pre-suppression ranking in a separate governed policy update. No behavior changed by this diagnostic.",
        "source_artifacts": {
            "portfolio_activation_gate_path": str(gate_path),
            "portfolio_state_path": str(state_path),
            "portfolio_scoring_path": str(scoring_path),
            "regime_confidence_path": str(regime_path),
        },
        "scoring_context": {
            "portfolio_scoring_intents_scored_count": int(scoring.get("intents_scored_count") or 0) if scoring else 0,
            "pre_suppression_candidate_count": len(scored),
            "score_basis": "Recomputed comparable score for rows that were ALLOW before ONE_PRIMARY_PER_REGIME_BUCKET suppression, using portfolio_scoring_v1 components with gate decision temporarily treated as ALLOW for diagnostics only.",
        },
        "buckets": bucket_reports,
        "artifact_path": str(report_path),
    }
    _write_json(report_path, payload)
    return payload


def load_regime_bucket_candidate_ranking_report_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    return _read_json(regime_bucket_candidate_ranking_report_path(truth_root=truth_root, day_utc=day_utc))
