from __future__ import annotations

import copy
import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional


PLACEHOLDER_VALUES = {
    "",
    "UNKNOWN",
    "unknown",
    "not reported",
    "Not reported",
    "No summary available.",
    "no summary available",
    "n/a",
    "N/A",
    "none",
    "None",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() not in PLACEHOLDER_VALUES
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


def _first_present(mapping: Mapping[str, Any], keys: Iterable[str]) -> Optional[Any]:
    for key in keys:
        value = mapping.get(key)
        if _present(value):
            return value
    return None


def _as_text(value: Any, fallback: str) -> str:
    if _present(value):
        if isinstance(value, (dict, list)):
            return json.dumps(value, sort_keys=True)
        return str(value)
    return fallback


def _hash_payload(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _candidate_id(candidate: Mapping[str, Any]) -> str:
    return str(candidate.get("candidate_id") or candidate.get("id") or "").strip()


def _all_candidate_rows(cockpit_payload: Mapping[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in _safe_list(cockpit_payload.get("top_candidates")):
        if isinstance(row, dict):
            rows.append(row)
    candidates = cockpit_payload.get("candidate_decisions_corrections")
    if isinstance(candidates, dict):
        for value in candidates.values():
            for row in _safe_list(value):
                if isinstance(row, dict):
                    rows.append(row)
    seen: set[str] = set()
    unique: List[Dict[str, Any]] = []
    for row in rows:
        key = _candidate_id(row) or f"{row.get('symbol')}|{row.get('sleeve_id')}|{row.get('generated_at')}"
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique


def find_candidate_for_decision_support_v1(cockpit_payload: Mapping[str, Any], candidate_id: str) -> Optional[Dict[str, Any]]:
    wanted = str(candidate_id or "").strip()
    for row in _all_candidate_rows(cockpit_payload):
        if _candidate_id(row) == wanted:
            return row
    return None


def _matching_sleeve_context(candidate: Mapping[str, Any], cockpit_payload: Mapping[str, Any]) -> Dict[str, Any]:
    sleeve_id = str(candidate.get("sleeve_id") or candidate.get("sleeve") or "").strip()
    sleeves = cockpit_payload.get("sleeve_warnings")
    if not sleeve_id or not isinstance(sleeves, dict):
        return {}
    for value in sleeves.values():
        for row in _safe_list(value):
            if isinstance(row, dict) and str(row.get("sleeve_id") or "").strip() == sleeve_id:
                return row
    return {}


def _matching_blocker_context(candidate: Mapping[str, Any], cockpit_payload: Mapping[str, Any]) -> List[Dict[str, Any]]:
    sleeve_id = str(candidate.get("sleeve_id") or candidate.get("sleeve") or "").strip()
    symbol = str(candidate.get("symbol") or "").strip()
    opportunities = cockpit_payload.get("opportunities") if isinstance(cockpit_payload.get("opportunities"), dict) else {}
    rows: List[Dict[str, Any]] = []
    for row in _safe_list(opportunities.get("sleeve_run_summary")):
        if isinstance(row, dict) and sleeve_id and str(row.get("sleeve_id") or "").strip() == sleeve_id:
            rows.append(row)
    market = opportunities.get("market_data_summary") if isinstance(opportunities.get("market_data_summary"), dict) else {}
    missing_or_stale = set(str(item) for item in _safe_list(market.get("missing_symbols")) + _safe_list(market.get("stale_symbols")))
    if symbol and symbol in missing_or_stale:
        rows.append({"blocker_type": "candidate_market_data", "symbol": symbol, "reason": "Candidate symbol has missing or stale market data."})
    return rows


def _evidence_item(name: str, present: bool, source_checked: str, why_missing: str, blocks: bool) -> Dict[str, Any]:
    return {
        "item": name,
        "status": "present" if present else "missing",
        "why_it_matters": "Available evidence can be inspected by the operator." if present else why_missing,
        "blocks_manual_capture_recommendation": bool(blocks and not present),
        "source_checked": source_checked,
    }


def build_candidate_decision_support_brief_v1(
    candidate: Mapping[str, Any],
    cockpit_payload: Mapping[str, Any],
    *,
    generated_at: Optional[str] = None,
) -> Dict[str, Any]:
    candidate_copy = copy.deepcopy(dict(candidate))
    sleeve_context = _matching_sleeve_context(candidate_copy, cockpit_payload)
    blockers = _matching_blocker_context(candidate_copy, cockpit_payload)
    runtime = cockpit_payload.get("runtime") if isinstance(cockpit_payload.get("runtime"), dict) else {}
    opportunities = cockpit_payload.get("opportunities") if isinstance(cockpit_payload.get("opportunities"), dict) else {}
    market_summary = opportunities.get("market_data_summary") if isinstance(opportunities.get("market_data_summary"), dict) else {}

    candidate_id = _candidate_id(candidate_copy) or "candidate_id_unavailable"
    symbol = str(candidate_copy.get("symbol") or "UNKNOWN_SYMBOL")
    direction = str(candidate_copy.get("direction") or candidate_copy.get("candidate_direction") or "UNKNOWN_DIRECTION")
    sleeve_id = str(candidate_copy.get("sleeve_id") or candidate_copy.get("sleeve") or "UNKNOWN_SLEEVE")
    signal_reason = _first_present(candidate_copy, ["why_now", "why_this_trade", "explanation", "signal_reason", "raw_signal_reason"])
    trigger_source = str(candidate_copy.get("sleeve_id") or candidate_copy.get("signal_source") or candidate_copy.get("generator") or "the candidate generator")

    if _present(signal_reason):
        why_triggered = str(signal_reason)
    else:
        why_triggered = (
            f"{symbol} {direction} was generated by {trigger_source}, but the candidate generator did not provide a detailed why-now note. "
            "This lowers trust because the operator cannot inspect the immediate trigger rationale."
        )

    expectancy = _first_present(candidate_copy, ["historical_expectancy", "post_cost_expectancy_reference", "expectancy"])
    event_study = _first_present(candidate_copy, ["event_study_summary", "evidence_summary", "event_study_evidence_id"])
    backtest = _first_present(candidate_copy, ["backtest_summary", "backtest_evidence_id"])
    longitudinal = _first_present(candidate_copy, ["longitudinal_summary", "longitudinal_run_id"])
    drift = _first_present(candidate_copy, ["drift_state", "expectancy_drift_status", "expectancy_drift_report_id"])
    fragility = _first_present(candidate_copy, ["fragility_state", "regime_fragility_status", "regime_fragility_report_id"])
    sleeve_health = _first_present(candidate_copy, ["sleeve_health", "stability_state"]) or _first_present(sleeve_context, ["recommendation", "bucket", "status"])
    paper_trial = _first_present(candidate_copy, ["paper_trial_status", "paper_trial_id"])
    observation_count = _first_present(candidate_copy, ["observation_count", "measured_candidate_count", "sample_size"])
    evidence_ids = _safe_list(candidate_copy.get("evidence_artifacts") or candidate_copy.get("source_artifacts") or candidate_copy.get("supporting_evidence_ids"))
    if _present(candidate_copy.get("source_artifact_path")):
        evidence_ids.append(str(candidate_copy.get("source_artifact_path")))

    evidence_checklist = [
        _evidence_item("Trigger reason", _present(signal_reason), "candidate why_now / signal_reason", "The operator cannot inspect why this setup fired right now.", True),
        _evidence_item("Why-now explanation", _present(_first_present(candidate_copy, ["why_now", "why_this_trade", "explanation"])), "candidate why_now fields", "The immediate trigger rationale is missing.", True),
        _evidence_item("Historical expectancy", _present(expectancy), "candidate expectancy fields", "No candidate-linked expectancy artifact was found for this sleeve/symbol/direction. This means Aegis cannot quantify historical post-cost behavior for this exact setup yet.", True),
        _evidence_item("Event-study support", _present(event_study), "candidate event-study fields", "Event-study support is not linked, so the operator cannot inspect event count, win rate, or post-event behavior.", True),
        _evidence_item("Drift status", _present(drift), "candidate drift fields / drift report ref", "Drift status is missing, so Aegis cannot show whether the edge is degrading.", False),
        _evidence_item("Fragility status", _present(fragility), "candidate fragility fields / fragility report ref", "Regime fragility is missing, so Aegis cannot show where this setup is vulnerable.", False),
        _evidence_item("Sleeve health", _present(sleeve_health), "candidate sleeve health / sleeve warnings", "Sleeve health context is missing, so the operator cannot judge whether the research sleeve is stable.", False),
        _evidence_item("Paper-trial context", _present(paper_trial) or _present(observation_count), "candidate paper-trial / observation fields", "Paper-trial observations are missing or insufficient.", False),
        _evidence_item("Market data freshness", not bool(blockers), "market_data_summary and sleeve_run_summary", "Candidate or sleeve market data is missing/stale.", True),
    ]

    missing_evidence = [item for item in evidence_checklist if item["status"] == "missing"]
    hard_blocker = bool(blockers) or str(candidate_copy.get("review_state") or "").upper() == "EXPIRED" or str(candidate_copy.get("candidate_status") or "").lower() == "expired"
    supporting_count = sum(1 for value in [event_study, backtest, longitudinal, expectancy] if _present(value))
    context_count = sum(1 for value in [drift, fragility, sleeve_health, paper_trial, observation_count] if _present(value))
    has_trigger = _present(signal_reason)
    critical_missing = [item for item in missing_evidence if item["blocks_manual_capture_recommendation"]]

    weakening_factors: List[str] = []
    if not has_trigger:
        weakening_factors.append("Why-now explanation missing: the immediate trigger rationale cannot be inspected.")
    if not _present(expectancy):
        weakening_factors.append("No historical expectancy linked: post-cost behavior for this exact setup is not quantified.")
    if not _present(event_study):
        weakening_factors.append("No event-study support linked: event count and event behavior cannot be reviewed.")
    if not _present(drift):
        weakening_factors.append("Drift status unavailable: degradation cannot be ruled out.")
    if not _present(fragility):
        weakening_factors.append("Regime fragility unavailable: vulnerability by regime cannot be inspected.")
    if not _present(observation_count):
        weakening_factors.append("Insufficient observations: forward observation history is not available for this candidate.")
    for blocker in blockers:
        weakening_factors.append(str(blocker.get("reason") or blocker.get("canonical_blocker") or blocker.get("blocker_type") or "Blocked sleeve or market-data input."))
    if not weakening_factors:
        weakening_factors.append("No weakening factor was reported by the available advisory projection.")

    if hard_blocker:
        trust_classification = "blocked"
        decision_guidance = "blocked_do_not_capture"
        recommended_operator_action = "dismiss"
        manual_capture_allowed = False
        direct_answer = "Do not capture; candidate is blocked."
    elif has_trigger and supporting_count >= 1 and context_count >= 2 and not critical_missing:
        trust_classification = "partially_supported" if missing_evidence else "supported"
        decision_guidance = "reasonable_for_manual_review" if trust_classification == "supported" else "caution_manual_review_only"
        recommended_operator_action = "review_and_decide"
        manual_capture_allowed = True
        direct_answer = "Manual capture may be reasonable after review." if trust_classification == "supported" else "Manual capture should be treated cautiously."
    elif has_trigger and supporting_count >= 1 and context_count >= 1 and not hard_blocker:
        trust_classification = "partially_supported"
        decision_guidance = "caution_manual_review_only"
        recommended_operator_action = "review_and_decide"
        manual_capture_allowed = True
        direct_answer = "Manual capture should be treated cautiously."
    elif supporting_count >= 1 or context_count >= 1:
        trust_classification = "weakly_supported"
        decision_guidance = "paper_observation_preferred"
        recommended_operator_action = "paper_observe_only"
        manual_capture_allowed = True
        direct_answer = "Paper observation is preferred."
    else:
        trust_classification = "unsupported"
        decision_guidance = "not_recommended_due_to_missing_evidence"
        recommended_operator_action = "request_more_evidence"
        manual_capture_allowed = True
        direct_answer = (
            "Manual capture is not recommended because supporting evidence is incomplete. "
            "Treat this as a watchlist or request-more-evidence candidate unless you have independent justification."
        )

    expected_holding_window = _as_text(
        _first_present(candidate_copy, ["expected_holding_period", "holding_period", "expected_holding_window"]),
        f"Review before candidate expiry: {candidate_copy.get('expires_at') or candidate_copy.get('review_expires_at')}" if _present(candidate_copy.get("expires_at") or candidate_copy.get("review_expires_at")) else "No holding window was supplied by this sleeve. Use manual review only.",
    )
    invalidation_conditions = [
        f"Candidate expires at {candidate_copy.get('expires_at') or candidate_copy.get('review_expires_at')}." if _present(candidate_copy.get("expires_at") or candidate_copy.get("review_expires_at")) else "Setup invalid if review deadline passes or candidate expires.",
        "Setup invalid if market data freshness becomes stale.",
        "Setup invalid if sleeve input requirements become blocked.",
        "Setup invalid if operator marks needs-more-evidence or dismisses.",
        "Setup invalid if the next candidate run reverses direction or removes the candidate.",
    ]

    historical_support_parts: List[str] = []
    if _present(expectancy):
        historical_support_parts.append(f"Expectancy: {expectancy}")
    if _present(event_study):
        historical_support_parts.append(f"Event study: {event_study}")
    if _present(backtest):
        historical_support_parts.append(f"Backtest: {backtest}")
    if _present(longitudinal):
        historical_support_parts.append(f"Longitudinal evidence: {longitudinal}")
    historical_support = historical_support_parts or [
        "No candidate-linked historical support artifact was found. Aegis cannot quantify this setup's post-cost historical behavior from the current projection."
    ]
    sleeve_health_context = _as_text(
        sleeve_health,
        "Sleeve health context is incomplete. The operator cannot confirm whether this sleeve is stable from the current projection.",
    )
    operator_summary = (
        f"{symbol} {direction} from {sleeve_id}: {direct_answer} "
        f"Main concern: {weakening_factors[0]}"
    )

    source_artifacts = evidence_ids + [
        str(cockpit_payload.get("source_paths", {}).get("canonical_operator_state") or ""),
        str(cockpit_payload.get("source_paths", {}).get("operator_brief") or ""),
    ]
    source_artifacts = [item for item in source_artifacts if _present(item)]

    brief: Dict[str, Any] = {
        "candidate_id": candidate_id,
        "symbol": symbol,
        "direction": direction,
        "sleeve_id": sleeve_id,
        "generated_at": str(candidate_copy.get("generated_at") or candidate_copy.get("generated_at_utc") or ""),
        "research_label": "ADVISORY_ONLY",
        "manual_capture_allowed": bool(manual_capture_allowed),
        "execution_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "order_routing_allowed": False,
        "automatic_approval_allowed": False,
        "automatic_promotion_allowed": False,
        "trust_classification": trust_classification,
        "trust_score_label": trust_classification.replace("_", " ").title(),
        "decision_guidance": decision_guidance,
        "direct_answer": direct_answer,
        "why_triggered": why_triggered,
        "historical_support": historical_support,
        "weakening_factors": weakening_factors,
        "sleeve_health_context": sleeve_health_context,
        "expected_holding_window": expected_holding_window,
        "invalidation_conditions": invalidation_conditions,
        "missing_evidence": missing_evidence,
        "evidence_checklist": evidence_checklist,
        "source_artifacts": source_artifacts,
        "operator_summary": operator_summary,
        "recommended_operator_action": recommended_operator_action,
        "audit_refs": [
            {"type": "canonical_operator_state", "path": cockpit_payload.get("source_paths", {}).get("canonical_operator_state", "")},
            {"type": "operator_brief", "path": cockpit_payload.get("source_paths", {}).get("operator_brief", "")},
        ],
        "runtime_context": {
            "readiness_status": opportunities.get("noon_preflight", {}).get("readiness_status") if isinstance(opportunities.get("noon_preflight"), dict) else None,
            "runtime_truth_classification": runtime.get("runtime_truth_classification"),
            "market_data_status": market_summary.get("status"),
        },
        "generated_at_utc": generated_at or _utc_now_iso(),
        "schema_version": "candidate_decision_support_brief.v1",
    }
    stable_hash_payload = {key: value for key, value in brief.items() if key != "generated_at_utc"}
    brief["content_hash"] = _hash_payload(stable_hash_payload)
    return brief


def build_candidate_decision_support_payload_v1(cockpit_payload: Mapping[str, Any]) -> Dict[str, Any]:
    briefs = [
        build_candidate_decision_support_brief_v1(candidate, cockpit_payload)
        for candidate in _all_candidate_rows(cockpit_payload)
    ]
    return {
        "ok": True,
        "read_only": True,
        "schema_version": "candidate_decision_support_payload.v1",
        "briefs": briefs,
        "by_candidate_id": {brief["candidate_id"]: brief for brief in briefs},
        "safety": {
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "order_routing_allowed": False,
            "automatic_approval_allowed": False,
            "automatic_promotion_allowed": False,
        },
    }
