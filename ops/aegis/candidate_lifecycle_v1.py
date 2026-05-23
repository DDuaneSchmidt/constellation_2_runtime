from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import latest_json_v1, write_json_v1


REPORT_FAMILY = "aegis_candidate_lifecycle_v1"
REVIEW_TTL_HOURS = 24
VALID_DECISIONS = {"TRADED_MANUALLY", "IGNORED", "DEFERRED", "EXPIRED", "INVALIDATED"}
VALID_OUTCOMES = {"OUTCOME_PENDING", "OUTCOME_WON", "OUTCOME_LOST", "OUTCOME_FLAT", "OUTCOME_UNKNOWN", "INVALIDATED"}
VALID_REVIEW_STATES = {"REVIEW_REQUIRED", "WATCHLISTED", "DISMISSED", "NEEDS_MORE_EVIDENCE", "EXPIRED"}
VALID_REVIEW_ACTIONS = {
    "WATCHLIST": "WATCHLISTED",
    "DISMISS": "DISMISSED",
    "NEEDS_MORE_EVIDENCE": "NEEDS_MORE_EVIDENCE",
    "ADD_NOTE": "",
}
VALID_CORRECTION_FIELDS = {
    "intended_shares",
    "decision",
    "risk_bucket",
    "decision_reason",
    "executed_confirmed",
    "operator_note",
    "manual_trade_receipt_id",
}


def build_candidate_lifecycle_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated = _generated_candidates(root=root, day_utc=day_utc)
    decisions = read_candidate_decisions_v1(truth_root=root, day_utc=day_utc)
    reviews = read_candidate_review_events_v1(truth_root=root, day_utc=day_utc)
    outcome_payload = read_candidate_outcomes_v1(truth_root=root, day_utc=day_utc)
    outcomes = {
        str(row.get("candidate_id") or ""): row
        for row in outcome_payload.get("outcomes", [])
        if isinstance(row, dict) and str(row.get("candidate_id") or "")
    }
    state_by_candidate = derive_candidate_decision_states_v1(decisions)
    review_by_candidate = derive_candidate_review_states_v1(generated_candidates=generated, review_events=reviews)
    for row in decisions:
        candidate_id = str(row.get("candidate_id") or "")
        if candidate_id and candidate_id not in state_by_candidate:
            state_by_candidate[candidate_id] = _empty_decision_state(candidate_id)
    candidates = []
    for candidate in generated:
        candidate_id = str(candidate.get("candidate_id") or "")
        decision = state_by_candidate.get(candidate_id, _empty_decision_state(candidate_id))
        review = review_by_candidate.get(candidate_id, _empty_review_state(candidate_id, candidate=candidate))
        outcome = outcomes.get(candidate_id, {})
        candidates.append(_candidate_record(candidate=candidate, decision=decision, review=review, outcome=outcome))
    for candidate_id, decision in sorted(state_by_candidate.items()):
        if candidate_id not in {row["candidate_id"] for row in candidates}:
            candidates.append(
                _candidate_record(
                    candidate={"candidate_id": candidate_id},
                    decision=decision,
                    review=review_by_candidate.get(candidate_id, _empty_review_state(candidate_id)),
                    outcome=outcomes.get(candidate_id, {}),
                )
            )
    for candidate_id, review in sorted(review_by_candidate.items()):
        if candidate_id and candidate_id not in {row["candidate_id"] for row in candidates}:
            candidates.append(_candidate_record(candidate={"candidate_id": candidate_id}, decision=_empty_decision_state(candidate_id), review=review, outcome=outcomes.get(candidate_id, {})))
    states = _state_counts(candidates)
    return {
        "schema_id": "aegis_candidate_lifecycle",
        "schema_version": "v1",
        "artifact_id": "aegis_candidate_lifecycle_v1",
        "generated_at": _now(),
        "day_utc": day_utc,
        "candidate_count": len(candidates),
        "state_counts": states,
        "candidates": candidates,
        "candidate_decisions_path": str(candidate_decisions_path_v1(truth_root=root, day_utc=day_utc)),
        "candidate_reviews_path": str(candidate_reviews_path_v1(truth_root=root, day_utc=day_utc)),
        "candidate_outcomes_path": str(candidate_outcomes_path_v1(truth_root=root, day_utc=day_utc)),
        "safety": _safety(),
    }


def write_candidate_lifecycle_reports_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any] | None = None) -> dict[str, str]:
    root = Path(truth_root).expanduser().resolve()
    payload = payload or build_candidate_lifecycle_v1(truth_root=root, day_utc=day_utc)
    out_dir = root / "reports" / REPORT_FAMILY / day_utc
    lifecycle_path = write_json_v1(out_dir / "candidate_lifecycle.v1.json", payload)
    outcomes_path = candidate_outcomes_path_v1(truth_root=root, day_utc=day_utc)
    if not outcomes_path.exists():
        write_json_v1(outcomes_path, _default_outcomes_payload(day_utc=day_utc, candidates=payload.get("candidates") or []))
    decisions_path = candidate_decisions_path_v1(truth_root=root, day_utc=day_utc)
    decisions_path.parent.mkdir(parents=True, exist_ok=True)
    decisions_path.touch(exist_ok=True)
    reviews_path = candidate_reviews_path_v1(truth_root=root, day_utc=day_utc)
    reviews_path.parent.mkdir(parents=True, exist_ok=True)
    reviews_path.touch(exist_ok=True)
    summary_path = out_dir / "candidate_lifecycle.summary.txt"
    summary_path.write_text(render_candidate_lifecycle_summary_v1(payload), encoding="utf-8")
    return {
        "lifecycle": str(lifecycle_path),
        "decisions": str(decisions_path),
        "reviews": str(reviews_path),
        "outcomes": str(outcomes_path),
        "summary": str(summary_path),
    }


def append_candidate_decision_v1(
    *,
    truth_root: Path,
    day_utc: str,
    candidate_id: str,
    decision: str,
    reason: str,
    operator: str,
    manual_trade_receipt_id: str = "",
    intended_shares: Any = None,
    risk_bucket: str = "",
    operator_note: str = "",
    executed_confirmed: Any = None,
) -> dict[str, Any]:
    if not candidate_id:
        raise ValueError("candidate_id is required")
    normalized = str(decision or "").strip().upper()
    if normalized not in VALID_DECISIONS:
        raise ValueError(f"decision must be one of {sorted(VALID_DECISIONS)}")
    if not reason:
        raise ValueError("reason is required")
    if not operator:
        raise ValueError("operator is required")
    timestamp = _now()
    event = {
        "event_type": "CANDIDATE_DECISION_RECORDED",
        "decision_event_id": f"candidate-decision:{candidate_id}:{timestamp}",
        "timestamp": timestamp,
        "timestamp_utc": timestamp,
        "candidate_id": candidate_id,
        "decision": normalized,
        "reason": reason,
        "decision_reason": reason,
        "operator": operator,
        "manual_trade_receipt_id": manual_trade_receipt_id,
        "intended_shares": _parse_intended_shares(intended_shares),
        "risk_bucket": str(risk_bucket or "").strip().upper(),
        "operator_note": str(operator_note or ""),
        "executed_confirmed": _parse_bool(executed_confirmed) if executed_confirmed is not None else False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "broker_submit_transmit_called": False,
    }
    path = candidate_decisions_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")
    write_candidate_lifecycle_reports_v1(truth_root=truth_root, day_utc=day_utc)
    return event


def append_candidate_review_action_v1(
    *,
    truth_root: Path,
    day_utc: str,
    candidate_id: str,
    action: str,
    operator: str,
    operator_note: str = "",
    source: str = "CLI",
) -> dict[str, Any]:
    if not candidate_id:
        raise ValueError("candidate_id is required")
    normalized = str(action or "").strip().upper().replace("-", "_")
    if normalized not in VALID_REVIEW_ACTIONS:
        raise ValueError(f"action must be one of {sorted(VALID_REVIEW_ACTIONS)}")
    if not operator:
        raise ValueError("operator is required")
    root = Path(truth_root).expanduser().resolve()
    generated = _generated_candidates(root=root, day_utc=day_utc)
    reviews = read_candidate_review_events_v1(truth_root=root, day_utc=day_utc)
    current = derive_candidate_review_states_v1(generated_candidates=generated, review_events=reviews).get(candidate_id, _empty_review_state(candidate_id))
    prior_state = str(current.get("review_state") or "REVIEW_REQUIRED")
    new_state = VALID_REVIEW_ACTIONS[normalized] or prior_state
    timestamp = _now()
    event = {
        "event_type": "CANDIDATE_REVIEW_RECORDED",
        "review_event_id": f"candidate-review:{candidate_id}:{timestamp}",
        "timestamp": timestamp,
        "timestamp_utc": timestamp,
        "candidate_id": candidate_id,
        "action": normalized,
        "prior_state": prior_state,
        "new_state": new_state,
        "operator": operator,
        "operator_note": str(operator_note or ""),
        "source": str(source or "CLI"),
        "review_only": True,
        "human_review_required": True,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "automatic_approval_allowed": False,
        "broker_submit_transmit_called": False,
    }
    path = candidate_reviews_path_v1(truth_root=root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")
    write_candidate_lifecycle_reports_v1(truth_root=root, day_utc=day_utc)
    return event


def append_candidate_decision_correction_v1(
    *,
    truth_root: Path,
    day_utc: str,
    candidate_id: str,
    field: str,
    new_value: Any,
    reason: str,
    operator: str,
    correction_type: str = "FIELD_CORRECTION",
) -> dict[str, Any]:
    if not candidate_id:
        raise ValueError("candidate_id is required")
    normalized_field = str(field or "").strip()
    if normalized_field not in VALID_CORRECTION_FIELDS:
        raise ValueError(f"field must be one of {sorted(VALID_CORRECTION_FIELDS)}")
    if not reason:
        raise ValueError("reason is required")
    if not operator:
        raise ValueError("operator is required")
    events = read_candidate_decisions_v1(truth_root=truth_root, day_utc=day_utc)
    current = derive_candidate_decision_states_v1(events).get(candidate_id, _empty_decision_state(candidate_id))
    old_value = _field_value(current, normalized_field)
    parsed_new = _parse_correction_value(normalized_field, new_value)
    timestamp = _now()
    prior_id = str(current.get("latest_decision_event_id") or "")
    correction_id = f"candidate-correction:{candidate_id}:{normalized_field}:{timestamp}"
    event = {
        "event_type": "CANDIDATE_DECISION_CORRECTED",
        "candidate_id": candidate_id,
        "prior_decision_event_id": prior_id,
        "correction_id": correction_id,
        "correction_type": correction_type,
        "corrected_fields": {
            normalized_field: {
                "old": old_value,
                "new": parsed_new,
            }
        },
        "reason": reason,
        "operator": operator,
        "timestamp_utc": timestamp,
        "timestamp": timestamp,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "broker_submit_transmit_called": False,
    }
    path = candidate_decisions_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")
    write_candidate_lifecycle_reports_v1(truth_root=truth_root, day_utc=day_utc)
    return event


def update_candidate_outcomes_v1(
    *,
    truth_root: Path,
    day_utc: str,
    candidate_id: str = "",
    outcome_status: str = "OUTCOME_PENDING",
    outcome_metrics: dict[str, Any] | None = None,
    outcome_window: str = "",
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    lifecycle = build_candidate_lifecycle_v1(truth_root=root, day_utc=day_utc)
    existing = read_candidate_outcomes_v1(truth_root=root, day_utc=day_utc)
    from ops.aegis.position_management_v1 import position_state_for_candidate_v1, stop_outcome_fields_v1

    by_id = {
        str(row.get("candidate_id") or ""): row
        for row in existing.get("outcomes", [])
        if isinstance(row, dict) and str(row.get("candidate_id") or "")
    }
    targets = [candidate_id] if candidate_id else [str(row.get("candidate_id") or "") for row in lifecycle.get("candidates", [])]
    status = str(outcome_status or "OUTCOME_PENDING").strip().upper()
    if status not in VALID_OUTCOMES:
        raise ValueError(f"outcome_status must be one of {sorted(VALID_OUTCOMES)}")
    for target in targets:
        if not target:
            continue
        position = position_state_for_candidate_v1(truth_root=root, day_utc=day_utc, candidate_id=target)
        stop_fields = stop_outcome_fields_v1(position) if position else {}
        existing_metrics = by_id.get(target, {}).get("outcome_metrics")
        merged_metrics = {
            **(existing_metrics if isinstance(existing_metrics, dict) else {}),
            **(outcome_metrics or {}),
            **stop_fields,
        }
        by_id[target] = {
            **by_id.get(target, {}),
            "candidate_id": target,
            "outcome_status": status if candidate_id else by_id.get(target, {}).get("outcome_status", "OUTCOME_PENDING"),
            "outcome_window": outcome_window or by_id.get(target, {}).get("outcome_window", "UNSPECIFIED"),
            "outcome_metrics": merged_metrics,
            **stop_fields,
            "updated_at": _now(),
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
        }
    payload = {
        "schema_id": "aegis_candidate_outcomes",
        "schema_version": "v1",
        "artifact_id": "candidate_outcomes_v1",
        "generated_at": _now(),
        "day_utc": day_utc,
        "outcomes": [by_id[key] for key in sorted(by_id)],
        "safety": _safety(),
    }
    write_json_v1(candidate_outcomes_path_v1(truth_root=root, day_utc=day_utc), payload)
    write_candidate_lifecycle_reports_v1(truth_root=root, day_utc=day_utc)
    return payload


def read_candidate_decisions_v1(*, truth_root: Path, day_utc: str) -> list[dict[str, Any]]:
    path = candidate_decisions_path_v1(truth_root=truth_root, day_utc=day_utc)
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def read_candidate_review_events_v1(*, truth_root: Path, day_utc: str) -> list[dict[str, Any]]:
    path = candidate_reviews_path_v1(truth_root=truth_root, day_utc=day_utc)
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def derive_candidate_decision_states_v1(events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    states: dict[str, dict[str, Any]] = {}
    for event in events:
        candidate_id = str(event.get("candidate_id") or "")
        if not candidate_id:
            continue
        state = states.setdefault(candidate_id, _empty_decision_state(candidate_id))
        event_type = str(event.get("event_type") or "CANDIDATE_DECISION_RECORDED")
        state["audit_history"].append(event)
        if event_type == "CANDIDATE_DECISION_CORRECTED":
            _apply_correction(state, event)
        else:
            _apply_decision(state, event)
    return states


def derive_candidate_review_states_v1(*, generated_candidates: list[dict[str, Any]], review_events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    states: dict[str, dict[str, Any]] = {}
    for candidate in generated_candidates:
        candidate_id = str(candidate.get("candidate_id") or "")
        if candidate_id:
            states[candidate_id] = _empty_review_state(candidate_id, candidate=candidate)
    for event in review_events:
        candidate_id = str(event.get("candidate_id") or "")
        if not candidate_id:
            continue
        state = states.setdefault(candidate_id, _empty_review_state(candidate_id))
        state["review_action_history"].append(event)
        state["review_action_history_count"] = int(state.get("review_action_history_count") or 0) + 1
        prior = str(event.get("prior_state") or state.get("review_state") or "REVIEW_REQUIRED")
        new_state = str(event.get("new_state") or prior).strip().upper()
        if new_state not in VALID_REVIEW_STATES:
            new_state = prior if prior in VALID_REVIEW_STATES else "REVIEW_REQUIRED"
        state["review_state"] = new_state
        state["operator_review_status"] = new_state
        state["latest_review_event_id"] = str(event.get("review_event_id") or "")
        state["latest_reviewed_at"] = str(event.get("timestamp_utc") or event.get("timestamp") or "")
        state["latest_review_source"] = str(event.get("source") or "")
        note = str(event.get("operator_note") or "")
        if note:
            state["latest_operator_note"] = note
    for state in states.values():
        prior_state = str(state.get("review_state") or "REVIEW_REQUIRED")
        if prior_state != "DISMISSED" and _review_expired(state):
            state["review_state"] = "EXPIRED"
            state["operator_review_status"] = "EXPIRED"
            state["review_expired"] = True
            state["expiration_reason"] = f"REVIEW_TTL_EXCEEDED_{REVIEW_TTL_HOURS}H"
            state["expired_from_review_state"] = prior_state
    return states


def read_candidate_outcomes_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    path = candidate_outcomes_path_v1(truth_root=truth_root, day_utc=day_utc)
    if not path.exists():
        return {"outcomes": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"outcomes": []}
    return payload if isinstance(payload, dict) else {"outcomes": []}


def candidate_decisions_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc / "candidate_decisions.v1.jsonl"


def candidate_reviews_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc / "candidate_reviews.v1.jsonl"


def candidate_outcomes_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc / "candidate_outcomes.v1.json"


def render_candidate_lifecycle_summary_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS CANDIDATE LIFECYCLE v1",
        f"day_utc: {payload.get('day_utc')}",
        f"candidate_count: {payload.get('candidate_count')}",
        "state_counts:",
    ]
    for key, value in sorted((payload.get("state_counts") or {}).items()):
        lines.append(f"- {key}: {value}")
    corrected = [row for row in payload.get("candidates", []) if row.get("correction_count")]
    lines.extend(
        [
            f"candidates_with_decisions: {sum(1 for row in payload.get('candidates', []) if row.get('current_operator_decision') != 'GENERATED')}",
            f"corrected_decisions: {len(corrected)}",
            f"pending_outcomes: {sum(1 for row in payload.get('candidates', []) if row.get('outcome_status') in {'OUTCOME_PENDING', 'OUTCOME_UNKNOWN'})}",
        ]
    )
    lines.extend(["", "safety:", "- advisory_only: true", "- broker_execution_allowed: false", "- autonomous_execution_allowed: false", ""])
    return "\n".join(lines)


def _generated_candidates(*, root: Path, day_utc: str) -> list[dict[str, Any]]:
    candidates = []
    for report_family, filename in (
        ("aegis_triggered_advisory_candidates_v1", "triggered_advisory_candidates.v1.json"),
        ("promoted_candidate_set_v1", "promoted_candidate_set.v1.json"),
    ):
        day_root = root / "reports" / report_family / day_utc
        if not day_root.exists():
            continue
        for path in sorted(day_root.rglob(filename)):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            for row in payload.get("candidates", []) if isinstance(payload.get("candidates"), list) else []:
                if isinstance(row, dict):
                    candidates.append({**row, "source_artifact_path": str(path), "source_artifact_hash": _sha256(path)})
    return candidates


def _candidate_record(*, candidate: dict[str, Any], decision: dict[str, Any], review: dict[str, Any], outcome: dict[str, Any]) -> dict[str, Any]:
    candidate_id = str(candidate.get("candidate_id") or decision.get("candidate_id") or "")
    operator_decision = str(decision.get("current_operator_decision") or decision.get("decision") or "GENERATED")
    outcome_status = str(outcome.get("outcome_status") or ("OUTCOME_PENDING" if operator_decision == "TRADED_MANUALLY" else "OUTCOME_UNKNOWN"))
    is_review_candidate = bool(candidate.get("review_only", False)) or bool(candidate.get("promotion_contract")) or bool(str(candidate.get("promotion_status") or "").strip())
    review_state = str(review.get("review_state") or "REVIEW_REQUIRED") if is_review_candidate else ""
    return {
        "candidate_id": candidate_id,
        "sleeve_id": str(candidate.get("sleeve_id") or "UNKNOWN"),
        "source_run_id": str(candidate.get("source_run_id") or candidate.get("trigger_id") or ""),
        "trigger_id": str(candidate.get("trigger_id") or ""),
        "generated_at": str(candidate.get("generated_at") or candidate.get("created_at") or ""),
        "symbol": str(candidate.get("symbol") or ""),
        "direction": str(candidate.get("direction") or candidate.get("side") or ""),
        "setup_type": str(candidate.get("setup_type") or candidate.get("edge_family") or ""),
        "evidence_artifacts": _list(candidate.get("evidence_artifacts")) or _list(candidate.get("source_artifact_path")),
        "evidence_hashes": _list(candidate.get("evidence_hashes")) or _list(candidate.get("source_artifact_hash")),
        "regime_context": candidate.get("regime_context") or candidate.get("regime_state") or {},
        "event_context": candidate.get("event_context") or candidate.get("trigger_id") or "",
        "ranking_score": candidate.get("ranking_score"),
        "explanation": candidate.get("explanation") or candidate.get("reason") or "",
        "promotion_status": str(candidate.get("promotion_status") or ""),
        "executable_status": str(candidate.get("executable_status") or ""),
        "governance_status": str(candidate.get("governance_status") or ""),
        "review_only": bool(candidate.get("review_only", False)),
        "human_review_required": bool(candidate.get("human_review_required", True)),
        "operator_review_status": review_state,
        "review_state": review_state,
        "review_ttl_hours": REVIEW_TTL_HOURS if is_review_candidate else 0,
        "review_expires_at": str(review.get("review_expires_at") or ""),
        "review_expired": bool(review.get("review_expired", False)),
        "expiration_reason": str(review.get("expiration_reason") or ""),
        "expired_from_review_state": str(review.get("expired_from_review_state") or ""),
        "latest_review_event_id": str(review.get("latest_review_event_id") or ""),
        "latest_reviewed_at": str(review.get("latest_reviewed_at") or ""),
        "latest_review_source": str(review.get("latest_review_source") or ""),
        "latest_operator_note": str(review.get("latest_operator_note") or ""),
        "review_action_history_count": int(review.get("review_action_history_count") or 0),
        "review_action_history": review.get("review_action_history") if isinstance(review.get("review_action_history"), list) else [],
        "raw_signal_id": str(candidate.get("raw_signal_id") or candidate.get("intent_id") or ""),
        "intent_id": str(candidate.get("intent_id") or candidate.get("raw_signal_id") or ""),
        "promotion_contract": candidate.get("promotion_contract") if isinstance(candidate.get("promotion_contract"), dict) else {},
        "operator_decision": operator_decision,
        "decision_reason": str(decision.get("current_decision_reason") or decision.get("reason") or ""),
        "manual_trade_receipt_id": str(decision.get("current_manual_trade_receipt_id") or decision.get("manual_trade_receipt_id") or ""),
        "intended_shares": decision.get("current_intended_shares"),
        "risk_bucket": str(decision.get("current_risk_bucket") or ""),
        "operator_note": str(decision.get("current_operator_note") or ""),
        "executed_confirmed": bool(decision.get("current_executed_confirmed", False)),
        "current_operator_decision": operator_decision,
        "current_intended_shares": decision.get("current_intended_shares"),
        "current_risk_bucket": str(decision.get("current_risk_bucket") or ""),
        "current_decision_reason": str(decision.get("current_decision_reason") or decision.get("reason") or ""),
        "current_operator_note": str(decision.get("current_operator_note") or ""),
        "current_manual_trade_receipt_id": str(decision.get("current_manual_trade_receipt_id") or ""),
        "current_executed_confirmed": bool(decision.get("current_executed_confirmed", False)),
        "decision_history_count": int(decision.get("decision_history_count") or 0),
        "correction_count": int(decision.get("correction_count") or 0),
        "latest_correction_id": str(decision.get("latest_correction_id") or ""),
        "audit_history": decision.get("audit_history") if isinstance(decision.get("audit_history"), list) else [],
        "outcome_status": outcome_status,
        "outcome_window": str(outcome.get("outcome_window") or ""),
        "outcome_metrics": outcome.get("outcome_metrics") if isinstance(outcome.get("outcome_metrics"), dict) else {},
        "state": _state(operator_decision=operator_decision, outcome_status=outcome_status, review_state=review_state),
        "feeds_attribution": True,
        "feeds_research": True,
        "safety": _safety(),
    }


def _state(*, operator_decision: str, outcome_status: str, review_state: str = "") -> str:
    if review_state in {"REVIEW_REQUIRED", "WATCHLISTED", "DISMISSED", "NEEDS_MORE_EVIDENCE", "EXPIRED"}:
        return review_state
    if operator_decision in {"IGNORED", "DEFERRED", "EXPIRED", "INVALIDATED"}:
        return operator_decision
    if operator_decision == "TRADED_MANUALLY":
        return outcome_status if outcome_status != "OUTCOME_UNKNOWN" else "OUTCOME_PENDING"
    return "GENERATED"


def _state_counts(candidates: list[dict[str, Any]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in candidates:
        state = str(row.get("state") or "UNKNOWN")
        out[state] = out.get(state, 0) + 1
    return out


def _default_outcomes_payload(*, day_utc: str, candidates: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_id": "aegis_candidate_outcomes",
        "schema_version": "v1",
        "artifact_id": "candidate_outcomes_v1",
        "generated_at": _now(),
        "day_utc": day_utc,
        "outcomes": [
            {
                "candidate_id": row["candidate_id"],
                "outcome_status": "OUTCOME_PENDING" if row.get("operator_decision") == "TRADED_MANUALLY" else "OUTCOME_UNKNOWN",
                "outcome_window": "UNSPECIFIED",
                "outcome_metrics": {},
            }
            for row in candidates
            if row.get("candidate_id")
        ],
        "safety": _safety(),
    }


def _empty_decision_state(candidate_id: str) -> dict[str, Any]:
    return {
        "candidate_id": candidate_id,
        "current_operator_decision": "GENERATED",
        "current_intended_shares": None,
        "current_risk_bucket": "",
        "current_decision_reason": "",
        "current_operator_note": "",
        "current_manual_trade_receipt_id": "",
        "current_executed_confirmed": False,
        "decision_history_count": 0,
        "correction_count": 0,
        "latest_correction_id": "",
        "latest_decision_event_id": "",
        "audit_history": [],
    }


def _empty_review_state(candidate_id: str, *, candidate: dict[str, Any] | None = None) -> dict[str, Any]:
    candidate = candidate or {}
    generated_at = str(candidate.get("generated_at") or candidate.get("created_at") or "")
    expires_at = _review_expires_at(generated_at)
    return {
        "candidate_id": candidate_id,
        "review_state": "REVIEW_REQUIRED",
        "operator_review_status": "REVIEW_REQUIRED",
        "review_ttl_hours": REVIEW_TTL_HOURS,
        "review_expires_at": expires_at,
        "review_expired": False,
        "expiration_reason": "",
        "expired_from_review_state": "",
        "latest_review_event_id": "",
        "latest_reviewed_at": "",
        "latest_review_source": "",
        "latest_operator_note": "",
        "review_action_history_count": 0,
        "review_action_history": [],
    }


def _review_expired(state: dict[str, Any]) -> bool:
    expires_at = _parse_time(str(state.get("review_expires_at") or ""))
    return bool(expires_at and datetime.now(UTC).replace(microsecond=0) > expires_at)


def _review_expires_at(generated_at: str) -> str:
    parsed = _parse_time(generated_at)
    if parsed is None:
        return ""
    return (parsed + timedelta(hours=REVIEW_TTL_HOURS)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_time(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _apply_decision(state: dict[str, Any], event: dict[str, Any]) -> None:
    state["current_operator_decision"] = str(event.get("decision") or state.get("current_operator_decision") or "GENERATED")
    state["current_intended_shares"] = event.get("intended_shares")
    state["current_risk_bucket"] = str(event.get("risk_bucket") or "")
    state["current_decision_reason"] = str(event.get("decision_reason") or event.get("reason") or "")
    state["current_operator_note"] = str(event.get("operator_note") or "")
    state["current_manual_trade_receipt_id"] = str(event.get("manual_trade_receipt_id") or "")
    state["current_executed_confirmed"] = bool(event.get("executed_confirmed", False))
    state["decision_history_count"] = int(state.get("decision_history_count") or 0) + 1
    state["latest_decision_event_id"] = str(event.get("decision_event_id") or "")


def _apply_correction(state: dict[str, Any], event: dict[str, Any]) -> None:
    corrected = event.get("corrected_fields") if isinstance(event.get("corrected_fields"), dict) else {}
    for field, change in corrected.items():
        if field not in VALID_CORRECTION_FIELDS or not isinstance(change, dict):
            continue
        value = change.get("new")
        if field == "decision":
            state["current_operator_decision"] = str(value or "GENERATED").upper()
        elif field == "intended_shares":
            state["current_intended_shares"] = _parse_intended_shares(value)
        elif field == "risk_bucket":
            state["current_risk_bucket"] = str(value or "").upper()
        elif field == "decision_reason":
            state["current_decision_reason"] = str(value or "")
        elif field == "operator_note":
            state["current_operator_note"] = str(value or "")
        elif field == "manual_trade_receipt_id":
            state["current_manual_trade_receipt_id"] = str(value or "")
        elif field == "executed_confirmed":
            state["current_executed_confirmed"] = _parse_bool(value)
    state["correction_count"] = int(state.get("correction_count") or 0) + 1
    state["latest_correction_id"] = str(event.get("correction_id") or "")


def _field_value(state: dict[str, Any], field: str) -> Any:
    mapping = {
        "decision": "current_operator_decision",
        "intended_shares": "current_intended_shares",
        "risk_bucket": "current_risk_bucket",
        "decision_reason": "current_decision_reason",
        "operator_note": "current_operator_note",
        "manual_trade_receipt_id": "current_manual_trade_receipt_id",
        "executed_confirmed": "current_executed_confirmed",
    }
    return state.get(mapping[field])


def _parse_correction_value(field: str, value: Any) -> Any:
    if field == "decision":
        normalized = str(value or "").strip().upper()
        if normalized not in VALID_DECISIONS and normalized != "GENERATED":
            raise ValueError(f"decision must be one of {sorted(VALID_DECISIONS)}")
        return normalized
    if field == "intended_shares":
        return _parse_intended_shares(value)
    if field == "executed_confirmed":
        return _parse_bool(value)
    if field == "risk_bucket":
        return str(value or "").strip().upper()
    return str(value or "")


def _parse_intended_shares(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError("intended_shares must be an integer") from exc
    if parsed < 0:
        raise ValueError("intended_shares must be zero or greater")
    return parsed


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    lowered = str(value).strip().lower()
    if lowered in {"true", "1", "yes", "y"}:
        return True
    if lowered in {"false", "0", "no", "n", ""}:
        return False
    raise ValueError("boolean value must be true or false")


def _list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if value:
        return [str(value)]
    return []


def _sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _safety() -> dict[str, bool]:
    return {
        "advisory_only": True,
        "human_review_required": True,
        "review_only": True,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "automatic_approval_allowed": False,
        "broker_submit_transmit_called": False,
    }


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
