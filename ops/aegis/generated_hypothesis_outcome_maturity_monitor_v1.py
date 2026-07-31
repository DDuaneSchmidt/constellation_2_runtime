from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1

FAMILY = "aegis_generated_hypothesis_outcome_maturity_monitor_v1"
FILENAME = "generated_hypothesis_outcome_maturity_monitor.v1.json"
POLICY_VERSION = "AEGIS_GENERATED_HYPOTHESIS_OUTCOME_MATURITY_MONITOR_V1"

READINESS_STATES = (
    "OUTCOME_READY",
    "HOLDING_PERIOD_NOT_ELAPSED",
    "WAITING_FOR_MARKET_DATA",
    "WAITING_FOR_CLOSE_CONDITION",
    "CLOSE_RULE_MISSING",
    "EXIT_PRICE_MISSING",
    "LINEAGE_MISMATCH",
    "UNSUPPORTED_GENERATED_HYPOTHESIS_OUTCOME",
    "UNKNOWN_DETERMINISTIC_BLOCKER",
)

SAFETY = {
    "research_only": True,
    "read_only": True,
    "monitoring_only": True,
    "no_broker_execution": True,
    "no_trade_advice": True,
    "no_live_trading": True,
    "no_real_capital": True,
    "no_autonomous_execution": True,
    "no_order_management": True,
    "no_forced_candidates": True,
    "no_candidate_mutation": True,
    "no_paper_observation_mutation": True,
    "no_outcome_mutation": True,
    "no_validation_mutation": True,
    "no_research_quality_mutation": True,
    "no_allocation_mutation": True,
    "safety_gates_changed": False,
    "candidate_created_by_this_artifact": False,
    "paper_observation_created_by_this_artifact": False,
    "outcome_created_by_this_artifact": False,
    "validation_sample_created_by_this_artifact": False,
    "research_quality_result_created_by_this_artifact": False,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
}


def generated_hypothesis_outcome_maturity_monitor_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, day_utc, FILENAME)


def build_generated_hypothesis_outcome_maturity_monitor_v1(
    *, truth_root: Path | str, day_utc: str, computed_at_utc: str | None = None
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    paths = _paths(root, day)
    payloads = {key: read_json_v1(path) for key, path in paths.items()}
    observations = _open_generated_hypothesis_observations(payloads, paths, day)
    observations.sort(key=lambda row: (row["earliest_outcome_eligible_date"], row["generated_hypothesis_id"], row["paper_position_id"]))
    summary = _portfolio_summary(observations)
    body: dict[str, Any] = {
        "schema_id": "aegis_generated_hypothesis_outcome_maturity_monitor",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "policy_version": POLICY_VERSION,
        "day_utc": day,
        "target_day": day,
        "computed_at_utc": computed_at_utc or _now(),
        "readiness_states": list(READINESS_STATES),
        "open_generated_hypothesis_observations": observations,
        "observations": observations,
        "portfolio_summary": summary,
        "summary": summary,
        "source_artifact_paths": {key: str(path) for key, path in sorted(paths.items())},
        "source_artifact_hashes": {key: file_hash_v1(path) for key, path in sorted(paths.items())},
        "validation_proof_advancement_allowed": False,
        "safety_statement": "Generated hypothesis outcome maturity monitor is read-only monitoring evidence. It creates no outcomes, validation samples, research quality results, trades, broker actions, allocations, close-rule changes, holding-period changes, or safety-gate changes.",
        **SAFETY,
        "safety": dict(SAFETY),
    }
    body["content_hash"] = stable_hash_v1(_without_time(body))
    return body


def write_generated_hypothesis_outcome_maturity_monitor_v1(
    *, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None
) -> Path:
    body = payload or build_generated_hypothesis_outcome_maturity_monitor_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(generated_hypothesis_outcome_maturity_monitor_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "paper_position_ledger": report_path_v1(root, "aegis_paper_position_ledger_v1", day, "paper_position_ledger.v1.json"),
        "paper_outcome_auto_closure": report_path_v1(root, "aegis_paper_outcome_auto_closure_v1", day, "paper_outcome_auto_closure.v1.json"),
        "exit_recommendations": report_path_v1(root, "aegis_exit_recommendations_v1", day, "exit_recommendations.v1.json"),
        "outcome_registry": report_path_v1(root, "aegis_outcome_registry_v1", day, "outcome_registry.v1.json"),
        "paper_observation_to_outcome": report_path_v1(root, "aegis_generated_hypothesis_paper_observation_to_outcome_v1", day, "generated_hypothesis_paper_observation_to_outcome.v1.json"),
        "generated_hypothesis_validation_proof": report_path_v1(root, "aegis_generated_hypothesis_validation_proof_v1", day, "generated_hypothesis_validation_proof.v1.json"),
    }


def _open_generated_hypothesis_observations(payloads: Mapping[str, Any], paths: Mapping[str, Path], day: str) -> list[dict[str, Any]]:
    ledger = _dict(payloads.get("paper_position_ledger"))
    rows = _dedupe_positions(_list(ledger.get("open_positions")) or _list(ledger.get("positions")))
    out: list[dict[str, Any]] = []
    for position in rows:
        if not isinstance(position, Mapping):
            continue
        if not _is_open(position):
            continue
        if not _looks_generated_hypothesis(position):
            continue
        out.append(_observation(position, payloads, paths, day))
    return out


def _observation(position: Mapping[str, Any], payloads: Mapping[str, Any], paths: Mapping[str, Path], day: str) -> dict[str, Any]:
    lineage = _dict(position.get("candidate_lineage"))
    candidate_id = _text(position.get("candidate_id") or lineage.get("candidate_id"))
    position_id = _text(position.get("position_id") or position.get("paper_position_id"))
    generated_hypothesis_id = _text(lineage.get("hypothesis_id") or position.get("hypothesis_id"))
    sleeve_id = _text(lineage.get("sleeve_id") or position.get("sleeve_id"))
    closure = _find_by_ids(payloads.get("paper_outcome_auto_closure"), ("rows", "closures", "manual_review_queue"), candidate_id, position_id)
    exit_rec = _find_by_ids(payloads.get("exit_recommendations"), ("recommendations", "rows", "exit_recommendations"), candidate_id, position_id)
    outcome = _find_by_ids(payloads.get("outcome_registry"), ("outcomes",), candidate_id, position_id)
    entry_datetime = _text(position.get("entry_time") or position.get("event_time_utc") or position.get("opened_at_utc") or position.get("open_time"))
    entry_day = _entry_date(entry_datetime, _text(position.get("originating_day") or outcome.get("open_day_utc") or day))
    target_day = _parse_day(day)
    age_calendar = max((target_day - entry_day).days, 0)
    minimum_holding_period = _minimum_holding_period(position, closure)
    age_trading = _trading_days_between(entry_day, target_day)
    earliest = _add_trading_days(entry_day, minimum_holding_period)
    holding_status = "ELAPSED" if age_trading >= minimum_holding_period else "NOT_ELAPSED"
    readiness = _readiness(
        position=position,
        lineage=lineage,
        closure=closure,
        exit_rec=exit_rec,
        outcome=outcome,
        holding_status=holding_status,
    )
    expected_next = _expected_next_check(target_day, earliest, readiness["outcome_readiness_status"])
    return {
        "generated_hypothesis_id": generated_hypothesis_id,
        "sleeve_id": sleeve_id,
        "candidate_contract_id": candidate_id,
        "candidate_id": candidate_id,
        "paper_observation_id": position_id,
        "paper_position_id": position_id,
        "paper_position_ledger_source": str(paths.get("paper_position_ledger") or ""),
        "entry_datetime": entry_datetime,
        "target_day": day,
        "age_in_calendar_days": age_calendar,
        "age_in_trading_days": age_trading,
        "minimum_holding_period": minimum_holding_period,
        "minimum_holding_period_unit": "TRADING_DAYS",
        "holding_period_status": holding_status,
        "close_rule_id": readiness["close_rule_id"],
        "close_rule_status": readiness["close_rule_status"],
        "mark_data_status": readiness["mark_data_status"],
        "exit_condition_status": readiness["exit_condition_status"],
        "stop_condition_status": readiness["stop_condition_status"],
        "target_condition_status": readiness["target_condition_status"],
        "outcome_readiness_status": readiness["outcome_readiness_status"],
        "earliest_outcome_eligible_date": earliest.isoformat(),
        "expected_next_check_date": expected_next.isoformat(),
        "remaining_blocker": readiness["remaining_blocker"],
        "blocker_reason": readiness["blocker_reason"],
        "owner": readiness["owner"],
        "david_action_required": readiness["david_action_required"],
        "lineage_status": readiness["lineage_status"],
        "outcome_id": _text(outcome.get("outcome_id")),
        "outcome_state": _text(outcome.get("outcome_state")),
        "exit_recommendation": _text(closure.get("exit_recommendation") or exit_rec.get("exit_recommendation")),
        "exit_trigger": _text(closure.get("exit_trigger") or outcome.get("exit_trigger")),
        "auto_closure_state": _text(closure.get("auto_closure_state") or closure.get("lifecycle_state")),
        "auto_closure_reason_codes": _reason_codes(closure),
        "source_artifact_paths": {key: str(path) for key, path in sorted(paths.items())},
        "source_artifact_hashes": {key: file_hash_v1(path) for key, path in sorted(paths.items())},
        **SAFETY,
    }


def _readiness(
    *,
    position: Mapping[str, Any],
    lineage: Mapping[str, Any],
    closure: Mapping[str, Any],
    exit_rec: Mapping[str, Any],
    outcome: Mapping[str, Any],
    holding_status: str,
) -> dict[str, Any]:
    candidate_id = _text(position.get("candidate_id") or lineage.get("candidate_id"))
    hypothesis_id = _text(lineage.get("hypothesis_id") or position.get("hypothesis_id"))
    position_id = _text(position.get("position_id") or position.get("paper_position_id"))
    closure_candidate = _text(closure.get("candidate_id"))
    closure_position = _text(closure.get("position_id") or closure.get("paper_position_id"))
    close_rule_id = _close_rule_id(closure, exit_rec)
    exit_source = _text(closure.get("exit_price_source_artifact") or outcome.get("exit_price_source_artifact") or outcome.get("closure_source"))
    exit_present = any(value not in {None, ""} for value in (closure.get("exit_mark"), closure.get("exit_price"), outcome.get("exit_mark"), outcome.get("exit_price")))
    source_has_mark_timestamp = bool(exit_source and _text(closure.get("exit_price_timestamp") or outcome.get("trigger_timestamp")))
    close_state = _upper(closure.get("auto_closure_state") or closure.get("lifecycle_state"))
    exit_rec_value = _upper(closure.get("exit_recommendation") or exit_rec.get("exit_recommendation"))
    exit_trigger = _upper(closure.get("exit_trigger") or outcome.get("exit_trigger"))
    reasons = set(_reason_codes(closure)) | set(_reason_codes(exit_rec)) | set(_reason_codes(outcome))

    base = {
        "close_rule_id": close_rule_id,
        "close_rule_status": "PRESENT" if close_rule_id else "MISSING",
        "mark_data_status": "PRESENT" if exit_present else ("SOURCE_PRESENT_PRICE_NOT_ACTIONABLE" if source_has_mark_timestamp else "MISSING"),
        "exit_condition_status": "NOT_MET",
        "stop_condition_status": _threshold_status(exit_trigger, "STOP"),
        "target_condition_status": _threshold_status(exit_trigger, "TARGET", "TAKE_PROFIT"),
        "lineage_status": "MATCHED",
        "owner": "AEGIS_SYSTEM",
        "david_action_required": False,
    }

    if not candidate_id or not hypothesis_id or not position_id:
        return {**base, "lineage_status": "MISMATCH", "outcome_readiness_status": "LINEAGE_MISMATCH", "remaining_blocker": "LINEAGE_MISMATCH", "blocker_reason": "Open paper observation is missing generated-hypothesis, candidate, or paper-position lineage required for outcome monitoring."}
    if closure and ((closure_candidate and closure_candidate != candidate_id) or (closure_position and closure_position != position_id)):
        return {**base, "lineage_status": "MISMATCH", "outcome_readiness_status": "LINEAGE_MISMATCH", "remaining_blocker": "LINEAGE_MISMATCH", "blocker_reason": "Paper outcome auto-closure row does not match the generated-hypothesis paper observation lineage."}
    if not close_rule_id or "EXIT_EVALUATION_MISSING" in reasons:
        return {**base, "outcome_readiness_status": "CLOSE_RULE_MISSING", "remaining_blocker": "CLOSE_RULE_MISSING", "blocker_reason": "No deterministic close rule or exit recommendation row exists for this generated-hypothesis paper observation."}
    if holding_status != "ELAPSED":
        return {**base, "outcome_readiness_status": "HOLDING_PERIOD_NOT_ELAPSED", "remaining_blocker": "HOLDING_PERIOD_NOT_ELAPSED", "blocker_reason": "Generated-hypothesis paper observation has not reached the deterministic minimum holding period for outcome auto-closure evaluation."}
    if not exit_source and not exit_present:
        return {**base, "outcome_readiness_status": "WAITING_FOR_MARKET_DATA", "remaining_blocker": "WAITING_FOR_MARKET_DATA", "blocker_reason": "No certified market-data source is present for deterministic outcome readiness evaluation."}
    if ("MISSING_EXIT_MARK" in reasons or "EXIT_MARK_TIMESTAMP_MISSING" in reasons) and not exit_present:
        return {**base, "outcome_readiness_status": "EXIT_PRICE_MISSING", "remaining_blocker": "EXIT_PRICE_MISSING", "blocker_reason": "A deterministic close condition requires an actionable certified exit price, but no exit mark is present."}
    close_triggered = exit_rec_value in {"EXIT", "CLOSE", "SELL"} or (exit_trigger and exit_trigger not in {"NO_EXIT_RULE_TRIGGERED", "HOLD", "NONE"}) or close_state in {"AUTO_CLOSURE_READY", "AUTO_CLOSED_PAPER_OUTCOME"}
    if not close_triggered or exit_rec_value == "HOLD" or "HOLD_RECOMMENDATION" in reasons:
        return {**base, "outcome_readiness_status": "WAITING_FOR_CLOSE_CONDITION", "remaining_blocker": "WAITING_FOR_CLOSE_CONDITION", "blocker_reason": "The observation is mature, but deterministic exit policy has not produced a stop, target, or other close condition."}
    if close_triggered and not exit_present:
        return {**base, "exit_condition_status": "MET", "outcome_readiness_status": "EXIT_PRICE_MISSING", "remaining_blocker": "EXIT_PRICE_MISSING", "blocker_reason": "A deterministic close condition is present, but the certified exit price is missing."}
    return {**base, "exit_condition_status": "MET", "outcome_readiness_status": "OUTCOME_READY", "remaining_blocker": "NONE", "blocker_reason": "Generated-hypothesis paper observation is mature and all deterministic close inputs are present.", "owner": "NONE"}


def _portfolio_summary(observations: list[Mapping[str, Any]]) -> dict[str, Any]:
    counts = {state: 0 for state in READINESS_STATES}
    for row in observations:
        status = _text(row.get("outcome_readiness_status"))
        if status in counts:
            counts[status] += 1
        else:
            counts["UNKNOWN_DETERMINISTIC_BLOCKER"] += 1
    next_dates = sorted(_text(row.get("expected_next_check_date")) for row in observations if _text(row.get("expected_next_check_date")))
    return {
        "total_open_generated_hypothesis_observations": len(observations),
        "outcome_ready_count": counts["OUTCOME_READY"],
        "holding_period_not_elapsed_count": counts["HOLDING_PERIOD_NOT_ELAPSED"],
        "waiting_for_market_data_count": counts["WAITING_FOR_MARKET_DATA"],
        "waiting_for_close_condition_count": counts["WAITING_FOR_CLOSE_CONDITION"],
        "close_rule_missing_count": counts["CLOSE_RULE_MISSING"],
        "exit_price_missing_count": counts["EXIT_PRICE_MISSING"],
        "lineage_mismatch_count": counts["LINEAGE_MISMATCH"],
        "unsupported_count": counts["UNSUPPORTED_GENERATED_HYPOTHESIS_OUTCOME"],
        "unknown_blocker_count": counts["UNKNOWN_DETERMINISTIC_BLOCKER"],
        "next_expected_outcome_check_date": next_dates[0] if next_dates else "",
    }


def _is_open(row: Mapping[str, Any]) -> bool:
    state = _upper(row.get("current_state") or row.get("position_state") or row.get("state"))
    if state in {"CLOSED", "INVALIDATED", "RESOLVED"}:
        return False
    if _text(row.get("exit_time") or row.get("closed_at_utc")):
        return False
    return True


def _looks_generated_hypothesis(row: Mapping[str, Any]) -> bool:
    lineage = _dict(row.get("candidate_lineage"))
    hypothesis_id = _text(lineage.get("hypothesis_id") or row.get("hypothesis_id"))
    if hypothesis_id.startswith("ehp_"):
        return True
    # Include malformed auto-promoted rows only to emit LINEAGE_MISMATCH instead of silently dropping them.
    return _upper(row.get("paper_tracking_mode")) == "AUTO_PROMOTED_RESEARCH_OBSERVATION" and not hypothesis_id


def _dedupe_positions(rows: list[Any]) -> list[Mapping[str, Any]]:
    seen: set[str] = set()
    out: list[Mapping[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        key = _text(row.get("position_id") or row.get("paper_position_id") or row.get("candidate_id"))
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        out.append(row)
    return out


def _find_by_ids(payload: Any, keys: tuple[str, ...], candidate_id: str, position_id: str) -> dict[str, Any]:
    payload = _dict(payload)
    for key in keys:
        for row in _list(payload.get(key)):
            if not isinstance(row, Mapping):
                continue
            if (candidate_id and _text(row.get("candidate_id") or row.get("candidate_contract_id")) == candidate_id) or (position_id and _text(row.get("position_id") or row.get("paper_position_id")) == position_id):
                return dict(row)
    return {}


def _close_rule_id(closure: Mapping[str, Any], exit_rec: Mapping[str, Any]) -> str:
    explicit = _text(closure.get("close_rule_id") or closure.get("exit_rule_id") or exit_rec.get("close_rule_id") or exit_rec.get("exit_rule_id"))
    if explicit:
        return explicit
    if closure or exit_rec:
        return _text(closure.get("deterministic_return_formula_version") or "AUTO_CLOSURE_EXIT_RECOMMENDATION_V1")
    return ""


def _minimum_holding_period(position: Mapping[str, Any], closure: Mapping[str, Any]) -> int:
    for key in ("minimum_holding_period", "minimum_holding_period_days", "minimum_holding_period_trading_days"):
        value = position.get(key) or closure.get(key)
        try:
            if value not in {None, ""}:
                return max(int(value), 0)
        except (TypeError, ValueError):
            pass
    if _upper(position.get("paper_tracking_mode")) == "AUTO_PROMOTED_RESEARCH_OBSERVATION" or "SAME_DAY_AUTO_PROMOTED_RESEARCH_OBSERVATION" in set(_reason_codes(closure)):
        return 1
    return 0


def _entry_date(entry_datetime: str, fallback_day: str) -> date:
    if entry_datetime:
        try:
            return datetime.fromisoformat(entry_datetime.replace("Z", "+00:00")).date()
        except ValueError:
            pass
    return _parse_day(fallback_day)


def _parse_day(value: str) -> date:
    return date.fromisoformat(value[:10])


def _trading_days_between(start: date, end: date) -> int:
    if end <= start:
        return 0
    days = 0
    cursor = start + timedelta(days=1)
    while cursor <= end:
        if cursor.weekday() < 5:
            days += 1
        cursor += timedelta(days=1)
    return days


def _add_trading_days(start: date, trading_days: int) -> date:
    if trading_days <= 0:
        return start
    cursor = start
    remaining = trading_days
    while remaining:
        cursor += timedelta(days=1)
        if cursor.weekday() < 5:
            remaining -= 1
    return cursor


def _next_trading_day(day: date) -> date:
    cursor = day + timedelta(days=1)
    while cursor.weekday() >= 5:
        cursor += timedelta(days=1)
    return cursor


def _expected_next_check(target_day: date, earliest: date, status: str) -> date:
    if status == "OUTCOME_READY":
        return target_day
    if target_day < earliest:
        return earliest
    return _next_trading_day(target_day)


def _threshold_status(trigger: str, *needles: str) -> str:
    if not trigger or trigger in {"NO_EXIT_RULE_TRIGGERED", "HOLD", "NONE"}:
        return "NOT_TRIGGERED"
    return "TRIGGERED" if any(needle in trigger for needle in needles) else "NOT_TRIGGERED"


def _reason_codes(row: Mapping[str, Any]) -> list[str]:
    codes: list[str] = []
    for key in ("reason_codes", "blocker_reason_codes", "auto_promotion_reason_codes", "auto_closure_reason_codes", "blocker_codes", "blocker_code"):
        value = row.get(key)
        if isinstance(value, list):
            codes.extend(str(item) for item in value if _text(item))
        elif _text(value):
            codes.append(_text(value))
    return [code for code in codes if code]


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _text(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _text(value).upper()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _without_time(payload: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(payload)
    out.pop("computed_at_utc", None)
    out.pop("content_hash", None)
    return out
