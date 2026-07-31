from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.future_target_day_audit_guard_v1 import read_future_target_day_audit_guard_v1
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1

FAMILY = "aegis_generated_hypothesis_outcome_eligibility_day_proof_v1"
FILENAME = "generated_hypothesis_outcome_eligibility_day_proof.v1.json"
POLICY_VERSION = "AEGIS_GENERATED_HYPOTHESIS_OUTCOME_ELIGIBILITY_DAY_PROOF_V1"
OIL_HYPOTHESIS_ID = "ehp_cdbd8fe683acb622"
OIL_CANDIDATE_ID = "candidate_contract_a3dd44d21952f8098131aa04"
OIL_SLEEVE_ID = "C2_OIL_SHOCK_REVERSAL_V1"

SAFETY = {
    "research_only": True,
    "read_only": True,
    "targeted_eligibility_day_proof": True,
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


def generated_hypothesis_outcome_eligibility_day_proof_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, day_utc, FILENAME)


def build_generated_hypothesis_outcome_eligibility_day_proof_v1(
    *, truth_root: Path | str, day_utc: str, computed_at_utc: str | None = None
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    paths = _paths(root, day)
    payloads = {key: read_json_v1(path) for key, path in paths.items()}
    proof = _proof(payloads, paths, day, computed_at_utc or _now())
    body: dict[str, Any] = {
        "schema_id": "aegis_generated_hypothesis_outcome_eligibility_day_proof",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "policy_version": POLICY_VERSION,
        "day_utc": day,
        "target_day": day,
        "computed_at_utc": proof["computed_at_utc"],
        "oil_shock": proof,
        "summary": {key: proof[key] for key in (
            "generated_hypothesis_id", "sleeve_id", "candidate_contract_id", "paper_observation_id",
            "paper_position_id", "original_entry_date", "target_day", "age_in_calendar_days",
            "age_in_trading_days", "minimum_holding_period", "holding_period_status",
            "mark_data_status", "exit_price_present", "exit_price_source", "close_rule_status",
            "close_condition_status", "outcome_readiness_status", "outcome_status", "outcome_id",
            "outcome_row_created", "furthest_stage_reached", "remaining_blocker", "blocker_code",
            "blocker_reason", "missing_fields", "required_inputs", "owner", "david_action_required"
        )},
        "source_artifact_paths": {key: str(path) for key, path in sorted(paths.items())},
        "source_artifact_hashes": {key: file_hash_v1(path) for key, path in sorted(paths.items())},
        "safety_statement": "Generated hypothesis outcome eligibility-day proof is read-only. It creates no outcomes, validation samples, research quality results, trades, broker actions, allocation changes, close-rule changes, holding-period changes, or safety-gate changes.",
        **SAFETY,
        "safety": dict(SAFETY),
    }
    body["content_hash"] = stable_hash_v1(_without_time(body))
    return body


def write_generated_hypothesis_outcome_eligibility_day_proof_v1(
    *, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None
) -> Path:
    body = payload or build_generated_hypothesis_outcome_eligibility_day_proof_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(generated_hypothesis_outcome_eligibility_day_proof_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "paper_position_ledger": report_path_v1(root, "aegis_paper_position_ledger_v1", day, "paper_position_ledger.v1.json"),
        "paper_outcome_auto_closure": report_path_v1(root, "aegis_paper_outcome_auto_closure_v1", day, "paper_outcome_auto_closure.v1.json"),
        "exit_recommendations": report_path_v1(root, "aegis_exit_recommendations_v1", day, "exit_recommendations.v1.json"),
        "outcome_registry": report_path_v1(root, "aegis_outcome_registry_v1", day, "outcome_registry.v1.json"),
        "paper_observation_to_outcome": report_path_v1(root, "aegis_generated_hypothesis_paper_observation_to_outcome_v1", day, "generated_hypothesis_paper_observation_to_outcome.v1.json"),
        "outcome_maturity_monitor": report_path_v1(root, "aegis_generated_hypothesis_outcome_maturity_monitor_v1", day, "generated_hypothesis_outcome_maturity_monitor.v1.json"),
        "generated_hypothesis_validation_proof": report_path_v1(root, "aegis_generated_hypothesis_validation_proof_v1", day, "generated_hypothesis_validation_proof.v1.json"),
        "future_target_day_guard": report_path_v1(root, "aegis_future_target_day_audit_guard_v1", day, "future_target_day_audit_guard.v1.json"),
    }


def _proof(payloads: Mapping[str, Any], paths: Mapping[str, Path], day: str, computed_at: str) -> dict[str, Any]:
    position = _find_position(payloads.get("paper_position_ledger"))
    closure = _find_by_ids(payloads.get("paper_outcome_auto_closure"), ("rows", "closures", "manual_review_queue"), OIL_CANDIDATE_ID, _text(position.get("position_id")))
    exit_rec = _find_by_ids(payloads.get("exit_recommendations"), ("recommendations", "rows", "exit_recommendations"), OIL_CANDIDATE_ID, _text(position.get("position_id")))
    outcome = _find_by_ids(payloads.get("outcome_registry"), ("outcomes",), OIL_CANDIDATE_ID, _text(position.get("position_id")))
    p18 = _dict(_dict(payloads.get("paper_observation_to_outcome")).get("summary"))
    guard = _dict(payloads.get("future_target_day_guard"))
    future_guarded = guard.get("guard_status") == "TARGET_DAY_IN_FUTURE_GUARDED" and guard.get("is_future_target_day") is True

    lineage = _dict(position.get("candidate_lineage"))
    entry_datetime = _text(position.get("entry_time") or position.get("event_time_utc"))
    entry_day = _entry_date(entry_datetime, _text(position.get("originating_day") or outcome.get("open_day_utc") or day))
    target_day = _parse_day(day)
    minimum_holding_period = _minimum_holding_period(position, closure)
    age_calendar = max((target_day - entry_day).days, 0)
    age_trading = _trading_days_between(entry_day, target_day)
    holding_status = "ELAPSED" if age_trading >= minimum_holding_period else "NOT_ELAPSED"

    exit_price_present = any(value not in {None, ""} for value in (closure.get("exit_mark"), closure.get("exit_price"), outcome.get("exit_mark"), outcome.get("exit_price")))
    exit_price_source = _text(closure.get("exit_price_source_artifact") or outcome.get("exit_price_source_artifact") or p18.get("exit_price_source"))
    mark_data_status = _mark_data_status(closure, outcome, exit_price_present, exit_price_source)
    close_rule_status = "PRESENT" if closure or exit_rec else "MISSING"
    close_condition_status = _close_condition_status(closure, exit_rec, outcome)
    outcome_created = _outcome_created(outcome) or bool(p18.get("outcome_row_created") is True and _text(p18.get("outcome_status")) == "OUTCOME_CREATED")

    blocker_code, blocker_reason, missing_fields, required_inputs, owner = _blocker(
        position=position,
        closure=closure,
        exit_rec=exit_rec,
        outcome=outcome,
        holding_status=holding_status,
        mark_data_status=mark_data_status,
        close_rule_status=close_rule_status,
        close_condition_status=close_condition_status,
        outcome_created=outcome_created,
        future_guarded=future_guarded,
    )
    remaining = "NONE" if blocker_code == "NONE" else blocker_code
    readiness = "READY" if outcome_created else "NOT_READY"
    return {
        "generated_hypothesis_id": _text(lineage.get("hypothesis_id") or position.get("hypothesis_id") or p18.get("generated_hypothesis_id") or OIL_HYPOTHESIS_ID),
        "sleeve_id": _text(lineage.get("sleeve_id") or position.get("sleeve_id") or closure.get("sleeve_id") or p18.get("sleeve_id") or OIL_SLEEVE_ID),
        "candidate_contract_id": _text(position.get("candidate_id") or p18.get("candidate_contract_id") or OIL_CANDIDATE_ID),
        "candidate_id": _text(position.get("candidate_id") or OIL_CANDIDATE_ID),
        "paper_observation_id": _text(position.get("position_id") or p18.get("paper_observation_id")),
        "paper_position_id": _text(position.get("position_id") or p18.get("paper_position_id")),
        "paper_position_ledger_source": str(paths.get("paper_position_ledger") or "") if position else "",
        "original_entry_datetime": entry_datetime,
        "original_entry_date": entry_day.isoformat(),
        "target_day": day,
        "age_in_calendar_days": age_calendar,
        "age_in_trading_days": age_trading,
        "minimum_holding_period": minimum_holding_period,
        "minimum_holding_period_unit": "TRADING_DAYS",
        "holding_period_status": holding_status,
        "mark_data_status": mark_data_status,
        "exit_price_present": exit_price_present,
        "exit_price_source": exit_price_source,
        "close_rule_status": close_rule_status,
        "close_condition_status": close_condition_status,
        "close_condition_reason": _text(closure.get("exit_trigger") or p18.get("close_condition_reason")),
        "outcome_readiness_status": readiness,
        "outcome_status": "OUTCOME_CREATED" if outcome_created else "OUTCOME_NOT_READY",
        "outcome_id": _text(outcome.get("outcome_id") or p18.get("outcome_id")),
        "outcome_row_created": outcome_created,
        "furthest_stage_reached": "OUTCOME_FLOW" if outcome_created else ("PAPER_OBSERVATION_FLOW" if position else "PAPER_OBSERVATION_NOT_FOUND"),
        "remaining_blocker": remaining,
        "blocker_code": blocker_code,
        "blocker_reason": blocker_reason,
        "missing_fields": missing_fields,
        "required_inputs": required_inputs,
        "owner": owner,
        "david_action_required": owner == "DAVID",
        "paper_outcome_auto_closure_state": _text(closure.get("auto_closure_state") or closure.get("lifecycle_state")),
        "paper_outcome_auto_closure_reason_codes": _reason_codes(closure),
        "exit_recommendation": _text(closure.get("exit_recommendation") or exit_rec.get("exit_recommendation")),
        "exit_trigger": _text(closure.get("exit_trigger") or outcome.get("exit_trigger")),
        "validation_proof_advancement_allowed": outcome_created and not future_guarded,
        "future_target_day_guarded": future_guarded,
        "future_target_day_guard_status": _text(guard.get("guard_status")),
        "computed_at_utc": computed_at,
        **SAFETY,
    }


def _blocker(*, position: Mapping[str, Any], closure: Mapping[str, Any], exit_rec: Mapping[str, Any], outcome: Mapping[str, Any], holding_status: str, mark_data_status: str, close_rule_status: str, close_condition_status: str, outcome_created: bool, future_guarded: bool = False) -> tuple[str, str, list[str], list[str], str]:
    if future_guarded and not outcome_created:
        return "TARGET_DAY_IN_FUTURE", "Target day is later than actual runtime date; final mark certification and outcome creation are blocked until target-day data is available.", ["target_day_final_mark"], ["actual runtime date reaches target day", "certified target-day final mark"], "AEGIS_SYSTEM"
    if not position:
        return "OUTCOME_BUILDER_LINEAGE_MISMATCH", "No Oil Shock generated-hypothesis paper observation exists in the target-day paper position ledger.", ["paper_position_id"], ["aegis_paper_position_ledger_v1 Oil Shock row"], "AEGIS_SYSTEM"
    if outcome_created:
        return "NONE", "Authoritative generated-hypothesis outcome row exists for the eligibility day.", [], [], "NONE"
    if holding_status != "ELAPSED":
        return "HOLDING_PERIOD_NOT_ELAPSED", "Oil Shock paper observation has not reached the minimum deterministic holding period on the target day.", [], ["next eligible trading day evaluation"], "AEGIS_SYSTEM"
    if close_rule_status == "MISSING":
        return "CLOSE_RULE_MISSING", "No deterministic close rule or exit recommendation row exists for the eligibility-day evaluation.", ["exit_recommendation"], ["aegis_exit_recommendations_v1 row", "aegis_paper_outcome_auto_closure_v1 row"], "AEGIS_SYSTEM"
    if mark_data_status == "MISSING":
        return "MARK_DATA_MISSING", "Oil Shock is holding-period eligible, but deterministic outcome closure lacks a certified current mark for the target day.", ["current_mark"], ["certified current mark", "mark timestamp", "mark source hash"], "AEGIS_SYSTEM"
    if mark_data_status == "EXIT_PRICE_MISSING":
        return "EXIT_PRICE_MISSING", "A deterministic close condition is present, but the certified exit price is missing.", ["exit_price"], ["certified actionable exit price"], "AEGIS_SYSTEM"
    if close_condition_status != "MET":
        return "CLOSE_CONDITION_NOT_MET", "Oil Shock is holding-period eligible, but the deterministic exit recommendation is HOLD and no stop, target, or other close condition triggered.", [], ["non-HOLD deterministic exit recommendation"], "AEGIS_SYSTEM"
    return "UNKNOWN_DETERMINISTIC_BLOCKER", "Oil Shock eligibility-day outcome readiness could not be classified from deterministic closure evidence.", [], ["matched closure, exit recommendation, and outcome registry evidence"], "AEGIS_SYSTEM"


def _find_position(payload: Any) -> dict[str, Any]:
    payload = _dict(payload)
    for key in ("open_positions", "positions", "historical_positions", "closed_positions"):
        for row in _list(payload.get(key)):
            if not isinstance(row, Mapping):
                continue
            lineage = _dict(row.get("candidate_lineage"))
            if _text(row.get("candidate_id")) == OIL_CANDIDATE_ID or _text(lineage.get("hypothesis_id") or row.get("hypothesis_id")) == OIL_HYPOTHESIS_ID:
                return dict(row)
    return {}


def _find_by_ids(payload: Any, keys: tuple[str, ...], candidate_id: str, position_id: str) -> dict[str, Any]:
    payload = _dict(payload)
    for key in keys:
        for row in _list(payload.get(key)):
            if not isinstance(row, Mapping):
                continue
            if (candidate_id and _text(row.get("candidate_id") or row.get("candidate_contract_id")) == candidate_id) or (position_id and _text(row.get("position_id") or row.get("paper_position_id")) == position_id):
                return dict(row)
    return {}


def _mark_data_status(closure: Mapping[str, Any], outcome: Mapping[str, Any], exit_price_present: bool, exit_source: str) -> str:
    if exit_price_present:
        return "PRESENT"
    trigger = _upper(closure.get("exit_trigger") or outcome.get("exit_trigger"))
    cert = _upper(closure.get("exit_mark_certification_status"))
    close_triggered = trigger and trigger not in {"HOLD", "NO_EXIT_RULE_TRIGGERED", "MISSING_CURRENT_MARK", "MISSING_EXIT_EVALUATION", "NONE"}
    if close_triggered and (exit_source or _text(closure.get("exit_price_timestamp") or outcome.get("trigger_timestamp"))):
        return "EXIT_PRICE_MISSING"
    if trigger == "MISSING_CURRENT_MARK" or cert in {"MISSING_MARK", "MISSING"} or not _text(closure.get("exit_price_timestamp") or outcome.get("trigger_timestamp")):
        return "MISSING"
    if exit_source:
        return "SOURCE_PRESENT_PRICE_NOT_ACTIONABLE"
    return "MISSING"


def _close_condition_status(closure: Mapping[str, Any], exit_rec: Mapping[str, Any], outcome: Mapping[str, Any]) -> str:
    if _outcome_created(outcome):
        return "MET"
    rec = _upper(closure.get("exit_recommendation") or exit_rec.get("exit_recommendation"))
    trigger = _upper(closure.get("exit_trigger") or outcome.get("exit_trigger"))
    if rec in {"EXIT", "CLOSE", "SELL", "EXIT_STOP_LOSS", "EXIT_TAKE_PROFIT", "EXIT_TRAILING_STOP", "EXIT_TIME_STOP", "EXIT_SIGNAL_INVALIDATED", "EXIT_REGIME_INVALIDATED"}:
        return "MET"
    if trigger and trigger not in {"HOLD", "NO_EXIT_RULE_TRIGGERED", "MISSING_CURRENT_MARK", "MISSING_EXIT_EVALUATION", "NONE"}:
        return "MET"
    return "NOT_MET"


def _outcome_created(outcome: Mapping[str, Any]) -> bool:
    state = _upper(outcome.get("outcome_state"))
    return bool(state and state not in {"OPEN", "UNKNOWN_BLOCKED"})


def _minimum_holding_period(position: Mapping[str, Any], closure: Mapping[str, Any]) -> int:
    for key in ("minimum_holding_period", "minimum_holding_period_days", "minimum_holding_period_trading_days"):
        value = position.get(key) or closure.get(key)
        try:
            if value not in {None, ""}:
                return max(int(value), 0)
        except (TypeError, ValueError):
            pass
    if _upper(position.get("paper_tracking_mode")) == "AUTO_PROMOTED_RESEARCH_OBSERVATION":
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


def _reason_codes(row: Mapping[str, Any]) -> list[str]:
    codes: list[str] = []
    for key in ("reason_codes", "blocker_reason_codes", "auto_closure_reason_codes", "blocker_codes", "blocker_code"):
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
    oil = dict(out.get("oil_shock") or {})
    oil.pop("computed_at_utc", None)
    out["oil_shock"] = oil
    return out
