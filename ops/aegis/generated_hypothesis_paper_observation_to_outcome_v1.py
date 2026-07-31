from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1

FAMILY = "aegis_generated_hypothesis_paper_observation_to_outcome_v1"
FILENAME = "generated_hypothesis_paper_observation_to_outcome.v1.json"
POLICY_VERSION = "AEGIS_GENERATED_HYPOTHESIS_PAPER_OBSERVATION_TO_OUTCOME_PROOF_V1"
OIL_HYPOTHESIS_ID = "ehp_cdbd8fe683acb622"
OIL_NAME = "Oil shock reversals across energy ETFs"
OIL_SIGNAL_ID = "c2_oil_shock_reversal_uso_2026-06-02_v1"
OIL_CANDIDATE_ID = "candidate_contract_a3dd44d21952f8098131aa04"

SAFETY = {
    "research_only": True,
    "read_only": True,
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
    "no_allocation_mutation": True,
    "safety_gates_changed": False,
    "candidate_created_by_this_artifact": False,
    "paper_observation_created_by_this_artifact": False,
    "outcome_created_by_this_artifact": False,
    "validation_sample_created_by_this_artifact": False,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
}


def generated_hypothesis_paper_observation_to_outcome_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, day_utc, FILENAME)


def build_generated_hypothesis_paper_observation_to_outcome_v1(
    *, truth_root: Path | str, day_utc: str, computed_at_utc: str | None = None
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    paths = _paths(root, str(day_utc))
    payloads = {key: read_json_v1(path) for key, path in paths.items()}
    row = _trace(payloads, paths, str(day_utc), computed_at_utc or _now())
    body: dict[str, Any] = {
        "schema_id": "aegis_generated_hypothesis_paper_observation_to_outcome",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "policy_version": POLICY_VERSION,
        "day_utc": str(day_utc),
        "target_day": str(day_utc),
        "computed_at_utc": row["computed_at_utc"],
        "oil_shock": row,
        "summary": {
            "generated_hypothesis_id": row["generated_hypothesis_id"],
            "sleeve_id": row["sleeve_id"],
            "candidate_contract_id": row["candidate_contract_id"],
            "paper_observation_found": row["paper_observation_found"],
            "paper_observation_id": row["paper_observation_id"],
            "paper_position_id": row["paper_position_id"],
            "paper_position_ledger_source": row["paper_position_ledger_source"],
            "outcome_readiness_status": row["outcome_readiness_status"],
            "outcome_status": row["outcome_status"],
            "outcome_id": row["outcome_id"],
            "outcome_row_created": row["outcome_row_created"],
            "exit_price_present": row["exit_price_present"],
            "exit_price_source": row["exit_price_source"],
            "close_condition_status": row["close_condition_status"],
            "close_condition_reason": row["close_condition_reason"],
            "holding_period_status": row["holding_period_status"],
            "mark_data_status": row["mark_data_status"],
            "furthest_stage_reached": row["furthest_stage_reached"],
            "remaining_blocker": row["remaining_blocker"],
            "blocker_code": row["blocker_code"],
            "blocker_reason": row["blocker_reason"],
            "missing_fields": row["missing_fields"],
            "required_inputs": row["required_inputs"],
            "owner": row["owner"],
            "david_action_required": row["david_action_required"],
        },
        "source_artifact_paths": {key: str(path) for key, path in sorted(paths.items())},
        "source_artifact_hashes": {key: file_hash_v1(path) for key, path in sorted(paths.items())},
        "safety_statement": "Generated hypothesis paper observation-to-outcome proof is read-only. It creates no candidates, paper observations, outcomes, validation samples, trades, broker actions, allocations, or safety-gate changes.",
        **SAFETY,
        "safety": dict(SAFETY),
    }
    body["content_hash"] = stable_hash_v1(_without_time(body))
    return body


def write_generated_hypothesis_paper_observation_to_outcome_v1(
    *, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None
) -> Path:
    body = payload or build_generated_hypothesis_paper_observation_to_outcome_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(generated_hypothesis_paper_observation_to_outcome_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "candidate_to_paper_proof": report_path_v1(root, "aegis_generated_hypothesis_candidate_to_paper_v1", day, "generated_hypothesis_candidate_to_paper.v1.json"),
        "candidate_lifecycle": report_path_v1(root, "aegis_candidate_to_paper_lifecycle_v1", day, "candidate_to_paper_lifecycle.v1.json"),
        "paper_position_ledger": report_path_v1(root, "aegis_paper_position_ledger_v1", day, "paper_position_ledger.v1.json"),
        "paper_outcome_auto_closure": report_path_v1(root, "aegis_paper_outcome_auto_closure_v1", day, "paper_outcome_auto_closure.v1.json"),
        "exit_recommendations": report_path_v1(root, "aegis_exit_recommendations_v1", day, "exit_recommendations.v1.json"),
        "outcome_registry": report_path_v1(root, "aegis_outcome_registry_v1", day, "outcome_registry.v1.json"),
        "validation_samples": report_path_v1(root, "aegis_validation_samples_v1", day, "validation_samples.v1.json"),
        "outcome_performance_metrics": report_path_v1(root, "aegis_outcome_performance_metrics_v1", day, "outcome_performance_metrics.v1.json"),
        "research_quality_engine": report_path_v1(root, "aegis_research_quality_engine_v1", day, "research_quality_engine.v1.json"),
    }


def _trace(payloads: Mapping[str, Any], paths: Mapping[str, Path], day: str, computed_at: str) -> dict[str, Any]:
    prior = _oil_row(payloads.get("candidate_to_paper_proof"))
    candidate_id = _text(prior.get("candidate_id") or OIL_CANDIDATE_ID)
    raw_signal_id = _text(prior.get("raw_signal_id") or OIL_SIGNAL_ID)
    lifecycle = _find_lifecycle(payloads.get("candidate_lifecycle"), candidate_id, raw_signal_id)
    paper_position_id = _text(prior.get("paper_position_id") or lifecycle.get("paper_position_id"))
    position = _find_position(payloads.get("paper_position_ledger"), candidate_id, paper_position_id)
    if position and not paper_position_id:
        paper_position_id = _text(position.get("position_id"))
    outcome = _find_outcome(payloads.get("outcome_registry"), candidate_id, paper_position_id, _text(prior.get("outcome_id") or lifecycle.get("outcome_id")))
    closure = _find_auto_closure(payloads.get("paper_outcome_auto_closure"), candidate_id, paper_position_id)
    exit_recommendation = _find_exit_recommendation(payloads.get("exit_recommendations"), candidate_id, paper_position_id)

    paper_observation_found = bool(position and paper_position_id)
    paper_observation_id = paper_position_id if paper_observation_found else ""
    outcome_created = _outcome_advancement_created(outcome)
    readiness = _readiness(position=position, outcome=outcome, closure=closure, exit_recommendation=exit_recommendation, outcome_created=outcome_created)
    sample = _find_sample(payloads.get("validation_samples"), candidate_id, paper_position_id, _text(outcome.get("outcome_id"))) if outcome_created else {}
    quality = _find_quality(payloads.get("research_quality_engine")) if outcome_created else {}
    performance = _find_performance(payloads.get("outcome_performance_metrics")) if outcome_created else {}
    sample_status = _validation_status(sample, outcome_created)
    quality_consumed, quality_reason = _research_quality_consumption(quality, performance, outcome_created, sample_status)

    blocker_code, blocker_reason, missing_fields, required_inputs, owner = _blocker(
        prior=prior,
        lifecycle=lifecycle,
        position=position,
        outcome=outcome,
        closure=closure,
        readiness=readiness,
        outcome_created=outcome_created,
    )
    remaining = "NONE" if blocker_code == "NONE" else blocker_code
    furthest = "OUTCOME_FLOW" if outcome_created else ("PAPER_OBSERVATION_FLOW" if paper_observation_found else "CANDIDATE_TO_PAPER_BLOCKED")
    source_paths = {key: str(path) for key, path in sorted(paths.items())}
    source_hashes = {key: file_hash_v1(path) for key, path in sorted(paths.items())}
    reason_codes = sorted({code for code in _reason_codes(prior) + _reason_codes(lifecycle) + _reason_codes(outcome) + _reason_codes(closure) + _reason_codes(exit_recommendation) + ([blocker_code] if blocker_code != "NONE" else []) if code})
    lineage = _dict(position.get("candidate_lineage"))
    sleeve_id = _text(position.get("sleeve_id") or closure.get("sleeve_id") or outcome.get("sleeve_id") or prior.get("sleeve_id") or lineage.get("sleeve_id"))

    return {
        "generated_hypothesis_id": OIL_HYPOTHESIS_ID,
        "hypothesis_id": _text(prior.get("hypothesis_id") or lifecycle.get("hypothesis_id") or OIL_HYPOTHESIS_ID),
        "hypothesis_name": _text(prior.get("hypothesis_name") or OIL_NAME),
        "sleeve_id": sleeve_id,
        "raw_signal_id": raw_signal_id,
        "candidate_contract_id": candidate_id,
        "candidate_id": candidate_id,
        "paper_observation_found": paper_observation_found,
        "paper_observation_id": paper_observation_id,
        "paper_position_id": paper_position_id,
        "paper_observation_source": "aegis_paper_position_ledger_v1" if paper_observation_found else "",
        "paper_position_ledger_source": str(paths.get("paper_position_ledger") or "") if paper_observation_found else "",
        "outcome_readiness_status": readiness["outcome_readiness_status"],
        "outcome_status": "OUTCOME_CREATED" if outcome_created else "OUTCOME_NOT_READY",
        "outcome_id": _text(outcome.get("outcome_id")),
        "outcome_state": _text(outcome.get("outcome_state")),
        "outcome_row_created": outcome_created,
        "exit_price_present": readiness["exit_price_present"],
        "exit_price_source": readiness["exit_price_source"],
        "close_condition_status": readiness["close_condition_status"],
        "close_condition_reason": readiness["close_condition_reason"],
        "holding_period_status": readiness["holding_period_status"],
        "mark_data_status": readiness["mark_data_status"],
        "paper_outcome_auto_closure_state": _text(closure.get("auto_closure_state") or closure.get("lifecycle_state")),
        "paper_outcome_auto_closure_reason_codes": _list(closure.get("auto_closure_reason_codes")),
        "exit_recommendation": _text(closure.get("exit_recommendation") or exit_recommendation.get("exit_recommendation")),
        "exit_trigger": _text(closure.get("exit_trigger") or outcome.get("exit_trigger")),
        "validation_sample_status": sample_status,
        "validation_sample_id": _text(sample.get("sample_id")),
        "validation_sample_inclusion_status": _text(sample.get("inclusion_status")),
        "validation_sample_exclusion_reason": _text(sample.get("exclusion_reason")),
        "research_quality_consumed": quality_consumed,
        "research_quality_consumed_reason": quality_reason,
        "furthest_stage_reached": furthest,
        "remaining_blocker": remaining,
        "blocker_code": blocker_code,
        "blocker_reason": blocker_reason,
        "missing_fields": missing_fields,
        "required_inputs": required_inputs,
        "owner": owner,
        "david_action_required": False,
        "source_artifact_paths": source_paths,
        "source_artifact_hashes": source_hashes,
        "reason_codes": reason_codes,
        "computed_at_utc": computed_at,
        **SAFETY,
    }


def _blocker(
    *,
    prior: Mapping[str, Any],
    lifecycle: Mapping[str, Any],
    position: Mapping[str, Any],
    outcome: Mapping[str, Any],
    closure: Mapping[str, Any],
    readiness: Mapping[str, Any],
    outcome_created: bool,
) -> tuple[str, str, list[str], list[str], str]:
    if not position:
        missing = [str(x) for x in _list(prior.get("paper_construction_missing_fields")) if _text(x)]
        if not missing:
            missing = [field[8:].lower() for field in _reason_codes(lifecycle) if field.startswith("MISSING_")]
        code = _text(prior.get("candidate_to_paper_status")) or _text(lifecycle.get("blocker_classification")) or "PAPER_OBSERVATION_NOT_CREATED"
        if code == "PAPER_OBSERVATION_CREATED":
            code = "PAPER_OBSERVATION_NOT_FOUND"
        reason = _text(prior.get("current_stop_reason")) or ";".join(_reason_codes(lifecycle)) or "No Oil Shock paper position exists in the authoritative paper position ledger."
        return code, reason, missing, ["paper_position_ledger row", "paper_position_id", "candidate-to-paper lifecycle paper_position_id"], "AEGIS_SYSTEM"
    if not outcome:
        return "OUTCOME_REGISTRY_ROW_MISSING", "Oil Shock paper observation exists, but no outcome registry row was produced after outcome validation.", [], ["aegis_outcome_registry_v1 outcome row"], "AEGIS_SYSTEM"
    if outcome_created:
        return "NONE", "Oil Shock paper observation has an authoritative resolved outcome row.", [], [], "NONE"
    if not closure:
        return "OUTCOME_READINESS_ROW_MISSING", "Oil Shock paper observation exists, but paper outcome auto-closure did not emit a readiness row.", [], ["aegis_paper_outcome_auto_closure_v1 readiness row"], "AEGIS_SYSTEM"
    return _text(readiness.get("blocker_code")) or "POSITION_STILL_OPEN", _text(readiness.get("blocker_reason")) or "Oil Shock paper observation remains open.", _list(readiness.get("missing_fields")), _list(readiness.get("required_inputs")), "AEGIS_SYSTEM"

def _research_quality_consumption(quality: Mapping[str, Any], performance: Mapping[str, Any], outcome_created: bool, sample_status: str) -> tuple[bool, str]:
    if not quality:
        return False, "RESEARCH_QUALITY_ROW_MISSING"
    if not outcome_created:
        return False, "NO_OUTCOME_FOR_RESEARCH_QUALITY"
    if not performance:
        return False, "OUTCOME_PERFORMANCE_METRICS_ROW_MISSING"
    if sample_status not in {"INCLUDED", "EXCLUDED"}:
        return False, "VALIDATION_SAMPLE_STATE_NOT_READY"
    return True, "RESEARCH_QUALITY_CONSUMED_GENERATED_HYPOTHESIS_OUTCOME_METRICS"


def _furthest(paper_observation_found: bool, outcome_created: bool, sample_status: str) -> str:
    if sample_status in {"INCLUDED", "EXCLUDED"}:
        return "VALIDATION_SAMPLE_REACHED"
    if outcome_created:
        return "OUTCOME_REACHED"
    if paper_observation_found:
        return "PAPER_OBSERVATION_REACHED"
    return "CANDIDATE_TO_PAPER_BLOCKED"



def _outcome_advancement_created(outcome: Mapping[str, Any]) -> bool:
    outcome_state = _upper(outcome.get("outcome_state"))
    if not outcome_state:
        return False
    if outcome_state in {"OPEN", "UNKNOWN_BLOCKED"}:
        return False
    return True


def _readiness(
    *,
    position: Mapping[str, Any],
    outcome: Mapping[str, Any],
    closure: Mapping[str, Any],
    exit_recommendation: Mapping[str, Any],
    outcome_created: bool,
) -> dict[str, Any]:
    exit_source = _text(closure.get("exit_price_source_artifact") or outcome.get("exit_price_source_artifact") or outcome.get("closure_source"))
    exit_present = any(
        value not in {None, ""}
        for value in (
            closure.get("exit_mark"),
            closure.get("exit_price"),
            outcome.get("exit_mark"),
            outcome.get("exit_price"),
        )
    )
    exit_recommendation_value = _upper(closure.get("exit_recommendation") or exit_recommendation.get("exit_recommendation"))
    closure_state = _upper(closure.get("auto_closure_state") or closure.get("lifecycle_state"))
    reason_codes = set(_reason_codes(closure))

    mark_data_status = "MISSING"
    missing_fields: list[str] = []
    required_inputs: list[str] = []
    if exit_present and exit_source:
        mark_data_status = "PRESENT"
    elif exit_source and _text(closure.get("exit_price_timestamp")):
        mark_data_status = "SOURCE_PRESENT_PRICE_NOT_ACTIONABLE"
    elif not exit_source:
        missing_fields.append("exit_price_source")
        required_inputs.append("certified exit mark source")
    else:
        missing_fields.append("exit_price")
        required_inputs.append("certified actionable exit mark")

    if outcome_created:
        return {
            "outcome_readiness_status": "READY",
            "exit_price_present": exit_present,
            "exit_price_source": exit_source,
            "close_condition_status": "MET",
            "close_condition_reason": _text(outcome.get("exit_trigger") or closure.get("exit_trigger") or "RESOLVED_OUTCOME_ROW_PRESENT"),
            "holding_period_status": "ELAPSED_OR_NOT_REQUIRED",
            "mark_data_status": "PRESENT" if exit_present else mark_data_status,
            "blocker_code": "NONE",
            "blocker_reason": "Oil Shock paper observation has an authoritative resolved outcome row.",
            "missing_fields": [],
            "required_inputs": [],
        }

    if not position:
        return {
            "outcome_readiness_status": "NOT_READY",
            "exit_price_present": exit_present,
            "exit_price_source": exit_source,
            "close_condition_status": "NOT_EVALUATED",
            "close_condition_reason": "PAPER_OBSERVATION_NOT_FOUND",
            "holding_period_status": "NOT_EVALUATED",
            "mark_data_status": mark_data_status,
            "blocker_code": "PAPER_OBSERVATION_NOT_FOUND",
            "blocker_reason": "No authoritative paper observation exists for outcome readiness evaluation.",
            "missing_fields": ["paper_position_id"],
            "required_inputs": ["paper_position_ledger row"],
        }

    if "SAME_DAY_AUTO_PROMOTED_RESEARCH_OBSERVATION" in reason_codes:
        return {
            "outcome_readiness_status": "NOT_READY",
            "exit_price_present": exit_present,
            "exit_price_source": exit_source,
            "close_condition_status": "NOT_MET",
            "close_condition_reason": "SAME_DAY_AUTO_PROMOTED_RESEARCH_OBSERVATION",
            "holding_period_status": "NOT_ELAPSED",
            "mark_data_status": mark_data_status,
            "blocker_code": "HOLDING_PERIOD_NOT_ELAPSED",
            "blocker_reason": "Oil Shock paper observation is same-day auto-promoted research-only paper tracking and is not eligible for deterministic auto-closure on the target day.",
            "missing_fields": [],
            "required_inputs": ["next eligible paper outcome auto-closure evaluation"],
        }

    if exit_recommendation_value == "HOLD" or "HOLD_RECOMMENDATION" in reason_codes:
        return {
            "outcome_readiness_status": "NOT_READY",
            "exit_price_present": exit_present,
            "exit_price_source": exit_source,
            "close_condition_status": "NOT_MET",
            "close_condition_reason": "HOLD_RECOMMENDATION",
            "holding_period_status": "ELAPSED_OR_NOT_REQUIRED",
            "mark_data_status": mark_data_status,
            "blocker_code": "POSITION_STILL_OPEN",
            "blocker_reason": "Oil Shock paper observation remains open because the deterministic exit recommendation is HOLD and no stop, target, or other close rule triggered.",
            "missing_fields": [],
            "required_inputs": ["non-HOLD deterministic exit recommendation"],
        }

    if "MISSING_EXIT_MARK" in reason_codes or "EXIT_MARK_TIMESTAMP_MISSING" in reason_codes:
        missing = list(dict.fromkeys(missing_fields + ["exit_price"]))
        return {
            "outcome_readiness_status": "NOT_READY",
            "exit_price_present": exit_present,
            "exit_price_source": exit_source,
            "close_condition_status": "NOT_EVALUATED",
            "close_condition_reason": "EXIT_PRICE_MISSING",
            "holding_period_status": "ELAPSED_OR_NOT_REQUIRED",
            "mark_data_status": "MISSING",
            "blocker_code": "EXIT_PRICE_MISSING",
            "blocker_reason": "Paper outcome auto-closure cannot create an outcome because certified actionable exit price data is missing.",
            "missing_fields": missing,
            "required_inputs": list(dict.fromkeys(required_inputs + ["certified actionable exit mark"])),
        }

    if "EXIT_EVALUATION_MISSING" in reason_codes:
        return {
            "outcome_readiness_status": "NOT_READY",
            "exit_price_present": exit_present,
            "exit_price_source": exit_source,
            "close_condition_status": "NOT_EVALUATED",
            "close_condition_reason": "CLOSE_RULE_MISSING",
            "holding_period_status": "NOT_EVALUATED",
            "mark_data_status": mark_data_status,
            "blocker_code": "CLOSE_RULE_MISSING",
            "blocker_reason": "Paper outcome auto-closure cannot evaluate Oil Shock because no deterministic exit recommendation row was found.",
            "missing_fields": ["exit_recommendation"],
            "required_inputs": ["aegis_exit_recommendations_v1 row"],
        }

    if mark_data_status == "MISSING":
        return {
            "outcome_readiness_status": "NOT_READY",
            "exit_price_present": exit_present,
            "exit_price_source": exit_source,
            "close_condition_status": "NOT_EVALUATED",
            "close_condition_reason": "MARK_DATA_MISSING",
            "holding_period_status": "ELAPSED_OR_NOT_REQUIRED",
            "mark_data_status": mark_data_status,
            "blocker_code": "MARK_DATA_MISSING",
            "blocker_reason": "Paper outcome readiness cannot be proven because required mark data is missing.",
            "missing_fields": missing_fields,
            "required_inputs": required_inputs,
        }

    if closure_state == "AUTO_CLOSURE_NOT_ELIGIBLE":
        reason = ",".join(sorted(reason_codes)) or _text(closure.get("exit_trigger") or "POSITION_STILL_OPEN")
        return {
            "outcome_readiness_status": "NOT_READY",
            "exit_price_present": exit_present,
            "exit_price_source": exit_source,
            "close_condition_status": "NOT_MET",
            "close_condition_reason": reason,
            "holding_period_status": "ELAPSED_OR_NOT_REQUIRED",
            "mark_data_status": mark_data_status,
            "blocker_code": "POSITION_STILL_OPEN",
            "blocker_reason": f"Oil Shock paper observation remains open; auto-closure state is {closure_state} with reason {reason}.",
            "missing_fields": [],
            "required_inputs": ["deterministic close condition"],
        }

    return {
        "outcome_readiness_status": "NOT_READY",
        "exit_price_present": exit_present,
        "exit_price_source": exit_source,
        "close_condition_status": "NOT_EVALUATED",
        "close_condition_reason": _text(closure_state or "OUTCOME_BUILDER_LINEAGE_MISMATCH"),
        "holding_period_status": "NOT_EVALUATED",
        "mark_data_status": mark_data_status,
        "blocker_code": "OUTCOME_BUILDER_LINEAGE_MISMATCH",
        "blocker_reason": "Oil Shock paper observation exists, but deterministic outcome readiness could not be matched to a supported auto-closure state.",
        "missing_fields": [],
        "required_inputs": ["matched paper outcome auto-closure readiness row"],
    }

def _validation_status(sample: Mapping[str, Any], outcome_created: bool) -> str:
    if not sample:
        return "NOT_READY"
    status = _upper(sample.get("inclusion_status") or sample.get("sample_state"))
    if status == "INCLUDED":
        return "INCLUDED"
    if status == "EXCLUDED" or status.startswith("EXCLUDED_"):
        return "EXCLUDED"
    return "NOT_READY" if outcome_created else "NOT_READY"


def _oil_row(payload: Any) -> dict[str, Any]:
    row = _dict(payload).get("oil_shock")
    return dict(row) if isinstance(row, Mapping) else {}


def _find_lifecycle(payload: Any, candidate_id: str, raw_signal_id: str) -> dict[str, Any]:
    for row in _list(_dict(payload).get("rows")):
        if (candidate_id and _text(row.get("candidate_id")) == candidate_id) or (raw_signal_id and _text(row.get("raw_signal_id")) == raw_signal_id):
            return dict(row)
    return {}


def _find_position(payload: Any, candidate_id: str, paper_position_id: str) -> dict[str, Any]:
    payload = _dict(payload)
    for key in ("positions", "open_positions", "closed_positions", "historical_positions"):
        for row in _list(payload.get(key)):
            lineage = row.get("candidate_lineage") if isinstance(row, Mapping) else {}
            lineage = lineage if isinstance(lineage, Mapping) else {}
            if (paper_position_id and _text(row.get("position_id")) == paper_position_id) or (candidate_id and _text(row.get("candidate_id") or lineage.get("candidate_id")) == candidate_id):
                return dict(row)
    return {}


def _find_outcome(payload: Any, candidate_id: str, paper_position_id: str, outcome_id: str) -> dict[str, Any]:
    for row in _list(_dict(payload).get("outcomes")):
        if (outcome_id and _text(row.get("outcome_id")) == outcome_id) or (paper_position_id and _text(row.get("position_id")) == paper_position_id) or (candidate_id and _text(row.get("candidate_id")) == candidate_id):
            return dict(row)
    return {}



def _find_auto_closure(payload: Any, candidate_id: str, paper_position_id: str) -> dict[str, Any]:
    payload = _dict(payload)
    for key in ("rows", "closures", "manual_review_queue"):
        for row in _list(payload.get(key)):
            if not isinstance(row, Mapping):
                continue
            if (paper_position_id and _text(row.get("position_id")) == paper_position_id) or (candidate_id and _text(row.get("candidate_id")) == candidate_id):
                return dict(row)
    return {}


def _find_exit_recommendation(payload: Any, candidate_id: str, paper_position_id: str) -> dict[str, Any]:
    payload = _dict(payload)
    for key in ("recommendations", "rows", "exit_recommendations"):
        for row in _list(payload.get(key)):
            if not isinstance(row, Mapping):
                continue
            if (paper_position_id and _text(row.get("position_id")) == paper_position_id) or (candidate_id and _text(row.get("candidate_id")) == candidate_id):
                return dict(row)
    return {}


def _find_sample(payload: Any, candidate_id: str, paper_position_id: str, outcome_id: str) -> dict[str, Any]:
    for row in _list(_dict(payload).get("samples")):
        if (outcome_id and _text(row.get("outcome_id")) == outcome_id) or (paper_position_id and _text(row.get("position_id")) == paper_position_id) or (candidate_id and _text(row.get("candidate_id")) == candidate_id):
            return dict(row)
    return {}


def _find_quality(payload: Any) -> dict[str, Any]:
    for row in _list(_dict(payload).get("hypotheses")):
        if _text(row.get("hypothesis_id")) == OIL_HYPOTHESIS_ID:
            return dict(row)
    return {}


def _find_performance(payload: Any) -> dict[str, Any]:
    for row in _list(_dict(payload).get("hypotheses")):
        if _text(row.get("hypothesis_id")) == OIL_HYPOTHESIS_ID:
            return dict(row)
    return {}


def _reason_codes(row: Mapping[str, Any]) -> list[str]:
    codes: list[str] = []
    for key in ("reason_codes", "blocker_reason_codes", "auto_promotion_reason_codes", "auto_closure_reason_codes", "blocker_codes", "blocker_code"):
        value = row.get(key)
        if isinstance(value, list):
            codes.extend(str(item) for item in value if _text(item))
        elif _text(value):
            codes.append(_text(value))
    for field in _list(row.get("missing_fields")) + _list(row.get("paper_construction_missing_fields")):
        if _text(field):
            codes.append("MISSING_" + _text(field).upper())
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
