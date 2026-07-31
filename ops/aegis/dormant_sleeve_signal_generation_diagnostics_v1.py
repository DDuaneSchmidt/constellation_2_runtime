from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1

FAMILY = "aegis_dormant_sleeve_signal_generation_diagnostics_v1"
FILENAME = "dormant_sleeve_signal_generation_diagnostics.v1.json"
POLICY_VERSION = "AEGIS_DORMANT_SLEEVE_SIGNAL_GENERATION_DIAGNOSTICS_READ_ONLY_V1"

DORMANT_REASON_CODES = [
    "VALID_NO_SIGNAL_CONDITIONS",
    "PRODUCER_NOT_REGISTERED",
    "PRODUCER_NOT_INVOKED",
    "PRODUCER_DISABLED",
    "PRODUCER_RUNTIME_ERROR",
    "MISSING_MARKET_DATA",
    "MISSING_EVENT_DATA",
    "MISSING_TRIGGER_POLICY",
    "TRIGGER_THRESHOLDS_NOT_MET",
    "UNSUPPORTED_HYPOTHESIS_TYPE",
    "LINEAGE_MISMATCH",
    "UNKNOWN_DETERMINISTIC_BLOCKER",
]

SAFETY = {
    "read_only": True,
    "diagnostics_only": True,
    "no_sleeve_mutation": True,
    "no_candidate_mutation": True,
    "no_quality_mutation": True,
    "no_allocation_mutation": True,
    "no_market_data_mutation": True,
    "no_signal_fabrication": True,
    "no_forced_candidates": True,
    "no_repair_performed": True,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
}


def dormant_sleeve_signal_generation_diagnostics_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, day_utc, FILENAME)


def build_dormant_sleeve_signal_generation_diagnostics_v1(
    *, truth_root: Path | str, day_utc: str, computed_at_utc: str | None = None
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    paths = _input_paths(root, day)
    payloads = {name: read_json_v1(path) for name, path in paths.items()}
    t01_rows = [
        row
        for row in _rows(payloads.get("sleeve_throughput_diagnostics"), "sleeves")
        if _upper(row.get("blocker_code")) == "NO_SIGNALS_GENERATED"
    ]
    rows = [
        _build_dormant_row(row, payloads, paths, day, computed_at_utc or f"{day}T00:00:00Z")
        for row in sorted(t01_rows, key=lambda item: _text(item.get("sleeve_id")))
    ]
    summary = _summary(rows)
    payload: dict[str, Any] = {
        "schema_id": "aegis_dormant_sleeve_signal_generation_diagnostics",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "policy_version": POLICY_VERSION,
        "day_utc": day,
        "target_day": day,
        "computed_at_utc": computed_at_utc or f"{day}T00:00:00Z",
        "dormant_sleeves": rows,
        "portfolio_summary": summary,
        "summary": summary,
        "required_analysis": _required_analysis(rows),
        "answer": _answer(rows),
        "source_artifact_paths": {name: str(path) for name, path in sorted(paths.items()) if path.exists()},
        "source_artifact_hashes": {name: file_hash_v1(path) for name, path in sorted(paths.items()) if path.exists()},
        "safety_statement": "Dormant sleeve signal generation diagnostics are read-only. This artifact explains existing zero-signal evidence only and performs no producer, sleeve, threshold, market data, candidate, quality, allocation, or repair mutation.",
        **SAFETY,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = stable_hash_v1({**payload, "content_hash": ""})
    return payload


def write_dormant_sleeve_signal_generation_diagnostics_v1(
    *, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None
) -> Path:
    body = payload or build_dormant_sleeve_signal_generation_diagnostics_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(dormant_sleeve_signal_generation_diagnostics_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _input_paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "sleeve_throughput_diagnostics": report_path_v1(root, "aegis_sleeve_throughput_diagnostics_v1", day, "sleeve_throughput_diagnostics.v1.json"),
        "candidate_generation_diagnostics": report_path_v1(root, "aegis_candidate_generation_diagnostics_v1", day, "candidate_generation_diagnostics.v1.json"),
        "sleeve_evaluation_rollup": report_path_v1(root, "sleeve_evaluation_kernel_v1", day, "sleeve_evaluation_rollup.v1.json"),
        "sleeve_input_contracts": report_path_v1(root, "aegis_sleeve_input_contracts_v1", day, "sleeve_input_contracts.v1.json"),
        "scheduled_run_reconciliation": report_path_v1(root, "aegis_scheduled_run_reconciliation_v1", day, "scheduled_run_reconciliation.v1.json"),
        "macro_calendar_data_readiness": report_path_v1(root, "aegis_macro_calendar_data_readiness_v1", day, "macro_calendar_data_readiness.v1.json"),
        "missing_market_data_requirement_resolver": report_path_v1(root, "aegis_missing_market_data_requirement_resolver_v1", day, "missing_market_data_requirement_resolver.v1.json"),
    }


def _build_dormant_row(t01: Mapping[str, Any], payloads: Mapping[str, Any], paths: Mapping[str, Path], day: str, computed_at: str) -> dict[str, Any]:
    sleeve_id = _text(t01.get("sleeve_id"))
    hypothesis_id = _text(t01.get("hypothesis_id"))
    diag = _first_match(_rows(payloads.get("candidate_generation_diagnostics"), "sleeves", "rows", "diagnostics"), sleeve_id, hypothesis_id)
    eval_row = _first_match(_rows(payloads.get("sleeve_evaluation_rollup"), "outcomes", "rows", "sleeves"), sleeve_id, hypothesis_id)
    contract = _first_match(_rows(payloads.get("sleeve_input_contracts"), "contracts", "rows", "sleeves"), sleeve_id, hypothesis_id)
    scheduled = _matching_rows(_rows(payloads.get("scheduled_run_reconciliation"), "scheduled_runs", "rows"), sleeve_id, hypothesis_id)

    producer_found = _producer_found(diag, eval_row)
    producer_registered = _producer_registered(diag, eval_row, contract)
    producer_invoked = _producer_invoked(diag, eval_row)
    raw_signal_count = _int(t01.get("raw_signal_count"), _int(diag.get("raw_signal_count"), _int(eval_row.get("output_count"))))
    market_data_status = _market_data_status(diag, eval_row, contract)
    missing_inputs = _missing_inputs(diag, eval_row, contract)
    expected_conditions = _expected_signal_conditions(diag, eval_row, contract)
    observed_conditions = _observed_market_conditions(diag, eval_row)
    thresholds_present = _thresholds_present(diag, eval_row)
    thresholds_met = _thresholds_met(diag, eval_row)
    trigger_status = _trigger_status(diag, eval_row, thresholds_present, thresholds_met)
    calendar_status = _calendar_or_event_dependency_status(sleeve_id, diag, eval_row, payloads)
    reason_code, owner, david = _classify(
        diag=diag,
        eval_row=eval_row,
        contract=contract,
        producer_found=producer_found,
        producer_registered=producer_registered,
        producer_invoked=producer_invoked,
        raw_signal_count=raw_signal_count,
        market_data_status=market_data_status,
        missing_inputs=missing_inputs,
        calendar_status=calendar_status,
        thresholds_present=thresholds_present,
        thresholds_met=thresholds_met,
        trigger_status=trigger_status,
    )
    row = {
        "schema_id": "aegis_dormant_sleeve_signal_generation_diagnostic_row",
        "schema_version": "v1",
        "day_utc": day,
        "computed_at_utc": computed_at,
        "sleeve_id": sleeve_id,
        "sleeve_name": _text(t01.get("sleeve_name")) or sleeve_id,
        "hypothesis_id": hypothesis_id,
        "sleeve_status": _text(t01.get("sleeve_status")) or "ACTIVE",
        "signal_producer_found": producer_found,
        "signal_producer_status": _producer_status(diag, eval_row, contract),
        "producer_registered": producer_registered,
        "producer_invoked": producer_invoked,
        "raw_signal_count": raw_signal_count,
        "expected_signal_conditions": expected_conditions,
        "observed_market_conditions": observed_conditions,
        "market_data_status": market_data_status,
        "trigger_evaluation_status": trigger_status,
        "trigger_thresholds_present": thresholds_present,
        "trigger_thresholds_met": thresholds_met,
        "calendar_or_event_dependency_status": calendar_status,
        "missing_inputs": missing_inputs,
        "furthest_stage_reached": _text(t01.get("furthest_stage_reached")) or "HYPOTHESIS",
        "dormant_reason_code": reason_code,
        "dormant_reason": _reason_text(reason_code, diag, eval_row, missing_inputs),
        "owner": owner,
        "david_action_required": david,
        "zero_signal_interpretation": _interpretation(reason_code),
        "source_diagnostics": {
            "candidate_generation": _compact_source(diag),
            "sleeve_evaluation": _compact_source(eval_row),
            "sleeve_input_contract": _compact_contract(contract),
            "scheduled_runs": [_compact_source(row) for row in scheduled],
        },
        "t01_link": {
            "artifact_id": "aegis_sleeve_throughput_diagnostics_v1",
            "path": str(paths["sleeve_throughput_diagnostics"]),
            "sleeve_id": sleeve_id,
            "blocker_code": _text(t01.get("blocker_code")),
            "throughput_status": _text(t01.get("throughput_status")),
        },
        "downstream_diagnostics": _downstream_diagnostics(reason_code, sleeve_id, paths),
        "source_artifact_paths": {name: str(path) for name, path in sorted(paths.items()) if path.exists()},
        "source_artifact_hashes": {name: file_hash_v1(path) for name, path in sorted(paths.items()) if path.exists()},
        **SAFETY,
    }
    row["row_hash"] = stable_hash_v1({**row, "row_hash": ""})
    return row


def _classify(**kwargs: Any) -> tuple[str, str, bool]:
    diag = kwargs["diag"]
    eval_row = kwargs["eval_row"]
    contract = kwargs["contract"]
    producer_found = kwargs["producer_found"]
    producer_registered = kwargs["producer_registered"]
    producer_invoked = kwargs["producer_invoked"]
    raw_signal_count = kwargs["raw_signal_count"]
    market_data_status = kwargs["market_data_status"]
    missing_inputs = kwargs["missing_inputs"]
    calendar_status = kwargs["calendar_status"]
    thresholds_present = kwargs["thresholds_present"]
    thresholds_met = kwargs["thresholds_met"]
    trigger_status = kwargs["trigger_status"]
    text = _evidence_text(diag, eval_row, contract)
    reason_codes = {_upper(code) for code in [*_list(diag.get("reason_codes")), *_list(eval_row.get("reason_codes"))]}

    if raw_signal_count > 0:
        return "LINEAGE_MISMATCH", "AEGIS_SYSTEM", False
    if not producer_registered:
        return "PRODUCER_NOT_REGISTERED", "AEGIS_SYSTEM", False
    if _upper(contract.get("enabled")) == "FALSE" or _upper(eval_row.get("activation_status")) in {"DISABLED", "INACTIVE"}:
        return "PRODUCER_DISABLED", "AEGIS_SYSTEM", False
    if not producer_found:
        return "UNSUPPORTED_HYPOTHESIS_TYPE", "AEGIS_SYSTEM", False
    if not producer_invoked:
        return "PRODUCER_NOT_INVOKED", "AEGIS_SYSTEM", False
    if _int(eval_row.get("exit_code")) not in (0, -1) or "TRACEBACK" in text or "PRODUCER_NONZERO_RC" in reason_codes:
        if "MISSING_REQUIRED_INPUTS" not in text:
            return "PRODUCER_RUNTIME_ERROR", "AEGIS_SYSTEM", False
    if calendar_status in {"MISSING_EVENT_DATA", "MISSING_CALENDAR_DATA"}:
        return "MISSING_EVENT_DATA", "DAVID", True
    if "ACTIVE_SIMULATOR_NOT_PRESTART_REQUIRED" in reason_codes or "OPTIONAL_SIMULATION" in reason_codes:
        return "VALID_NO_SIGNAL_CONDITIONS", "AEGIS_SYSTEM", False
    if market_data_status in {"MISSING", "BLOCKED", "STALE_OR_MISSING"} or _has_market_missing(missing_inputs, text):
        return "MISSING_MARKET_DATA", "AEGIS_SYSTEM", False
    if not thresholds_present:
        return "MISSING_TRIGGER_POLICY", "AEGIS_SYSTEM", False
    if thresholds_met is False:
        return "TRIGGER_THRESHOLDS_NOT_MET", "AEGIS_SYSTEM", False
    if "NO_INTENT_DECLARED" in reason_codes or trigger_status in {"EVALUATED_NO_SIGNAL", "EVALUATED"}:
        return "VALID_NO_SIGNAL_CONDITIONS", "AEGIS_SYSTEM", False
    return "UNKNOWN_DETERMINISTIC_BLOCKER", "AEGIS_SYSTEM", False


def _summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {code: sum(1 for row in rows if row["dormant_reason_code"] == code) for code in DORMANT_REASON_CODES}
    return {
        "total_dormant_sleeves": len(rows),
        "valid_no_signal_conditions_count": counts["VALID_NO_SIGNAL_CONDITIONS"],
        "producer_not_registered_count": counts["PRODUCER_NOT_REGISTERED"],
        "producer_not_invoked_count": counts["PRODUCER_NOT_INVOKED"],
        "producer_disabled_count": counts["PRODUCER_DISABLED"],
        "producer_runtime_error_count": counts["PRODUCER_RUNTIME_ERROR"],
        "missing_market_data_count": counts["MISSING_MARKET_DATA"],
        "missing_event_data_count": counts["MISSING_EVENT_DATA"],
        "missing_trigger_policy_count": counts["MISSING_TRIGGER_POLICY"],
        "trigger_thresholds_not_met_count": counts["TRIGGER_THRESHOLDS_NOT_MET"],
        "unsupported_hypothesis_type_count": counts["UNSUPPORTED_HYPOTHESIS_TYPE"],
        "lineage_mismatch_count": counts["LINEAGE_MISMATCH"],
        "unknown_blocker_count": counts["UNKNOWN_DETERMINISTIC_BLOCKER"],
    }


def _required_analysis(rows: list[dict[str, Any]]) -> dict[str, list[str]]:
    buckets = {
        "healthy_selectivity": ["VALID_NO_SIGNAL_CONDITIONS", "TRIGGER_THRESHOLDS_NOT_MET"],
        "data_starvation": ["MISSING_MARKET_DATA", "MISSING_EVENT_DATA"],
        "wiring_failure": ["PRODUCER_NOT_REGISTERED", "PRODUCER_NOT_INVOKED", "LINEAGE_MISMATCH"],
        "logic_or_configuration_failure": ["MISSING_TRIGGER_POLICY"],
        "runtime_failure": ["PRODUCER_RUNTIME_ERROR"],
        "unsupported_sleeve_type": ["UNSUPPORTED_HYPOTHESIS_TYPE", "UNKNOWN_DETERMINISTIC_BLOCKER"],
    }
    return {name: [row["sleeve_id"] for row in rows if row["dormant_reason_code"] in codes] for name, codes in buckets.items()}


def _answer(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "No T01 NO_SIGNALS_GENERATED sleeves were present for this day."
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["dormant_reason_code"]] = counts.get(row["dormant_reason_code"], 0) + 1
    parts = ", ".join(f"{code}={count}" for code, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])))
    return f"{len(rows)} dormant sleeves had zero raw signals. Deterministic signal-generation reasons: {parts}."


def _producer_found(diag: Mapping[str, Any], eval_row: Mapping[str, Any]) -> bool:
    return bool(_text(diag.get("producer_command") or diag.get("runner_command") or eval_row.get("producer_command") or eval_row.get("artifact_path")))


def _producer_registered(diag: Mapping[str, Any], eval_row: Mapping[str, Any], contract: Mapping[str, Any]) -> bool:
    if contract:
        return bool(contract.get("enabled", True))
    return bool(diag or eval_row)


def _producer_invoked(diag: Mapping[str, Any], eval_row: Mapping[str, Any]) -> bool:
    status = _upper(diag.get("run_status") or diag.get("evaluation_status") or eval_row.get("status") or eval_row.get("current_status"))
    if status in {"RAN", "NO_INTENT", "INTENT_CREATED", "FILTERED_OUT"}:
        return True
    if status == "BLOCKED" and _text(diag.get("producer_command") or diag.get("runner_command")):
        return True
    if _text(eval_row.get("started_at_utc") or eval_row.get("completed_at_utc")) and _text(eval_row.get("producer_command")):
        return True
    if _text(eval_row.get("producer_command")) and (_text(eval_row.get("exit_code")) or _upper(eval_row.get("status") or eval_row.get("current_status")) == "BLOCKED"):
        return True
    return False


def _producer_status(diag: Mapping[str, Any], eval_row: Mapping[str, Any], contract: Mapping[str, Any]) -> str:
    return _text(diag.get("evaluation_status") or diag.get("run_status") or eval_row.get("status") or eval_row.get("current_status") or contract.get("contract_status")) or "UNKNOWN"


def _market_data_status(diag: Mapping[str, Any], eval_row: Mapping[str, Any], contract: Mapping[str, Any]) -> str:
    explicit = _text(diag.get("market_data_status"))
    if explicit:
        return explicit
    manifest = _dict(eval_row.get("market_data_manifest_check"))
    if manifest:
        status = _upper(manifest.get("status"))
        blocker = _upper(manifest.get("canonical_blocker"))
        if status == "PASS" and not blocker:
            return "AVAILABLE"
        if status == "BLOCKED":
            return "BLOCKED"
    checks = [*_list(diag.get("required_inputs_status")), *_list(eval_row.get("required_inputs_status")), *_list(contract.get("required_inputs_status"))]
    statuses = {_upper(row.get("status")) for row in checks if isinstance(row, Mapping)}
    if {"MISSING", "STALE"} & statuses:
        return "STALE_OR_MISSING"
    if checks or contract:
        return "AVAILABLE"
    return "UNKNOWN"


def _missing_inputs(diag: Mapping[str, Any], eval_row: Mapping[str, Any], contract: Mapping[str, Any]) -> list[str]:
    values: list[str] = []
    for source in (diag, eval_row):
        values.extend(_string_list(source.get("missing_inputs")))
        values.extend(_string_list(source.get("blocking_inputs")))
        values.extend(_string_list(source.get("reason_codes")))
        stdout = _parse_json_tail(_text(source.get("stdout_summary")))
        values.extend(_string_list(stdout.get("reason_codes")))
    for source in (diag, eval_row, contract):
        for row in [*_list(source.get("required_inputs_status")), *_list(source.get("optional_inputs_status"))]:
            if isinstance(row, Mapping) and (row.get("blocking") is True or _upper(row.get("status")) in {"MISSING", "STALE"}):
                values.append(_text(row.get("data_item_id")) or _text(row.get("reason")) or _upper(row.get("status")))
    values.extend(_missing_paths_from_text(_text(eval_row.get("stderr_summary"))))
    return sorted({value for value in values if value})


def _expected_signal_conditions(diag: Mapping[str, Any], eval_row: Mapping[str, Any], contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    out = []
    for threshold in _string_list(diag.get("thresholds_applied")):
        out.append({"condition": threshold, "source": "candidate_generation_diagnostics.thresholds_applied"})
    stdout = _parse_json_tail(_text(eval_row.get("stdout_summary")))
    if stdout:
        for key in ("z_enter", "pairs", "rule", "suite"):
            if key in stdout:
                out.append({"condition": key, "value": stdout.get(key), "source": "sleeve_evaluation.stdout_summary"})
    if contract:
        out.append({"condition": "required_inputs", "count": len(_list(contract.get("required_inputs"))), "source": "sleeve_input_contracts"})
    return out


def _observed_market_conditions(diag: Mapping[str, Any], eval_row: Mapping[str, Any]) -> dict[str, Any]:
    stdout = _parse_json_tail(_text(eval_row.get("stdout_summary")))
    observed: dict[str, Any] = {}
    if stdout:
        observed.update({key: stdout[key] for key in ("status", "reason_codes", "evaluations", "z_enter", "rule") if key in stdout})
    if not observed:
        observed["reason_codes"] = [*_string_list(diag.get("reason_codes")), *_string_list(eval_row.get("reason_codes"))]
    return observed


def _thresholds_present(diag: Mapping[str, Any], eval_row: Mapping[str, Any]) -> bool:
    if diag.get("trigger_thresholds_present") is not None:
        return bool(diag.get("trigger_thresholds_present"))
    if _list(diag.get("thresholds_applied")):
        return True
    stdout = _parse_json_tail(_text(eval_row.get("stdout_summary")))
    return bool(stdout and ("z_enter" in stdout or "rule" in stdout or "evaluations" in stdout))


def _thresholds_met(diag: Mapping[str, Any], eval_row: Mapping[str, Any]) -> bool | None:
    if diag.get("trigger_thresholds_met") is not None:
        return bool(diag.get("trigger_thresholds_met"))
    stdout = _parse_json_tail(_text(eval_row.get("stdout_summary")))
    if stdout and _upper(stdout.get("status")) == "NO_INTENT" and ("z_enter" in stdout or "evaluations" in stdout):
        return False
    return None


def _trigger_status(diag: Mapping[str, Any], eval_row: Mapping[str, Any], thresholds_present: bool, thresholds_met: bool | None) -> str:
    explicit = _text(diag.get("trigger_evaluation_status"))
    if explicit:
        return explicit
    if thresholds_present and thresholds_met is False:
        return "EVALUATED_NO_SIGNAL"
    if thresholds_present:
        return "EVALUATED"
    if _upper(eval_row.get("status") or eval_row.get("current_status")) == "BLOCKED":
        return "BLOCKED_BEFORE_TRIGGER"
    return "UNKNOWN"


def _calendar_or_event_dependency_status(sleeve_id: str, diag: Mapping[str, Any], eval_row: Mapping[str, Any], payloads: Mapping[str, Any]) -> str:
    reason_text = " ".join([*_string_list(diag.get("reason_codes")), *_string_list(eval_row.get("reason_codes")), *_string_list(diag.get("missing_inputs")), *_string_list(eval_row.get("missing_inputs"))]).upper()
    explicit_event_dependency = any(token in reason_text for token in ("EVENT_CALENDAR", "MACRO_CALENDAR", "CALENDAR_MISSING", "MISSING_EVENT_DATA"))
    if explicit_event_dependency:
        macro = _dict(payloads.get("macro_calendar_data_readiness"))
        if macro.get("macro_calendar_ready") is False or _upper(macro.get("status")) == "NEEDS_SOURCE":
            return "MISSING_EVENT_DATA"
        return "EVENT_DEPENDENCY_EVALUATED"
    if "EVENT" in sleeve_id:
        return "EVENT_DEPENDENCY_NOT_REQUIRED_BY_EVIDENCE"
    return "NONE"


def _reason_text(reason_code: str, diag: Mapping[str, Any], eval_row: Mapping[str, Any], missing_inputs: list[str]) -> str:
    if reason_code == "VALID_NO_SIGNAL_CONDITIONS":
        return "Producer ran and declared no intent/no signal under evaluated conditions."
    if reason_code == "MISSING_MARKET_DATA":
        return "Producer or input evidence reports missing or blocked market/account/position inputs: " + ", ".join(missing_inputs[:8])
    if reason_code == "MISSING_EVENT_DATA":
        return "Event or calendar dependency evidence is missing before signal generation can be evaluated."
    if reason_code == "PRODUCER_NOT_REGISTERED":
        return "No sleeve input contract or runtime row registers a deterministic producer for this sleeve."
    if reason_code == "PRODUCER_NOT_INVOKED":
        return "Producer evidence exists, but no run/invocation evidence is present for the target day."
    if reason_code == "PRODUCER_DISABLED":
        return "Registry or contract evidence marks the producer disabled/inactive."
    if reason_code == "PRODUCER_RUNTIME_ERROR":
        return _text(eval_row.get("stderr_summary"))[:500] or "Producer runtime returned an error before signal output."
    if reason_code == "MISSING_TRIGGER_POLICY":
        return "Producer/input evidence exists, but no deterministic trigger threshold or trigger policy evidence is present."
    if reason_code == "TRIGGER_THRESHOLDS_NOT_MET":
        return "Trigger thresholds were evaluated and no configured signal threshold was met."
    if reason_code == "UNSUPPORTED_HYPOTHESIS_TYPE":
        return "No deterministic signal path exists in the available producer evidence for this sleeve type."
    if reason_code == "LINEAGE_MISMATCH":
        return "T01 classified the sleeve as zero-signal, but T02 input evidence contains raw signal output."
    return "Available deterministic evidence does not fit a more specific dormant reason category."


def _interpretation(reason_code: str) -> str:
    if reason_code in {"VALID_NO_SIGNAL_CONDITIONS", "TRIGGER_THRESHOLDS_NOT_MET"}:
        return "HEALTHY_SELECTIVITY"
    if reason_code in {"MISSING_MARKET_DATA", "MISSING_EVENT_DATA"}:
        return "DATA_STARVATION"
    if reason_code in {"PRODUCER_NOT_REGISTERED", "PRODUCER_NOT_INVOKED", "LINEAGE_MISMATCH"}:
        return "WIRING_FAILURE"
    if reason_code == "MISSING_TRIGGER_POLICY":
        return "LOGIC_OR_CONFIGURATION_FAILURE"
    if reason_code == "PRODUCER_RUNTIME_ERROR":
        return "RUNTIME_FAILURE"
    return "UNSUPPORTED_OR_UNKNOWN"


def _has_market_missing(missing_inputs: list[str], text: str) -> bool:
    missing_text = " ".join(missing_inputs).upper()
    if any(token in missing_text for token in ("MARKET_DATA", "MARKET.PRICE", "SNAPSHOT", "MISSING_BAR", "NAV", "POSITION")):
        return True
    return "MISSING_REQUIRED_INPUTS" in text


def _downstream_diagnostics(reason_code: str, sleeve_id: str, paths: Mapping[str, Path]) -> dict[str, Any]:
    if reason_code != "MISSING_MARKET_DATA":
        return {}
    path = paths["missing_market_data_requirement_resolver"]
    return {
        "artifact_id": "aegis_missing_market_data_requirement_resolver_v1",
        "path": str(path),
        "sleeve_id": sleeve_id,
        "relationship": "resolves_missing_market_data_requirement",
        "artifact_exists": path.exists(),
        "content_hash": file_hash_v1(path) if path.exists() else "",
    }


def _compact_source(row: Mapping[str, Any]) -> dict[str, Any]:
    keys = ("sleeve_id", "status", "current_status", "run_status", "evaluation_status", "enabled", "activation_status", "canonical_blocker", "reason_codes", "producer_command", "runner_command", "exit_code", "stdout_summary", "stderr_summary", "started_at_utc", "completed_at_utc")
    return {key: row.get(key) for key in keys if key in row}


def _compact_contract(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "sleeve_id": row.get("sleeve_id"),
        "enabled": row.get("enabled"),
        "contract_status": row.get("contract_status"),
        "allowed_symbols": row.get("allowed_symbols"),
        "required_input_count": len(_list(row.get("required_inputs"))),
        "optional_input_count": len(_list(row.get("optional_inputs"))),
        "candidate_generation_policy": row.get("candidate_generation_policy"),
    }


def _matching_rows(rows: list[dict[str, Any]], sleeve_id: str, hypothesis_id: str) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        text = json.dumps(row, sort_keys=True, default=str)
        if _text(row.get("sleeve_id") or row.get("engine_id")) == sleeve_id or (hypothesis_id and _text(row.get("hypothesis_id")) == hypothesis_id):
            out.append(row)
        elif sleeve_id and sleeve_id in text:
            out.append(row)
        elif hypothesis_id and hypothesis_id in text:
            out.append(row)
    return out


def _first_match(rows: list[dict[str, Any]], sleeve_id: str, hypothesis_id: str) -> dict[str, Any]:
    matches = _matching_rows(rows, sleeve_id, hypothesis_id)
    return matches[0] if matches else {}


def _rows(payload: Any, *keys: str) -> list[dict[str, Any]]:
    data = _dict(payload)
    for key in keys:
        value = data.get(key)
        if isinstance(value, list):
            return [dict(row) for row in value if isinstance(row, Mapping)]
    return []


def _parse_json_tail(text: str) -> dict[str, Any]:
    if not text:
        return {}
    start = text.find("{")
    if start < 0:
        return {}
    try:
        obj = json.loads(text[start:])
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _missing_paths_from_text(text: str) -> list[str]:
    if not text:
        return []
    return re.findall(r"/[^\s;,\"]+", text)


def _evidence_text(*rows: Mapping[str, Any]) -> str:
    return json.dumps(rows, sort_keys=True, default=str).upper()


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [_text(item) for item in value if _text(item)]
    if _text(value):
        return [_text(value)]
    return []


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _text(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _text(value).upper()
