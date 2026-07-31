from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1

FAMILY = "aegis_event_dislocation_governance_trace_diagnostic_v1"
FILENAME = "event_dislocation_governance_trace_diagnostic.v1.json"
POLICY_VERSION = "AEGIS_EVENT_DISLOCATION_GOVERNANCE_TRACE_DIAGNOSTIC_READ_ONLY_V1"
SLEEVE_ID = "C2_EVENT_DISLOCATION_V1"
SLEEVE_NAME = "Event Dislocation Repricing"

FAILURE_CLASSIFICATIONS = {
    "FIELD_MISSING_FROM_SIGNAL",
    "FIELD_PRESENT_IN_SIGNAL_NOT_MAPPED_TO_CANDIDATE",
    "FIELD_PRESENT_IN_CANDIDATE_INVALID_VALUE",
    "INSTRUMENT_NOT_IN_GOVERNED_REGISTRY",
    "INSTRUMENT_TYPE_VALUE_NOT_GOVERNED",
    "GOVERNANCE_STATUS_VALUE_NOT_GOVERNED",
    "DIRECTION_VALUE_NOT_GOVERNED",
    "CONTRACT_SCHEMA_MISMATCH",
    "BUILDER_OUTPUT_CONTRACT_INPUT_MISMATCH",
    "UNKNOWN_DETERMINISTIC_BLOCKER",
}

GOVERNED_DIRECTIONS = {"LONG", "SHORT", "BUY", "SELL", "CREDIT", "DEBIT"}
GOVERNED_GOVERNANCE_STATUSES = {"GOVERNED", "RESEARCH_ONLY", "PAPER_ELIGIBLE"}
CONTRACT_INPUT_FIELDS = (
    "raw_signal_id",
    "intent_id",
    "sleeve_id",
    "symbol",
    "direction",
    "instrument_type",
    "entry_reference_price",
    "risk_per_trade",
    "stop_price",
    "stop_loss_bps",
    "executable_status",
    "governance_status",
)

SAFETY = {
    "read_only": True,
    "diagnostics_only": True,
    "no_strategy_logic_mutation": True,
    "no_threshold_mutation": True,
    "no_candidate_scoring_mutation": True,
    "no_risk_policy_mutation": True,
    "no_quality_mutation": True,
    "no_allocation_mutation": True,
    "no_signal_fabrication": True,
    "no_candidate_fabrication": True,
    "no_candidate_mutation": True,
    "no_repair_performed": True,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
}


def event_dislocation_governance_trace_diagnostic_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, day_utc, FILENAME)


def build_event_dislocation_governance_trace_diagnostic_v1(
    *, truth_root: Path | str, repo_root: Path | str | None = None, day_utc: str, computed_at_utc: str | None = None
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    repo = Path(repo_root or Path.cwd()).expanduser().resolve()
    day = str(day_utc)
    computed_at = computed_at_utc or f"{day}T00:00:00Z"
    paths = _input_paths(root, repo, day)
    payloads = {name: read_json_v1(path) for name, path in paths.items()}

    signal_rows = _event_rows(_rows(payloads.get("signal_evidence_graph"), "signals", "raw_signals"))
    rejected_rows = _event_rows(_rows(payloads.get("candidate_contracts"), "rejected_raw_signals", "rejected_candidates"))
    pre_contract_suppressed_rows = _event_rows(_rows(payloads.get("candidate_contracts"), "pre_contract_suppressed_raw_signals"))
    signal_by_id = {_signal_id(row): row for row in signal_rows if _signal_id(row)}
    governed_values = _governed_instrument_type_values(payloads)

    traces: list[dict[str, Any]] = []
    for row in rejected_rows:
        signal_id = _signal_id(row)
        signal_row = signal_by_id.get(signal_id, {})
        signal_path = _text(row.get("evidence_path") or signal_row.get("evidence_path") or row.get("source_artifact_path") or signal_row.get("source_artifact_path"))
        raw_signal = _read_if_exists(signal_path)
        lookup = _instrument_registry_lookup(payloads, signal_row or row, governed_values)
        contract_input = _contract_input(row)
        raw_fields = _raw_signal_fields(raw_signal)
        classification = _classify_trace(
            signal_row=signal_row,
            contract_row=row,
            raw_signal=raw_signal,
            lookup=lookup,
            governed_instrument_types=governed_values,
        )
        missing_fields = _missing_fields(signal_row, row, contract_input)
        invalid_fields = _invalid_fields(contract_input, governed_values, lookup)
        ambiguous_fields = _ambiguous_fields(signal_row, raw_signal)
        rejection_codes = _rejection_codes(row, signal_row)
        trace = {
            "signal_id": signal_id,
            "signal_path": signal_path,
            "signal_instrument": _symbol(signal_row or row or raw_signal),
            "signal_action": _signal_action(signal_row, raw_signal),
            "signal_direction": _text(signal_row.get("direction") or raw_fields.get("direction")),
            "signal_confidence": _text(signal_row.get("confidence") or raw_fields.get("confidence")),
            "raw_signal_fields": raw_fields,
            "candidate_draft_id": _text(row.get("candidate_id") or row.get("raw_intent_id") or row.get("intent_id")),
            "candidate_draft_fields": _candidate_draft_fields(signal_row),
            "candidate_contract_input": contract_input,
            "candidate_direction": _text(contract_input.get("direction")),
            "candidate_governance_status": _text(contract_input.get("governance_status")),
            "candidate_instrument": _text(contract_input.get("symbol")),
            "candidate_instrument_type": _text(contract_input.get("instrument_type")),
            "instrument_registry_lookup_attempted": True,
            "instrument_registry_lookup_key": lookup["lookup_key"],
            "instrument_registry_lookup_result": lookup,
            "governed_instrument_type_values": governed_values,
            "contract_schema_validation_result": "MISSING_FIELDS" if missing_fields else "PASS",
            "governance_validation_result": "REJECTED" if _text(row.get("rejection_reason")) else "UNKNOWN",
            "rejection_reason_codes": rejection_codes,
            "missing_fields": missing_fields,
            "invalid_fields": invalid_fields,
            "ambiguous_fields": ambiguous_fields,
            "exact_rejection_stage": "CANDIDATE_CONTRACT_VALIDATION",
            "exact_rejection_message": _exact_message(row, missing_fields, invalid_fields, lookup),
            "failure_classification": classification,
            "owner": _owner(classification, lookup),
            "david_action_required": False,
        }
        traces.append(trace)

    classification_counts = Counter(_text(row.get("failure_classification")) for row in traces)
    missing_field_counts = Counter(field for row in traces for field in _list(row.get("missing_fields")))
    invalid_field_counts = Counter(field for row in traces for field in _list(row.get("invalid_fields")))
    aggregate_classification = _aggregate_classification(classification_counts)
    post_repair_status = "UNGOVERNED_SIGNALS_SUPPRESSED" if pre_contract_suppressed_rows and not rejected_rows else ("CANDIDATE_CONTRACT_REJECTED" if rejected_rows else "NO_REJECTED_CANDIDATE_ATTEMPTS")
    payload: dict[str, Any] = {
        "schema_id": "aegis_event_dislocation_governance_trace_diagnostic",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "policy_version": POLICY_VERSION,
        "day_utc": day,
        "target_day": day,
        "computed_at_utc": computed_at,
        "sleeve_id": SLEEVE_ID,
        "sleeve_name": SLEEVE_NAME,
        "rejected_candidate_attempt_count": len(traces),
        "signal_count": len({_text(row.get("signal_id")) for row in traces if _text(row.get("signal_id"))}),
        "classification": aggregate_classification,
        "post_repair_status": post_repair_status,
        "pre_contract_suppressed_signal_count": _unique_signal_count(pre_contract_suppressed_rows),
        "pre_contract_suppression_reasons": sorted({_text(row.get("pre_contract_suppression_reason") or row.get("rejection_reason")) for row in pre_contract_suppressed_rows if _text(row.get("pre_contract_suppression_reason") or row.get("rejection_reason"))}),
        "classification_counts": dict(sorted(classification_counts.items())),
        "missing_field_counts": dict(sorted(missing_field_counts.items())),
        "invalid_field_counts": dict(sorted(invalid_field_counts.items())),
        "governed_instrument_type_values": governed_values,
        "representative_rejected_candidate": traces[0] if traces else {},
        "candidate_traces": traces,
        "answer": _answer(traces, aggregate_classification, pre_contract_suppressed_rows),
        "root_cause_source": _root_cause_source(aggregate_classification, traces),
        "owner": _owner(aggregate_classification, traces[0]["instrument_registry_lookup_result"] if traces else {}),
        "david_action_required": False,
        "source_artifact_paths": {name: str(path) for name, path in sorted(paths.items()) if path.exists()},
        "source_artifact_hashes": {name: file_hash_v1(path) for name, path in sorted(paths.items()) if path.exists()},
        "safety_statement": "T07A is read-only governance trace diagnostics. It reads signal, candidate contract, and governance registry evidence only; it does not repair, normalize, mutate, fabricate, score, allocate, or enable broker/live/autonomous trading.",
        **SAFETY,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = stable_hash_v1({**payload, "content_hash": ""})
    return payload


def write_event_dislocation_governance_trace_diagnostic_v1(
    *, truth_root: Path | str, repo_root: Path | str | None = None, day_utc: str, payload: dict[str, Any] | None = None
) -> Path:
    body = payload or build_event_dislocation_governance_trace_diagnostic_v1(truth_root=truth_root, repo_root=repo_root, day_utc=day_utc)
    return write_json_v1(event_dislocation_governance_trace_diagnostic_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _input_paths(root: Path, repo: Path, day: str) -> dict[str, Path]:
    return {
        "signal_evidence_graph": report_path_v1(root, "aegis_signal_evidence_graph_v1", day, "signal_evidence_graph.v1.json"),
        "candidate_contracts": report_path_v1(root, "aegis_candidate_contracts_v1", day, "candidate_contracts.v1.json"),
        "t06_candidate_suppression": report_path_v1(root, "aegis_event_dislocation_candidate_suppression_diagnostics_v1", day, "event_dislocation_candidate_suppression_diagnostics.v1.json"),
        "engine_registry": repo / "governance" / "02_REGISTRIES" / "ENGINE_MODEL_REGISTRY_V1.json",
        "risk_policy": repo / "governance" / "02_REGISTRIES" / "C2_RISK_POLICY_REGISTRY_V1.json",
        "equity_structure_policy": repo / "governance" / "02_REGISTRIES" / "C2_EQUITY_STRUCTURE_POLICY_V1.json",
        "options_intent_policy": repo / "governance" / "02_REGISTRIES" / "C2_EXPOSURE_TO_OPTIONS_INTENT_POLICY_V1.json",
    }


def _contract_input(row: Mapping[str, Any]) -> dict[str, Any]:
    return {field: _text(row.get(field)) for field in CONTRACT_INPUT_FIELDS}


def _candidate_draft_fields(signal_row: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "raw_signal_id",
        "intent_id",
        "sleeve_id",
        "symbol",
        "signal_type",
        "direction",
        "instrument_type",
        "risk_per_trade",
        "executable_status",
        "governance_status",
        "candidate_contract_status",
        "missing_candidate_fields",
        "rejection_stage",
        "rejection_reason",
    )
    return {key: signal_row.get(key, "" if key != "missing_candidate_fields" else []) for key in keys}


def _raw_signal_fields(raw_signal: Mapping[str, Any]) -> dict[str, Any]:
    if not raw_signal:
        return {}
    keys = (
        "intent_id",
        "raw_signal_id",
        "sleeve_id",
        "engine_id",
        "symbol",
        "symbol_or_pair",
        "action",
        "allowed_action",
        "direction",
        "instrument_type",
        "exposure_type",
        "governance_status",
        "confidence",
    )
    out = {key: raw_signal.get(key) for key in keys if key in raw_signal}
    constraints = raw_signal.get("constraints")
    if isinstance(constraints, dict):
        out["constraints"] = constraints
    return out


def _instrument_registry_lookup(payloads: Mapping[str, Any], row: Mapping[str, Any], governed_values: list[str]) -> dict[str, Any]:
    sleeve_id = _upper(row.get("sleeve_id") or SLEEVE_ID)
    symbol = _symbol(row)
    engine_row = _engine_row(payloads.get("engine_registry"), sleeve_id)
    engine_allowed_symbols = [_upper(item) for item in _list(engine_row.get("allowed_symbols"))]
    policy_rows = _policy_rows(payloads)
    matching_policy_rows = [policy for policy in policy_rows if _upper(policy.get("engine_id") or policy.get("sleeve_id")) == sleeve_id]
    symbol_in_allowed = bool(symbol and symbol in engine_allowed_symbols) if engine_allowed_symbols else False
    instrument_type = ""
    if matching_policy_rows:
        instrument_type = _policy_instrument_type(matching_policy_rows[0])
    return {
        "lookup_key": f"{sleeve_id}:{symbol or 'UNKNOWN_SYMBOL'}",
        "sleeve_id": sleeve_id,
        "symbol": symbol,
        "engine_registry_found": bool(engine_row),
        "engine_allowed_symbols": engine_allowed_symbols,
        "symbol_in_engine_allowed_symbols": symbol_in_allowed,
        "structure_policy_found": bool(matching_policy_rows),
        "matching_policy_count": len(matching_policy_rows),
        "governed_policy_row_found": bool(matching_policy_rows and instrument_type in governed_values),
        "resolved_instrument_type": instrument_type,
        "result": "FOUND" if matching_policy_rows and instrument_type and symbol_in_allowed else "NOT_FOUND",
    }


def _governed_instrument_type_values(payloads: Mapping[str, Any]) -> list[str]:
    values: set[str] = set()
    for row in _policy_rows(payloads):
        value = _policy_instrument_type(row)
        if value:
            values.add(value)
    return sorted(values)


def _policy_rows(payloads: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for name in ("equity_structure_policy", "options_intent_policy"):
        payload = payloads.get(name)
        if not isinstance(payload, dict):
            continue
        for key in ("engine_policies", "sleeves", "policies"):
            value = payload.get(key)
            if isinstance(value, list):
                rows.extend([item for item in value if isinstance(item, dict)])
            elif isinstance(value, dict):
                for policy_id, item in value.items():
                    if isinstance(item, dict):
                        rows.append({**item, "engine_id": item.get("engine_id") or item.get("sleeve_id") or policy_id})
    return rows


def _policy_instrument_type(row: Mapping[str, Any]) -> str:
    return _upper(row.get("instrument_type") or row.get("exposure_type") or _safe_dict(row.get("exposure_requirements")).get("exposure_type"))


def _engine_row(payload: Any, sleeve_id: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    for key in ("engines", "engine_models", "models", "rows"):
        for row in _list(payload.get(key)):
            if isinstance(row, dict) and _upper(row.get("engine_id") or row.get("sleeve_id")) == sleeve_id:
                return row
    for key, row in payload.items():
        if isinstance(row, dict) and _upper(row.get("engine_id") or row.get("sleeve_id") or key) == sleeve_id:
            return row
    return {}


def _classify_trace(
    *,
    signal_row: Mapping[str, Any],
    contract_row: Mapping[str, Any],
    raw_signal: Mapping[str, Any],
    lookup: Mapping[str, Any],
    governed_instrument_types: list[str],
) -> str:
    contract_input = _contract_input(contract_row)
    missing_fields = _missing_fields(signal_row, contract_row, contract_input)
    if any(not field.startswith("candidate.") for field in missing_fields):
        return "CONTRACT_SCHEMA_MISMATCH"
    if _text(contract_input.get("instrument_type")) and _upper(contract_input.get("instrument_type")) not in set(governed_instrument_types):
        return "INSTRUMENT_TYPE_VALUE_NOT_GOVERNED"
    if _text(contract_input.get("governance_status")) and _upper(contract_input.get("governance_status")) not in GOVERNED_GOVERNANCE_STATUSES:
        return "GOVERNANCE_STATUS_VALUE_NOT_GOVERNED"
    if _text(contract_input.get("direction")) and _upper(contract_input.get("direction")) not in GOVERNED_DIRECTIONS:
        return "DIRECTION_VALUE_NOT_GOVERNED"
    if _signal_has_candidate_fields(signal_row, raw_signal) and not _contract_has_candidate_fields(contract_input):
        if lookup.get("result") == "NOT_FOUND" and not _signal_has_full_candidate_fields(signal_row, raw_signal):
            return "INSTRUMENT_NOT_IN_GOVERNED_REGISTRY"
        return "FIELD_PRESENT_IN_SIGNAL_NOT_MAPPED_TO_CANDIDATE"
    if _text(contract_input.get("symbol")) and lookup.get("result") == "NOT_FOUND" and _contract_has_candidate_fields(contract_input):
        return "INSTRUMENT_NOT_IN_GOVERNED_REGISTRY"
    if any(field in missing_fields for field in ("candidate.direction", "candidate.instrument_type", "candidate.governance_status")):
        return "FIELD_MISSING_FROM_SIGNAL"
    if _invalid_fields(contract_input, governed_instrument_types, lookup):
        return "FIELD_PRESENT_IN_CANDIDATE_INVALID_VALUE"
    return "UNKNOWN_DETERMINISTIC_BLOCKER"


def _missing_fields(signal_row: Mapping[str, Any], contract_row: Mapping[str, Any], contract_input: Mapping[str, Any]) -> list[str]:
    fields: set[str] = set()
    for source in (signal_row, contract_row):
        fields.update(_list(source.get("missing_candidate_fields")))
        fields.update(_list(source.get("missing_contract_fields")))
        fields.update(_list(source.get("failed_candidate_contract_fields")))
    for field in ("direction", "instrument_type", "governance_status"):
        if not _text(contract_input.get(field)):
            fields.add(f"candidate.{field}")
    return sorted(str(field) for field in fields if _text(field))


def _invalid_fields(contract_input: Mapping[str, Any], governed_instrument_types: list[str], lookup: Mapping[str, Any]) -> list[str]:
    invalid: list[str] = []
    instrument_type = _upper(contract_input.get("instrument_type"))
    governance_status = _upper(contract_input.get("governance_status"))
    direction = _upper(contract_input.get("direction"))
    if instrument_type and instrument_type not in set(governed_instrument_types):
        invalid.append("candidate.instrument_type")
    if governance_status and governance_status not in GOVERNED_GOVERNANCE_STATUSES:
        invalid.append("candidate.governance_status")
    if direction and direction not in GOVERNED_DIRECTIONS:
        invalid.append("candidate.direction")
    if lookup.get("result") == "NOT_FOUND" and _text(contract_input.get("symbol")) and _contract_has_candidate_fields(contract_input):
        invalid.append("candidate.instrument")
    return sorted(set(invalid))


def _ambiguous_fields(signal_row: Mapping[str, Any], raw_signal: Mapping[str, Any]) -> list[str]:
    ambiguous: list[str] = []
    raw_direction = _upper(raw_signal.get("direction"))
    graph_direction = _upper(signal_row.get("direction"))
    if raw_direction and graph_direction and raw_direction != graph_direction:
        ambiguous.append("candidate.direction")
    raw_instrument_type = _upper(raw_signal.get("instrument_type") or raw_signal.get("exposure_type"))
    graph_instrument_type = _upper(signal_row.get("instrument_type"))
    if raw_instrument_type and graph_instrument_type and raw_instrument_type != graph_instrument_type:
        ambiguous.append("candidate.instrument_type")
    return ambiguous


def _rejection_codes(contract_row: Mapping[str, Any], signal_row: Mapping[str, Any]) -> list[str]:
    codes: set[str] = set()
    for key in ("rejection_reason", "market_data_status", "stop_price_missing_reason"):
        if _text(contract_row.get(key)):
            codes.add(_text(contract_row.get(key)))
    for key in ("detail_reason_codes", "reason_codes", "entry_reference_price_certification_reason_codes"):
        codes.update(_list(contract_row.get(key)))
    if _text(signal_row.get("rejection_reason")):
        codes.add(_text(signal_row.get("rejection_reason")))
    return sorted(codes)


def _exact_message(row: Mapping[str, Any], missing_fields: list[str], invalid_fields: list[str], lookup: Mapping[str, Any]) -> str:
    reason = _text(row.get("rejection_reason")) or "UNKNOWN_REJECTION_REASON"
    if "candidate.instrument_type" in missing_fields:
        return (
            f"candidate.instrument_type is empty in the candidate contract input, so candidate_contracts_v1 selected {reason}. "
            f"Also missing: {', '.join(missing_fields)}. Governance lookup {lookup.get('lookup_key')} returned {lookup.get('result')}."
        )
    if invalid_fields:
        return f"Candidate contract input contains invalid governed fields {', '.join(invalid_fields)}; rejection reason is {reason}."
    if missing_fields:
        return f"Candidate contract input is missing {', '.join(missing_fields)}; rejection reason is {reason}."
    return f"Candidate contract was rejected with {reason}; no missing governed field was detected by this trace."


def _answer(traces: list[Mapping[str, Any]], classification: str, pre_contract_suppressed_rows: list[Mapping[str, Any]] | None = None) -> str:
    if not traces:
        if pre_contract_suppressed_rows:
            return "No Event Dislocation rejected candidate attempts were found after repair; ungoverned signals were suppressed before candidate contract validation."
        return "No Event Dislocation rejected candidate attempts were found in candidate_contracts_v1 for the target day."
    rep = traces[0]
    contract_input = _safe_dict(rep.get("candidate_contract_input"))
    missing = ", ".join(_list(rep.get("missing_fields"))) or "none"
    return (
        "Event Dislocation candidate governance rejects because candidate.instrument_type is empty in the exact candidate contract input. "
        f"candidate.direction={contract_input.get('direction')!r}, candidate.governance_status={contract_input.get('governance_status')!r}, "
        f"candidate.instrument_type={contract_input.get('instrument_type')!r}; missing_fields={missing}; aggregate_classification={classification}."
    )


def _root_cause_source(classification: str, traces: list[Mapping[str, Any]]) -> str:
    if classification == "FIELD_PRESENT_IN_SIGNAL_NOT_MAPPED_TO_CANDIDATE":
        return "candidate_builder_mapping"
    if classification == "CONTRACT_SCHEMA_MISMATCH":
        return "candidate_contract_schema"
    if classification in {"INSTRUMENT_NOT_IN_GOVERNED_REGISTRY", "INSTRUMENT_TYPE_VALUE_NOT_GOVERNED", "GOVERNANCE_STATUS_VALUE_NOT_GOVERNED", "DIRECTION_VALUE_NOT_GOVERNED"}:
        return "governance_registry"
    if classification == "FIELD_MISSING_FROM_SIGNAL":
        if traces and not _safe_dict(traces[0].get("instrument_registry_lookup_result")).get("structure_policy_found"):
            return "governance_registry"
        return "signal_producer_output"
    return "unknown"


def _owner(classification: str, lookup: Mapping[str, Any]) -> str:
    if classification in {"INSTRUMENT_NOT_IN_GOVERNED_REGISTRY", "INSTRUMENT_TYPE_VALUE_NOT_GOVERNED", "GOVERNANCE_STATUS_VALUE_NOT_GOVERNED", "DIRECTION_VALUE_NOT_GOVERNED"}:
        return "AEGIS_GOVERNANCE_REGISTRY"
    if classification == "FIELD_PRESENT_IN_SIGNAL_NOT_MAPPED_TO_CANDIDATE":
        return "AEGIS_CANDIDATE_MAPPING"
    if classification == "CONTRACT_SCHEMA_MISMATCH":
        return "AEGIS_CANDIDATE_CONTRACTS"
    if classification == "FIELD_MISSING_FROM_SIGNAL" and not lookup.get("structure_policy_found"):
        return "AEGIS_GOVERNANCE_REGISTRY"
    return "AEGIS_SYSTEM"


def _aggregate_classification(counts: Counter[str]) -> str:
    if not counts:
        return "UNKNOWN_DETERMINISTIC_BLOCKER"
    classification, _count = counts.most_common(1)[0]
    return classification if classification in FAILURE_CLASSIFICATIONS else "UNKNOWN_DETERMINISTIC_BLOCKER"


def _signal_has_candidate_fields(signal_row: Mapping[str, Any], raw_signal: Mapping[str, Any]) -> bool:
    for field in ("direction", "instrument_type", "governance_status"):
        if _text(signal_row.get(field)) or _text(raw_signal.get(field)):
            return True
    if _text(raw_signal.get("exposure_type")):
        return True
    return False


def _signal_has_full_candidate_fields(signal_row: Mapping[str, Any], raw_signal: Mapping[str, Any]) -> bool:
    direction = _text(signal_row.get("direction") or raw_signal.get("direction"))
    instrument_type = _text(signal_row.get("instrument_type") or raw_signal.get("instrument_type"))
    governance_status = _text(signal_row.get("governance_status") or raw_signal.get("governance_status"))
    return bool(direction and instrument_type and governance_status)


def _contract_has_candidate_fields(contract_input: Mapping[str, Any]) -> bool:
    return bool(_text(contract_input.get("direction")) or _text(contract_input.get("instrument_type")) or _text(contract_input.get("governance_status")))


def _signal_action(signal_row: Mapping[str, Any], raw_signal: Mapping[str, Any]) -> str:
    return _text(raw_signal.get("action") or raw_signal.get("allowed_action") or signal_row.get("action") or signal_row.get("signal_action"))


def _read_if_exists(path_text: str) -> dict[str, Any]:
    if not path_text:
        return {}
    path = Path(path_text).expanduser()
    if not path.exists():
        return {}
    return read_json_v1(path)


def _event_rows(rows: list[Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict) and _upper(row.get("sleeve_id") or row.get("engine_id")) == SLEEVE_ID:
            out.append(row)
    return out


def _rows(payload: Any, *keys: str) -> list[Any]:
    if not isinstance(payload, dict):
        return []
    out: list[Any] = []
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            out.extend(value)
    return out


def _unique_signal_count(rows: list[Mapping[str, Any]]) -> int:
    ids = {_signal_id(row) for row in rows if _signal_id(row)}
    return len(ids) if ids else len(rows)


def _signal_id(row: Mapping[str, Any]) -> str:
    return _text(row.get("raw_signal_id") or row.get("signal_id") or row.get("intent_id") or row.get("raw_intent_id"))


def _symbol(row: Mapping[str, Any]) -> str:
    return _upper(row.get("symbol") or row.get("symbol_or_pair") or row.get("instrument"))


def _safe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    return [value]


def _text(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _text(value).upper()
