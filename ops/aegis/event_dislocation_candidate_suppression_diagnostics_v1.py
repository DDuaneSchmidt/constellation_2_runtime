from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1

FAMILY = "aegis_event_dislocation_candidate_suppression_diagnostics_v1"
FILENAME = "event_dislocation_candidate_suppression_diagnostics.v1.json"
POLICY_VERSION = "AEGIS_EVENT_DISLOCATION_CANDIDATE_SUPPRESSION_DIAGNOSTICS_READ_ONLY_V1"
SLEEVE_ID = "C2_EVENT_DISLOCATION_V1"
SLEEVE_NAME = "Event Dislocation Repricing"

SUPPRESSION_CODES = {
    "SIGNALS_PRESENT_BUT_CANDIDATE_BUILDER_NOT_INVOKED",
    "SIGNAL_VALIDATION_FAILED",
    "SIGNAL_SCHEMA_INVALID",
    "SIGNAL_CONFIDENCE_BELOW_MINIMUM",
    "SIGNAL_QUALITY_GATE_FAILED",
    "CANDIDATE_CONSTRUCTION_POLICY_MISSING",
    "CANDIDATE_CONSTRUCTION_FAILED",
    "CANDIDATE_CONTRACT_REJECTED",
    "RISK_POLICY_REJECTED",
    "DUPLICATE_SUPPRESSED",
    "COOLDOWN_ACTIVE",
    "EXPOSURE_LIMIT_REACHED",
    "DATA_QUALITY_GATE_FAILED",
    "LINEAGE_MISMATCH",
    "UNGOVERNED_SIGNALS_SUPPRESSED",
    "UNKNOWN_DETERMINISTIC_BLOCKER",
}

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


def event_dislocation_candidate_suppression_diagnostics_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, day_utc, FILENAME)


def build_event_dislocation_candidate_suppression_diagnostics_v1(
    *, truth_root: Path | str, repo_root: Path | str | None = None, day_utc: str, computed_at_utc: str | None = None
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    repo = Path(repo_root or Path.cwd()).expanduser().resolve()
    day = str(day_utc)
    computed_at = computed_at_utc or f"{day}T00:00:00Z"
    paths = _input_paths(root, repo, day)
    payloads = {name: read_json_v1(path) for name, path in paths.items()}

    signals = _event_signals(payloads)
    raw_signal_ids = sorted({_signal_id(row) for row in signals if _signal_id(row)})
    signal_source_paths = sorted({_text(row.get("evidence_path") or row.get("source_artifact_path") or row.get("raw_intent_path")) for row in signals if _text(row.get("evidence_path") or row.get("source_artifact_path") or row.get("raw_intent_path"))})
    schema_missing = _schema_missing(signals)
    lineage_missing = _lineage_missing(signals)
    confidence_failures = _confidence_failures(signals)
    quality_failures = _quality_failures(signals)

    valid_contracts = _event_rows(_rows(payloads.get("candidate_contracts"), "candidate_contracts", "contracts", "candidates"))
    rejected_contracts = _event_rows(_rows(payloads.get("candidate_contracts"), "rejected_raw_signals", "rejected_candidates"))
    pre_contract_suppressed = _event_rows(_rows(payloads.get("candidate_contracts"), "pre_contract_suppressed_raw_signals"))
    signal_suppressed = [row for row in signals if _upper(row.get("candidate_contract_status")) == "SUPPRESSED" and _upper(row.get("pre_contract_suppression_reason") or row.get("rejection_reason")) == "UNGOVERNED_SYMBOL_SUPPRESSED"]
    raw_rejections = _event_rows(_rows(payloads.get("candidate_generation_diagnostics"), "raw_signal_rejections", "rejected_raw_signal_details"))
    suppressed_signal_ids = {_signal_id(row) for row in pre_contract_suppressed + signal_suppressed if _signal_id(row)}
    if suppressed_signal_ids:
        raw_rejections = [row for row in raw_rejections if _signal_id(row) not in suppressed_signal_ids]
    manifest_rows = _event_rows(_manifest_candidate_rows(root, day))
    attempted_rows = [row for row in manifest_rows if _upper(row.get("status")) in {"CANDIDATE_CREATED", "FAILED", "BLOCKED", "REJECTED"} or _text(row.get("raw_intent_id"))]
    candidate_generation_invoked = bool(attempted_rows or rejected_contracts or raw_rejections or pre_contract_suppressed or signal_suppressed)
    construction_policy_exists = _construction_policy_exists(paths, rejected_contracts)

    duplicate_rows = _matching_reason_rows(rejected_contracts + raw_rejections + attempted_rows, {"DUPLICATE", "DUPLICATE_SUPPRESSED"})
    cooldown_rows = _matching_reason_rows(rejected_contracts + raw_rejections + attempted_rows, {"COOLDOWN", "COOLDOWN_ACTIVE"})
    exposure_rows = _matching_reason_rows(rejected_contracts + raw_rejections + attempted_rows, {"EXPOSURE_LIMIT", "EXPOSURE_LIMIT_REACHED"})
    data_quality_rows = _matching_reason_rows(rejected_contracts + raw_rejections + attempted_rows, {"DATA_QUALITY", "DATA_QUALITY_GATE_FAILED", "STALE_MARKET_DATA", "MISSING_MARKET_DATA"})
    risk_rows = _matching_reason_rows(rejected_contracts + raw_rejections + attempted_rows, {"RISK_POLICY", "RISK_POLICY_REJECTED"})
    contract_reject_rows = [row for row in rejected_contracts if _upper(row.get("contract_validation_status")) == "REJECTED" or _text(row.get("rejection_reason"))]
    construction_fail_rows = [row for row in attempted_rows if _upper(row.get("status")) == "FAILED" and not _matching_reason_rows([row], {"DUPLICATE", "COOLDOWN", "EXPOSURE_LIMIT", "DATA_QUALITY", "RISK_POLICY"})]

    suppression_code, suppression_stage, suppression_reason = _classify(
        raw_signal_count=len(raw_signal_ids) if raw_signal_ids else len(signals),
        schema_missing=schema_missing,
        lineage_missing=lineage_missing,
        confidence_failures=confidence_failures,
        quality_failures=quality_failures,
        candidate_generation_invoked=candidate_generation_invoked,
        construction_policy_exists=construction_policy_exists,
        construction_fail_rows=construction_fail_rows,
        contract_reject_rows=contract_reject_rows,
        duplicate_rows=duplicate_rows,
        cooldown_rows=cooldown_rows,
        exposure_rows=exposure_rows,
        data_quality_rows=data_quality_rows,
        risk_rows=risk_rows,
        raw_rejections=raw_rejections,
        pre_contract_suppressed=pre_contract_suppressed or signal_suppressed,
    )
    post_status = _post_status(suppression_code, len(valid_contracts))
    missing_fields = sorted({field for row in rejected_contracts + raw_rejections + signals for field in _list(row.get("missing_contract_fields")) + _list(row.get("failed_candidate_contract_fields")) + _list(row.get("missing_candidate_fields")) + _list(row.get("missing_fields"))})
    required_inputs = _required_inputs(signals, payloads)
    candidate_ids = sorted({_text(row.get("candidate_id")) for row in valid_contracts if _text(row.get("candidate_id"))})
    attempted_candidate_ids = sorted({_text(row.get("candidate_id") or row.get("raw_intent_id")) for row in attempted_rows + rejected_contracts + raw_rejections if _text(row.get("candidate_id") or row.get("raw_intent_id"))})

    payload: dict[str, Any] = {
        "schema_id": "aegis_event_dislocation_candidate_suppression_diagnostics",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "policy_version": POLICY_VERSION,
        "day_utc": day,
        "target_day": day,
        "computed_at_utc": computed_at,
        "sleeve_id": SLEEVE_ID,
        "sleeve_name": SLEEVE_NAME,
        "raw_signal_count": len(raw_signal_ids) if raw_signal_ids else len(signals),
        "signal_ids": raw_signal_ids,
        "signal_source_paths": signal_source_paths,
        "signal_validation_status": _signal_validation_status(schema_missing, lineage_missing, confidence_failures, quality_failures),
        "candidate_generation_invoked": candidate_generation_invoked,
        "candidate_generation_status": _candidate_generation_status(candidate_generation_invoked, suppression_code, len(valid_contracts), rejected_contracts, raw_rejections),
        "candidate_count": len(valid_contracts),
        "candidate_ids": candidate_ids,
        "candidate_attempt_count": len(attempted_candidate_ids),
        "candidate_attempt_ids": attempted_candidate_ids,
        "candidate_suppression_count": _suppression_count(duplicate_rows, cooldown_rows, exposure_rows, data_quality_rows, pre_contract_suppressed or signal_suppressed),
        "candidate_rejection_count": _unique_signal_count(rejected_contracts + raw_rejections + construction_fail_rows),
        "furthest_stage_reached": _furthest_stage(candidate_generation_invoked, len(valid_contracts), rejected_contracts, raw_rejections),
        "suppression_stage": suppression_stage,
        "suppression_code": suppression_code,
        "suppression_reason": suppression_reason,
        "rejected_by_gate": bool(raw_rejections or rejected_contracts or construction_fail_rows or pre_contract_suppressed or signal_suppressed),
        "rejected_by_policy": suppression_code in {"CANDIDATE_CONSTRUCTION_POLICY_MISSING", "CANDIDATE_CONTRACT_REJECTED", "RISK_POLICY_REJECTED"},
        "rejected_by_contract": bool(contract_reject_rows),
        "rejected_by_risk": bool(risk_rows) or suppression_code == "RISK_POLICY_REJECTED",
        "rejected_by_duplicate_control": bool(duplicate_rows),
        "rejected_by_cooldown": bool(cooldown_rows),
        "rejected_by_exposure_limit": bool(exposure_rows),
        "rejected_by_confidence_gate": bool(confidence_failures),
        "rejected_by_data_quality_gate": bool(data_quality_rows),
        "missing_fields": missing_fields,
        "required_inputs": required_inputs,
        "candidate_construction_policy_exists": construction_policy_exists,
        "post_diagnostic_status": post_status,
        "owner": _owner(suppression_code),
        "david_action_required": False,
        "answer": _answer(suppression_code, suppression_reason, len(signals), len(valid_contracts)),
        "source_artifact_paths": {name: str(path) for name, path in sorted(paths.items()) if path.exists()},
        "source_artifact_hashes": {name: file_hash_v1(path) for name, path in sorted(paths.items()) if path.exists()},
        "pre_contract_suppressed_signal_count": _unique_signal_count(pre_contract_suppressed or signal_suppressed),
        "pre_contract_suppression_reasons": sorted({_text(row.get("pre_contract_suppression_reason") or row.get("rejection_reason")) for row in pre_contract_suppressed + signal_suppressed if _text(row.get("pre_contract_suppression_reason") or row.get("rejection_reason"))}),
        "diagnostic_rows": _diagnostic_rows(signals, rejected_contracts + pre_contract_suppressed + signal_suppressed, raw_rejections),
        "safety_statement": "T06 is read-only diagnostics. It explains existing Event Dislocation signal-to-candidate suppression evidence and does not change strategy logic, thresholds, scoring, risk policy, allocation, research quality, broker/live trading, or fabricate signals/candidates.",
        **SAFETY,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = stable_hash_v1({**payload, "content_hash": ""})
    return payload


def write_event_dislocation_candidate_suppression_diagnostics_v1(
    *, truth_root: Path | str, repo_root: Path | str | None = None, day_utc: str, payload: dict[str, Any] | None = None
) -> Path:
    body = payload or build_event_dislocation_candidate_suppression_diagnostics_v1(truth_root=truth_root, repo_root=repo_root, day_utc=day_utc)
    return write_json_v1(event_dislocation_candidate_suppression_diagnostics_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _input_paths(root: Path, repo: Path, day: str) -> dict[str, Path]:
    return {
        "t01_sleeve_throughput": report_path_v1(root, "aegis_sleeve_throughput_diagnostics_v1", day, "sleeve_throughput_diagnostics.v1.json"),
        "t05_position_state_repair": report_path_v1(root, "aegis_event_dislocation_position_state_freshness_repair_v1", day, "event_dislocation_position_state_freshness_repair.v1.json"),
        "candidate_generation_diagnostics": report_path_v1(root, "aegis_candidate_generation_diagnostics_v1", day, "candidate_generation_diagnostics.v1.json"),
        "candidate_contracts": report_path_v1(root, "aegis_candidate_contracts_v1", day, "candidate_contracts.v1.json"),
        "signal_evidence_graph": report_path_v1(root, "aegis_signal_evidence_graph_v1", day, "signal_evidence_graph.v1.json"),
        "sleeve_evaluation": root / "reports" / "sleeve_evaluation_kernel_v1" / day / SLEEVE_ID / "sleeve_evaluation.v1.json",
        "sleeve_input_contracts": report_path_v1(root, "aegis_sleeve_input_contracts_v1", day, "sleeve_input_contracts.v1.json"),
        "risk_policy": repo / "governance" / "02_REGISTRIES" / "C2_RISK_POLICY_REGISTRY_V1.json",
        "equity_structure_policy": repo / "governance" / "02_REGISTRIES" / "C2_EQUITY_STRUCTURE_POLICY_V1.json",
        "options_intent_policy": repo / "governance" / "02_REGISTRIES" / "C2_EXPOSURE_TO_OPTIONS_INTENT_POLICY_V1.json",
    }


def _event_signals(payloads: Mapping[str, Any]) -> list[dict[str, Any]]:
    signals = _event_rows(_rows(payloads.get("signal_evidence_graph"), "signals", "raw_signals"))
    if signals:
        return signals
    rejections = _event_rows(_rows(payloads.get("candidate_generation_diagnostics"), "raw_signal_rejections", "rejected_raw_signal_details"))
    if rejections:
        return rejections
    return [row for row in _event_rows(_rows(payloads.get("candidate_generation_diagnostics"), "raw_signal_inventory")) if _signal_id(row)]


def _manifest_candidate_rows(root: Path, day: str) -> list[dict[str, Any]]:
    base = root / "reports" / "candidate_generation_manifest_v1" / day
    rows: list[dict[str, Any]] = []
    if not base.exists():
        return rows
    for path in sorted(base.glob("*/candidate_generation_manifest.v1.json")):
        payload = read_json_v1(path)
        for row in _rows(payload, "candidate_rows", "rows", "candidates"):
            if isinstance(row, dict):
                rows.append({**row, "manifest_path": str(path)})
    return rows


def _schema_missing(signals: list[Mapping[str, Any]]) -> list[str]:
    missing: set[str] = set()
    for signal in signals:
        if not _signal_id(signal):
            missing.add("raw_signal_id")
        if _text(signal.get("sleeve_id") or signal.get("engine_id")) != SLEEVE_ID:
            missing.add("sleeve_id")
        if not _text(signal.get("symbol") or signal.get("symbol_or_pair")):
            missing.add("symbol")
        if not _text(signal.get("evidence_path") or signal.get("raw_intent_path") or signal.get("source_artifact_path")):
            missing.add("evidence_path")
    return sorted(missing)


def _lineage_missing(signals: list[Mapping[str, Any]]) -> list[str]:
    missing: list[str] = []
    for signal in signals:
        path = _text(signal.get("evidence_path") or signal.get("raw_intent_path"))
        source_hash = _text(signal.get("source_hash") or signal.get("intent_hash") or signal.get("raw_intent_hash"))
        if not path and not source_hash:
            missing.append(_signal_id(signal) or "UNKNOWN_SIGNAL")
    return sorted(set(missing))


def _confidence_failures(signals: list[Mapping[str, Any]]) -> list[str]:
    failures: list[str] = []
    for signal in signals:
        confidence = signal.get("confidence")
        minimum = signal.get("minimum_confidence") or signal.get("min_confidence")
        if confidence is not None and minimum is not None:
            try:
                if float(confidence) < float(minimum):
                    failures.append(_signal_id(signal) or "UNKNOWN_SIGNAL")
            except (TypeError, ValueError):
                failures.append(_signal_id(signal) or "UNKNOWN_SIGNAL")
        if _upper(signal.get("confidence_status")) in {"BELOW_MINIMUM", "FAIL", "FAILED"}:
            failures.append(_signal_id(signal) or "UNKNOWN_SIGNAL")
    return sorted(set(failures))


def _quality_failures(signals: list[Mapping[str, Any]]) -> list[str]:
    failures: list[str] = []
    for signal in signals:
        if _upper(signal.get("quality_status") or signal.get("signal_quality_status")) in {"FAIL", "FAILED", "REJECTED"}:
            failures.append(_signal_id(signal) or "UNKNOWN_SIGNAL")
    return sorted(set(failures))


def _construction_policy_exists(paths: Mapping[str, Path], rejected_contracts: list[Mapping[str, Any]]) -> bool:
    if any(paths[name].exists() for name in ("risk_policy", "equity_structure_policy", "options_intent_policy")):
        return True
    return any(_text(row.get("risk_policy_source_path")) or _text(row.get("governance_policy_source_path")) for row in rejected_contracts)


def _classify(**kwargs: Any) -> tuple[str, str, str]:
    if kwargs["raw_signal_count"] <= 0:
        return "UNKNOWN_DETERMINISTIC_BLOCKER", "SIGNAL", "No raw Event Dislocation signals were found in the candidate suppression evidence."
    if kwargs["schema_missing"]:
        return "SIGNAL_SCHEMA_INVALID", "SIGNAL_VALIDATION", "Signal schema is missing required fields: " + ", ".join(kwargs["schema_missing"])
    if kwargs["lineage_missing"]:
        return "LINEAGE_MISMATCH", "SIGNAL_VALIDATION", "Signal lineage is missing for: " + ", ".join(kwargs["lineage_missing"])
    if kwargs["confidence_failures"]:
        return "SIGNAL_CONFIDENCE_BELOW_MINIMUM", "SIGNAL_VALIDATION", "Signal confidence is below minimum for: " + ", ".join(kwargs["confidence_failures"])
    if kwargs["quality_failures"]:
        return "SIGNAL_QUALITY_GATE_FAILED", "SIGNAL_VALIDATION", "Signal quality gate failed for: " + ", ".join(kwargs["quality_failures"])
    if not kwargs["candidate_generation_invoked"]:
        return "SIGNALS_PRESENT_BUT_CANDIDATE_BUILDER_NOT_INVOKED", "SIGNAL", "Raw signals are present, but no candidate construction attempt evidence exists."
    if not kwargs["construction_policy_exists"]:
        return "CANDIDATE_CONSTRUCTION_POLICY_MISSING", "CANDIDATE_CONSTRUCTION", "Candidate construction policy evidence is missing."
    if kwargs["duplicate_rows"]:
        return "DUPLICATE_SUPPRESSED", "CANDIDATE_SUPPRESSION", _reason(kwargs["duplicate_rows"][0])
    if kwargs["cooldown_rows"]:
        return "COOLDOWN_ACTIVE", "CANDIDATE_SUPPRESSION", _reason(kwargs["cooldown_rows"][0])
    if kwargs["exposure_rows"]:
        return "EXPOSURE_LIMIT_REACHED", "CANDIDATE_SUPPRESSION", _reason(kwargs["exposure_rows"][0])
    if kwargs["data_quality_rows"]:
        return "DATA_QUALITY_GATE_FAILED", "DATA_QUALITY", _reason(kwargs["data_quality_rows"][0])
    if kwargs["risk_rows"]:
        return "RISK_POLICY_REJECTED", "RISK_POLICY", _reason(kwargs["risk_rows"][0])
    if kwargs["contract_reject_rows"]:
        return "CANDIDATE_CONTRACT_REJECTED", "CANDIDATE_CONTRACT", _reason(kwargs["contract_reject_rows"][0])
    if kwargs.get("pre_contract_suppressed"):
        return "UNGOVERNED_SIGNALS_SUPPRESSED", "PRE_CONTRACT_GOVERNED_UNIVERSE", "Event Dislocation signals outside the governed universe were suppressed before candidate contract validation."
    if kwargs["construction_fail_rows"]:
        return "CANDIDATE_CONSTRUCTION_FAILED", "CANDIDATE_CONSTRUCTION", _reason(kwargs["construction_fail_rows"][0])
    if kwargs["raw_rejections"]:
        return "CANDIDATE_CONSTRUCTION_FAILED", "CANDIDATE_CONVERSION", _reason(kwargs["raw_rejections"][0])
    return "UNKNOWN_DETERMINISTIC_BLOCKER", "UNKNOWN", "Candidate suppression could not be deterministically classified from available evidence."


def _post_status(code: str, candidate_count: int) -> str:
    if candidate_count > 0:
        return "FLOWING"
    if code in {"SIGNALS_PRESENT_BUT_CANDIDATE_BUILDER_NOT_INVOKED", "UNKNOWN_DETERMINISTIC_BLOCKER"}:
        return "SIGNALS_PRESENT_BUT_NO_CANDIDATES" if code != "UNKNOWN_DETERMINISTIC_BLOCKER" else "UNKNOWN_BLOCKER"
    if code in {"CANDIDATE_CONSTRUCTION_POLICY_MISSING", "RISK_POLICY_REJECTED"}:
        return "BLOCKED_BY_POLICY"
    if code == "UNGOVERNED_SIGNALS_SUPPRESSED":
        return "UNGOVERNED_SIGNALS_SUPPRESSED"
    if code in {"SIGNAL_CONFIDENCE_BELOW_MINIMUM", "SIGNAL_QUALITY_GATE_FAILED", "DATA_QUALITY_GATE_FAILED", "DUPLICATE_SUPPRESSED", "COOLDOWN_ACTIVE", "EXPOSURE_LIMIT_REACHED"}:
        return "BLOCKED_BY_CANDIDATE_SUPPRESSION"
    if code in {"SIGNAL_SCHEMA_INVALID", "LINEAGE_MISMATCH", "CANDIDATE_CONSTRUCTION_FAILED", "CANDIDATE_CONTRACT_REJECTED"}:
        return "BLOCKED_BY_CANDIDATE_SUPPRESSION"
    return "UNKNOWN_BLOCKER"


def _candidate_generation_status(invoked: bool, code: str, candidate_count: int, rejected_contracts: list[Mapping[str, Any]], raw_rejections: list[Mapping[str, Any]]) -> str:
    if candidate_count > 0:
        return "CANDIDATES_CREATED"
    if not invoked:
        return "NOT_INVOKED"
    if rejected_contracts:
        return "CONTRACT_REJECTED"
    if code == "UNGOVERNED_SIGNALS_SUPPRESSED":
        return "UNGOVERNED_SIGNALS_SUPPRESSED"
    if raw_rejections:
        return "CANDIDATE_CONVERSION_REJECTED"
    return code


def _signal_validation_status(schema_missing: list[str], lineage_missing: list[str], confidence_failures: list[str], quality_failures: list[str]) -> str:
    if schema_missing:
        return "SCHEMA_INVALID"
    if lineage_missing:
        return "LINEAGE_INVALID"
    if confidence_failures:
        return "CONFIDENCE_BELOW_MINIMUM"
    if quality_failures:
        return "QUALITY_GATE_FAILED"
    return "VALID"


def _furthest_stage(invoked: bool, candidate_count: int, rejected_contracts: list[Mapping[str, Any]], raw_rejections: list[Mapping[str, Any]]) -> str:
    if candidate_count > 0:
        return "CANDIDATE_CREATED"
    if rejected_contracts:
        return "CANDIDATE_CONTRACT_VALIDATION"
    if raw_rejections:
        return "CANDIDATE_CONVERSION"
    if invoked:
        return "CANDIDATE_CONSTRUCTION_ATTEMPT"
    return "SIGNAL"


def _suppression_count(*row_groups: list[Mapping[str, Any]]) -> int:
    return sum(len(rows) for rows in row_groups)


def _unique_signal_count(rows: list[Mapping[str, Any]]) -> int:
    ids = {_signal_id(row) for row in rows if _signal_id(row)}
    return len(ids) if ids else len(rows)


def _required_inputs(signals: list[Mapping[str, Any]], payloads: Mapping[str, Any]) -> list[str]:
    required = {
        "raw_signal",
        "candidate_construction_policy",
        "candidate_contract_policy",
        "entry_reference_price",
        "risk_policy",
    }
    for signal in signals:
        for item in _list(signal.get("required_inputs")):
            required.add(str(item))
        for evidence in _list(signal.get("required_evidence")):
            if isinstance(evidence, dict) and _text(evidence.get("data_item_id")):
                required.add(_text(evidence.get("data_item_id")))
    for row in _event_rows(_rows(payloads.get("sleeve_input_contracts"), "contracts", "rows", "sleeves")):
        for item in _list(row.get("required_inputs")) + _list(row.get("required_data_items")):
            required.add(str(item))
    return sorted(required)


def _diagnostic_rows(signals: list[Mapping[str, Any]], rejected_contracts: list[Mapping[str, Any]], raw_rejections: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_signal: dict[str, dict[str, Any]] = {}
    for signal in signals:
        sid = _signal_id(signal)
        if sid:
            by_signal[sid] = {
                "raw_signal_id": sid,
                "symbol": _text(signal.get("symbol") or signal.get("symbol_or_pair")),
                "signal_status": _text(signal.get("fulfillment_status") or signal.get("status") or "PRESENT"),
                "candidate_contract_status": _text(signal.get("candidate_contract_status")),
                "rejection_stage": _text(signal.get("rejection_stage")),
                "rejection_reason": _text(signal.get("rejection_reason")),
                "missing_fields": _list(signal.get("missing_candidate_fields")) + _list(signal.get("missing_fields")),
                "source_paths": [_text(signal.get("evidence_path") or signal.get("source_artifact_path") or signal.get("raw_intent_path"))],
            }
    for row in rejected_contracts + raw_rejections:
        sid = _signal_id(row)
        if not sid:
            continue
        target = by_signal.setdefault(sid, {"raw_signal_id": sid, "symbol": _text(row.get("symbol") or row.get("symbol_or_pair")), "source_paths": []})
        target["candidate_id"] = _text(row.get("candidate_id"))
        target["candidate_contract_status"] = _text(row.get("contract_validation_status") or row.get("candidate_contract_status"))
        target["rejection_stage"] = _text(row.get("rejection_stage")) or target.get("rejection_stage", "")
        target["rejection_reason"] = _reason(row)
        target["missing_fields"] = sorted(set(_list(target.get("missing_fields")) + _list(row.get("missing_contract_fields")) + _list(row.get("failed_candidate_contract_fields")) + _list(row.get("missing_candidate_fields"))))
        target["source_paths"] = sorted(set(_list(target.get("source_paths")) + _list(row.get("evidence_paths")) + _list(row.get("source_artifact_paths")) + [_text(row.get("evidence_path") or row.get("source_artifact"))]))
    return [row for _, row in sorted(by_signal.items())]


def _owner(code: str) -> str:
    if code == "UNKNOWN_DETERMINISTIC_BLOCKER":
        return "AEGIS_SYSTEM"
    return "AEGIS_SYSTEM"


def _answer(code: str, reason: str, signal_count: int, candidate_count: int) -> str:
    if candidate_count > 0:
        return f"Event Dislocation is flowing: {signal_count} raw signals produced {candidate_count} valid candidate contracts."
    return f"Event Dislocation has {signal_count} raw signals and zero valid candidate contracts because {code}: {reason}"


def _matching_reason_rows(rows: list[Mapping[str, Any]], tokens: set[str]) -> list[Mapping[str, Any]]:
    out = []
    for row in rows:
        haystack = " ".join([_upper(row.get("rejection_reason")), _upper(row.get("human_readable_explanation")), *[_upper(item) for item in _list(row.get("reason_codes"))], *[_upper(item) for item in _list(row.get("detail_reason_codes"))]])
        if any(token in haystack for token in tokens):
            out.append(row)
    return out


def _event_rows(rows: list[Any]) -> list[dict[str, Any]]:
    return [row for row in rows if isinstance(row, dict) and _text(row.get("sleeve_id") or row.get("engine_id")) == SLEEVE_ID]


def _rows(payload: Any, *keys: str) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    out: list[Any] = []
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            out.extend(value)
    return out


def _signal_id(row: Mapping[str, Any]) -> str:
    return _text(row.get("raw_signal_id") or row.get("intent_id") or row.get("raw_intent_id") or row.get("signal_id") or row.get("candidate_id"))


def _reason(row: Mapping[str, Any]) -> str:
    return _text(row.get("rejection_reason") or row.get("human_readable_explanation") or row.get("failure_reason") or row.get("reason") or ";".join(_list(row.get("reason_codes")))) or "No detailed reason supplied by source evidence."


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else ([] if value in (None, "") else [value])


def _text(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _text(value).upper()
