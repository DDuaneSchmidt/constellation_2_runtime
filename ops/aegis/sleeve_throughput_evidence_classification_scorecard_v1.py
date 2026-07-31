from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1

FAMILY = "aegis_sleeve_throughput_evidence_classification_scorecard_v1"
FILENAME = "sleeve_throughput_evidence_classification_scorecard.v1.json"
POLICY_VERSION = "AEGIS_SLEEVE_THROUGHPUT_EVIDENCE_CLASSIFICATION_SCORECARD_READ_ONLY_V1"

FLOWING = "FLOWING"
HEALTHY_NO_CANDIDATE = "HEALTHY_NO_CANDIDATE"
BLOCKED = "BLOCKED"
NEEDS_DATA = "NEEDS_DATA"
NEEDS_GOVERNANCE = "NEEDS_GOVERNANCE"
NEEDS_REPAIR = "NEEDS_REPAIR"

SAFETY = {
    "read_only": True,
    "diagnostics_only": True,
    "no_sleeve_mutation": True,
    "no_candidate_mutation": True,
    "no_strategy_logic_mutation": True,
    "no_threshold_mutation": True,
    "no_scoring_mutation": True,
    "no_risk_policy_mutation": True,
    "no_allocation_mutation": True,
    "no_repair_performed": True,
    "no_fabricated_evidence": True,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
}


def sleeve_throughput_evidence_classification_scorecard_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, day_utc, FILENAME)


def build_sleeve_throughput_evidence_classification_scorecard_v1(
    *, truth_root: Path | str, day_utc: str, computed_at_utc: str | None = None
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    paths = _input_paths(root, day)
    payloads = {name: read_json_v1(path) for name, path in paths.items()}

    signal_counts = _signal_counts(_list(_dict(payloads.get("signal_evidence_graph")).get("signals")))
    contract_counts = _contract_counts(_dict(payloads.get("candidate_contracts")))
    t07b = _dict(payloads.get("event_dislocation_governed_universe_policy_repair"))

    rows = []
    for sleeve in _list(_dict(payloads.get("sleeve_throughput_diagnostics")).get("sleeves")):
        rows.append(_build_row(sleeve, signal_counts, contract_counts, t07b))
    rows = sorted(rows, key=lambda row: str(row["sleeve_id"]))

    summary = _summary(rows)
    payload: dict[str, Any] = {
        "schema_id": "aegis_sleeve_throughput_evidence_classification_scorecard",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "policy_version": POLICY_VERSION,
        "day_utc": day,
        "target_day": day,
        "computed_at_utc": computed_at_utc or f"{day}T00:00:00Z",
        "sleeves": rows,
        "portfolio_summary": summary,
        "summary": summary,
        "classification_rules": {
            FLOWING: "Governed candidates or paper observations exist.",
            HEALTHY_NO_CANDIDATE: "System ran correctly but no valid governed candidate exists.",
            BLOCKED: "Deterministic failure prevents normal evaluation.",
            NEEDS_DATA: "External or internal data is missing.",
            NEEDS_GOVERNANCE: "Governance policy is missing or incomplete.",
            NEEDS_REPAIR: "AEGIS-owned system defect remains.",
        },
        "source_artifact_paths": {name: str(path) for name, path in sorted(paths.items()) if path.exists()},
        "source_artifact_hashes": {name: file_hash_v1(path) for name, path in sorted(paths.items()) if path.exists()},
        "safety_statement": "This scorecard is diagnostics/reporting only. It reads verified Aegis evidence and performs no strategy, threshold, candidate, scoring, risk, allocation, broker/live, autonomous, or safety-gate mutation.",
        **SAFETY,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = stable_hash_v1({**payload, "content_hash": ""})
    return payload


def write_sleeve_throughput_evidence_classification_scorecard_v1(
    *, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None
) -> Path:
    body = payload or build_sleeve_throughput_evidence_classification_scorecard_v1(
        truth_root=truth_root,
        day_utc=day_utc,
    )
    return write_json_v1(sleeve_throughput_evidence_classification_scorecard_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _input_paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "sleeve_throughput_diagnostics": report_path_v1(root, "aegis_sleeve_throughput_diagnostics_v1", day, "sleeve_throughput_diagnostics.v1.json"),
        "signal_evidence_graph": report_path_v1(root, "aegis_signal_evidence_graph_v1", day, "signal_evidence_graph.v1.json"),
        "candidate_contracts": report_path_v1(root, "aegis_candidate_contracts_v1", day, "candidate_contracts.v1.json"),
        "event_dislocation_governed_universe_policy_repair": report_path_v1(root, "aegis_event_dislocation_governed_universe_policy_repair_v1", day, "event_dislocation_governed_universe_policy_repair.v1.json"),
        "dormant_sleeve_signal_generation_diagnostics": report_path_v1(root, "aegis_dormant_sleeve_signal_generation_diagnostics_v1", day, "dormant_sleeve_signal_generation_diagnostics.v1.json"),
        "missing_market_data_requirement_resolver": report_path_v1(root, "aegis_missing_market_data_requirement_resolver_v1", day, "missing_market_data_requirement_resolver.v1.json"),
    }


def _build_row(
    sleeve: Mapping[str, Any],
    signal_counts: Mapping[str, Mapping[str, int]],
    contract_counts: Mapping[str, Mapping[str, int]],
    t07b: Mapping[str, Any],
) -> dict[str, Any]:
    sleeve_id = _text(sleeve.get("sleeve_id"))
    raw_signal_count = _int(sleeve.get("raw_signal_count"))
    governed_signal_count = _int(signal_counts.get(sleeve_id, {}).get("governed"))
    ungoverned_signal_count = _int(signal_counts.get(sleeve_id, {}).get("ungoverned"))
    if sleeve_id == "C2_EVENT_DISLOCATION_V1" and t07b:
        raw_signal_count = max(raw_signal_count, _int(t07b.get("signal_count_before_repair")))
        governed_signal_count = max(governed_signal_count, len(_list(t07b.get("governed_symbols_before_repair"))) if _int(t07b.get("candidate_count_after_repair")) else 0)
        ungoverned_signal_count = max(ungoverned_signal_count, _int(t07b.get("suppressed_ungoverned_signal_count")))

    candidate_count = max(_int(sleeve.get("candidate_count")), _int(contract_counts.get(sleeve_id, {}).get("valid")))
    paper_observation_count = _int(sleeve.get("paper_observation_count"))
    outcome_count = _int(sleeve.get("outcome_count"))
    validation_sample_count = _int(sleeve.get("validation_sample_count"))
    current_status = _text(sleeve.get("throughput_status")) or _text(sleeve.get("sleeve_status"))
    blocker_code = _text(sleeve.get("blocker_code")) or "NONE"
    blocker_reason = _text(sleeve.get("blocker_reason"))
    owner = _text(sleeve.get("owner")) or "NONE"
    david_action_required = bool(sleeve.get("david_action_required"))

    classification, blocker_code, blocker_reason, owner, david_action_required, next_action = _classify(
        sleeve_id=sleeve_id,
        current_status=current_status,
        blocker_code=blocker_code,
        blocker_reason=blocker_reason,
        owner=owner,
        david_action_required=david_action_required,
        raw_signal_count=raw_signal_count,
        governed_signal_count=governed_signal_count,
        ungoverned_signal_count=ungoverned_signal_count,
        candidate_count=candidate_count,
        paper_observation_count=paper_observation_count,
        contract_rejection_count=_int(contract_counts.get(sleeve_id, {}).get("rejected")),
        pre_contract_suppressed_count=_int(contract_counts.get(sleeve_id, {}).get("suppressed")),
        t07b=t07b,
    )

    return {
        "sleeve_id": sleeve_id,
        "sleeve_name": _text(sleeve.get("sleeve_name")) or sleeve_id,
        "raw_signal_count": raw_signal_count,
        "governed_signal_count": governed_signal_count,
        "ungoverned_signal_count": ungoverned_signal_count,
        "candidate_count": candidate_count,
        "paper_observation_count": paper_observation_count,
        "outcome_count": outcome_count,
        "validation_sample_count": validation_sample_count,
        "current_status": current_status,
        "evidence_classification": classification,
        "blocker_code": blocker_code,
        "blocker_reason": blocker_reason,
        "owner": owner,
        "david_action_required": david_action_required,
        "next_best_action": next_action,
    }


def _classify(
    *,
    sleeve_id: str,
    current_status: str,
    blocker_code: str,
    blocker_reason: str,
    owner: str,
    david_action_required: bool,
    raw_signal_count: int,
    governed_signal_count: int,
    ungoverned_signal_count: int,
    candidate_count: int,
    paper_observation_count: int,
    contract_rejection_count: int,
    pre_contract_suppressed_count: int,
    t07b: Mapping[str, Any],
) -> tuple[str, str, str, str, bool, str]:
    if candidate_count > 0 or paper_observation_count > 0:
        return FLOWING, "NONE", "Governed candidates or paper observations exist.", "NONE", False, "Continue monitoring normal sleeve throughput."

    if sleeve_id == "C2_EVENT_DISLOCATION_V1" and _text(t07b.get("repair_status")) == "REPAIRED":
        code = _text(t07b.get("post_repair_t06_status")) or "UNGOVERNED_SIGNALS_SUPPRESSED"
        reason = "Event Dislocation ran correctly; all target-day signals were ungoverned and suppressed before candidate contract validation."
        return HEALTHY_NO_CANDIDATE, code, reason, "NONE", False, "No repair action. Continue monitoring for governed Event Dislocation signals."

    if david_action_required or owner == "DAVID" or blocker_code in {"INSUFFICIENT_MARKET_DATA", "MISSING_MARKET_DATA", "NEEDS_SOURCE", "DATA_SOURCE_MISSING"}:
        return NEEDS_DATA, blocker_code, blocker_reason or "Data is missing for normal evaluation.", owner or "DAVID", True, "Provide or mark unavailable the missing data source."

    governance_codes = {"GOVERNANCE_POLICY_MISSING", "NO_GOVERNED_UNIVERSE", "INSTRUMENT_NOT_IN_GOVERNED_REGISTRY", "BLOCKED_BY_GOVERNANCE_POLICY"}
    if blocker_code in governance_codes:
        return NEEDS_GOVERNANCE, blocker_code, blocker_reason or "Governance policy is missing or incomplete.", owner or "AEGIS_GOVERNANCE_REGISTRY", False, "Update governed policy evidence; do not broaden allowlists without source."

    healthy_codes = {"NO_SIGNALS_GENERATED", "VALID_NO_SIGNAL_CONDITIONS", "TRIGGER_THRESHOLDS_NOT_MET", "NO_MARKET_SETUP", "UNGOVERNED_SIGNALS_SUPPRESSED"}
    if blocker_code in healthy_codes or (raw_signal_count == 0 and current_status == "DORMANT"):
        normalized_code = _healthy_code(blocker_code, sleeve_id)
        return HEALTHY_NO_CANDIDATE, normalized_code, _healthy_reason(normalized_code), "NONE", False, "No repair action. Continue monitoring for governed candidate conditions."

    if raw_signal_count > 0 and governed_signal_count == 0 and ungoverned_signal_count > 0 and pre_contract_suppressed_count >= ungoverned_signal_count and contract_rejection_count == 0:
        return HEALTHY_NO_CANDIDATE, "UNGOVERNED_SIGNALS_SUPPRESSED", "All signals were ungoverned and suppressed before candidate contract validation.", "NONE", False, "No repair action. Continue monitoring for governed signal symbols."

    if owner == "AEGIS_SYSTEM":
        return NEEDS_REPAIR, blocker_code, blocker_reason or "AEGIS-owned system defect remains.", owner, False, "Investigate the AEGIS-owned deterministic blocker."

    if current_status == "BLOCKED" or blocker_code not in {"", "NONE"}:
        return BLOCKED, blocker_code, blocker_reason or "Deterministic blocker prevents normal evaluation.", owner or "AEGIS_SYSTEM", david_action_required, "Review blocker evidence and assign the precise owner."

    return HEALTHY_NO_CANDIDATE, "VALID_NO_CANDIDATE_CONDITIONS", "No governed candidate conditions were present on the target day.", "NONE", False, "No repair action. Continue monitoring."


def _healthy_code(blocker_code: str, sleeve_id: str) -> str:
    if blocker_code == "NO_SIGNALS_GENERATED" and sleeve_id in {"C2_DEFENSIVE_TAIL_V1", "C2_INTENT_SIMULATOR_V1"}:
        return "VALID_NO_SIGNAL_CONDITIONS"
    if blocker_code == "NO_SIGNALS_GENERATED" and sleeve_id == "C2_MARKET_NEUTRAL_SPREAD_V1":
        return "TRIGGER_THRESHOLDS_NOT_MET"
    return blocker_code or "VALID_NO_CANDIDATE_CONDITIONS"


def _healthy_reason(blocker_code: str) -> str:
    if blocker_code == "VALID_NO_SIGNAL_CONDITIONS":
        return "Sleeve evaluated normally and no signal conditions were valid on the target day."
    if blocker_code == "TRIGGER_THRESHOLDS_NOT_MET":
        return "Sleeve evaluated normally and trigger thresholds were not met."
    if blocker_code == "NO_MARKET_SETUP":
        return "Sleeve evaluated normally and no market setup was present."
    if blocker_code == "UNGOVERNED_SIGNALS_SUPPRESSED":
        return "Signals were present but no governed candidate conditions existed."
    return "System ran correctly but no valid governed candidate exists."


def _summary(rows: list[Mapping[str, Any]]) -> dict[str, int]:
    counts = Counter(_text(row.get("evidence_classification")) for row in rows)
    return {
        "total_sleeves": len(rows),
        "flowing_count": counts[FLOWING],
        "healthy_no_candidate_count": counts[HEALTHY_NO_CANDIDATE],
        "blocked_count": counts[BLOCKED],
        "needs_data_count": counts[NEEDS_DATA],
        "needs_governance_count": counts[NEEDS_GOVERNANCE],
        "needs_repair_count": counts[NEEDS_REPAIR],
        "david_action_required_count": sum(1 for row in rows if row.get("david_action_required")),
        "aegis_system_action_required_count": sum(1 for row in rows if row.get("owner") == "AEGIS_SYSTEM"),
    }


def _signal_counts(signals: list[Mapping[str, Any]]) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = defaultdict(lambda: {"raw": 0, "governed": 0, "ungoverned": 0})
    for signal in signals:
        sid = _text(signal.get("sleeve_id"))
        if not sid:
            continue
        counts[sid]["raw"] += 1
        governance = _text(signal.get("governance_status") or signal.get("symbol_governance_status")).upper()
        if governance == "GOVERNED":
            counts[sid]["governed"] += 1
        elif governance == "UNGOVERNED" or _text(signal.get("pre_contract_suppression_reason")) == "UNGOVERNED_SYMBOL_SUPPRESSED":
            counts[sid]["ungoverned"] += 1
    return counts


def _contract_counts(contracts_payload: Mapping[str, Any]) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = defaultdict(lambda: {"valid": 0, "rejected": 0, "suppressed": 0})
    for row in _list(contracts_payload.get("candidate_contracts")):
        sid = _text(row.get("sleeve_id"))
        if sid and _text(row.get("contract_validation_status")).upper() == "VALID":
            counts[sid]["valid"] += 1
    for row in _list(contracts_payload.get("rejected_raw_signals")) + _list(contracts_payload.get("candidates_rejected")):
        sid = _text(row.get("sleeve_id"))
        if sid:
            counts[sid]["rejected"] += 1
    for row in _list(contracts_payload.get("pre_contract_suppressed_raw_signals")):
        sid = _text(row.get("sleeve_id"))
        if sid:
            counts[sid]["suppressed"] += 1
    return counts


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
