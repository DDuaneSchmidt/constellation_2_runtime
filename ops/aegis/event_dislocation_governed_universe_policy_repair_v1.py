from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.candidate_contracts_v1 import build_candidate_contracts_v1, write_candidate_contracts_v1
from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1
from ops.aegis.signal_evidence_graph_v1 import build_signal_evidence_graph_v1, write_signal_evidence_graph_v1

FAMILY = "aegis_event_dislocation_governed_universe_policy_repair_v1"
FILENAME = "event_dislocation_governed_universe_policy_repair.v1.json"
SLEEVE_ID = "C2_EVENT_DISLOCATION_V1"
SLEEVE_NAME = "Event Dislocation Repricing"
UNGOVERNED_SYMBOL_SUPPRESSED = "UNGOVERNED_SYMBOL_SUPPRESSED"

SAFETY = {
    "strategy_behavior_changed": False,
    "strategy_thresholds_changed": False,
    "candidate_scoring_changed": False,
    "risk_policy_changed": False,
    "safety_gates_unchanged": True,
    "contract_strictness_weakened": False,
    "broad_allowlist_introduced": False,
    "fabricated_governance_policy": False,
    "fabricated_signals": False,
    "fabricated_candidates": False,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "autonomous_execution_allowed": False,
}


def event_dislocation_governed_universe_policy_repair_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, day_utc, FILENAME)


def build_event_dislocation_governed_universe_policy_repair_v1(
    *,
    truth_root: Path | str,
    repo_root: Path | str | None = None,
    day_utc: str,
    computed_at_utc: str | None = None,
    rebuild_runtime_evidence: bool = True,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    repo = Path(repo_root or Path.cwd()).expanduser().resolve()
    day = str(day_utc)
    computed_at = computed_at_utc or f"{day}T00:00:00Z"
    paths = _input_paths(root, repo, day)

    t07a_before = read_json_v1(paths["t07a_governance_trace"])
    signal_graph_before = read_json_v1(paths["signal_evidence_graph"])
    before_signals = _event_rows(_rows(signal_graph_before, "signals", "raw_signals"))
    before_symbols = sorted({_symbol(row) for row in before_signals if _symbol(row)})

    engine_registry = read_json_v1(paths["engine_registry"])
    policy_payload = read_json_v1(paths["equity_structure_policy"])
    governed_symbols = _governed_symbols(engine_registry)
    event_policy = _event_policy(policy_payload)
    policy_found_after = bool(event_policy)
    policy_found_before = _t07a_policy_found(t07a_before)
    ungoverned_symbols = sorted(symbol for symbol in before_symbols if symbol not in governed_symbols)

    if rebuild_runtime_evidence:
        signal_graph = build_signal_evidence_graph_v1(truth_root=root, day_utc=day, repo_root=repo)
        write_signal_evidence_graph_v1(truth_root=root, day_utc=day, payload=signal_graph)
        candidate_contracts = build_candidate_contracts_v1(truth_root=root, day_utc=day, repo_root=repo)
        write_candidate_contracts_v1(truth_root=root, day_utc=day, payload=candidate_contracts)
    else:
        signal_graph = read_json_v1(paths["signal_evidence_graph"])
        candidate_contracts = read_json_v1(paths["candidate_contracts"])

    after_signals = _event_rows(_rows(signal_graph, "signals", "raw_signals"))
    created = _event_rows(_rows(candidate_contracts, "candidate_contracts", "contracts", "candidates"))
    rejected = _event_rows(_rows(candidate_contracts, "rejected_raw_signals", "rejected_candidates"))
    suppressed = _event_rows(_rows(candidate_contracts, "pre_contract_suppressed_raw_signals"))
    if not suppressed:
        suppressed = [row for row in after_signals if _upper(row.get("candidate_contract_status")) == "SUPPRESSED" and _upper(row.get("pre_contract_suppression_reason") or row.get("rejection_reason")) == UNGOVERNED_SYMBOL_SUPPRESSED]

    rejection_reasons = Counter(_text(row.get("rejection_reason")) or "UNKNOWN" for row in rejected)
    candidate_ids = sorted({_text(row.get("candidate_id")) for row in created if _text(row.get("candidate_id"))})
    candidate_attempt_count = len(created) + len(rejected)
    suppressed_count = _unique_signal_count(suppressed)

    status, remaining_blocker, blocker_reason = _repair_status(
        signal_count=len(before_signals),
        governed_symbols=governed_symbols,
        policy_found=policy_found_after,
        rejected=rejected,
        created=created,
        suppressed_count=suppressed_count,
        after_signals=after_signals,
    )
    post_t06 = "CANDIDATE_CONTRACT_VALID" if created else ("UNGOVERNED_SIGNALS_SUPPRESSED" if suppressed_count else "VALID_NO_CANDIDATE_CONDITIONS")
    post_t07a = "NO_CONTRACT_REJECTIONS" if not rejected else "CANDIDATE_CONTRACT_REJECTED"

    payload: dict[str, Any] = {
        "schema_id": "aegis_event_dislocation_governed_universe_policy_repair",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "target_day": day,
        "day_utc": day,
        "computed_at_utc": computed_at,
        "sleeve_id": SLEEVE_ID,
        "sleeve_name": SLEEVE_NAME,
        "signal_count_before_repair": len(before_signals),
        "signal_symbols_before_repair": before_symbols,
        "governed_symbols_before_repair": governed_symbols,
        "ungoverned_symbols_before_repair": ungoverned_symbols,
        "governed_universe_source": f"ENGINE_MODEL_REGISTRY_V1:{SLEEVE_ID}.allowed_symbols",
        "governed_universe_policy_found_before_repair": policy_found_before,
        "governed_universe_policy_found_after_repair": policy_found_after,
        "repair_attempted": True,
        "repair_status": status,
        "repair_type": "UNGOVERNED_SIGNAL_SUPPRESSION" if suppressed_count else ("CANDIDATE_METADATA_POLICY_REPAIR" if created else "GOVERNED_UNIVERSE_POLICY_REPAIR"),
        "candidate_filtering_applied": bool(suppressed_count),
        "suppressed_ungoverned_signal_count": suppressed_count,
        "candidate_attempt_count_after_repair": candidate_attempt_count,
        "candidate_count_after_repair": len(created),
        "candidate_ids_after_repair": candidate_ids,
        "candidate_rejection_count_after_repair": len(rejected),
        "rejection_reasons_after_repair": [{"reason": reason, "count": count} for reason, count in rejection_reasons.most_common()],
        "normalized_direction_source": _direction_source(event_policy),
        "normalized_governance_status_source": _governance_status_source(event_policy),
        "normalized_instrument_type_source": _instrument_type_source(event_policy),
        "post_repair_t06_status": post_t06,
        "post_repair_t07a_status": post_t07a,
        "remaining_blocker": remaining_blocker,
        "blocker_reason": blocker_reason,
        "owner": "AEGIS_GOVERNANCE_REGISTRY" if status.startswith("NOT_REPAIRED") else "NONE",
        "david_action_required": False,
        "suppression_reason": UNGOVERNED_SYMBOL_SUPPRESSED if suppressed_count else "",
        "suppressed_signal_ids": sorted({_signal_id(row) for row in suppressed if _signal_id(row)}),
        "governed_candidate_signal_ids": sorted({_signal_id(row) for row in created if _signal_id(row)}),
        "source_artifact_paths": {name: str(path) for name, path in sorted(paths.items()) if path.exists()},
        "source_artifact_hashes": {name: file_hash_v1(path) for name, path in sorted(paths.items()) if path.exists()},
        "safety_statement": "T07B restricts Event Dislocation candidate conversion to the governed engine universe and suppresses ungoverned symbols before contract validation. It does not weaken candidate contracts, broaden symbol governance, change thresholds, scoring, risk policy, safety gates, or broker/live/autonomous behavior.",
        **SAFETY,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = stable_hash_v1({**payload, "content_hash": ""})
    return payload


def write_event_dislocation_governed_universe_policy_repair_v1(
    *, truth_root: Path | str, repo_root: Path | str | None = None, day_utc: str, payload: dict[str, Any] | None = None
) -> Path:
    body = payload or build_event_dislocation_governed_universe_policy_repair_v1(truth_root=truth_root, repo_root=repo_root, day_utc=day_utc)
    return write_json_v1(event_dislocation_governed_universe_policy_repair_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _input_paths(root: Path, repo: Path, day: str) -> dict[str, Path]:
    return {
        "signal_evidence_graph": report_path_v1(root, "aegis_signal_evidence_graph_v1", day, "signal_evidence_graph.v1.json"),
        "candidate_contracts": report_path_v1(root, "aegis_candidate_contracts_v1", day, "candidate_contracts.v1.json"),
        "t06_candidate_suppression": report_path_v1(root, "aegis_event_dislocation_candidate_suppression_diagnostics_v1", day, "event_dislocation_candidate_suppression_diagnostics.v1.json"),
        "t07a_governance_trace": report_path_v1(root, "aegis_event_dislocation_governance_trace_diagnostic_v1", day, "event_dislocation_governance_trace_diagnostic.v1.json"),
        "engine_registry": repo / "governance" / "02_REGISTRIES" / "ENGINE_MODEL_REGISTRY_V1.json",
        "equity_structure_policy": repo / "governance" / "02_REGISTRIES" / "C2_EQUITY_STRUCTURE_POLICY_V1.json",
    }


def _repair_status(*, signal_count: int, governed_symbols: list[str], policy_found: bool, rejected: list[Mapping[str, Any]], created: list[Mapping[str, Any]], suppressed_count: int, after_signals: list[Mapping[str, Any]]) -> tuple[str, str, str]:
    if not governed_symbols:
        return "NOT_REPAIRED_NO_GOVERNED_UNIVERSE", "NO_GOVERNED_UNIVERSE", "Event Dislocation has no governed engine symbols."
    if not policy_found:
        return "NOT_REPAIRED_DIRECTION_POLICY_MISSING", "EVENT_DISLOCATION_STRUCTURE_POLICY_MISSING", "Event Dislocation has a governed engine universe but no candidate structure policy."
    if rejected:
        return "NOT_REPAIRED_CONTRACT_STILL_REJECTED", "CANDIDATE_CONTRACT_REJECTED", "Candidate contract validation still rejected Event Dislocation rows after universe repair."
    if created:
        return "REPAIRED", "NONE", "Governed Event Dislocation signals reached valid candidate contracts."
    if signal_count and suppressed_count == signal_count:
        return "REPAIRED", "NONE", "All target-day Event Dislocation signals were outside the governed universe and were suppressed before contract validation."
    if signal_count and suppressed_count:
        unresolved = signal_count - suppressed_count - len(created)
        if unresolved > 0:
            return "UNKNOWN_DETERMINISTIC_BLOCKER", "UNRESOLVED_EVENT_DISLOCATION_SIGNALS", f"{unresolved} Event Dislocation signals were neither candidates nor pre-contract suppressions."
        return "REPAIRED", "NONE", "Ungoverned Event Dislocation signals were suppressed and no contract rejections remain."
    if signal_count == 0:
        return "NO_REPAIR_REQUIRED", "NONE", "No Event Dislocation signals were present for the target day."
    if all(_upper(row.get("symbol_governance_status")) == "UNGOVERNED" for row in after_signals):
        return "NOT_REPAIRED_UNGOVERNED_SYMBOLS_ONLY", "UNGOVERNED_SYMBOLS_ONLY", "Signals are ungoverned but were not suppressed before contract validation."
    return "UNKNOWN_DETERMINISTIC_BLOCKER", "UNKNOWN_BLOCKER", "Unable to classify Event Dislocation governed universe repair state."


def _t07a_policy_found(payload: Mapping[str, Any]) -> bool:
    rep = payload.get("representative_rejected_candidate") if isinstance(payload, dict) else {}
    lookup = rep.get("instrument_registry_lookup_result") if isinstance(rep, dict) else {}
    if isinstance(lookup, dict):
        return bool(lookup.get("structure_policy_found"))
    return False


def _event_policy(payload: Mapping[str, Any]) -> dict[str, Any]:
    for row in _rows(payload, "engine_policies", "policies"):
        if isinstance(row, dict) and _upper(row.get("engine_id") or row.get("sleeve_id")) == SLEEVE_ID:
            return row
    return {}


def _governed_symbols(payload: Mapping[str, Any]) -> list[str]:
    for row in _rows(payload, "engines", "engine_models", "models", "rows"):
        if isinstance(row, dict) and _upper(row.get("engine_id") or row.get("sleeve_id")) == SLEEVE_ID:
            return sorted({_upper(item) for item in _list(row.get("allowed_symbols")) if _upper(item)})
    return []


def _direction_source(policy: Mapping[str, Any]) -> str:
    if _upper(_safe_dict(policy.get("structure_template")).get("allowed_action")) == "BUY" and _upper(_safe_dict(policy.get("exposure_requirements")).get("exposure_type")) == "LONG_EQUITY":
        return "C2_EQUITY_STRUCTURE_POLICY_V1:C2_EVENT_DISLOCATION_V1.structure_template.allowed_action"
    return ""


def _governance_status_source(policy: Mapping[str, Any]) -> str:
    return "C2_EQUITY_STRUCTURE_POLICY_V1:C2_EVENT_DISLOCATION_V1" if policy else ""


def _instrument_type_source(policy: Mapping[str, Any]) -> str:
    if _upper(_safe_dict(policy.get("exposure_requirements")).get("exposure_type")):
        return "C2_EQUITY_STRUCTURE_POLICY_V1:C2_EVENT_DISLOCATION_V1.exposure_requirements.exposure_type"
    return ""


def _event_rows(rows: list[Any]) -> list[dict[str, Any]]:
    return [row for row in rows if isinstance(row, dict) and _upper(row.get("sleeve_id") or row.get("engine_id")) == SLEEVE_ID]


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
        elif isinstance(value, dict):
            for policy_id, item in value.items():
                if isinstance(item, dict):
                    out.append({**item, "engine_id": item.get("engine_id") or item.get("sleeve_id") or policy_id})
    return out


def _unique_signal_count(rows: list[Mapping[str, Any]]) -> int:
    ids = {_signal_id(row) for row in rows if _signal_id(row)}
    return len(ids) if ids else len(rows)


def _signal_id(row: Mapping[str, Any]) -> str:
    return _text(row.get("raw_signal_id") or row.get("signal_id") or row.get("intent_id") or row.get("raw_intent_id") or row.get("candidate_id"))


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
