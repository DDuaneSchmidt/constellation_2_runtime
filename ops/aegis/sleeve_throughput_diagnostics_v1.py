from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import SLEEVE_RULES, file_hash_v1, report_path_v1, stable_hash_v1

FAMILY = "aegis_sleeve_throughput_diagnostics_v1"
FILENAME = "sleeve_throughput_diagnostics.v1.json"
POLICY_VERSION = "AEGIS_SLEEVE_THROUGHPUT_DIAGNOSTICS_READ_ONLY_V1"

STAGES = [
    "HYPOTHESIS",
    "SIGNAL",
    "CANDIDATE",
    "CANDIDATE_CONTRACT",
    "PAPER_OBSERVATION",
    "OUTCOME",
    "VALIDATION_SAMPLE",
]

SAFETY = {
    "read_only": True,
    "diagnostics_only": True,
    "no_sleeve_mutation": True,
    "no_candidate_mutation": True,
    "no_quality_mutation": True,
    "no_allocation_mutation": True,
    "no_repair_performed": True,
    "no_fabricated_evidence": True,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
}


def sleeve_throughput_diagnostics_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, day_utc, FILENAME)


def build_sleeve_throughput_diagnostics_v1(
    *, truth_root: Path | str, day_utc: str, computed_at_utc: str | None = None
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    paths = _input_paths(root, day)
    payloads = {name: read_json_v1(path) for name, path in paths.items()}
    sleeve_rows = _sleeve_inventory(payloads)
    rows = [_build_sleeve_row(sleeve, payloads, paths, day, computed_at_utc or f"{day}T00:00:00Z") for sleeve in sleeve_rows]
    rows = sorted(rows, key=lambda row: str(row["sleeve_id"]))

    summary = {
        "total_sleeves": len(rows),
        "sleeves_flowing": sum(1 for row in rows if row["throughput_status"] == "FLOWING"),
        "sleeves_dormant": sum(1 for row in rows if row["throughput_status"] == "DORMANT"),
        "sleeves_blocked": sum(1 for row in rows if row["throughput_status"] == "BLOCKED"),
        "sleeves_underproducing": sum(1 for row in rows if row["throughput_status"] == "UNDERPRODUCING"),
    }
    answer = _minority_candidate_answer(rows)
    payload: dict[str, Any] = {
        "schema_id": "aegis_sleeve_throughput_diagnostics",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "policy_version": POLICY_VERSION,
        "day_utc": day,
        "target_day": day,
        "computed_at_utc": computed_at_utc or f"{day}T00:00:00Z",
        "sleeves": rows,
        "portfolio_summary": summary,
        "summary": summary,
        "rankings": {
            "most_evidence_produced": _rank(rows, ("total_evidence_count", "candidate_count", "raw_signal_count"), reverse=True),
            "most_candidates_produced": _rank(rows, ("candidate_count", "candidate_contract_count", "raw_signal_count"), reverse=True),
            "most_blocked": _rank(rows, ("blocked_rank_score", "total_evidence_count"), reverse=True),
            "least_active": _rank(rows, ("least_activity_rank_score", "total_evidence_count"), reverse=True),
        },
        "most_important_question": {
            "question": "Why are only a minority of sleeves producing candidates?",
            "answer": answer,
            "candidate_producing_sleeves": [row["sleeve_id"] for row in rows if row["candidate_count"] > 0],
            "non_candidate_producing_sleeves": [row["sleeve_id"] for row in rows if row["candidate_count"] == 0],
            "deterministic_evidence": _minority_evidence(rows),
        },
        "source_artifact_paths": {name: str(path) for name, path in sorted(paths.items()) if path.exists()},
        "source_artifact_hashes": {name: file_hash_v1(path) for name, path in sorted(paths.items()) if path.exists()},
        "safety_statement": "Sleeve throughput diagnostics are read-only. This artifact measures existing truth evidence only and performs no sleeve, candidate, quality, allocation, paper, outcome, validation, broker, or repair mutation.",
        **SAFETY,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = stable_hash_v1({**payload, "content_hash": ""})
    return payload


def write_sleeve_throughput_diagnostics_v1(
    *, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None
) -> Path:
    body = payload or build_sleeve_throughput_diagnostics_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(sleeve_throughput_diagnostics_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _input_paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "research_portfolio": report_path_v1(root, "aegis_research_portfolio_v1", day, "research_portfolio.v1.json"),
        "hypothesis_workflow_state": report_path_v1(root, "aegis_hypothesis_workflow_state_v1", day, "hypothesis_workflow_state.v1.json"),
        "generated_hypothesis_throughput": report_path_v1(root, "aegis_generated_hypothesis_throughput_v1", day, "generated_hypothesis_throughput.v1.json"),
        "signal_evidence_graph": report_path_v1(root, "aegis_signal_evidence_graph_v1", day, "signal_evidence_graph.v1.json"),
        "candidate_contracts": report_path_v1(root, "aegis_candidate_contracts_v1", day, "candidate_contracts.v1.json"),
        "candidate_lifecycle": report_path_v1(root, "aegis_candidate_to_paper_lifecycle_v1", day, "candidate_to_paper_lifecycle.v1.json"),
        "paper_position_ledger": report_path_v1(root, "aegis_paper_position_ledger_v1", day, "paper_position_ledger.v1.json"),
        "outcome_registry": report_path_v1(root, "aegis_outcome_registry_v1", day, "outcome_registry.v1.json"),
        "validation_samples": report_path_v1(root, "aegis_validation_samples_v1", day, "validation_samples.v1.json"),
        "research_quality_engine": report_path_v1(root, "aegis_research_quality_engine_v1", day, "research_quality_engine.v1.json"),
        "macro_calendar_data_readiness": report_path_v1(root, "aegis_macro_calendar_data_readiness_v1", day, "macro_calendar_data_readiness.v1.json"),
        "market_data_universe_consistency": report_path_v1(root, "aegis_market_data_universe_consistency_v1", day, "market_data_universe_consistency.v1.json"),
        "operator_action_queue": report_path_v1(root, "aegis_operator_action_queue_v1", day, "operator_action_queue.v1.json"),
        "dormant_sleeve_signal_generation_diagnostics": report_path_v1(root, "aegis_dormant_sleeve_signal_generation_diagnostics_v1", day, "dormant_sleeve_signal_generation_diagnostics.v1.json"),
    }


def _sleeve_inventory(payloads: Mapping[str, Any]) -> list[dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    for sleeve_id, rule in SLEEVE_RULES.items():
        rows[sleeve_id] = {
            "sleeve_id": sleeve_id,
            "sleeve_name": rule.get("hypothesis_name", sleeve_id),
            "hypothesis_id": rule.get("hypothesis_id", ""),
            "source_type": "SLEEVE_RULE",
        }
    for item in _list(_dict(payloads.get("research_portfolio")).get("sleeve_implementations")):
        sid = _text(item.get("sleeve_id"))
        if sid:
            rows[sid] = {
                "sleeve_id": sid,
                "sleeve_name": _text(item.get("hypothesis_name")) or SLEEVE_RULES.get(sid, {}).get("hypothesis_name", sid),
                "hypothesis_id": _text(item.get("hypothesis_id")),
                "source_type": "RESEARCH_PORTFOLIO",
            }
    for item in _list(_dict(payloads.get("generated_hypothesis_throughput")).get("generated_hypotheses")):
        hid = _text(item.get("hypothesis_id"))
        if hid and hid not in {row["hypothesis_id"] for row in rows.values()}:
            rows[hid] = {
                "sleeve_id": hid,
                "sleeve_name": _text(item.get("hypothesis_name")) or hid,
                "hypothesis_id": hid,
                "source_type": "GENERATED_HYPOTHESIS",
            }
    return sorted(rows.values(), key=lambda row: row["sleeve_id"])


def _build_sleeve_row(sleeve: Mapping[str, str], payloads: Mapping[str, Any], paths: Mapping[str, Path], day: str, computed_at: str) -> dict[str, Any]:
    sleeve_id = sleeve["sleeve_id"]
    hypothesis_id = sleeve.get("hypothesis_id", "")
    signals = _matching_rows(_rows(payloads.get("signal_evidence_graph"), "signals"), sleeve_id, hypothesis_id)
    contracts = _matching_rows(_rows(payloads.get("candidate_contracts"), "candidate_contracts"), sleeve_id, hypothesis_id)
    rejections = _matching_rows(_rows(payloads.get("candidate_contracts"), "rejected_raw_signals", "candidates_rejected"), sleeve_id, hypothesis_id)
    lifecycle = _matching_rows(_rows(payloads.get("candidate_lifecycle"), "rows", "candidate_lifecycle_rows", "paper_positions", "valid_candidate_contracts"), sleeve_id, hypothesis_id)
    positions = _matching_rows(_rows(payloads.get("paper_position_ledger"), "positions", "paper_positions", "open_positions"), sleeve_id, hypothesis_id)
    outcomes = _matching_rows(_rows(payloads.get("outcome_registry"), "outcomes", "outcome_rows", "rows"), sleeve_id, hypothesis_id)
    samples = _matching_rows(_rows(payloads.get("validation_samples"), "validation_samples", "samples", "rows"), sleeve_id, hypothesis_id)
    quality = _matching_rows(_rows(payloads.get("research_quality_engine"), "hypotheses", "quality_rows", "rows"), sleeve_id, hypothesis_id)
    generated = _matching_rows(_rows(payloads.get("generated_hypothesis_throughput"), "generated_hypotheses"), sleeve_id, hypothesis_id)
    workflow = _matching_rows(_rows(payloads.get("hypothesis_workflow_state"), "hypotheses"), sleeve_id, hypothesis_id)
    actions = _matching_rows(_rows(payloads.get("operator_action_queue"), "actions"), sleeve_id, hypothesis_id)

    candidate_ids = sorted({cid for cid in [_text(row.get("candidate_id")) for row in [*contracts, *lifecycle, *positions, *outcomes, *samples]] if cid})
    candidate_count = max(len(candidate_ids), len(contracts), _max_int(generated, "candidate_count"))
    paper_observation_count = max(len(positions), _max_int(generated, "paper_observation_count"), len(_paper_lifecycle_rows(lifecycle)))
    outcome_count = len(outcomes)
    validation_sample_count = len(samples)
    raw_signal_count = max(len(signals), _max_int(generated, "raw_signal_count"))
    contract_count = max(len([row for row in contracts if _upper(row.get("contract_validation_status")) != "REJECTED"]), _max_int(generated, "candidate_contract_count"))
    rejection_count = max(len(rejections), len([row for row in contracts if _upper(row.get("contract_validation_status")) == "REJECTED"]))

    furthest = _furthest(raw_signal_count, candidate_count, contract_count, paper_observation_count, outcome_count, validation_sample_count)
    status, blocker_code, blocker_stage, reason, owner, david = _classification(
        raw_signal_count=raw_signal_count,
        candidate_count=candidate_count,
        contract_count=contract_count,
        rejection_count=rejection_count,
        paper_observation_count=paper_observation_count,
        outcome_count=outcome_count,
        validation_sample_count=validation_sample_count,
        workflow=workflow,
        generated=generated,
        quality=quality,
        actions=actions,
        payloads=payloads,
    )
    timestamps = _timestamps([*signals, *contracts, *rejections, *lifecycle, *positions, *outcomes, *samples, *quality, *generated, *workflow])
    total_evidence = raw_signal_count + candidate_count + contract_count + rejection_count + paper_observation_count + outcome_count + validation_sample_count
    row = {
        "schema_id": "aegis_sleeve_throughput_diagnostic_row",
        "schema_version": "v1",
        "day_utc": day,
        "computed_at_utc": computed_at,
        "sleeve_id": sleeve_id,
        "sleeve_name": sleeve.get("sleeve_name") or sleeve_id,
        "sleeve_status": _sleeve_status(workflow, quality),
        "hypothesis_id": hypothesis_id,
        "raw_signal_count": raw_signal_count,
        "candidate_count": candidate_count,
        "candidate_contract_count": contract_count,
        "candidate_rejection_count": rejection_count,
        "paper_observation_count": paper_observation_count,
        "outcome_count": outcome_count,
        "validation_sample_count": validation_sample_count,
        "most_recent_activity_timestamp": max(timestamps) if timestamps else "",
        "throughput_status": status,
        "furthest_stage_reached": furthest,
        "blocker_stage": blocker_stage,
        "blocker_code": blocker_code,
        "blocker_reason": reason,
        "owner": owner,
        "david_action_required": david,
        "expected_or_abnormal": _expected_or_abnormal(status, blocker_code, quality, workflow),
        "stage_counts": {
            "hypothesis": 1,
            "signal": raw_signal_count,
            "candidate": candidate_count,
            "candidate_contract": contract_count,
            "paper_observation": paper_observation_count,
            "outcome": outcome_count,
            "validation_sample": validation_sample_count,
        },
        "source_evidence_counts": {
            "signals": len(signals),
            "candidate_contracts": len(contracts),
            "candidate_rejections": len(rejections),
            "candidate_lifecycle_rows": len(lifecycle),
            "paper_positions": len(positions),
            "outcomes": len(outcomes),
            "validation_samples": len(samples),
            "research_quality_rows": len(quality),
            "workflow_rows": len(workflow),
            "generated_throughput_rows": len(generated),
        },
        "source_artifact_paths": {name: str(path) for name, path in sorted(paths.items()) if path.exists()},
        "source_artifact_hashes": {name: file_hash_v1(path) for name, path in sorted(paths.items()) if path.exists()},
        "downstream_diagnostics": _downstream_diagnostics(status, blocker_code, sleeve_id, paths),
        "total_evidence_count": total_evidence,
        "blocked_rank_score": 1 if status == "BLOCKED" else 0,
        "least_activity_rank_score": 10_000 - total_evidence,
        **SAFETY,
    }
    row["row_hash"] = stable_hash_v1({**row, "row_hash": ""})
    return row


def _classification(**kwargs: Any) -> tuple[str, str, str, str, str, bool]:
    raw_signal_count = kwargs["raw_signal_count"]
    candidate_count = kwargs["candidate_count"]
    contract_count = kwargs["contract_count"]
    rejection_count = kwargs["rejection_count"]
    paper_observation_count = kwargs["paper_observation_count"]
    outcome_count = kwargs["outcome_count"]
    validation_sample_count = kwargs["validation_sample_count"]
    workflow = kwargs["workflow"]
    generated = kwargs["generated"]
    actions = kwargs["actions"]
    quality = kwargs["quality"]
    payloads = kwargs["payloads"]

    if validation_sample_count > 0:
        return "FLOWING", "NONE", "NONE", "Evidence has reached validation sample stage for this sleeve.", "NONE", False
    if candidate_count and contract_count and paper_observation_count and outcome_count:
        return "FLOWING", "NONE", "NONE", "Evidence has reached candidate, contract, paper observation, and outcome stages.", "NONE", False
    if validation_sample_count == 0 and outcome_count > 0:
        return "UNDERPRODUCING", "NO_VALIDATION_SAMPLES_AVAILABLE", "VALIDATION_SAMPLE", "Outcomes exist but no validation sample rows are present for this sleeve.", "AEGIS_SYSTEM", False
    if paper_observation_count > 0 and outcome_count == 0:
        return "UNDERPRODUCING", "NO_OUTCOMES_AVAILABLE", "OUTCOME", "Paper observations exist, but no outcome rows are available yet for this sleeve.", "AEGIS_SYSTEM", False
    if candidate_count > 0 and contract_count == 0:
        return "BLOCKED", "CANDIDATES_REJECTED_BY_CONTRACTS" if rejection_count else "CANDIDATES_PRESENT_BUT_NO_CONTRACTS", "CANDIDATE_CONTRACT", "Candidates or raw candidate evidence exist, but no valid candidate contracts are present.", "AEGIS_SYSTEM", False
    if contract_count > 0 and paper_observation_count == 0:
        return "BLOCKED", "PAPER_CONSTRUCTION_FAILED", "PAPER_OBSERVATION", "Valid candidate contracts exist, but paper observation construction has not produced rows.", "AEGIS_SYSTEM", False
    if raw_signal_count > 0 and candidate_count == 0:
        return "BLOCKED", "SIGNALS_PRESENT_BUT_NO_CANDIDATES", "CANDIDATE", "Raw signals exist, but no candidate evidence is present for this sleeve.", "AEGIS_SYSTEM", False
    if actions:
        return "BLOCKED", _first_action_code(actions), "SIGNAL", _first_action_reason(actions), "DAVID", True
    if any(str(row.get("throughput_status") or "") == "NEEDS_DATA" for row in generated) or any(str(row.get("current_state") or "") == "NEEDS_DATA" for row in workflow):
        return "BLOCKED", "INSUFFICIENT_MARKET_DATA" if _macro_needs_data(payloads) else "NEEDS_DATA", "SIGNAL", "Workflow evidence says required research data is missing before candidate flow can continue.", "DAVID", True
    if raw_signal_count == 0 and candidate_count == 0:
        if quality:
            return "DORMANT", "NO_SIGNALS_GENERATED", "SIGNAL", "Research quality/workflow evidence exists, but no raw signals were generated for the target day.", "AEGIS_SYSTEM", False
        return "DORMANT", "NO_SIGNALS_GENERATED", "SIGNAL", "No raw signals, candidates, contracts, paper observations, outcomes, or validation samples exist for the target day.", "AEGIS_SYSTEM", False
    return "UNDERPRODUCING", "INSUFFICIENT_EVIDENCE_FLOW", "UNKNOWN", "Evidence exists, but the full throughput chain has not reached validation.", "AEGIS_SYSTEM", False


def _minority_candidate_answer(rows: list[dict[str, Any]]) -> str:
    total = len(rows)
    producing = sum(1 for row in rows if row["candidate_count"] > 0)
    blockers: dict[str, int] = {}
    for row in rows:
        if row["candidate_count"] == 0:
            blockers[row["blocker_code"]] = blockers.get(row["blocker_code"], 0) + 1
    top = sorted(blockers.items(), key=lambda item: (-item[1], item[0]))
    reason = ", ".join(f"{code}={count}" for code, count in top) or "none"
    return f"{producing} of {total} sleeves produced candidates. Non-producing sleeves stop before the candidate stage with deterministic blocker counts: {reason}."


def _minority_evidence(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "sleeve_id": row["sleeve_id"],
            "raw_signal_count": row["raw_signal_count"],
            "candidate_count": row["candidate_count"],
            "furthest_stage_reached": row["furthest_stage_reached"],
            "blocker_code": row["blocker_code"],
            "throughput_status": row["throughput_status"],
        }
        for row in rows
    ]


def _rank(rows: list[dict[str, Any]], keys: tuple[str, ...], *, reverse: bool) -> list[dict[str, Any]]:
    ranked = sorted(rows, key=lambda row: tuple(row.get(key, 0) for key in keys), reverse=reverse)
    return [
        {
            "rank": idx + 1,
            "sleeve_id": row["sleeve_id"],
            "sleeve_name": row["sleeve_name"],
            "throughput_status": row["throughput_status"],
            "total_evidence_count": row["total_evidence_count"],
            "candidate_count": row["candidate_count"],
            "blocker_code": row["blocker_code"],
            "most_recent_activity_timestamp": row["most_recent_activity_timestamp"],
        }
        for idx, row in enumerate(ranked)
    ]


def _downstream_diagnostics(status: str, blocker_code: str, sleeve_id: str, paths: Mapping[str, Path]) -> dict[str, Any]:
    if status != "DORMANT" or blocker_code != "NO_SIGNALS_GENERATED":
        return {}
    path = paths["dormant_sleeve_signal_generation_diagnostics"]
    return {
        "artifact_id": "aegis_dormant_sleeve_signal_generation_diagnostics_v1",
        "path": str(path),
        "sleeve_id": sleeve_id,
        "relationship": "explains_no_signals_generated",
        "artifact_exists": path.exists(),
        "content_hash": file_hash_v1(path) if path.exists() else "",
    }


def _matching_rows(rows: list[dict[str, Any]], sleeve_id: str, hypothesis_id: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        text = json.dumps(row, sort_keys=True, default=str)
        if _text(row.get("sleeve_id")) == sleeve_id or _text(row.get("engine_id")) == sleeve_id or (hypothesis_id and _text(row.get("hypothesis_id")) == hypothesis_id):
            out.append(row)
        elif sleeve_id and sleeve_id in text:
            out.append(row)
        elif hypothesis_id and hypothesis_id in text:
            out.append(row)
    return out


def _rows(payload: Any, *keys: str) -> list[dict[str, Any]]:
    data = _dict(payload)
    for key in keys:
        value = data.get(key)
        if isinstance(value, list):
            return [dict(row) for row in value if isinstance(row, Mapping)]
    return []


def _paper_lifecycle_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if _text(row.get("paper_position_id") or row.get("paper_observation_id") or row.get("position_id")) or _upper(row.get("candidate_to_paper_status")) == "PAPER_OBSERVATION_CREATED"]


def _furthest(raw: int, candidates: int, contracts: int, observations: int, outcomes: int, samples: int) -> str:
    values = [1, raw, candidates, contracts, observations, outcomes, samples]
    furthest = "HYPOTHESIS"
    for stage, count in zip(STAGES, values):
        if count > 0:
            furthest = stage
    return furthest


def _sleeve_status(workflow: list[dict[str, Any]], quality: list[dict[str, Any]]) -> str:
    if workflow:
        return _text(workflow[0].get("current_state")) or "ACTIVE"
    if quality:
        return _text(quality[0].get("quality_status")) or "ACTIVE"
    return "ACTIVE"


def _expected_or_abnormal(status: str, blocker_code: str, quality: list[dict[str, Any]], workflow: list[dict[str, Any]]) -> str:
    if status == "FLOWING":
        return "EXPECTED_FLOWING"
    reason_text = json.dumps({"quality": quality, "workflow": workflow}, sort_keys=True, default=str)
    if "UNDERPOWERED" in reason_text or blocker_code in {"NO_OUTCOMES_AVAILABLE", "NO_VALIDATION_SAMPLES_AVAILABLE", "NO_SIGNALS_GENERATED"}:
        return "EXPECTED_UNDERPOWERED_OR_ACCUMULATING"
    if blocker_code in {"NEEDS_DATA", "INSUFFICIENT_MARKET_DATA"}:
        return "EXPECTED_BLOCKED_BY_DECLARED_DATA_NEED"
    return "ABNORMAL_OR_REQUIRES_REVIEW"


def _timestamps(rows: list[dict[str, Any]]) -> list[str]:
    keys = ("generated_at_utc", "generated_at", "computed_at_utc", "candidate_snapshot_timestamp_utc", "timestamp_utc", "state_entered_at_utc")
    out = []
    for row in rows:
        for key in keys:
            value = _text(row.get(key))
            if value:
                out.append(value)
    return out


def _first_action_code(actions: list[dict[str, Any]]) -> str:
    action = actions[0]
    if _text(action.get("action_type")) == "PROVIDE_DATA_SOURCE":
        return "INSUFFICIENT_MARKET_DATA"
    return _text(action.get("action_type")) or "OPERATOR_ACTION_REQUIRED"


def _first_action_reason(actions: list[dict[str, Any]]) -> str:
    action = actions[0]
    return _text(action.get("why_action_needed") or action.get("blocking_what")) or "Operator action queue contains an action for this sleeve."


def _macro_needs_data(payloads: Mapping[str, Any]) -> bool:
    macro = _dict(payloads.get("macro_calendar_data_readiness"))
    return _text(macro.get("status")) == "NEEDS_SOURCE" or macro.get("macro_calendar_ready") is False


def _max_int(rows: list[dict[str, Any]], key: str) -> int:
    values = []
    for row in rows:
        try:
            values.append(int(row.get(key) or 0))
        except (TypeError, ValueError):
            values.append(0)
    return max(values) if values else 0


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _text(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _text(value).upper()
