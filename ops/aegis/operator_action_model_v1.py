from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

REPORT_FAMILY = "aegis_operator_action_model_v1"
REPORT_FILENAME = "operator_action_model.v1.json"
REQUIRED_CAPABILITIES = [
    "CANDIDATE_GENERATION",
    "PAPER_MONITORING",
    "OUTCOME_REALIZATION",
    "HYPOTHESIS_VALIDATION",
    "TRADE_RECOMMENDATION",
    "MANUAL_TRADE_CAPTURE",
    "BROKER_EXECUTION",
    "DAVID_ACTION",
]
ALLOWED_STATUSES = {"READY", "COMPLETE", "ACTIVE", "WAITING", "BLOCKED", "NOT_APPLICABLE", "DISABLED_BY_POLICY"}


def operator_action_model_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / REPORT_FAMILY / day_utc / REPORT_FILENAME


def build_operator_action_model_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).resolve()
    sources = _source_paths(root, day_utc)
    payloads = {name: _read_json(path) for name, path in sources.items()}
    source_artifacts = {name: str(path) for name, path in sorted(sources.items()) if path.exists()}
    source_hashes = {str(path): _sha256(path) for path in sorted(sources.values(), key=lambda item: item.as_posix()) if path.exists()}
    generated_at = _derived_generated_at(payloads, day_utc)

    runtime = payloads.get("runtime_truth_kernel", {})
    control = payloads.get("chatgpt_control_packet", {})
    canonical = payloads.get("canonical_operator_state", {})
    candidate_state = payloads.get("candidate_state", {})
    outcome = payloads.get("outcome_registry", {})
    sufficiency = payloads.get("statistical_sufficiency", {})
    paper_ledger = payloads.get("paper_position_ledger", {})
    queue_audit = payloads.get("command_center_queue_audit", {})

    candidate_projection = canonical.get("candidate_ui_projection") if isinstance(canonical.get("candidate_ui_projection"), Mapping) else {}
    run_summary = candidate_projection.get("run_summary") if isinstance(candidate_projection.get("run_summary"), Mapping) else {}
    output_count = _int(
        run_summary.get("diagnostics_candidates_generated"),
        run_summary.get("diagnostic_candidate_outputs"),
        candidate_projection.get("diagnostic_candidate_outputs"),
        candidate_projection.get("diagnostics_candidates_generated"),
        candidate_projection.get("current_day_candidate_contract_count"),
        0,
    )
    rejection_count = _int(candidate_projection.get("diagnostic_rejection_count"), 0)
    candidate_run_completed = bool(
        candidate_projection.get("diagnostics_status") == "AVAILABLE"
        or run_summary.get("diagnostics_completed_at")
        or run_summary.get("completed_at")
        or candidate_projection.get("canonical_generated_at")
    )
    open_positions = _int(
        outcome.get("summary", {}).get("open_outcome_count") if isinstance(outcome.get("summary"), Mapping) else None,
        outcome.get("summary", {}).get("open_outcomes") if isinstance(outcome.get("summary"), Mapping) else None,
        outcome.get("open_outcome_count"),
        paper_ledger.get("open_position_count"),
        candidate_state.get("active_candidate_count"),
        0,
    )
    closed_positions = _int(
        outcome.get("summary", {}).get("closed_outcome_count") if isinstance(outcome.get("summary"), Mapping) else None,
        outcome.get("summary", {}).get("closed_outcomes") if isinstance(outcome.get("summary"), Mapping) else None,
        outcome.get("closed_outcome_count"),
        paper_ledger.get("closed_position_count"),
        0,
    )
    validation_samples = _int(
        outcome.get("summary", {}).get("validation_sample_count") if isinstance(outcome.get("summary"), Mapping) else None,
        payloads.get("validation_samples", {}).get("summary", {}).get("included_sample_count") if isinstance(payloads.get("validation_samples", {}).get("summary"), Mapping) else None,
        payloads.get("validation_samples", {}).get("summary", {}).get("included_samples") if isinstance(payloads.get("validation_samples", {}).get("summary"), Mapping) else None,
        0,
    )
    suff_summary = sufficiency.get("summary") if isinstance(sufficiency.get("summary"), Mapping) else {}
    underpowered = _int(suff_summary.get("UNDERPOWERED"), suff_summary.get("underpowered"), len(sufficiency.get("hypotheses") or []), 0)

    trade_advice_allowed = bool(runtime.get("trade_advice_allowed") is True or control.get("trade_advice_allowed") is True)
    manual_capture_allowed = bool(runtime.get("manual_trade_capture_allowed") is True or control.get("manual_trade_capture_allowed") is True)
    broker_allowed = bool(runtime.get("broker_submit_required") is True or runtime.get("broker_execution_allowed") is True)
    autonomous_allowed = bool(runtime.get("autonomous_execution_allowed") is True or control.get("autonomous_execution_allowed") is True)
    runtime_class = str(runtime.get("runtime_truth_classification") or control.get("runtime_truth_classification") or "UNKNOWN")
    blocked_capabilities = [str(item) for item in runtime.get("blocked_capabilities") or []]
    manual_packet_count = _int((control.get("aegis_lite_status") or {}).get("manual_packet_actionable_count") if isinstance(control.get("aegis_lite_status"), Mapping) else None, 0)
    manual_packet_present = bool((control.get("aegis_lite_status") or {}).get("manual_trade_packet_present") is True) if isinstance(control.get("aegis_lite_status"), Mapping) else False
    eligible_manual_packet = manual_packet_count > 0 or manual_packet_present

    queue_summary = queue_audit.get("summary") if isinstance(queue_audit.get("summary"), Mapping) else {}
    real_human_task_count = _int(queue_summary.get("operator_action_required_count"), 0)

    rows = [
        _candidate_generation_row(sources, source_hashes, candidate_run_completed, output_count, rejection_count),
        _paper_monitoring_row(sources, source_hashes, open_positions, paper_ledger),
        _outcome_realization_row(sources, source_hashes, open_positions, closed_positions, outcome),
        _hypothesis_validation_row(sources, source_hashes, validation_samples, underpowered, sufficiency),
        _trade_recommendation_row(sources, source_hashes, trade_advice_allowed, runtime_class, blocked_capabilities),
        _manual_trade_capture_row(sources, source_hashes, manual_capture_allowed, eligible_manual_packet, manual_packet_count, runtime_class),
        _broker_execution_row(sources, source_hashes, broker_allowed, autonomous_allowed),
        _david_action_row(sources, source_hashes, real_human_task_count),
    ]
    capabilities_by_id = {row["capability_id"]: row for row in rows}
    counts = {status: sum(1 for row in rows if row["status"] == status) for status in sorted(ALLOWED_STATUSES)}
    david_action_required = bool(capabilities_by_id["DAVID_ACTION"]["david_action_required"])
    monitoring_active = capabilities_by_id["PAPER_MONITORING"]["status"] in {"ACTIVE", "COMPLETE"}
    candidate_complete = capabilities_by_id["CANDIDATE_GENERATION"]["status"] in {"COMPLETE", "ACTIVE"}
    top_level = "David action required." if david_action_required else "Monitoring only. No David action required."
    explanatory = "Aegis is blocked from trade advice/manual capture, but not blocked from monitoring."
    if trade_advice_allowed or manual_capture_allowed:
        explanatory = "Aegis capability status is separated from David action status."

    return {
        "schema_id": "aegis_operator_action_model",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day_utc,
        "generated_at": generated_at,
        "summary": {
            "top_level_summary": top_level,
            "explanatory_sentence": explanatory,
            "aegis_can_operate": bool(monitoring_active or candidate_complete),
            "david_action_required": david_action_required,
            "blocked_capability_count": counts.get("BLOCKED", 0),
            "waiting_capability_count": counts.get("WAITING", 0),
            "disabled_by_policy_count": counts.get("DISABLED_BY_POLICY", 0),
            "active_capability_count": counts.get("ACTIVE", 0),
            "complete_capability_count": counts.get("COMPLETE", 0),
            "open_paper_position_count": open_positions,
            "closed_paper_position_count": closed_positions,
            "current_day_candidate_count": output_count,
            "validation_sample_count": validation_samples,
        },
        "capability_matrix": rows,
        "capabilities_by_id": capabilities_by_id,
        "source_artifacts": source_artifacts,
        "source_hashes": source_hashes,
        "safety": {
            "trade_advice_allowed": trade_advice_allowed,
            "manual_trade_capture_allowed": manual_capture_allowed,
            "broker_execution_allowed": broker_allowed,
            "autonomous_execution_allowed": autonomous_allowed,
            "live_trading_allowed": False,
            "policy_changed": False,
        },
    }


def write_operator_action_model_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    root = Path(truth_root).resolve()
    path = operator_action_model_path_v1(truth_root=root, day_utc=day_utc)
    body = dict(payload or build_operator_action_model_v1(truth_root=root, day_utc=day_utc))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _candidate_generation_row(sources: Mapping[str, Path], hashes: Mapping[str, str], completed: bool, output_count: int, rejection_count: int) -> dict[str, Any]:
    if completed:
        if output_count > 0:
            return _row("CANDIDATE_GENERATION", "COMPLETE", ["CANDIDATE_RUN_COMPLETED", "CANDIDATES_GENERATED"], f"Current-day candidate generation completed with {output_count} output candidate(s).", [sources["canonical_operator_state"], sources["candidate_state"]], hashes, "Wait for the next scheduled candidate generation run.", False)
        return _row("CANDIDATE_GENERATION", "COMPLETE", ["CANDIDATE_RUN_COMPLETED", "ZERO_OUTPUT_CANDIDATES"], f"Current-day candidate generation completed with 0 output candidates and {rejection_count} rejected/suppressed raw signals.", [sources["canonical_operator_state"], sources["candidate_state"]], hashes, "Wait for the next scheduled candidate generation run.", False)
    return _row("CANDIDATE_GENERATION", "BLOCKED", ["CANDIDATE_RUN_NOT_CONFIRMED"], "Current-day candidate generation completion is not confirmed by canonical operator state.", [sources["canonical_operator_state"]], hashes, "Generate canonical operator state and candidate diagnostics.", False)


def _paper_monitoring_row(sources: Mapping[str, Path], hashes: Mapping[str, str], open_positions: int, ledger: Mapping[str, Any]) -> dict[str, Any]:
    if open_positions > 0:
        return _row("PAPER_MONITORING", "ACTIVE", ["OPEN_PAPER_POSITIONS_MONITORED"], f"{open_positions} open paper position(s) are available for monitoring.", [sources["paper_position_ledger"], sources["outcome_registry"]], hashes, "Continue mark and outcome monitoring until positions close or expire.", False)
    if ledger:
        return _row("PAPER_MONITORING", "COMPLETE", ["NO_OPEN_PAPER_POSITIONS"], "No open paper positions are currently recorded.", [sources["paper_position_ledger"]], hashes, "Wait for new paper positions before monitoring resumes.", False)
    return _row("PAPER_MONITORING", "BLOCKED", ["PAPER_POSITION_LEDGER_MISSING"], "Paper monitoring cannot be explained because the paper position ledger is missing.", [sources["paper_position_ledger"]], hashes, "Regenerate paper position ledger.", False)


def _outcome_realization_row(sources: Mapping[str, Path], hashes: Mapping[str, str], open_positions: int, closed_positions: int, outcome: Mapping[str, Any]) -> dict[str, Any]:
    if closed_positions > 0:
        return _row("OUTCOME_REALIZATION", "ACTIVE", ["CLOSED_OUTCOMES_AVAILABLE"], f"{closed_positions} closed paper outcome(s) are available; {open_positions} remain open.", [sources["outcome_registry"], sources["paper_position_ledger"]], hashes, "Convert resolved outcomes into validation samples.", False)
    if open_positions > 0:
        return _row("OUTCOME_REALIZATION", "WAITING", ["OPEN_POSITIONS_NOT_RESOLVED"], f"{open_positions} paper position(s) are still open; no closed outcomes are available yet.", [sources["outcome_registry"], sources["paper_position_ledger"]], hashes, "Wait for positions to close, expire, or otherwise resolve.", False)
    if outcome:
        return _row("OUTCOME_REALIZATION", "NOT_APPLICABLE", ["NO_PAPER_POSITIONS_TO_RESOLVE"], "No paper positions currently require outcome realization.", [sources["outcome_registry"]], hashes, "Wait for new paper positions.", False)
    return _row("OUTCOME_REALIZATION", "BLOCKED", ["OUTCOME_REGISTRY_MISSING"], "Outcome realization cannot be explained because the outcome registry is missing.", [sources["outcome_registry"]], hashes, "Regenerate outcome validation artifacts.", False)


def _hypothesis_validation_row(sources: Mapping[str, Path], hashes: Mapping[str, str], sample_count: int, underpowered: int, sufficiency: Mapping[str, Any]) -> dict[str, Any]:
    if not sufficiency:
        return _row("HYPOTHESIS_VALIDATION", "BLOCKED", ["STATISTICAL_SUFFICIENCY_MISSING"], "Hypothesis validation status is unavailable because statistical sufficiency evidence is missing.", [sources["statistical_sufficiency"]], hashes, "Regenerate outcome validation maturity artifacts.", False)
    if sample_count <= 0 or underpowered > 0:
        return _row("HYPOTHESIS_VALIDATION", "WAITING", ["INSUFFICIENT_CLOSED_OUTCOMES", "NO_INCLUDED_VALIDATION_SAMPLES"], "Hypothesis validation is waiting for closed, traceable outcomes; no included validation samples are available yet.", [sources["statistical_sufficiency"], sources["outcome_registry"]], hashes, "Wait for paper positions to produce resolved validation samples.", False)
    return _row("HYPOTHESIS_VALIDATION", "ACTIVE", ["VALIDATION_SAMPLES_AVAILABLE"], f"{sample_count} validation sample(s) are available for hypothesis validation.", [sources["statistical_sufficiency"]], hashes, "Continue accumulating samples until sufficiency thresholds pass.", False)


def _trade_recommendation_row(sources: Mapping[str, Path], hashes: Mapping[str, str], allowed: bool, runtime_class: str, blocked: list[str]) -> dict[str, Any]:
    if allowed:
        return _row("TRADE_RECOMMENDATION", "READY", ["TRADE_ADVICE_ALLOWED_BY_RUNTIME_TRUTH"], "Runtime truth currently allows trade recommendations.", [sources["runtime_truth_kernel"], sources["chatgpt_control_packet"]], hashes, "Only issue recommendations through governed advisory workflow.", False)
    codes = ["TRADE_ADVICE_DISABLED_BY_RUNTIME_TRUTH"]
    if runtime_class:
        codes.append(f"RUNTIME_TRUTH_{runtime_class.upper()}")
    if "TRADE_ADVICE_ALLOWED" in blocked:
        codes.append("TRADE_ADVICE_ALLOWED_BLOCKED")
    return _row("TRADE_RECOMMENDATION", "BLOCKED", codes, "Trade recommendations are blocked by runtime truth and must remain unavailable.", [sources["runtime_truth_kernel"], sources["chatgpt_control_packet"]], hashes, "Refresh runtime truth dependencies before advisory workflows can be considered.", False)


def _manual_trade_capture_row(sources: Mapping[str, Path], hashes: Mapping[str, str], allowed: bool, eligible_packet: bool, packet_count: int, runtime_class: str) -> dict[str, Any]:
    if allowed and eligible_packet:
        return _row("MANUAL_TRADE_CAPTURE", "READY", ["ELIGIBLE_MANUAL_PACKET_PRESENT", "MANUAL_CAPTURE_ALLOWED"], f"{packet_count or 1} eligible manual trade packet(s) are present and runtime truth allows capture.", [sources["runtime_truth_kernel"], sources["chatgpt_control_packet"]], hashes, "Capture only externally executed fills with governed receipt evidence.", True)
    if eligible_packet and not allowed:
        return _row("MANUAL_TRADE_CAPTURE", "BLOCKED", ["MANUAL_CAPTURE_DISABLED_BY_RUNTIME_TRUTH", f"RUNTIME_TRUTH_{runtime_class.upper()}"], "An eligible manual packet may exist, but manual capture is blocked by runtime truth.", [sources["runtime_truth_kernel"], sources["chatgpt_control_packet"]], hashes, "Refresh runtime/manual capture dependencies before capture can be considered.", False)
    return _row("MANUAL_TRADE_CAPTURE", "NOT_APPLICABLE", ["NO_ELIGIBLE_MANUAL_TRADE_PACKET"], "No eligible manual trade packet exists, so manual trade capture is not applicable.", [sources["chatgpt_control_packet"], sources["canonical_operator_state"]], hashes, "Wait for an eligible manual packet before capture can apply.", False)


def _broker_execution_row(sources: Mapping[str, Path], hashes: Mapping[str, str], broker_allowed: bool, autonomous_allowed: bool) -> dict[str, Any]:
    if broker_allowed or autonomous_allowed:
        return _row("BROKER_EXECUTION", "BLOCKED", ["BROKER_POLICY_UNEXPECTEDLY_ENABLED"], "Broker or autonomous execution evidence is enabled unexpectedly; review policy before proceeding.", [sources["runtime_truth_kernel"], sources["chatgpt_control_packet"]], hashes, "Investigate policy state immediately.", False)
    return _row("BROKER_EXECUTION", "DISABLED_BY_POLICY", ["BROKER_EXECUTION_DISABLED_BY_DESIGN", "AUTONOMOUS_EXECUTION_DISABLED_BY_DESIGN"], "Broker execution and autonomous execution are disabled by design and out of scope.", [sources["runtime_truth_kernel"], sources["chatgpt_control_packet"]], hashes, "No broker execution event is expected.", False)


def _david_action_row(sources: Mapping[str, Path], hashes: Mapping[str, str], task_count: int) -> dict[str, Any]:
    if task_count > 0:
        return _row("DAVID_ACTION", "ACTIVE", ["REAL_OPERATOR_TASK_PRESENT"], f"{task_count} real Command Center operator task(s) require David action.", [sources["command_center_queue_audit"]], hashes, "Review the current Command Center action queue.", True)
    return _row("DAVID_ACTION", "COMPLETE", ["NO_REAL_OPERATOR_TASKS"], "No real David action is currently required; capability blocks are shown separately.", [sources["command_center_queue_audit"], sources["chatgpt_control_packet"]], hashes, "No human action is expected until a real operator task appears.", False)


def _row(capability_id: str, status: str, reason_codes: list[str], reason: str, source_paths: list[Path], hashes: Mapping[str, str], next_event: str, david_action_required: bool) -> dict[str, Any]:
    if status not in ALLOWED_STATUSES:
        raise ValueError(f"invalid status {status}")
    artifacts = sorted(str(path) for path in source_paths)
    return {
        "capability_id": capability_id,
        "status": status,
        "status_label": status.replace("_", " ").title(),
        "reason_codes": sorted(set(reason_codes)),
        "human_readable_reason": reason,
        "source_artifacts": artifacts,
        "source_hashes": {path: hashes[path] for path in artifacts if path in hashes},
        "next_expected_event": next_event,
        "david_action_required": bool(david_action_required),
    }


def _source_paths(root: Path, day: str) -> dict[str, Path]:
    reports = root / "reports"
    return {
        "runtime_truth_kernel": reports / "aegis_runtime_truth_kernel_v1" / day / "runtime_truth_kernel.v1.json",
        "chatgpt_control_packet": reports / "aegis_chatgpt_control_packet_v1" / day / "aegis_chatgpt_control_packet.v1.json",
        "canonical_operator_state": reports / "aegis_canonical_operator_state_v1" / day / "canonical_operator_state.v1.json",
        "candidate_state": reports / "aegis_candidate_state_v1" / day / "candidate_state.v1.json",
        "candidate_generation_diagnostics": reports / "aegis_candidate_generation_diagnostics_v1" / day / "candidate_generation_diagnostics.v1.json",
        "sleeve_analytics": reports / "aegis_sleeve_analytics_v1" / day / "sleeve_analytics.v1.json",
        "outcome_registry": reports / "aegis_outcome_registry_v1" / day / "outcome_registry.v1.json",
        "validation_samples": reports / "aegis_validation_samples_v1" / day / "validation_samples.v1.json",
        "statistical_sufficiency": reports / "aegis_statistical_sufficiency_v1" / day / "statistical_sufficiency.v1.json",
        "research_portfolio": reports / "aegis_research_portfolio_v1" / day / "research_portfolio.v1.json",
        "research_capital_allocation": reports / "aegis_research_capital_allocation_v1" / day / "research_capital_allocation.v1.json",
        "paper_position_ledger": reports / "aegis_paper_position_ledger_v1" / day / "paper_position_ledger.v1.json",
        "command_center_queue_audit": reports / "aegis_command_center_queue_audit_v1" / day / "command_center_queue_audit.v1.json",
        "portal_runtime_model": reports / "aegis_verified_runtime_graph_v1" / day / "portal_runtime_model.v1.json",
    }


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _int(*values: Any) -> int:
    for value in values:
        if value is None or value == "":
            continue
        try:
            return int(value)
        except Exception:
            try:
                return int(float(value))
            except Exception:
                continue
    return 0


def _derived_generated_at(payloads: Mapping[str, Mapping[str, Any]], day: str) -> str:
    values: list[str] = []
    for payload in payloads.values():
        for key in ("generated_at", "generated_at_utc", "packet_generated_at_utc"):
            value = payload.get(key) if isinstance(payload, Mapping) else None
            if value:
                values.append(str(value))
    return sorted(values)[-1] if values else f"{day}T00:00:00Z"
