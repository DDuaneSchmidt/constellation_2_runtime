from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any

from ops.aegis.candidate_generation_visibility_v1 import build_candidate_generation_visibility_v1
from ops.aegis.intelligence_common_v1 import ai_evidence_v1, latest_json_v1, read_json_v1, write_json_v1
from ops.aegis.research_hypothesis_classification_v1 import build_research_hypothesis_classification_v1
from ops.aegis.run_history_v1 import latest_run_row_v1


REPORT_FAMILY = "aegis_canonical_operator_state_v1"
SOURCE_SPECS = {
    "runtime_truth": ("aegis_runtime_truth_kernel_v1", "runtime_truth_kernel.v1.json", True),
    "candidate_lifecycle": ("aegis_candidate_lifecycle_v1", "candidate_lifecycle.v1.json", True),
    "position_management": ("aegis_position_management_v1", "position_management.v1.json", False),
    "candidate_review_ledger": ("aegis_candidate_review_ledger_v1", "candidate_review_ledger.v1.json", False),
    "candidate_ranking": ("aegis_candidate_ranking_v1", "candidate_ranking.v1.json", False),
    "candidate_portfolio_selection": ("aegis_candidate_portfolio_selection_v1", "candidate_portfolio_selection.v1.json", False),
    "candidate_generation_diagnostics": ("aegis_candidate_generation_diagnostics_v1", "candidate_generation_diagnostics.v1.json", False),
    "candidate_contracts": ("aegis_candidate_contracts_v1", "candidate_contracts.v1.json", False),
    "entry_reference_price_certification": ("aegis_entry_reference_price_certification_v1", "entry_reference_price_certification.v1.json", False),
    "candidate_to_paper_lifecycle": ("aegis_candidate_to_paper_lifecycle_v1", "candidate_to_paper_lifecycle.v1.json", False),
    "candidate_review_packet": ("aegis_candidate_review_packet_v1", "candidate_review_packet.v1.json", False),
    "paper_review_queue": ("aegis_paper_review_queue_v1", "paper_review_queue.v1.json", False),
    "paper_trade_outcomes": ("aegis_paper_trade_outcomes_v1", "paper_trade_outcomes.v1.json", False),
    "paper_position_ledger": ("aegis_paper_position_ledger_v1", "paper_position_ledger.v1.json", False),
    "paper_lifecycle_reconciliation": ("aegis_paper_lifecycle_reconciliation_v1", "paper_lifecycle_reconciliation.v1.json", False),
    "trading_lifecycle_state": ("aegis_trading_lifecycle_state_v1", "trading_lifecycle_state.v1.json", False),
    "exit_recommendations": ("aegis_exit_recommendations_v1", "exit_recommendations.v1.json", False),
    "exit_logic_review": ("aegis_exit_logic_review_v1", "exit_logic_review.v1.json", False),
    "candidate_state": ("aegis_candidate_state_v1", "candidate_state.v1.json", False),
    "run_history": ("aegis_run_history_v1", "run_history.v1.json", False),
    "data_remediation": ("aegis_data_remediation_v1", "data_remediation_latest.v1.json", False),
    "data_registry": ("aegis_data_registry_v1", "data_registry.v1.json", False),
    "sleeve_input_contracts": ("aegis_sleeve_input_contracts_v1", "sleeve_input_contracts.v1.json", False),
    "input_contract_reconciliation": ("aegis_input_contract_reconciliation_v1", "input_contract_reconciliation.v1.json", False),
    "market_data_coverage": ("aegis_market_data_coverage_v1", "market_data_coverage.v1.json", False),
    "sleeve_readiness": ("aegis_sleeve_readiness_v1", "sleeve_readiness.v1.json", False),
    "noon_preflight": ("aegis_noon_preflight_v1", "noon_preflight.v1.json", False),
    "sleeve_performance_analytics": ("aegis_sleeve_performance_analytics_v1", "sleeve_performance_analytics.v1.json", False),
    "advisory_quality": ("aegis_sleeve_performance_analytics_v1", "advisory_quality.v1.json", False),
    "research_lab": ("aegis_research_lab_execution_loop_v1", "research_lab_execution_loop.v1.json", False),
    "sleeve_challenger": ("aegis_sleeve_challenger_v1", "sleeve_challenger.v1.json", False),
    "regime_outcome_memory": ("aegis_regime_outcome_memory_v1", "regime_outcome_memory.v1.json", False),
    "intelligence_governance": ("aegis_intelligence_governance_kernel_v1", "intelligence_governance_kernel.v1.json", False),
    "triggered_sleeve_runs": ("aegis_triggered_sleeve_runs_v1", "triggered_sleeve_runs.v1.json", False),
    "journal_timeline": ("aegis_journal_timeline_v1", "journal_timeline.v1.json", False),
}


def build_canonical_operator_state_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    sources = _load_sources(root, day_utc)
    payloads = {key: row["payload"] for key, row in sources.items()}
    paths = [row["path"] for row in sources.values() if row["path"]]
    hashes = {key: row["hash"] for key, row in sources.items() if row["hash"]}
    research_pipeline_path, research_pipeline_payload = latest_json_v1(root, "aegis_research_pipeline_v1", day_utc, "research_pipeline.v1.json")
    if research_pipeline_path:
        paths.append(str(research_pipeline_path))
        hashes["research_pipeline"] = _file_hash(research_pipeline_path)
    operator_snapshot_path, operator_snapshot_payload = latest_json_v1(root, "operator_state_snapshot_v1", day_utc, "operator_state_snapshot.v1.json")
    if operator_snapshot_path:
        paths.append(str(operator_snapshot_path))
        hashes["operator_state_snapshot_v1"] = _file_hash(operator_snapshot_path)
    operator_snapshot = operator_snapshot_payload if isinstance(operator_snapshot_payload, dict) else {}
    freshness = {key: {k: v for k, v in row.items() if k in {"path", "hash", "found", "generated_at", "freshness_status"}} for key, row in sources.items()}
    missing = [
        {
            "source": key,
            "family": SOURCE_SPECS[key][0],
            "filename": SOURCE_SPECS[key][1],
            "critical": SOURCE_SPECS[key][2],
            "status": "MISSING",
        }
        for key, row in sources.items()
        if not row["found"] and SOURCE_SPECS[key][2]
    ]
    runtime = _runtime_section(payloads["runtime_truth"])
    candidates = _candidate_sections(payloads["candidate_lifecycle"])
    ranking_by_id = _ranking_by_id(payloads["candidate_ranking"])
    top_candidates = _top_candidates(candidates, ranking_by_id)
    positions = _positions_section(payloads["position_management"], candidates=candidates, source=sources.get("position_management", {}), paper_position_ledger=payloads.get("paper_position_ledger", {}), paper_position_source=sources.get("paper_position_ledger", {}))
    conflicts = _conflicts(payloads=payloads, candidates=candidates, ranking_by_id=ranking_by_id)
    sleeves = _sleeve_sections(payloads["sleeve_challenger"], payloads["sleeve_performance_analytics"])
    performance = _performance_section(payloads["sleeve_performance_analytics"], payloads["advisory_quality"])
    research = _research_section(root, day_utc, payloads["research_lab"])
    research_pipeline = _research_pipeline_section(research_pipeline_payload)
    research["pipeline"] = research_pipeline.get("pipeline", {})
    research["priority_pipeline"] = research_pipeline.get("priority_pipeline", {})
    research["priority_counts"] = research_pipeline.get("priority_counts", {})
    research["recommended_focus_today"] = research_pipeline.get("recommended_focus_today", [])
    research["pipeline_counts"] = research_pipeline.get("counts", {})
    research["pipeline_blocked_items"] = research_pipeline.get("blocked_items", [])
    research["pipeline_next_operator_actions"] = research_pipeline.get("next_operator_actions", [])
    paths = sorted(set(paths + [str(path) for path in research.get("source_artifacts", []) if path]))
    hashes = {
        **hashes,
        **{f"research_hypothesis:{key}": value for key, value in (research.get("source_hashes") or {}).items() if value},
    }
    governance = _governance_section(payloads["intelligence_governance"])
    regime = _regime_section(payloads["regime_outcome_memory"])
    event_triggers = _event_triggers(payloads["triggered_sleeve_runs"])
    warnings = _warnings(runtime=runtime, missing=missing, sleeves=sleeves, conflicts=conflicts)
    actions = _actions_required(runtime=runtime, candidates=candidates, governance=governance, sleeves=sleeves, research=research, missing=missing, sources=sources)
    candidate_ui_projection = build_candidate_ui_projection_v1(
        payloads=payloads,
        sources=sources,
        day_utc=day_utc,
        canonical_generated_at=_deterministic_generated_at(sources, day_utc),
    )
    opportunities = _opportunities_projection(
        candidates=candidates,
        top_candidates=top_candidates,
        candidate_review_ledger=payloads["candidate_review_ledger"],
        event_triggers=event_triggers,
        diagnostics=payloads["candidate_generation_diagnostics"],
        data_remediation=payloads["data_remediation"],
        data_registry=payloads["data_registry"],
        sleeve_readiness=payloads["sleeve_readiness"],
        noon_preflight=payloads["noon_preflight"],
        positions=positions,
        portfolio_selection=payloads["candidate_portfolio_selection"],
    )
    edge_lab = _edge_lab_projection(research=research, sleeves=sleeves, pipeline=research_pipeline)
    performance = {**performance, **_performance_workflow_projection(candidates=candidates, performance=performance, sleeves=sleeves, research=research, regime=regime)}
    journal = _journal_projection(
        sources=sources,
        candidates=candidates,
        research=research,
        governance=governance,
        drilldowns=_drilldown_index(sources, candidates, top_candidates, sleeves, research, governance),
        journal_timeline=payloads["journal_timeline"],
    )
    ai = ai_evidence_v1(repo_root)
    return {
        "schema_id": "aegis_canonical_operator_state",
        "schema_version": "v1",
        "artifact_id": "aegis_canonical_operator_state_v1",
        "day_utc": day_utc,
        "generated_at_utc": _deterministic_generated_at(sources, day_utc),
        "truth_root": str(root),
        "projection_semantics": {
            "read_only": True,
            "source_of_truth": False,
            "may_mutate_runtime_truth": False,
            "may_mutate_candidates": False,
            "may_mutate_sleeves": False,
            "may_approve_recommendations": False,
        },
        "source_artifacts": paths,
        "source_hashes": hashes,
        "freshness": freshness,
        "conflicts": conflicts,
        "missing_inputs": missing,
        "runtime": runtime,
        "operator_state_snapshot_v1": operator_snapshot,
        "trade_ticket_projection_v1": operator_snapshot.get("trade_ticket_projection_v1") if isinstance(operator_snapshot.get("trade_ticket_projection_v1"), dict) else None,
        "legacy_module_runtime_quarantine": operator_snapshot.get("legacy_module_runtime_quarantine") if isinstance(operator_snapshot.get("legacy_module_runtime_quarantine"), dict) else None,
        "actions_required": actions,
        "candidates": candidates,
        "positions": positions,
        "top_candidates": top_candidates,
        "sleeves": sleeves,
        "performance": performance,
        "research": research,
        "governance": governance,
        "regime": regime,
        "event_triggers": event_triggers,
        "warnings": warnings,
        "opportunities": opportunities,
        "candidate_ui_projection": candidate_ui_projection,
        "edge_lab": edge_lab,
        "journal": journal,
        "no_action_now": _no_action_now(actions, top_candidates, warnings),
        "drilldown_index": _drilldown_index(sources, candidates, top_candidates, sleeves, research, governance),
        "ai_used": bool(ai["ai_used"]),
        "deterministic_fallback": bool(ai["deterministic_fallback"]),
        "safety": {
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "automatic_sleeve_mutation_allowed": False,
            "automatic_approval_allowed": False,
        },
    }


def write_canonical_operator_state_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "canonical_operator_state.v1.json", payload)
    summary_path = out_dir / "canonical_operator_state.summary.txt"
    matrix_path = out_dir / "canonical_operator_state.matrix.csv"
    summary_path.write_text(render_canonical_operator_state_summary_v1(payload), encoding="utf-8")
    matrix_path.write_text(render_canonical_operator_state_matrix_csv_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path), "matrix": str(matrix_path)}


def render_canonical_operator_state_summary_v1(payload: dict[str, Any]) -> str:
    runtime = payload.get("runtime") if isinstance(payload.get("runtime"), dict) else {}
    candidates = payload.get("candidates") if isinstance(payload.get("candidates"), dict) else {}
    governance = payload.get("governance") if isinstance(payload.get("governance"), dict) else {}
    lines = [
        "AEGIS CANONICAL OPERATOR STATE v1",
        f"day_utc: {payload.get('day_utc')}",
        "read_only_projection: true",
        f"runtime_truth_classification: {runtime.get('runtime_truth_classification', 'UNKNOWN')}",
        f"highest_readiness_layer: {runtime.get('highest_readiness_layer', 'UNKNOWN')}",
        f"actions_required: {len(payload.get('actions_required') or [])}",
        f"review_required: {len(candidates.get('review_required') or candidates.get('awaiting_decision') or [])}",
        f"watchlisted: {len(candidates.get('watchlisted') or [])}",
        f"needs_more_evidence: {len(candidates.get('needs_more_evidence') or [])}",
        f"dismissed: {len(candidates.get('dismissed') or [])}",
        f"expired: {len(candidates.get('expired') or [])}",
        f"awaiting_outcome: {len(candidates.get('awaiting_outcome') or [])}",
        f"open_positions: {len((payload.get('positions') or {}).get('open_positions') or [])}",
        f"stopped_positions: {len((payload.get('positions') or {}).get('stopped_positions') or [])}",
        f"pending_risk_plan: {len((payload.get('positions') or {}).get('pending_risk_plan') or [])}",
        f"corrected_candidates: {len(candidates.get('corrected') or [])}",
        f"top_candidates: {len(payload.get('top_candidates') or [])}",
        f"portfolio_selected_candidates: {len(((payload.get('opportunities') or {}).get('selected_candidates') or []))}",
        f"portfolio_suppressed_candidates: {len(((payload.get('opportunities') or {}).get('suppressed_candidates') or []))}",
        f"candidate_contract_count: {((payload.get('candidate_ui_projection') or {}).get('candidate_contract_count') or 0)}",
        f"awaiting_paper_review: {((payload.get('candidate_ui_projection') or {}).get('awaiting_review_count') or 0)}",
        f"captured_hypotheses: {len((payload.get('research') or {}).get('captured_hypotheses') or [])}",
        f"governance_awaiting_approval: {len(governance.get('awaiting_approval') or [])}",
        f"missing_inputs: {len(payload.get('missing_inputs') or [])}",
        f"conflicts: {len(payload.get('conflicts') or [])}",
        "broker_execution_allowed: false",
        "autonomous_execution_allowed: false",
        "",
        "actions:",
    ]
    for action in payload.get("actions_required") or [{"priority": "INFO", "title": "No operator action required."}]:
        lines.append(f"- {action.get('priority')}: {action.get('title')}")
    lines.append("")
    return "\n".join(lines)


def render_canonical_operator_state_matrix_csv_v1(payload: dict[str, Any]) -> str:
    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=["section", "id", "status", "priority", "title", "source_artifact"])
    writer.writeheader()
    for action in payload.get("actions_required") or []:
        writer.writerow({"section": "action", "id": action.get("action_id", ""), "status": action.get("type", ""), "priority": action.get("priority", ""), "title": action.get("title", ""), "source_artifact": action.get("source_artifact", "")})
    for row in payload.get("top_candidates") or []:
        writer.writerow({"section": "top_candidate", "id": row.get("candidate_id", ""), "status": row.get("current_operator_decision", ""), "priority": row.get("priority", ""), "title": row.get("why_this_trade", ""), "source_artifact": row.get("source_artifact", "")})
    positions = payload.get("positions") if isinstance(payload.get("positions"), dict) else {}
    for section in ("open_positions", "stopped_positions", "pending_risk_plan", "pending_stop_review", "corrected_position_events"):
        for row in positions.get(section) or []:
            writer.writerow({"section": section, "id": row.get("candidate_id", ""), "status": row.get("stop_status", ""), "priority": "", "title": row.get("symbol", ""), "source_artifact": (row.get("source_artifacts") or [""])[0]})
    opportunities = payload.get("opportunities") if isinstance(payload.get("opportunities"), dict) else {}
    for section in ("selected_candidates", "suppressed_candidates", "watchlist_candidates"):
        for row in opportunities.get(section) or []:
            writer.writerow({"section": section, "id": row.get("candidate_id", ""), "status": row.get("selection_status", ""), "priority": row.get("score_band", ""), "title": row.get("operator_explanation", ""), "source_artifact": ""})
    for warning in payload.get("warnings") or []:
        writer.writerow({"section": "warning", "id": warning.get("warning_id", ""), "status": warning.get("status", ""), "priority": warning.get("priority", ""), "title": warning.get("message", ""), "source_artifact": warning.get("source_artifact", "")})
    research = payload.get("research") if isinstance(payload.get("research"), dict) else {}
    for row in research.get("captured_hypotheses") or []:
        writer.writerow({"section": "captured_hypothesis", "id": row.get("hypothesis_id", ""), "status": row.get("status", ""), "priority": row.get("classification", ""), "title": row.get("title", ""), "source_artifact": row.get("source_artifact", "")})
    return out.getvalue()


def _load_sources(root: Path, day_utc: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for key, (family, filename, _critical) in SOURCE_SPECS.items():
        path, payload = latest_json_v1(root, family, day_utc, filename)
        generated = _generated_at(payload)
        out[key] = {
            "path": str(path or ""),
            "payload": payload,
            "hash": _file_hash(path),
            "found": bool(path and payload),
            "generated_at": generated,
            "freshness_status": _freshness(path=path, generated_at=generated, day_utc=day_utc),
        }
    return out


def _runtime_section(kernel: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "AVAILABLE" if kernel else "PARTIAL",
        "runtime_truth_classification": kernel.get("runtime_truth_classification", "UNKNOWN"),
        "highest_readiness_layer": kernel.get("highest_readiness_layer", "UNKNOWN"),
        "target_operating_mode": kernel.get("target_operating_mode", "UNKNOWN"),
        "human_approved_advisory_runtime_ready": bool(kernel.get("human_approved_advisory_runtime_ready", False)),
        "trade_advice_allowed": bool(kernel.get("trade_advice_allowed", False)),
        "manual_trade_capture_allowed": bool(kernel.get("manual_trade_capture_allowed", False)),
        "disabled_by_policy": {
            "live_broker_trading": kernel.get("live_broker_trading_policy", "DISABLED_BY_DESIGN"),
            "autonomous_execution": kernel.get("autonomous_execution_policy", "DISABLED_BY_DESIGN"),
            "broker_submit_transmit": kernel.get("broker_submit_transmit_policy", "DISABLED_BY_DESIGN"),
        },
        "blocked_capabilities": kernel.get("blocked_capabilities") or [],
        "missing_or_stale_source_count": kernel.get("missing_or_stale_source_count", 0),
        "operator_action_required": bool(kernel.get("operator_action_required", False)),
        "operator_action_reason": kernel.get("operator_action_reason", ""),
        "do_not_claim": kernel.get("do_not_claim") or [],
        "source_authority": "Runtime Truth Kernel",
    }


def _candidate_sections(lifecycle: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    sections = {
        key: []
        for key in (
            "awaiting_decision",
            "review_required",
            "watchlisted",
            "needs_more_evidence",
            "dismissed",
            "expired",
            "approved_or_traded",
            "ignored",
            "deferred",
            "awaiting_outcome",
            "corrected",
        )
    }
    for row in lifecycle.get("candidates", []) if isinstance(lifecycle.get("candidates"), list) else []:
        if not isinstance(row, dict):
            continue
        candidate = _candidate_projection(row)
        review_state = str(row.get("review_state") or row.get("operator_review_status") or "").upper()
        decision = str(row.get("current_operator_decision") or row.get("operator_decision") or "GENERATED")
        outcome = str(row.get("outcome_status") or "")
        if review_state == "REVIEW_REQUIRED":
            sections["review_required"].append(candidate)
            sections["awaiting_decision"].append(candidate)
        elif review_state == "WATCHLISTED":
            sections["watchlisted"].append(candidate)
            sections["awaiting_decision"].append(candidate)
        elif review_state == "NEEDS_MORE_EVIDENCE":
            sections["needs_more_evidence"].append(candidate)
            sections["awaiting_decision"].append(candidate)
        elif review_state == "DISMISSED":
            sections["dismissed"].append(candidate)
        elif review_state == "EXPIRED":
            sections["expired"].append(candidate)
        elif decision in {"", "GENERATED", "REVIEWED"}:
            sections["awaiting_decision"].append(candidate)
        if decision == "TRADED_MANUALLY":
            sections["approved_or_traded"].append(candidate)
        if decision == "IGNORED":
            sections["ignored"].append(candidate)
        if decision == "DEFERRED":
            sections["deferred"].append(candidate)
        if decision == "TRADED_MANUALLY" and outcome in {"OUTCOME_PENDING", "OUTCOME_UNKNOWN", ""}:
            sections["awaiting_outcome"].append(candidate)
        if int(row.get("correction_count") or 0) > 0:
            sections["corrected"].append(candidate)
    return sections


def _candidate_projection(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": row.get("candidate_id"),
        "sleeve_id": row.get("sleeve_id"),
        "symbol": row.get("symbol"),
        "direction": row.get("direction"),
        "trigger_id": row.get("trigger_id"),
        "current_operator_decision": row.get("current_operator_decision") or row.get("operator_decision"),
        "current_intended_shares": row.get("current_intended_shares"),
        "current_risk_bucket": row.get("current_risk_bucket"),
        "current_decision_reason": row.get("current_decision_reason"),
        "current_operator_note": row.get("current_operator_note"),
        "manual_trade_receipt_id": row.get("current_manual_trade_receipt_id") or row.get("manual_trade_receipt_id"),
        "outcome_status": row.get("outcome_status"),
        "correction_count": row.get("correction_count", 0),
        "latest_correction_id": row.get("latest_correction_id", ""),
        "audit_history_count": len(row.get("audit_history") or []),
        "promotion_status": row.get("promotion_status"),
        "executable_status": row.get("executable_status"),
        "governance_status": row.get("governance_status"),
        "review_only": bool(row.get("review_only", False)),
        "human_review_required": bool(row.get("human_review_required", True)),
        "operator_review_status": row.get("operator_review_status"),
        "review_state": row.get("review_state") or row.get("operator_review_status"),
        "review_ttl_hours": row.get("review_ttl_hours"),
        "review_expires_at": row.get("review_expires_at"),
        "review_expired": bool(row.get("review_expired", False)),
        "expiration_reason": row.get("expiration_reason", ""),
        "expired_from_review_state": row.get("expired_from_review_state", ""),
        "latest_review_event_id": row.get("latest_review_event_id", ""),
        "latest_reviewed_at": row.get("latest_reviewed_at", ""),
        "latest_review_source": row.get("latest_review_source", ""),
        "latest_operator_note": row.get("latest_operator_note", ""),
        "review_action_history_count": row.get("review_action_history_count", 0),
        "review_action_history": row.get("review_action_history") if isinstance(row.get("review_action_history"), list) else [],
        "raw_signal_id": row.get("raw_signal_id"),
        "intent_id": row.get("intent_id"),
        "promotion_contract": row.get("promotion_contract") if isinstance(row.get("promotion_contract"), dict) else {},
        "source_artifact": (row.get("evidence_artifacts") or [""])[0],
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "automatic_approval_allowed": False,
    }


def _positions_section(position_management: dict[str, Any], *, candidates: dict[str, list[dict[str, Any]]], source: dict[str, Any] | None = None, paper_position_ledger: dict[str, Any] | None = None, paper_position_source: dict[str, Any] | None = None) -> dict[str, Any]:
    source = source if isinstance(source, dict) else {}
    source_artifacts = [str(source.get("path") or "")] if source.get("path") else []
    source_hashes = {"position_management": source.get("hash")} if source.get("hash") else {}
    rows = []
    for row in position_management.get("positions", []) if isinstance(position_management.get("positions"), list) else []:
        if not isinstance(row, dict):
            continue
        plan = row.get("risk_plan") if isinstance(row.get("risk_plan"), dict) else {}
        item = {
            "candidate_id": row.get("candidate_id"),
            "position_id": row.get("position_id"),
            "symbol": row.get("symbol"),
            "sleeve_id": row.get("sleeve_id"),
            "quantity": row.get("quantity"),
            "entry_price": row.get("entry_price"),
            "stop_price": plan.get("stop_price"),
            "max_planned_loss": plan.get("max_planned_loss"),
            "risk_plan": plan,
            "stop_status": row.get("stop_status"),
            "latest_event": row.get("latest_event") if isinstance(row.get("latest_event"), dict) else {},
            "latest_stop_event": row.get("latest_stop_event") if isinstance(row.get("latest_stop_event"), dict) else {},
            "correction_count": int(row.get("correction_count") or 0),
            "source_artifacts": source_artifacts or row.get("source_artifacts") or [],
            "source_hashes": source_hashes or row.get("source_hashes") or {},
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
        }
        rows.append(item)
    by_candidate = {str(row.get("candidate_id") or ""): row for row in rows}
    pending_risk = []
    for candidate in candidates.get("approved_or_traded", []):
        candidate_id = str(candidate.get("candidate_id") or "")
        if candidate_id and candidate_id not in by_candidate:
            pending_risk.append({
                "candidate_id": candidate_id,
                "position_id": f"position:{candidate_id}",
                "symbol": candidate.get("symbol"),
                "sleeve_id": candidate.get("sleeve_id"),
                "quantity": candidate.get("current_intended_shares"),
                "entry_price": None,
                "stop_price": None,
                "max_planned_loss": None,
                "stop_status": "PENDING_RISK_PLAN",
                "latest_event": {},
                "correction_count": 0,
                "source_artifacts": [candidate.get("source_artifact")] if candidate.get("source_artifact") else [],
                "source_hashes": {},
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
            })
    stopped = [row for row in rows if row.get("stop_status") in {"STOPPED_OUT", "STOP_NOT_HONORED"}]
    pending_stop = [row for row in rows if row.get("stop_status") == "PENDING_STOP_REVIEW"]
    open_rows = [row for row in rows if row.get("stop_status") not in {"STOPPED_OUT", "STOP_NOT_HONORED", "EXITED"}]
    corrected = [row for row in rows if int(row.get("correction_count") or 0) > 0]
    paper_position_ledger = paper_position_ledger if isinstance(paper_position_ledger, dict) else {}
    paper_position_source = paper_position_source if isinstance(paper_position_source, dict) else {}
    paper_open = _list_value(paper_position_ledger, "open_positions")
    paper_closed = _list_value(paper_position_ledger, "closed_positions", "historical_positions")
    legacy_captures = _list_value(paper_position_ledger, "legacy_captures")
    paper_source_path = str(paper_position_source.get("path") or "")
    return {
        "open_positions": open_rows,
        "stopped_positions": stopped,
        "pending_risk_plan": pending_risk,
        "pending_stop_review": pending_stop,
        "corrected_position_events": corrected,
        "paper_position_ledger_status": "AVAILABLE" if paper_position_ledger else "MISSING",
        "paper_position_ledger_path": paper_source_path,
        "open_paper_positions": paper_open,
        "closed_paper_positions": paper_closed,
        "historical_paper_positions": paper_closed,
        "legacy_captures": legacy_captures,
        "open_paper_position_count": _int_value(paper_position_ledger.get("open_position_count"), len(paper_open)),
        "closed_paper_position_count": _int_value(paper_position_ledger.get("closed_position_count"), len(paper_closed)),
        "legacy_capture_count": _int_value(paper_position_ledger.get("legacy_capture_count"), len(legacy_captures)),
        "paper_position_ledger_mismatch_count": _int_value(paper_position_ledger.get("paper_position_ledger_mismatch_count"), len(_list_value(paper_position_ledger, "paper_position_ledger_mismatches"))),
        "paper_position_ledger_mismatches": _list_value(paper_position_ledger, "paper_position_ledger_mismatches"),
    }


def _ranking_by_id(ranking: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(row.get("candidate_id")): row for row in ranking.get("ranked_candidates", []) if isinstance(row, dict) and row.get("candidate_id")}


def _top_candidates(candidates: dict[str, list[dict[str, Any]]], ranking_by_id: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    seen = set()
    for bucket in ("awaiting_decision", "watchlisted", "needs_more_evidence", "review_required", "deferred"):
        for candidate in candidates[bucket]:
            candidate_id = str(candidate.get("candidate_id") or "")
            if candidate_id in seen:
                continue
            seen.add(candidate_id)
            rank = ranking_by_id.get(str(candidate.get("candidate_id")), {})
            rows.append({**candidate, "rank": rank.get("rank", 9999), "priority": rank.get("priority", "UNKNOWN"), "ranking_score": rank.get("ranking_score"), "why_this_trade": rank.get("why_this_trade", ""), "why_now": rank.get("why_now", ""), "why_not": rank.get("why_not", "")})
    return sorted(rows, key=lambda row: (int(row.get("rank") or 9999), str(row.get("candidate_id") or "")))[:10]


def _sleeve_sections(challenger: dict[str, Any], performance: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    sections = {"healthy": [], "watch": [], "challenged": [], "insufficient_data": []}
    for row in challenger.get("challenges", []) if isinstance(challenger.get("challenges"), list) else []:
        item = {"sleeve_id": row.get("sleeve_id"), "recommendation": row.get("recommendation"), "confidence": row.get("confidence"), "source": "Sleeve Challenger"}
        rec = str(row.get("recommendation") or "")
        if rec == "KEEP":
            sections["healthy"].append(item)
        elif rec in {"WATCH"}:
            sections["watch"].append(item)
        elif rec in {"NEEDS_MORE_EVIDENCE"}:
            sections["insufficient_data"].append(item)
        else:
            sections["challenged"].append(item)
    if not any(sections.values()):
        for row in performance.get("sleeve_metrics", []) if isinstance(performance.get("sleeve_metrics"), list) else []:
            sections["insufficient_data"].append({"sleeve_id": row.get("sleeve_id"), "recommendation": "INSUFFICIENT_DATA", "source": "Performance Attribution"})
    return sections


def _performance_section(performance: dict[str, Any], advisory_quality: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "AVAILABLE" if performance or advisory_quality else "MISSING",
        "source_authority": "Performance Attribution",
        "performance_attribution_engine_status": performance.get("performance_attribution_engine_status", "UNKNOWN"),
        "sleeve_metrics": performance.get("sleeve_metrics") or [],
        "portfolio_attribution": performance.get("portfolio_attribution") or {},
        "advisory_quality": advisory_quality.get("advisory_quality") or performance.get("advisory_quality") or {},
    }



def build_candidate_ui_projection_v1(
    *,
    payloads: dict[str, dict[str, Any]],
    sources: dict[str, dict[str, Any]],
    day_utc: str,
    canonical_generated_at: str = "",
) -> dict[str, Any]:
    diagnostics = payloads.get("candidate_generation_diagnostics") if isinstance(payloads.get("candidate_generation_diagnostics"), dict) else {}
    contracts = payloads.get("candidate_contracts") if isinstance(payloads.get("candidate_contracts"), dict) else {}
    review_packet = payloads.get("candidate_review_packet") if isinstance(payloads.get("candidate_review_packet"), dict) else {}
    paper_queue = payloads.get("paper_review_queue") if isinstance(payloads.get("paper_review_queue"), dict) else {}
    entry_price_certification = payloads.get("entry_reference_price_certification") if isinstance(payloads.get("entry_reference_price_certification"), dict) else {}
    candidate_to_paper_lifecycle = payloads.get("candidate_to_paper_lifecycle") if isinstance(payloads.get("candidate_to_paper_lifecycle"), dict) else {}
    outcomes = payloads.get("paper_trade_outcomes") if isinstance(payloads.get("paper_trade_outcomes"), dict) else {}
    paper_position_ledger = payloads.get("paper_position_ledger") if isinstance(payloads.get("paper_position_ledger"), dict) else {}
    paper_lifecycle_reconciliation = payloads.get("paper_lifecycle_reconciliation") if isinstance(payloads.get("paper_lifecycle_reconciliation"), dict) else {}
    trading_lifecycle_state = payloads.get("trading_lifecycle_state") if isinstance(payloads.get("trading_lifecycle_state"), dict) else {}
    exit_recommendations = payloads.get("exit_recommendations") if isinstance(payloads.get("exit_recommendations"), dict) else {}
    exit_logic_review = payloads.get("exit_logic_review") if isinstance(payloads.get("exit_logic_review"), dict) else {}
    candidate_state = payloads.get("candidate_state") if isinstance(payloads.get("candidate_state"), dict) else {}
    run_history = payloads.get("run_history") if isinstance(payloads.get("run_history"), dict) else {}
    input_contract_reconciliation = payloads.get("input_contract_reconciliation") if isinstance(payloads.get("input_contract_reconciliation"), dict) else {}
    market_data_coverage = payloads.get("market_data_coverage") if isinstance(payloads.get("market_data_coverage"), dict) else {}
    latest_run = latest_run_row_v1(run_history)
    candidate_lifecycle_summary = candidate_to_paper_lifecycle.get("summary") if isinstance(candidate_to_paper_lifecycle.get("summary"), dict) else {}

    contract_rows = _list_value(contracts, "candidate_contracts", "contracts", "rows")
    queue_rows = _list_value(paper_queue, "rows", "queue_items", "items")
    state_rows = _list_value(candidate_state, "active_candidates", "candidates")
    existing_queue_ids = {str(row.get("candidate_id") or "") for row in queue_rows if isinstance(row, dict)}
    for state_row in state_rows:
        if isinstance(state_row, dict) and str(state_row.get("candidate_id") or "") not in existing_queue_ids and str(state_row.get("current_state") or "") in {"AWAITING_REVIEW", "APPROVED_FOR_PAPER", "PAPER_POSITION_OPEN"}:
            queue_rows.append(state_row)
    review_rows = _list_value(review_packet, "review_candidates", "candidates", "rows")
    outcome_rows = (
        _list_value(outcomes, "outcomes", "paper_trade_outcomes", "rows")
        + _list_value(outcomes, "open_trades")
        + _list_value(outcomes, "closed_trades")
    )
    status_counts = paper_queue.get("status_counts") if isinstance(paper_queue.get("status_counts"), dict) else {}
    awaiting_review_count = _int_value(
        status_counts.get("AWAITING_REVIEW"),
        sum(1 for row in queue_rows if str(row.get("status") or row.get("review_status") or "").upper() == "AWAITING_REVIEW"),
    )
    reviewable_candidate_count = _int_value(review_packet.get("candidate_count"), len(review_rows))

    source_keys = ("candidate_generation_diagnostics", "candidate_contracts", "entry_reference_price_certification", "candidate_to_paper_lifecycle", "candidate_review_packet", "paper_review_queue", "paper_trade_outcomes", "paper_position_ledger", "paper_lifecycle_reconciliation", "trading_lifecycle_state", "exit_recommendations", "exit_logic_review", "candidate_state", "run_history", "input_contract_reconciliation", "market_data_coverage")
    source_artifacts = [_candidate_source_ref(key, payloads.get(key) if isinstance(payloads.get(key), dict) else {}, sources.get(key, {}), day_utc) for key in source_keys]
    mismatch_reasons: list[str] = []
    for ref in source_artifacts:
        if ref["found"] and ref["day_utc"] and ref["day_utc"] != day_utc:
            mismatch_reasons.append(f"WRONG_DAY:{ref['source']}:{ref['day_utc']}!=%s" % day_utc)
    if sources.get("candidate_generation_diagnostics", {}).get("found") and not diagnostics:
        mismatch_reasons.append("DIAGNOSTICS_PATH_FOUND_BUT_PAYLOAD_UNREADABLE")
    if sources.get("paper_review_queue", {}).get("found") and len(queue_rows) != reviewable_candidate_count:
        mismatch_reasons.append(f"REVIEW_PACKET_QUEUE_COUNT_MISMATCH:{reviewable_candidate_count}!={len(queue_rows)}")
    if contract_rows and queue_rows and len(contract_rows) != len(queue_rows):
        mismatch_reasons.append(f"CONTRACT_QUEUE_COUNT_MISMATCH:{len(contract_rows)}!={len(queue_rows)}")

    generated_times = [str(ref.get("generated_at") or "") for ref in source_artifacts if ref.get("generated_at")]
    max_source_generated_at = sorted(generated_times)[-1] if generated_times else ""
    stale = bool(canonical_generated_at and max_source_generated_at and canonical_generated_at < max_source_generated_at)
    if stale:
        mismatch_reasons.append(f"CANONICAL_OLDER_THAN_CANDIDATE_SOURCES:{canonical_generated_at}<{max_source_generated_at}")

    projection_status = "AVAILABLE"
    if mismatch_reasons:
        projection_status = "PROJECTION_MISMATCH"
    if stale:
        projection_status = "CANONICAL_PROJECTION_STALE"

    diagnostics_found = bool(sources.get("candidate_generation_diagnostics", {}).get("found") and diagnostics)
    run_history_found = bool(sources.get("run_history", {}).get("found") and run_history and latest_run)
    contracts_found = bool(sources.get("candidate_contracts", {}).get("found") and contracts)
    queue_found = bool(sources.get("paper_review_queue", {}).get("found") and paper_queue)
    packet_found = bool(sources.get("candidate_review_packet", {}).get("found") and review_packet)
    review_by_id = {str(row.get("candidate_id") or ""): row for row in review_rows if str(row.get("candidate_id") or "")}
    contract_by_id = {str(row.get("candidate_id") or ""): row for row in contract_rows if str(row.get("candidate_id") or "")}
    ledger_open_rows = _list_value(paper_position_ledger, "open_positions")
    ledger_closed_rows = _list_value(paper_position_ledger, "closed_positions", "historical_positions")
    ledger_legacy_rows = _list_value(paper_position_ledger, "legacy_captures")
    open_source_rows = ledger_open_rows if ledger_open_rows or paper_position_ledger else _list_value(outcomes, "open_trades")
    closed_source_rows = ledger_closed_rows if ledger_closed_rows or paper_position_ledger else _list_value(outcomes, "closed_trades")
    open_by_id = {str(row.get("candidate_id") or row.get("linked_candidate_id") or ""): row for row in open_source_rows if str(row.get("candidate_id") or row.get("linked_candidate_id") or "")}
    closed_by_id = {str(row.get("candidate_id") or row.get("linked_candidate_id") or ""): row for row in closed_source_rows if str(row.get("candidate_id") or row.get("linked_candidate_id") or "")}
    exit_recommendation_rows = _list_value(exit_recommendations, "recommendations", "rows")
    exit_recommendation_by_candidate = {str(row.get("candidate_id") or ""): row for row in exit_recommendation_rows if str(row.get("candidate_id") or "")}
    workflow_rows = []
    for queue_row in queue_rows:
        candidate_id = str(queue_row.get("candidate_id") or "")
        review_row = review_by_id.get(candidate_id, {})
        contract_row = contract_by_id.get(candidate_id, {})
        open_row = open_by_id.get(candidate_id, {})
        closed_row = closed_by_id.get(candidate_id, {})
        queue_status = str(queue_row.get("status") or queue_row.get("current_state") or "AWAITING_REVIEW").upper()
        if queue_status == "REJECTED_BY_OPERATOR":
            workflow_state = "REJECTED_BY_OPERATOR"
        elif queue_status == "EXPIRED":
            workflow_state = "EXPIRED"
        elif closed_row:
            workflow_state = "PAPER_POSITION_CLOSED"
        elif open_row:
            workflow_state = "PAPER_POSITION_OPEN"
        elif queue_status == "APPROVED_FOR_PAPER":
            workflow_state = "AWAITING_REVIEW"
        else:
            workflow_state = "AWAITING_REVIEW"
        workflow_rows.append({
            **queue_row,
            "workflow_state": workflow_state,
            "canonical_workflow_state": workflow_state,
            "sleeve_id": str(review_row.get("sleeve_id") or contract_row.get("sleeve_id") or queue_row.get("sleeve_id") or ""),
            "raw_signal_id": str(review_row.get("raw_signal_id") or contract_row.get("raw_signal_id") or queue_row.get("raw_signal_id") or ""),
            "direction": str(review_row.get("direction") or contract_row.get("direction") or queue_row.get("direction") or ""),
            "entry_reference_price": str(review_row.get("entry_reference_price") or contract_row.get("entry_reference_price") or queue_row.get("entry_reference_price") or ""),
            "thesis_reason_codes": _list_value({"rows": review_row.get("thesis_reason_codes") if isinstance(review_row.get("thesis_reason_codes"), list) else queue_row.get("thesis_reason_codes") if isinstance(queue_row.get("thesis_reason_codes"), list) else []}, "rows"),
            "thesis_reason_code_text": [str(item) for item in (review_row.get("thesis_reason_codes") if isinstance(review_row.get("thesis_reason_codes"), list) else queue_row.get("thesis_reason_codes") if isinstance(queue_row.get("thesis_reason_codes"), list) else [])],
            "evidence_paths": [str(item) for item in (review_row.get("evidence_paths") if isinstance(review_row.get("evidence_paths"), list) else queue_row.get("evidence_paths") if isinstance(queue_row.get("evidence_paths"), list) else [])],
            "evidence_hashes": review_row.get("evidence_hashes") if isinstance(review_row.get("evidence_hashes"), dict) else {},
            "warnings": [str(item) for item in (review_row.get("disqualifying_warnings") if isinstance(review_row.get("disqualifying_warnings"), list) else [])],
            "paper_only": True,
            "human_review_required": True,
            "live_trade_eligible": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "workflow_badges": ["PAPER ONLY", "HUMAN REVIEW REQUIRED", "LIVE TRADING DISABLED"],
            "workflow_timeline": {
                "reviewed": queue_status == "REJECTED_BY_OPERATOR" or bool(open_row or closed_row),
                "approved": bool(open_row or closed_row),
                "entry_recorded": bool(open_row or closed_row),
                "exit_recorded": bool(closed_row),
                "outcome_closed": bool(closed_row),
                "reviewed_at_utc": str(queue_row.get("timestamp") or "") if queue_status == "REJECTED_BY_OPERATOR" else "",
                "entry_recorded_at_utc": str((open_row or closed_row).get("timestamp_utc") or "") if (open_row or closed_row) else "",
                "exit_recorded_at_utc": str(closed_row.get("exit_timestamp_utc") or closed_row.get("timestamp_utc") or "") if closed_row else "",
            },
            "open_paper_position": open_row,
            "closed_paper_position": closed_row,
            "exit_recommendation": exit_recommendation_by_candidate.get(candidate_id, {}),
            "originating_day": str(queue_row.get("originating_day") or day_utc),
            "latest_projection_day": str(queue_row.get("latest_projection_day") or day_utc),
            "rollover_status": str(queue_row.get("rollover_status") or ("CURRENT_DAY" if str(queue_row.get("originating_day") or day_utc) == day_utc else "CARRIED_FORWARD")),
            "age_days": queue_row.get("age_days", 0),
            "expiration_status": str(queue_row.get("expiration_status") or ("EXPIRED" if workflow_state == "EXPIRED" else "ACTIVE")),
        })
    operator_awaiting_review_count = sum(1 for row in workflow_rows if str(row.get("workflow_state") or "").upper() == "AWAITING_REVIEW")
    if workflow_rows:
        reviewable_candidate_count = len([row for row in workflow_rows if str(row.get("workflow_state") or "") in {"AWAITING_REVIEW", "PAPER_POSITION_OPEN"}])
    raw_signal_count = _first_int_value(latest_run.get("raw_signals"), diagnostics.get("total_raw_signals"), 0)
    diagnostic_candidate_outputs = _first_int_value(latest_run.get("diagnostic_candidate_outputs"), diagnostics.get("diagnostic_candidate_outputs"), diagnostics.get("total_candidates_generated"), 0)
    diagnostics_candidates_generated = _first_int_value(latest_run.get("diagnostics_candidates_generated"), diagnostics.get("total_candidates_generated"), 0)
    candidate_contracts_created = _first_int_value(latest_run.get("valid_candidate_contracts"), latest_run.get("candidate_contracts_created"), contracts.get("candidates_created"), len(contract_rows))
    valid_candidate_contracts = candidate_contracts_created
    rejected_candidate_contracts = _first_int_value(latest_run.get("rejected_candidate_contracts"), diagnostics.get("rejected_candidate_contracts"), contracts.get("candidates_rejected"), len(_list_value(contracts, "rejected_raw_signals")))
    reviewable_current_day_candidates = _first_int_value(latest_run.get("reviewable_current_day_candidates"), valid_candidate_contracts)
    carried_forward_candidates = _first_int_value(latest_run.get("carried_forward_candidates"), sum(1 for row in workflow_rows if str(row.get("originating_day") or day_utc) != day_utc))
    rejected_count = _first_int_value(latest_run.get("rejected_count"), diagnostics.get("total_candidates_rejected"), 0)
    sleeves_expected = _first_int_value(latest_run.get("sleeves_expected"), diagnostics.get("total_sleeves_expected"), 0)
    sleeves_run = _first_int_value(latest_run.get("sleeves_run"), diagnostics.get("total_sleeves_run"), 0)
    diagnostic_rejection_rows = _diagnostic_rejection_rows(
        diagnostics=diagnostics,
        diagnostics_path=str(sources.get("candidate_generation_diagnostics", {}).get("path") or ""),
        total_rejected=rejected_count,
    )
    input_contract_reconciliation_rows = _list_value(input_contract_reconciliation, "rows")
    market_data_coverage_rows = _list_value(market_data_coverage, "coverage_rows")
    artifact_mismatch_status = str(latest_run.get("artifact_mismatch_status") or "")
    artifact_mismatch_reason = str(latest_run.get("artifact_mismatch_reason") or latest_run.get("mismatch_reason") or "")
    if not artifact_mismatch_status and diagnostic_candidate_outputs > valid_candidate_contracts and valid_candidate_contracts == 0 and rejected_candidate_contracts > 0:
        artifact_mismatch_status = "DIAGNOSTIC_OUTPUTS_REJECTED_BY_CONTRACT_VALIDATION"
        artifact_mismatch_reason = _top_contract_rejection_reason(contracts)
    run_visibility_status = "RUN_HISTORY_AVAILABLE" if run_history_found else ("RUN_SUMMARY_WITHOUT_DIAGNOSTICS" if run_history and not diagnostics_found else ("DIAGNOSTICS_AVAILABLE_NO_RUN_HISTORY" if diagnostics_found else "DIAGNOSTICS_MISSING"))
    if artifact_mismatch_status:
        run_visibility_status = artifact_mismatch_status
    projection_generated_at = canonical_generated_at or max_source_generated_at
    diagnostics_completed_at = str(latest_run.get("diagnostics_completed_at") or latest_run.get("completed_at") or diagnostics.get("completed_at_utc") or diagnostics.get("run_completed_at_utc") or diagnostics.get("generated_at_utc") or diagnostics.get("generated_at") or "")
    diagnostics_started_at = str(latest_run.get("diagnostics_started_at") or diagnostics.get("diagnostics_started_at") or diagnostics.get("diagnostics_started_at_utc") or "NOT_RECORDED")
    actual_started_at = str(latest_run.get("started_at") or "")
    derived_mismatch_explanation = _candidate_contract_mismatch_explanation(status=artifact_mismatch_status, reason=artifact_mismatch_reason, contracts=contracts, diagnostics=diagnostics, sources=sources)
    if diagnostic_rejection_rows:
        derived_mismatch_explanation["diagnostic_rejection_rows"] = diagnostic_rejection_rows
        derived_mismatch_explanation["diagnostic_rejection_count"] = len(diagnostic_rejection_rows)
        derived_mismatch_explanation["diagnostic_rejection_source_path"] = str(sources.get("candidate_generation_diagnostics", {}).get("path") or "")
    elif rejected_count > 0:
        derived_mismatch_explanation["diagnostic_rejection_count"] = 0
        derived_mismatch_explanation["diagnostic_rejection_detail_status"] = "NO_PER_OUTPUT_REJECTION_ROWS"
        derived_mismatch_explanation["diagnostic_rejection_source_path"] = str(sources.get("candidate_generation_diagnostics", {}).get("path") or "")
    if input_contract_reconciliation_rows:
        derived_mismatch_explanation["input_contract_reconciliation_rows"] = input_contract_reconciliation_rows
        derived_mismatch_explanation["input_contract_reconciliation_count"] = len(input_contract_reconciliation_rows)
        derived_mismatch_explanation["input_contract_reconciliation_source_path"] = str(sources.get("input_contract_reconciliation", {}).get("path") or "")
    latest_mismatch_explanation = latest_run.get("mismatch_explanation") if isinstance(latest_run.get("mismatch_explanation"), dict) else {}
    mismatch_explanation = {**derived_mismatch_explanation, **latest_mismatch_explanation}
    run_summary = {
        "run_id": str(latest_run.get("run_id") or ""),
        "started_at": actual_started_at,
        "run_start": actual_started_at or "NOT_RECORDED",
        "completed_at": diagnostics_completed_at,
        "run_end": diagnostics_completed_at,
        "market_snapshot_time": str(latest_run.get("market_snapshot_time") or ""),
        "candidate_snapshot_time": str(latest_run.get("candidate_snapshot_time") or ""),
        "diagnostics_started_at": diagnostics_started_at,
        "diagnostics_completed_at": diagnostics_completed_at,
        "projection_generated_at": projection_generated_at,
        "dashboard_rendered_at": "CLIENT_RENDER_TIME",
        "command": str(latest_run.get("command") or "npm run aegis:candidate-diagnostics" if diagnostics_found else ""),
        "target_day": day_utc,
        "status": str(latest_run.get("status") or ("SUCCESS" if diagnostics_found else "UNKNOWN")),
        "classification": str(latest_run.get("classification") or diagnostics.get("operator_interpretation") or "UNKNOWN"),
        "sleeves_expected": sleeves_expected,
        "sleeves_run": sleeves_run,
        "raw_signals": raw_signal_count,
        "diagnostic_candidate_outputs": diagnostic_candidate_outputs,
        "diagnostics_candidates_generated": diagnostics_candidates_generated,
        "valid_candidate_contracts": valid_candidate_contracts,
        "candidate_contracts_created": candidate_contracts_created,
        "certified_price_candidate_count": _int_value(entry_price_certification.get("certified_count"), 0),
        "review_eligible_count": _int_value(candidate_lifecycle_summary.get("review_eligible_count"), 0),
        "promotion_eligible_count": _int_value(candidate_lifecycle_summary.get("promotion_eligible_count"), 0),
        "auto_promoted_to_paper_tracking_count": _int_value(candidate_lifecycle_summary.get("auto_promoted_to_paper_tracking_count"), 0),
        "human_approved_for_paper_count": _int_value(candidate_lifecycle_summary.get("human_approved_for_paper_count"), 0),
        "auto_promotion_blocked_count": _int_value(candidate_lifecycle_summary.get("auto_promotion_blocked_count"), 0),
        "auto_promotion_not_eligible_count": _int_value(candidate_lifecycle_summary.get("auto_promotion_not_eligible_count"), 0),
        "paper_positions_created_count": _int_value(candidate_lifecycle_summary.get("paper_positions_created_count"), 0),
        "blocked_from_paper_count": _int_value(candidate_lifecycle_summary.get("blocked_from_paper_count"), 0),
        "rejected_candidate_contracts": rejected_candidate_contracts,
        "reviewable_current_day_candidates": reviewable_current_day_candidates,
        "carried_forward_candidates": carried_forward_candidates,
        "rejected_count": rejected_count,
        "diagnostic_rejection_count": len(diagnostic_rejection_rows),
        "diagnostic_rejection_rows": diagnostic_rejection_rows,
        "diagnostic_rejection_source_path": str(sources.get("candidate_generation_diagnostics", {}).get("path") or ""),
        "input_contract_reconciliation_count": len(input_contract_reconciliation_rows),
        "input_contract_reconciliation_rows": input_contract_reconciliation_rows,
        "input_contract_reconciliation_source_path": str(sources.get("input_contract_reconciliation", {}).get("path") or ""),
        "market_data_coverage": _market_data_coverage_summary_v1(market_data_coverage, sources.get("market_data_coverage", {})),
        "market_data_coverage_rows": market_data_coverage_rows,
        "diagnostics_path": str(latest_run.get("diagnostics_path") or sources.get("candidate_generation_diagnostics", {}).get("path") or ""),
        "candidate_contracts_path": str(latest_run.get("candidate_contracts_path") or sources.get("candidate_contracts", {}).get("path") or ""),
        "paper_review_queue_path": str(latest_run.get("paper_review_queue_path") or sources.get("paper_review_queue", {}).get("path") or ""),
        "source_hashes": latest_run.get("source_hashes") if isinstance(latest_run.get("source_hashes"), dict) else {},
        "artifact_mismatch": bool(artifact_mismatch_status),
        "artifact_mismatch_status": artifact_mismatch_status,
        "artifact_mismatch_reason": artifact_mismatch_reason,
        "mismatch_reason": artifact_mismatch_reason,
        "mismatch_explanation": mismatch_explanation,
        "run_visibility_status": run_visibility_status,
    }
    return {
        "schema_id": "aegis_candidate_ui_projection",
        "schema_version": "v1",
        "artifact_id": f"aegis_candidate_ui_projection:{day_utc}",
        "day_utc": day_utc,
        "generated_at": projection_generated_at,
        "projection_generated_at": projection_generated_at,
        "dashboard_rendered_at": "CLIENT_RENDER_TIME",
        "run_summary": run_summary,
        "run_history_status": "AVAILABLE" if run_history_found else "MISSING",
        "run_history_path": str(sources.get("run_history", {}).get("path") or ""),
        "run_visibility_status": run_visibility_status,
        "raw_signal_count": raw_signal_count,
        "diagnostic_candidate_outputs": diagnostic_candidate_outputs,
        "diagnostics_candidates_generated": diagnostics_candidates_generated,
        "valid_candidate_contracts": valid_candidate_contracts,
        "candidate_contracts_created": candidate_contracts_created,
        "certified_price_candidate_count": _int_value(entry_price_certification.get("certified_count"), 0),
        "review_eligible_count": _int_value(candidate_lifecycle_summary.get("review_eligible_count"), 0),
        "promotion_eligible_count": _int_value(candidate_lifecycle_summary.get("promotion_eligible_count"), 0),
        "auto_promoted_to_paper_tracking_count": _int_value(candidate_lifecycle_summary.get("auto_promoted_to_paper_tracking_count"), 0),
        "human_approved_for_paper_count": _int_value(candidate_lifecycle_summary.get("human_approved_for_paper_count"), 0),
        "auto_promotion_blocked_count": _int_value(candidate_lifecycle_summary.get("auto_promotion_blocked_count"), 0),
        "auto_promotion_not_eligible_count": _int_value(candidate_lifecycle_summary.get("auto_promotion_not_eligible_count"), 0),
        "paper_positions_created_count": _int_value(candidate_lifecycle_summary.get("paper_positions_created_count"), 0),
        "blocked_from_paper_count": _int_value(candidate_lifecycle_summary.get("blocked_from_paper_count"), 0),
        "rejected_candidate_contracts": rejected_candidate_contracts,
        "reviewable_current_day_candidates": reviewable_current_day_candidates,
        "carried_forward_candidates": carried_forward_candidates,
        "rejected_count": rejected_count,
        "diagnostic_rejection_count": len(diagnostic_rejection_rows),
        "diagnostic_rejection_rows": diagnostic_rejection_rows,
        "diagnostic_rejection_source_path": str(sources.get("candidate_generation_diagnostics", {}).get("path") or ""),
        "input_contract_reconciliation_count": len(input_contract_reconciliation_rows),
        "input_contract_reconciliation_rows": input_contract_reconciliation_rows,
        "input_contract_reconciliation_source_path": str(sources.get("input_contract_reconciliation", {}).get("path") or ""),
        "market_data_coverage": _market_data_coverage_summary_v1(market_data_coverage, sources.get("market_data_coverage", {})),
        "market_data_coverage_rows": market_data_coverage_rows,
        "sleeves_expected": sleeves_expected,
        "sleeves_run": sleeves_run,
        "projection_status": projection_status,
        "diagnostics_status": "AVAILABLE" if diagnostics_found else "MISSING",
        "paper_review_queue_status": "AVAILABLE" if queue_found else "MISSING",
        "candidate_contracts_status": "AVAILABLE" if contracts_found else "MISSING",
        "candidate_review_packet_status": "AVAILABLE" if packet_found else "MISSING",
        "candidate_contract_count": valid_candidate_contracts,
        "current_day_candidate_contract_count": valid_candidate_contracts,
        "valid_candidate_contract_count": len(contract_rows),
        "paper_review_queue_count": len(queue_rows),
        "candidate_to_paper_lifecycle_status": "AVAILABLE" if candidate_to_paper_lifecycle else "MISSING",
        "candidate_to_paper_lifecycle_path": str(sources.get("candidate_to_paper_lifecycle", {}).get("path") or ""),
        "candidate_to_paper_lifecycle_counts": candidate_lifecycle_summary,
        "candidate_to_paper_blocker_counts": candidate_to_paper_lifecycle.get("blocker_counts") if isinstance(candidate_to_paper_lifecycle.get("blocker_counts"), dict) else {},
        "entry_reference_price_certification_status": "AVAILABLE" if entry_price_certification else "MISSING",
        "entry_reference_price_certification_path": str(sources.get("entry_reference_price_certification", {}).get("path") or ""),
        "awaiting_review_count": operator_awaiting_review_count,
        "reviewable_candidate_count": reviewable_candidate_count,
        "paper_trade_outcome_count": len(outcome_rows),
        "paper_position_ledger_status": "AVAILABLE" if paper_position_ledger else "MISSING",
        "paper_position_ledger_path": str(sources.get("paper_position_ledger", {}).get("path") or ""),
        "exit_recommendations_status": "AVAILABLE" if exit_recommendations else "MISSING",
        "exit_recommendations_path": str(sources.get("exit_recommendations", {}).get("path") or ""),
        "exit_logic_review_status": "AVAILABLE" if exit_logic_review else "MISSING",
        "exit_logic_review_path": str(sources.get("exit_logic_review", {}).get("path") or ""),
        "exit_recommendations": exit_recommendation_rows,
        "exit_recommendation_count": _int_value(exit_recommendations.get("recommendation_count"), len(exit_recommendation_rows)),
        "exit_recommendation_counts": exit_recommendations.get("pnl_report_hooks", {}).get("recommendation_counts") if isinstance(exit_recommendations.get("pnl_report_hooks"), dict) else {},
        "paper_position_open_count": _int_value(paper_position_ledger.get("open_position_count"), len(ledger_open_rows)),
        "paper_position_closed_count": _int_value(paper_position_ledger.get("closed_position_count"), len(ledger_closed_rows)),
        "legacy_capture_count": _int_value(paper_position_ledger.get("legacy_capture_count"), len(ledger_legacy_rows)),
        "paper_position_ledger_mismatch_count": _int_value(paper_position_ledger.get("paper_position_ledger_mismatch_count"), len(_list_value(paper_position_ledger, "paper_position_ledger_mismatches"))),
        "paper_position_ledger_mismatches": _list_value(paper_position_ledger, "paper_position_ledger_mismatches"),
        "paper_lifecycle_reconciliation_status": "AVAILABLE" if paper_lifecycle_reconciliation else "MISSING",
        "paper_lifecycle_reconciliation_path": str(sources.get("paper_lifecycle_reconciliation", {}).get("path") or ""),
        "paper_lifecycle_counts": paper_lifecycle_reconciliation.get("counts") if isinstance(paper_lifecycle_reconciliation.get("counts"), dict) else {},
        "paper_lifecycle_operator_summary_messages": _string_list_value(paper_lifecycle_reconciliation, "operator_summary_messages"),
        "paper_lifecycle_failed_actions": _list_value(paper_lifecycle_reconciliation, "failed_actions"),
        "paper_lifecycle_expected_open_but_missing": _list_value(paper_lifecycle_reconciliation, "expected_open_but_missing"),
        "paper_lifecycle_receipt_without_ledger_event": _list_value(paper_lifecycle_reconciliation, "receipt_without_ledger_event"),
        "trading_lifecycle_state_status": "AVAILABLE" if trading_lifecycle_state else "MISSING",
        "trading_lifecycle_state_path": str(sources.get("trading_lifecycle_state", {}).get("path") or ""),
        "trading_lifecycle_counts": trading_lifecycle_state.get("counts") if isinstance(trading_lifecycle_state.get("counts"), dict) else {},
        "trading_lifecycle_items": _list_value(trading_lifecycle_state, "lifecycle_items"),
        "trading_lifecycle_open_governed_positions": _list_value(trading_lifecycle_state, "open_governed_positions"),
        "trading_lifecycle_legacy_partial_open_positions": _list_value(trading_lifecycle_state, "legacy_partial_open_positions"),
        "trading_lifecycle_failed_paper_trade_attempts": _list_value(trading_lifecycle_state, "failed_paper_trade_attempts"),
        "trading_lifecycle_awaiting_paper_trade": _list_value(trading_lifecycle_state, "awaiting_paper_trade"),
        "trading_lifecycle_closed_positions": _list_value(trading_lifecycle_state, "closed_positions"),
        "trading_lifecycle_unclassified_items": _list_value(trading_lifecycle_state, "unclassified_lifecycle_items"),
        "open_paper_positions": ledger_open_rows,
        "closed_paper_positions": ledger_closed_rows,
        "historical_paper_positions": ledger_closed_rows,
        "legacy_captures": ledger_legacy_rows,
        "persistent_candidate_state_count": len(state_rows),
        "candidate_state_status": "AVAILABLE" if candidate_state else "MISSING",
        "candidate_state_path": str(sources.get("candidate_state", {}).get("path") or ""),
        "paper_review_queue_rows": workflow_rows,
        "paper_workflow_rows": workflow_rows,
        "candidate_contract_rows": contract_rows,
        "candidate_review_rows": review_rows,
        "candidate_diagnostics_path": str(sources.get("candidate_generation_diagnostics", {}).get("path") or ""),
        "candidate_contracts_path": str(sources.get("candidate_contracts", {}).get("path") or ""),
        "candidate_review_packet_path": str(sources.get("candidate_review_packet", {}).get("path") or ""),
        "paper_review_queue_path": str(sources.get("paper_review_queue", {}).get("path") or ""),
        "paper_trade_outcomes_path": str(sources.get("paper_trade_outcomes", {}).get("path") or ""),
        "source_artifact_hashes": {key: str(sources.get(key, {}).get("hash") or "") for key in source_keys if sources.get(key, {}).get("hash")},
        "source_artifacts": source_artifacts,
        "source_max_generated_at": max_source_generated_at,
        "canonical_generated_at": canonical_generated_at,
        "mismatch_reasons": mismatch_reasons,
        "repair_commands": [
            f"TARGET_DAY={day_utc} npm run aegis:candidate-diagnostics",
            f"TARGET_DAY={day_utc} npm run aegis:candidate-contracts",
            f"TARGET_DAY={day_utc} npm run aegis:paper:review-queue",
            f"TARGET_DAY={day_utc} npm run aegis:paper-position-ledger",
            f"TARGET_DAY={day_utc} npm run aegis:exit-recommendations",
            f"TARGET_DAY={day_utc} npm run aegis:input-contract-reconciliation",
            f"TARGET_DAY={day_utc} npm run aegis:repair-input-contracts",
        ],
        "source_authority": "canonical_operator_state_v1 + governed candidate contracts + paper_review_queue_v1 + persistent candidate_state_v1",
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }



def _market_data_coverage_summary_v1(payload: dict[str, Any], source: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict) or not payload:
        return {
            "status": "MISSING",
            "source_path": str(source.get("path") or ""),
            "repair_action": "npm run aegis:market-data-coverage",
        }
    return {
        "status": str(payload.get("status") or "UNKNOWN"),
        "required_symbol_count": _int_value(payload.get("required_symbol_count"), 0),
        "certified_symbol_count": _int_value(payload.get("certified_symbol_count"), 0),
        "missing_symbol_count": _int_value(payload.get("missing_symbol_count"), 0),
        "stale_symbol_count": _int_value(payload.get("stale_symbol_count"), 0),
        "unavailable_symbol_count": _int_value(payload.get("unavailable_symbol_count"), 0),
        "coverage_pct": payload.get("coverage_pct", 0),
        "missing_symbols": _string_list_value(payload, "missing_symbols"),
        "stale_symbols": _string_list_value(payload, "stale_symbols"),
        "issue_groups": _list_value(payload, "issue_groups"),
        "stale_symbols_by_cause_provider": _list_value(payload, "stale_symbols_by_cause_provider"),
        "blocking_consumers": _string_list_value(payload, "blocking_consumers"),
        "consumer_sleeves": _string_list_value(payload, "consumer_sleeves"),
        "repair_action": str(payload.get("repair_action") or ""),
        "source_path": str(source.get("path") or ""),
    }

def _diagnostic_rejection_rows(*, diagnostics: dict[str, Any], diagnostics_path: str, total_rejected: int = 0) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    raw_rejections = diagnostics.get("raw_signal_rejections") if isinstance(diagnostics.get("raw_signal_rejections"), list) else []
    for idx, row in enumerate(raw_rejections, start=1):
        if not isinstance(row, dict):
            continue
        rows.append(_diagnostic_rejection_row(
            row=row,
            idx=idx,
            diagnostics_path=diagnostics_path,
            source_collection="raw_signal_rejections",
            default_status="REJECTED",
        ))

    rejected_details = diagnostics.get("rejected_raw_signal_details") if isinstance(diagnostics.get("rejected_raw_signal_details"), list) else []
    for idx, row in enumerate(rejected_details, start=len(rows) + 1):
        if not isinstance(row, dict):
            continue
        rows.append(_diagnostic_rejection_row(
            row=row,
            idx=idx,
            diagnostics_path=diagnostics_path,
            source_collection="rejected_raw_signal_details",
            default_status="REJECTED",
        ))

    failed_producers = diagnostics.get("failed_producers") if isinstance(diagnostics.get("failed_producers"), list) else []
    for idx, row in enumerate(failed_producers, start=len(rows) + 1):
        if not isinstance(row, dict):
            continue
        rows.append(_diagnostic_rejection_row(
            row=row,
            idx=idx,
            diagnostics_path=diagnostics_path,
            source_collection="failed_producers",
            default_status=str(row.get("run_status") or "BLOCKED"),
        ))

    if rows:
        return rows
    if total_rejected > 0:
        return [{
            "rejection_id": "diagnostic-rejection-detail-missing",
            "source_collection": "diagnostics_summary",
            "status": "NO_PER_OUTPUT_REJECTION_ROWS",
            "run_status": "NO_PER_OUTPUT_REJECTION_ROWS",
            "sleeve_id": "UNKNOWN",
            "symbol": "",
            "canonical_blocker": "NO_PER_OUTPUT_REJECTION_ROWS",
            "reason": "Diagnostics reported rejected candidates, but no per-output rejection rows were present.",
            "reason_codes": ["NO_PER_OUTPUT_REJECTION_ROWS"],
            "next_repair_action": "Regenerate candidate diagnostics and canonical operator state for the displayed day.",
            "source_artifact_path": diagnostics_path,
        }]
    return []


def _diagnostic_rejection_row(*, row: dict[str, Any], idx: int, diagnostics_path: str, source_collection: str, default_status: str) -> dict[str, Any]:
    reason_codes = row.get("reason_codes") if isinstance(row.get("reason_codes"), list) else row.get("rejection_reasons") if isinstance(row.get("rejection_reasons"), list) else []
    reason_codes = [str(item) for item in reason_codes if str(item)]
    reason = str(
        row.get("rejection_reason")
        or row.get("canonical_blocker")
        or row.get("reason_no_candidate")
        or row.get("reason")
        or (", ".join(reason_codes) if reason_codes else "UNKNOWN")
    )
    status = str(row.get("run_status") or row.get("status") or default_status or "UNKNOWN")
    return {
        "rejection_id": str(row.get("rejection_id") or row.get("raw_signal_id") or row.get("candidate_id") or f"diagnostic-rejection-{idx}"),
        "source_collection": source_collection,
        "status": status,
        "run_status": status,
        "sleeve_id": str(row.get("sleeve_id") or row.get("producer_id") or "UNKNOWN"),
        "symbol": str(row.get("symbol") or row.get("underlying_symbol") or ""),
        "candidate_id": str(row.get("candidate_id") or ""),
        "raw_signal_id": str(row.get("raw_signal_id") or ""),
        "canonical_blocker": str(row.get("canonical_blocker") or reason),
        "rejection_stage": str(row.get("rejection_stage") or source_collection),
        "rejection_reason": reason,
        "reason": reason,
        "reason_codes": reason_codes,
        "human_readable_explanation": str(row.get("human_readable_explanation") or row.get("message") or reason),
        "next_repair_action": str(row.get("next_repair_action") or row.get("required_next_action") or "npm run aegis:repair-candidate-readiness"),
        "producer_command": str(row.get("producer_command") or row.get("command") or ""),
        "source_artifact_path": diagnostics_path,
    }


def _top_contract_rejection_reason(contracts: dict[str, Any]) -> str:
    reasons = contracts.get("rejection_reasons") if isinstance(contracts.get("rejection_reasons"), list) else []
    for row in reasons:
        if isinstance(row, dict) and row.get("reason"):
            return str(row.get("reason"))
    return "CONTRACT_VALIDATION_REJECTED"


def _candidate_contract_mismatch_explanation(*, status: str, reason: str, contracts: dict[str, Any], diagnostics: dict[str, Any], sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    if not status:
        return {}
    rejected = contracts.get("rejected_raw_signals") if isinstance(contracts.get("rejected_raw_signals"), list) else []
    reasons = contracts.get("rejection_reasons") if isinstance(contracts.get("rejection_reasons"), list) else []
    sample = next((row for row in rejected if isinstance(row, dict)), {})
    return {
        "status": status,
        "reason": reason or _top_contract_rejection_reason(contracts),
        "operator_message": "Diagnostics found candidate-like outputs, but 0 passed candidate contract validation.",
        "diagnostic_candidate_outputs": _first_int_value(diagnostics.get("diagnostic_candidate_outputs"), diagnostics.get("total_candidates_generated"), 0),
        "valid_candidate_contracts": _first_int_value(contracts.get("candidates_created"), len(_list_value(contracts, "candidate_contracts"))),
        "rejected_candidate_contracts": _first_int_value(contracts.get("candidates_rejected"), len(rejected)),
        "rejection_reasons": reasons,
        "sample_rejected_symbols": [str(row.get("symbol") or "") for row in rejected[:10] if isinstance(row, dict)],
        "sample_stale_diagnostics": _sample_stale_diagnostics(sample),
        "price_timestamp": str(sample.get("price_timestamp") or sample.get("entry_reference_price_timestamp_utc") or ""),
        "candidate_snapshot_timestamp": str(sample.get("candidate_snapshot_timestamp") or sample.get("candidate_snapshot_timestamp_utc") or ""),
        "freshness_window_seconds": sample.get("freshness_window_seconds"),
        "stale_by_seconds": sample.get("stale_by_seconds"),
        "freshness_policy_mode": str(sample.get("freshness_policy_mode") or ""),
        "stale_reason": str(sample.get("stale_reason") or ""),
        "evidence_path": str(sources.get("candidate_contracts", {}).get("path") or sources.get("candidate_generation_diagnostics", {}).get("path") or ""),
        "next_repair_action": "Refresh current-session entry reference price evidence, then run signal evidence graph, candidate contracts, candidate diagnostics, paper review queue, and canonical operator state.",
    }


def _sample_stale_diagnostics(row: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(row, dict):
        return {}
    freshness = row.get("entry_reference_price_freshness") if isinstance(row.get("entry_reference_price_freshness"), dict) else {}
    return {
        "symbol": str(row.get("symbol") or ""),
        "raw_signal_id": str(row.get("raw_signal_id") or ""),
        "price_timestamp": str(row.get("price_timestamp") or row.get("entry_reference_price_timestamp_utc") or freshness.get("price_timestamp") or ""),
        "candidate_snapshot_timestamp": str(row.get("candidate_snapshot_timestamp") or row.get("candidate_snapshot_timestamp_utc") or freshness.get("candidate_snapshot_timestamp") or ""),
        "freshness_window_seconds": row.get("freshness_window_seconds", freshness.get("freshness_window_seconds")),
        "stale_by_seconds": row.get("stale_by_seconds", freshness.get("stale_by_seconds")),
        "freshness_policy_mode": str(row.get("freshness_policy_mode") or freshness.get("freshness_policy_mode") or ""),
        "stale_reason": str(row.get("stale_reason") or freshness.get("stale_reason") or ""),
    }


def _candidate_source_ref(key: str, payload: dict[str, Any], source: dict[str, Any], day_utc: str) -> dict[str, Any]:
    return {
        "source": key,
        "path": str(source.get("path") or ""),
        "hash": str(source.get("hash") or ""),
        "found": bool(source.get("found")),
        "schema_id": str(payload.get("schema_id") or ""),
        "artifact_id": str(payload.get("artifact_id") or ""),
        "day_utc": str(payload.get("day_utc") or day_utc if source.get("found") else ""),
        "generated_at": str(source.get("generated_at") or _generated_at(payload)),
        "freshness_status": str(source.get("freshness_status") or "UNKNOWN"),
    }


def _list_value(payload: dict[str, Any], *keys: str) -> list[dict[str, Any]]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
    return []


def _string_list_value(payload: dict[str, Any], *keys: str) -> list[str]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [str(item) for item in value if str(item)]
    return []



def _first_int_value(*values: Any) -> int:
    for value in values:
        if value is None or value == "":
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return 0

def _int_value(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default or 0)

def _opportunities_projection(
    *,
    candidates: dict[str, list[dict[str, Any]]],
    top_candidates: list[dict[str, Any]],
    candidate_review_ledger: dict[str, Any],
    event_triggers: list[dict[str, Any]],
    diagnostics: dict[str, Any],
    data_remediation: dict[str, Any],
    data_registry: dict[str, Any],
    sleeve_readiness: dict[str, Any],
    noon_preflight: dict[str, Any],
    positions: dict[str, list[dict[str, Any]]] | None = None,
    portfolio_selection: dict[str, Any] | None = None,
) -> dict[str, Any]:
    open_rows = top_candidates or candidates.get("awaiting_decision", [])
    candidate_review_ledger = candidate_review_ledger if isinstance(candidate_review_ledger, dict) else {}
    diagnostics = diagnostics if isinstance(diagnostics, dict) else {}
    data_registry = data_registry if isinstance(data_registry, dict) else {}
    data_remediation = data_remediation if isinstance(data_remediation, dict) else {}
    sleeve_readiness = sleeve_readiness if isinstance(sleeve_readiness, dict) else {}
    noon_preflight = noon_preflight if isinstance(noon_preflight, dict) else {}
    positions = positions if isinstance(positions, dict) else {}
    portfolio_selection = portfolio_selection if isinstance(portfolio_selection, dict) else {}
    selected_candidates = portfolio_selection.get("selected_candidates") if isinstance(portfolio_selection.get("selected_candidates"), list) else []
    suppressed_candidates = portfolio_selection.get("suppressed_candidates") if isinstance(portfolio_selection.get("suppressed_candidates"), list) else []
    watchlist_candidates = portfolio_selection.get("watchlist_candidates") if isinstance(portfolio_selection.get("watchlist_candidates"), list) else []
    same_sleeve_summary = portfolio_selection.get("same_sleeve_selection_summary") if isinstance(portfolio_selection.get("same_sleeve_selection_summary"), list) else []
    exposure_cluster_summary = portfolio_selection.get("exposure_cluster_summary") if isinstance(portfolio_selection.get("exposure_cluster_summary"), list) else []
    portfolio_policy = portfolio_selection.get("portfolio_selection_policy") if isinstance(portfolio_selection.get("portfolio_selection_policy"), dict) else {}
    open_projection = selected_candidates or list(open_rows)
    candidate_generation_visibility = build_candidate_generation_visibility_v1(diagnostics)
    return {
        "open": open_projection,
        "selected_candidates": selected_candidates,
        "suppressed_candidates": suppressed_candidates,
        "watchlist_candidates": watchlist_candidates,
        "same_sleeve_selection_summary": same_sleeve_summary,
        "exposure_cluster_summary": exposure_cluster_summary,
        "portfolio_selection_policy": portfolio_policy,
        "candidate_portfolio_selection": portfolio_selection,
        "triggered": list(event_triggers),
        "awaiting_decision": list(candidates.get("awaiting_decision", [])),
        "review_required": list(candidates.get("review_required", [])),
        "watchlisted": list(candidates.get("watchlisted", [])),
        "needs_more_evidence": list(candidates.get("needs_more_evidence", [])),
        "dismissed": list(candidates.get("dismissed", [])),
        "expired": list(candidates.get("expired", [])),
        "candidate_review_ledger": candidate_review_ledger,
        "candidate_review_ledger_rows": candidate_review_ledger.get("candidates") if isinstance(candidate_review_ledger.get("candidates"), list) else [],
        "candidate_review_active_rows": candidate_review_ledger.get("filtered_candidates") if candidate_review_ledger.get("filter") == "active" and isinstance(candidate_review_ledger.get("filtered_candidates"), list) else [
            row for row in candidate_review_ledger.get("candidates", []) if isinstance(row, dict) and bool(row.get("active"))
        ],
        "candidate_review_historical_rows": [
            row for row in candidate_review_ledger.get("candidates", []) if isinstance(row, dict) and not bool(row.get("active"))
        ],
        "deferred": list(candidates.get("deferred", [])),
        "awaiting_outcome": list(candidates.get("awaiting_outcome", [])),
        "position_management": positions,
        "positions": positions,
        "diagnostics": diagnostics,
        "data_registry": data_registry,
        "data_remediation": data_remediation,
        "market_data_summary": {**(diagnostics.get("market_data_summary") if isinstance(diagnostics.get("market_data_summary"), dict) else _market_data_summary_from_registry(data_registry)), "data_remediation": data_remediation},
        "sleeve_readiness": sleeve_readiness,
        "noon_preflight": noon_preflight,
        "noon_preflight_alert": _noon_preflight_alert(noon_preflight),
        "no_opportunity_explanation": _no_opportunity_explanation(diagnostics=diagnostics, open_rows=list(open_rows)),
        "candidate_generation_visibility": candidate_generation_visibility,
        "execution_coverage": candidate_generation_visibility.get("execution_coverage", {}),
        "sleeve_execution_summary": candidate_generation_visibility.get("sleeve_execution_summary", []),
        "candidate_contracts": diagnostics.get("candidate_contracts") if isinstance(diagnostics.get("candidate_contracts"), dict) else {},
        "real_candidate_contracts": candidate_generation_visibility.get("real_candidate_contracts", []),
        "real_signal_death_report": diagnostics.get("real_signal_death_report") if isinstance(diagnostics.get("real_signal_death_report"), dict) else {},
        "real_raw_signals": candidate_generation_visibility.get("real_raw_signals", []),
        "rejection_stage_counts": candidate_generation_visibility.get("rejection_stage_counts", {}),
        "top_missing_candidate_fields": candidate_generation_visibility.get("top_missing_candidate_fields", []),
        "paper_golden_path_comparison_summary": candidate_generation_visibility.get("paper_golden_path_comparison_summary", {}),
        "exact_next_repair_actions": candidate_generation_visibility.get("exact_next_repair_actions", []),
        "sleeve_run_summary": diagnostics.get("sleeves") if isinstance(diagnostics.get("sleeves"), list) else [],
        "sleeve_readiness_summary": sleeve_readiness.get("sleeves") if isinstance(sleeve_readiness.get("sleeves"), list) else diagnostics.get("per_sleeve_readiness", []),
        "global_context_summary": _global_context_summary(data_registry=data_registry, diagnostics=diagnostics),
        "breadth_drop_validation": diagnostics.get("breadth_drop_validation") if isinstance(diagnostics.get("breadth_drop_validation"), dict) else {},
        "breadth_drop_validation_path": str(diagnostics.get("breadth_drop_validation_path") or ""),
        "vix_source_verification": diagnostics.get("vix_source_verification") if isinstance(diagnostics.get("vix_source_verification"), dict) else {},
        "vix_source_verification_path": str(diagnostics.get("vix_source_verification_path") or ""),
        "context_readiness_repair": diagnostics.get("context_readiness_repair") if isinstance(diagnostics.get("context_readiness_repair"), dict) else {},
        "context_readiness_repair_path": str(diagnostics.get("context_readiness_repair_path") or ""),
        "market_context_demand": diagnostics.get("market_context_demand") if isinstance(diagnostics.get("market_context_demand"), dict) else {},
        "market_context_demand_path": str(diagnostics.get("market_context_demand_path") or ""),
        "market_context_provider_health": diagnostics.get("market_context_provider_health") if isinstance(diagnostics.get("market_context_provider_health"), dict) else {},
        "market_context_provider_health_path": str(diagnostics.get("market_context_provider_health_path") or ""),
        "market_context_status_counts": diagnostics.get("market_context_status_counts") if isinstance(diagnostics.get("market_context_status_counts"), dict) else {},
        "market_context_blockers": diagnostics.get("market_context_blockers") if isinstance(diagnostics.get("market_context_blockers"), list) else [],
        "rejected_candidate_summary": candidate_generation_visibility.get("rejected_candidate_visibility", []) or _rejected_candidate_summary(diagnostics),
        "no_candidate_explanations": candidate_generation_visibility.get("no_candidate_explanations", []),
        "trigger_summary": diagnostics.get("trigger_evaluation") if isinstance(diagnostics.get("trigger_evaluation"), dict) else {},
        "source_authority": "Candidate Lifecycle + Candidate Ranking + Candidate Portfolio Selection + Triggered Sleeve Runs",
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def _noon_preflight_alert(noon_preflight: dict[str, Any]) -> dict[str, Any]:
    if not noon_preflight:
        return {"available": False, "reason": "NOON_PREFLIGHT_MISSING"}
    failed = str(noon_preflight.get("status") or "").upper() in {"FAILED", "PARTIAL"}
    alert_required = bool(noon_preflight.get("operator_alert_required"))
    email_status = str(noon_preflight.get("email_status") or "NOT_CONFIGURED").upper()
    email_delivered = email_status == "SENT"
    if not (failed and alert_required and not email_delivered):
        return {"available": False, "reason": "NO_ALERT_OR_EMAIL_DELIVERED", "email_status": email_status}
    return {
        "available": True,
        "title": "Noon preflight failed and no email alert was delivered.",
        "reason": noon_preflight.get("alert_reason") or "Noon preflight failed.",
        "timestamp": noon_preflight.get("generated_at_utc") or "",
        "next_action": noon_preflight.get("next_safe_command") or "npm run aegis:candidate-diagnostics",
        "email_status": email_status,
        "setup_instructions": "Configure C2_EMAIL_SMTP_HOST, C2_EMAIL_SMTP_PORT, C2_EMAIL_USERNAME, C2_EMAIL_PASSWORD, C2_EMAIL_FROM, and C2_EMAIL_TO.",
        "blocking_items": noon_preflight.get("blocking_items") if isinstance(noon_preflight.get("blocking_items"), list) else [],
    }


def _global_context_summary(*, data_registry: dict[str, Any], diagnostics: dict[str, Any]) -> list[dict[str, Any]]:
    items = data_registry.get("data_items") if isinstance(data_registry.get("data_items"), list) else []
    wanted = {"market.price.SPY", "market.price.QQQ", "market.volatility.VIX", "market.breadth.down_pct", "market.breadth.advance_decline_delta"}
    rows = [
        {
            "data_item_id": item.get("data_item_id"),
            "status": item.get("status"),
            "provider": item.get("provider"),
            "data_timestamp_utc": item.get("data_timestamp_utc"),
            "notes": item.get("notes") if isinstance(item.get("notes"), list) else [],
            "label": "Global context; not necessarily required by every sleeve.",
        }
        for item in items
        if isinstance(item, dict) and item.get("data_item_id") in wanted
    ]
    if rows:
        return rows
    for row in diagnostics.get("global_context_warnings") or []:
        if isinstance(row, dict):
            rows.append({"data_item_id": row.get("data_item_id"), "status": "MISSING_OR_STALE", "provider": "", "data_timestamp_utc": "", "notes": [row.get("message") or ""], "label": "Global context; not necessarily required by every sleeve."})
    return rows


def _market_data_summary_from_registry(data_registry: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(data_registry, dict) or not data_registry:
        return {
            "available": False,
            "status": "MISSING",
            "provider_config": {"configured": False, "primary": "", "fallback": ""},
            "requested_symbols": [],
            "fetched_symbols": [],
            "missing_symbols": [],
            "stale_symbols": [],
            "mapping_missing_symbols": [],
            "provider_failed_symbols": [],
            "failure_reason": "MARKET_DATA_REPORT_MISSING",
            "usable_for_candidate_generation": False,
            "runtime_universe_mode": "",
            "production_scan_dataset_id": "",
            "dataset_snapshot_id": "",
            "production_scan_universe_count": 0,
            "sleeve_required_symbol_count": 0,
            "total_requested_symbol_count": 0,
            "requested_symbols_source": "",
        }
    return {
        "available": True,
        "status": "UNKNOWN",
        "provider_config": data_registry.get("provider_config") if isinstance(data_registry.get("provider_config"), dict) else {"configured": False, "primary": "", "fallback": ""},
        "requested_symbols": data_registry.get("requested_symbols") if isinstance(data_registry.get("requested_symbols"), list) else [],
        "fetched_symbols": data_registry.get("fetched_symbols") if isinstance(data_registry.get("fetched_symbols"), list) else [],
        "missing_symbols": data_registry.get("missing_symbols") if isinstance(data_registry.get("missing_symbols"), list) else [],
        "stale_symbols": data_registry.get("stale_symbols") if isinstance(data_registry.get("stale_symbols"), list) else [],
        "mapping_missing_symbols": data_registry.get("mapping_missing_symbols") if isinstance(data_registry.get("mapping_missing_symbols"), list) else [],
        "provider_failed_symbols": data_registry.get("provider_failed_symbols") if isinstance(data_registry.get("provider_failed_symbols"), list) else [],
        "failure_reason": "",
        "usable_for_candidate_generation": not data_registry.get("missing_items") and not data_registry.get("stale_items"),
        "runtime_universe_mode": str(data_registry.get("runtime_universe_mode") or ""),
        "production_scan_dataset_id": str(data_registry.get("production_scan_dataset_id") or ""),
        "dataset_snapshot_id": str(data_registry.get("dataset_snapshot_id") or data_registry.get("production_scan_dataset_id") or ""),
        "production_scan_universe_count": int(data_registry.get("production_scan_universe_count") or 0),
        "sleeve_required_symbol_count": int(data_registry.get("sleeve_required_symbol_count") or 0),
        "total_requested_symbol_count": int(data_registry.get("total_requested_symbol_count") or len(data_registry.get("requested_symbols") or [])),
        "requested_symbols_source": str(data_registry.get("requested_symbols_source") or ""),
    }


def _no_opportunity_explanation(*, diagnostics: dict[str, Any], open_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if open_rows:
        return {
            "available": False,
            "reason": "OPEN_CANDIDATES_PRESENT",
            "title": "",
            "message": "",
            "operator_interpretation": "NORMAL_NO_SIGNAL",
            "recommended_next_steps": [],
        }
    if not diagnostics:
        return {
            "available": False,
            "reason": "CANDIDATE_DIAGNOSTICS_MISSING",
            "title": "Candidate diagnostics missing.",
            "message": "Run candidate diagnostics before claiming there are no opportunities.",
            "operator_interpretation": "UNKNOWN",
            "recommended_next_steps": ["Run candidate diagnostics."],
        }
    interpretation = str(diagnostics.get("operator_interpretation") or "UNKNOWN")
    raw_rejections = diagnostics.get("raw_signal_rejections") if isinstance(diagnostics.get("raw_signal_rejections"), list) else []
    blocked_sleeves = [
        {
            "sleeve_id": row.get("sleeve_id"),
            "blocker": row.get("canonical_blocker") or ", ".join(str(item) for item in row.get("blocking_inputs") or []) or row.get("reason_no_candidate") or "",
            "reason": row.get("reason_no_candidate") or "",
        }
        for row in diagnostics.get("sleeves") or []
        if isinstance(row, dict) and str(row.get("run_status") or "").upper() == "BLOCKED"
    ]
    return {
        "available": True,
        "reason": "ZERO_CANDIDATES",
        "title": "No opportunities today.",
        "message": str(diagnostics.get("zero_candidate_explanation") or ""),
        "why_zero": str(diagnostics.get("zero_candidate_explanation") or ""),
        "partial_run_completed": interpretation == "PARTIAL_RUN",
        "operator_interpretation": interpretation,
        "candidate_generation_status": diagnostics.get("candidate_generation_status") or "UNKNOWN",
        "raw_signal_rejections": raw_rejections,
        "blocked_sleeves": blocked_sleeves,
        "summary": {
            "sleeves_evaluated": int(diagnostics.get("total_sleeves_expected") or 0),
            "sleeves_run": int(diagnostics.get("total_sleeves_run") or 0),
            "raw_signals": int(diagnostics.get("total_raw_signals") or 0),
            "candidates_generated": int(diagnostics.get("total_candidates_generated") or 0),
            "candidates_rejected": int(diagnostics.get("total_candidates_rejected") or 0),
        },
        "recommended_next_steps": diagnostics.get("recommended_next_steps") if isinstance(diagnostics.get("recommended_next_steps"), list) else [],
    }


def _rejected_candidate_summary(diagnostics: dict[str, Any]) -> list[dict[str, Any]]:
    raw_rejections = diagnostics.get("raw_signal_rejections") if isinstance(diagnostics.get("raw_signal_rejections"), list) else []
    if raw_rejections:
        return [
            {
                "raw_signal_id": row.get("raw_signal_id") or "",
                "candidate_id": row.get("candidate_id") or "",
                "sleeve_id": row.get("sleeve_id") or "UNKNOWN",
                "symbol": row.get("symbol") or "",
                "rejection_stage": row.get("rejection_stage") or "UNKNOWN",
                "rejection_reason": row.get("rejection_reason") or "",
                "reason": row.get("rejection_reason") or "",
                "human_readable_explanation": row.get("human_readable_explanation") or "",
                "required_next_action": row.get("required_next_action") or "",
                "rejection_classification": row.get("rejection_classification") or "UNKNOWN",
                "safety_related": bool(row.get("safety_related")),
                "error": bool(row.get("error")),
                "raw_signal_count": 1,
                "rejected_count": 1,
            }
            for row in raw_rejections
            if isinstance(row, dict)
        ]
    rows = diagnostics.get("sleeves") if isinstance(diagnostics.get("sleeves"), list) else []
    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        for reason in row.get("rejection_reasons") or []:
            out.append(
                {
                    "sleeve_id": row.get("sleeve_id") or "UNKNOWN",
                    "reason": reason,
                    "raw_signal_count": row.get("raw_signal_count", 0),
                    "rejected_count": row.get("rejected_count", 0),
                }
            )
    return out


def _edge_lab_projection(*, research: dict[str, Any], sleeves: dict[str, list[dict[str, Any]]], pipeline: dict[str, Any] | None = None) -> dict[str, Any]:
    pipeline = pipeline if isinstance(pipeline, dict) else {}
    active = [_edge_hypothesis_projection(row) for row in research.get("active_hypotheses", []) or research.get("captured_hypotheses", []) or []]
    review_required = [_edge_hypothesis_projection(row) for row in research.get("review_required", []) or []]
    completed = [_edge_hypothesis_projection(row) for row in research.get("completed", []) or research.get("validated_hypotheses", []) or []]
    rejected = [_edge_hypothesis_projection(row) for row in research.get("rejected_hypotheses", []) or research.get("rejected", []) or []]
    archived = [_edge_hypothesis_projection(row) for row in research.get("archived_hypotheses", []) or []]
    duplicates = [_edge_hypothesis_projection(row) for row in research.get("duplicates", []) or []]
    fixtures = [_edge_hypothesis_projection(row) for row in research.get("test_fixtures_excluded", []) or []]
    challenger = [
        {**row, "human_approval_required": True, "automated_change_allowed": False}
        for bucket in ("watch", "challenged", "insufficient_data")
        for row in research_safe_sleeves(sleeves, bucket)
    ]
    return {
        "active_hypotheses": active,
        "research_queue": list(research.get("priority_tasks", []) or []) + list(research.get("new_tasks", []) or []),
        "challenger_findings": challenger,
        "review_required": review_required,
        "completed": completed,
        "rejected": rejected,
        "archived": archived,
        "duplicates": duplicates,
        "test_fixtures_excluded": fixtures,
        "pipeline": pipeline.get("pipeline") or research.get("pipeline") or _empty_pipeline(),
        "priority_pipeline": pipeline.get("priority_pipeline") or research.get("priority_pipeline") or {},
        "priority_counts": pipeline.get("priority_counts") or research.get("priority_counts") or {},
        "recommended_focus_today": pipeline.get("recommended_focus_today") or research.get("recommended_focus_today") or [],
        "counts": pipeline.get("counts") or research.get("pipeline_counts") or {},
        "blocked_items": pipeline.get("blocked_items") or research.get("pipeline_blocked_items") or [],
        "next_operator_actions": pipeline.get("next_operator_actions") or research.get("pipeline_next_operator_actions") or [],
        "source_authority": "Research Lab + Sleeve Challenger",
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "automatic_sleeve_mutation_allowed": False,
    }


def _empty_pipeline() -> dict[str, list[Any]]:
    return {
        "inbox": [],
        "triage": [],
        "test_plan": [],
        "testing": [],
        "result_review": [],
        "paper_trial": [],
        "sleeve_review": [],
        "active_or_adopted": [],
        "rejected_archived": [],
    }


def _research_pipeline_section(pipeline: dict[str, Any]) -> dict[str, Any]:
    if not pipeline:
        return {"pipeline": _empty_pipeline(), "priority_pipeline": {}, "priority_counts": {}, "recommended_focus_today": [], "counts": {}, "blocked_items": [], "next_operator_actions": []}
    return {
        "pipeline": pipeline.get("pipeline") or _empty_pipeline(),
        "priority_pipeline": pipeline.get("priority_pipeline") or {},
        "priority_counts": pipeline.get("priority_counts") or {},
        "recommended_focus_today": pipeline.get("recommended_focus_today") or [],
        "counts": pipeline.get("counts") or {},
        "blocked_items": pipeline.get("blocked_items") or [],
        "next_operator_actions": pipeline.get("next_operator_actions") or [],
        "items": pipeline.get("items") or [],
    }


def research_safe_sleeves(sleeves: dict[str, list[dict[str, Any]]], bucket: str) -> list[dict[str, Any]]:
    return [row for row in sleeves.get(bucket, []) if isinstance(row, dict)]


def _edge_hypothesis_projection(row: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(row, dict):
        return {}
    status = str(row.get("normalized_status") or row.get("status") or row.get("classification") or "UNKNOWN").upper()
    stage = _edge_stage(status)
    return {
        **row,
        "edge_type": row.get("edge_type") or row.get("edge_family") or "UNKNOWN",
        "lifecycle_stage": row.get("lifecycle_stage") or stage,
        "stage_confidence": row.get("stage_confidence") or ("MEDIUM" if row.get("lifecycle_stage") else "LOW"),
        "stage_source": row.get("stage_source") or row.get("source_artifact") or "Research hypothesis classification",
        "next_step": row.get("next_step") or _edge_next_step(stage),
        "human_approval_required": True,
        "automated_change_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def _edge_stage(status: str) -> str:
    normalized = str(status or "UNKNOWN").upper()
    if normalized in {"IDEA", "ACTIVE", "PROPOSED"}:
        return "IDEA"
    if normalized in {"QUEUED", "RESEARCH_QUEUED"}:
        return "RESEARCH_QUEUED"
    if normalized == "DATA_NEEDED":
        return "DATA_NEEDED"
    if normalized == "BACKTEST_READY":
        return "BACKTEST_READY"
    if normalized == "BACKTEST_RUNNING":
        return "BACKTEST_RUNNING"
    if normalized in {"BACKTEST_COMPLETE", "VALIDATED", "COMPLETED"}:
        return "BACKTEST_COMPLETE"
    if normalized in {"REVIEW_REQUIRED", "RESULT_REVIEW_REQUIRED"}:
        return "REVIEW_REQUIRED"
    if normalized == "PAPER_TEST_CANDIDATE":
        return "PAPER_TEST_CANDIDATE"
    if normalized == "SLEEVE_REVIEW_CANDIDATE":
        return "SLEEVE_REVIEW_CANDIDATE"
    if normalized == "REJECTED":
        return "REJECTED"
    if normalized == "ARCHIVED":
        return "ARCHIVED"
    return "NEEDS_MORE_EVIDENCE"


def _edge_next_step(stage: str) -> str:
    return {
        "IDEA": "queue research",
        "RESEARCH_QUEUED": "attach dataset",
        "DATA_NEEDED": "attach dataset",
        "BACKTEST_READY": "run backtest",
        "BACKTEST_RUNNING": "review result",
        "BACKTEST_COMPLETE": "review result",
        "REVIEW_REQUIRED": "review result",
        "PAPER_TEST_CANDIDATE": "promote to sleeve review",
        "SLEEVE_REVIEW_CANDIDATE": "promote to sleeve review",
        "REJECTED": "reject/archive",
        "ARCHIVED": "no action needed",
    }.get(str(stage or ""), "needs manual classification")


def _performance_workflow_projection(*, candidates: dict[str, list[dict[str, Any]]], performance: dict[str, Any], sleeves: dict[str, list[dict[str, Any]]], research: dict[str, Any], regime: dict[str, Any]) -> dict[str, Any]:
    advisory_quality = performance.get("advisory_quality") if isinstance(performance.get("advisory_quality"), dict) else {}
    metric_names = [
        "candidate_hit_rate",
        "false_positive_rate",
        "false_negative_proxy",
        "ignored_candidate_opportunity_cost",
        "realized_vs_advisory_gap",
        "recommendation_accuracy",
        "regime_adjusted_recommendation_accuracy",
        "regime_specific_candidate_quality",
        "sleeve_candidate_quality",
        "traded_vs_ignored_performance",
    ]
    advanced = [_metric_projection(name, advisory_quality.get(name)) for name in metric_names]
    readiness = _performance_readiness(advanced)
    outcome_followups = list(candidates.get("awaiting_outcome", []))
    sleeve_quality = [
        row
        for bucket in ("watch", "challenged", "insufficient_data")
        for row in sleeves.get(bucket, [])
        if isinstance(row, dict)
    ]
    lessons = []
    if readiness.get("status") in {"EARLY_SIGNAL", "MEANINGFUL", "DEGRADED"}:
        lessons.append({"lesson_id": "attribution_signal", "summary": "Performance attribution has enough candidate history to review.", "source": "Performance Attribution"})
    return {
        "readiness": readiness,
        "lessons_learned": lessons,
        "outcome_followups": outcome_followups,
        "sleeve_quality": sleeve_quality,
        "advanced_metrics": advanced,
        "source_authority": "Performance Attribution",
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def _metric_projection(name: str, metric: Any) -> dict[str, Any]:
    row = metric if isinstance(metric, dict) else {}
    return {
        "metric": name,
        "value": row.get("value"),
        "metric_status": row.get("metric_status") or row.get("status") or "MISSING_INPUT",
        "sample_size": row.get("sample_size", 0),
        "minimum_sample_size": row.get("minimum_sample_size", 20),
        "formula_version": row.get("formula_version", ""),
        "evidence_quality": row.get("evidence_quality", "UNKNOWN"),
        "confidence": row.get("confidence", "UNKNOWN"),
        "input_artifacts": row.get("input_artifacts") or [],
        "input_hashes": row.get("input_hashes") or [],
    }


def _performance_readiness(metrics: list[dict[str, Any]]) -> dict[str, Any]:
    sample = max([int(row.get("sample_size") or 0) for row in metrics] or [0])
    minimum = max([int(row.get("minimum_sample_size") or 0) for row in metrics] or [20])
    statuses = {str(row.get("metric_status") or "UNKNOWN") for row in metrics}
    if not metrics:
        status = "UNKNOWN"
    elif statuses <= {"INSUFFICIENT_DATA", "MISSING_INPUT", "UNKNOWN"}:
        status = "NOT_ENOUGH_DATA"
    elif sample < minimum:
        status = "EARLY_SIGNAL"
    else:
        status = "MEANINGFUL"
    return {
        "status": status,
        "sample_size": sample,
        "minimum_sample_size": minimum,
        "summary": f"{sample}/{minimum}",
    }


def _journal_projection(
    *,
    sources: dict[str, dict[str, Any]],
    candidates: dict[str, list[dict[str, Any]]],
    research: dict[str, Any],
    governance: dict[str, list[dict[str, Any]]],
    drilldowns: list[dict[str, Any]],
    journal_timeline: dict[str, Any],
) -> dict[str, Any]:
    timeline_summary = journal_timeline.get("timeline_summary") if isinstance(journal_timeline.get("timeline_summary"), dict) else {}
    recent_events = journal_timeline.get("recent_events") if isinstance(journal_timeline.get("recent_events"), list) else []
    entity_index = journal_timeline.get("entity_index") if isinstance(journal_timeline.get("entity_index"), dict) else {}
    audit_drilldowns = journal_timeline.get("audit_drilldowns") if isinstance(journal_timeline.get("audit_drilldowns"), list) else drilldowns
    diagnostics = journal_timeline.get("diagnostics") if isinstance(journal_timeline.get("diagnostics"), list) else []
    return {
        "timeline_summary": {
            "total_events": int(timeline_summary.get("total_events") or 0),
            "latest_event_at": timeline_summary.get("latest_event_at"),
            "candidate_events": int(timeline_summary.get("candidate_events") or 0),
            "edge_events": int(timeline_summary.get("edge_events") or 0),
            "sleeve_events": int(timeline_summary.get("sleeve_events") or 0),
            "performance_events": int(timeline_summary.get("performance_events") or 0),
            "runtime_events": int(timeline_summary.get("runtime_events") or 0),
            "governance_events": int(timeline_summary.get("governance_events") or 0),
            "system_events": int(timeline_summary.get("system_events") or 0),
            "family_counts": timeline_summary.get("family_counts") or {},
        },
        "recent_events": recent_events[:100],
        "entity_index": {
            "candidates": list(entity_index.get("candidates") or []),
            "hypotheses": list(entity_index.get("hypotheses") or []),
            "sleeves": list(entity_index.get("sleeves") or []),
            "regimes": list(entity_index.get("regimes") or []),
        },
        "audit_drilldowns": audit_drilldowns,
        "diagnostics": diagnostics,
        "filters": list(journal_timeline.get("filters") or ["All", "Candidates", "Edges", "Sleeves", "Performance", "Runtime", "Governance", "System"]),
        "candidate_history": [row for bucket in candidates.values() for row in bucket],
        "edge_history": list(research.get("captured_hypotheses", []) or []) + list(research.get("rejected_hypotheses", []) or []) + list(research.get("duplicates", []) or []),
        "system_artifacts": [{"id": key, "path": row.get("path", ""), "hash": row.get("hash", ""), "freshness_status": row.get("freshness_status", "UNKNOWN")} for key, row in sorted(sources.items())],
        "audit_links": list(drilldowns),
        "governance_history": [row for bucket in governance.values() for row in bucket],
        "source_authority": "Aegis Journal Timeline",
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def _research_section(root: Path, day_utc: str, research_lab: dict[str, Any]) -> dict[str, Any]:
    tasks = research_lab.get("research_tasks") if isinstance(research_lab.get("research_tasks"), list) else []
    results = research_lab.get("research_results") if isinstance(research_lab.get("research_results"), list) else []
    reviews = research_lab.get("sleeve_review_candidates") if isinstance(research_lab.get("sleeve_review_candidates"), list) else []
    classification_report = build_research_hypothesis_classification_v1(truth_root=root, day_utc=day_utc)
    hypotheses = classification_report.get("hypotheses") if isinstance(classification_report.get("hypotheses"), list) else []
    visible = [row for row in hypotheses if row.get("appears_in_research_ui") is True]
    active_hypotheses = [
        row
        for row in visible
        if row.get("classification") in {"REAL_OPERATOR_HYPOTHESIS", "SYSTEM_GENERATED_HYPOTHESIS"}
        and row.get("normalized_status") in {"ACTIVE", "QUEUED"}
    ]
    validated_hypotheses = [
        row
        for row in visible
        if row.get("classification") in {"REAL_OPERATOR_HYPOTHESIS", "SYSTEM_GENERATED_HYPOTHESIS"}
        and row.get("normalized_status") in {"VALIDATED", "COMPLETED"}
    ]
    rejected_hypotheses = [row for row in visible if row.get("classification") == "REJECTED" or row.get("normalized_status") == "REJECTED"]
    archived_hypotheses = [row for row in visible if row.get("classification") == "ARCHIVED" or row.get("normalized_status") == "ARCHIVED"]
    needs_classification = [row for row in visible if row.get("classification") in {"NEEDS_MANUAL_CLASSIFICATION", "UNKNOWN"} or row.get("normalized_status") == "NEEDS_CLASSIFICATION"]
    test_fixtures_excluded = [row for row in hypotheses if row.get("classification") == "TEST_FIXTURE"]
    duplicates = [row for row in visible if row.get("classification") == "LEGACY_DUPLICATE" or row.get("duplicate_of")]
    return {
        "captured_hypotheses": active_hypotheses + validated_hypotheses,
        "active_hypotheses": active_hypotheses,
        "validated_hypotheses": validated_hypotheses,
        "priority_tasks": tasks[:10],
        "new_tasks": [row for row in tasks if row.get("lifecycle_state") in {"RESEARCH_QUEUED", "DATA_NEEDED"}],
        "review_required": [row for row in results if row.get("status") == "RESULT_REVIEW_REQUIRED"] + reviews,
        "completed": validated_hypotheses,
        "rejected": rejected_hypotheses,
        "rejected_hypotheses": rejected_hypotheses,
        "archived_hypotheses": archived_hypotheses,
        "needs_classification": needs_classification,
        "test_fixtures_excluded": test_fixtures_excluded,
        "duplicates": duplicates,
        "orphaned": [row for row in hypotheses if row.get("orphaned") is True],
        "classification_summary": classification_report.get("summary_counts") or {},
        "source_artifacts": [row["source_artifact"] for row in hypotheses if row.get("source_artifact")],
        "source_hashes": {str(row["hypothesis_id"]): row["source_hash"] for row in hypotheses if row.get("hypothesis_id") and row.get("source_hash")},
        "source_authority": "Research Lab artifacts",
    }


def _discover_research_hypotheses(root: Path, day_utc: str) -> list[dict[str, Any]]:
    search_roots = [root / "research_lab"]
    if root.name == "truth":
        search_roots.append(root.parent / "research_lab")
    rows: list[dict[str, Any]] = []
    canonical_ids: set[str] = set()
    canonical_keys: set[str] = set()
    for base in search_roots:
        if not base.exists():
            continue
        for path in sorted(base.rglob("research_hypothesis.v1.json")):
            payload = read_json_v1(path)
            row = _normalize_research_hypothesis(path, payload, legacy_source=False, day_utc=day_utc)
            if row:
                rows.append(row)
                canonical_ids.add(str(row.get("hypothesis_id") or ""))
                if row.get("source_idea_id"):
                    canonical_keys.add(str(row["source_idea_id"]).lower())
                canonical_keys.add(str(row.get("hypothesis_id") or "").replace("rh-", "").lower())
        for path in sorted(base.rglob("*.edge_hypothesis.v1.json")):
            payload = read_json_v1(path)
            row = _normalize_research_hypothesis(path, payload, legacy_source=True, day_utc=day_utc)
            if row:
                idea_key = str(row.get("source_idea_id") or row.get("hypothesis_id") or "").lower()
                canonical_key = str(row.get("canonical_hypothesis_id") or "").replace("rh-", "").lower()
                row["orphaned"] = idea_key not in canonical_keys and canonical_key not in canonical_keys and row.get("canonical_hypothesis_id") not in canonical_ids
                rows.append(row)
    return sorted(rows, key=lambda row: (str(row.get("created_at_utc") or ""), str(row.get("hypothesis_id") or ""), str(row.get("source_artifact") or "")))


def _normalize_research_hypothesis(path: Path, payload: dict[str, Any], *, legacy_source: bool, day_utc: str) -> dict[str, Any] | None:
    if not payload:
        return None
    if legacy_source:
        idea_id = str(payload.get("idea_id") or path.stem.replace(".edge_hypothesis.v1", ""))
        hypothesis_id = idea_id
        title = str(payload.get("title") or payload.get("hypothesis") or idea_id)
        status = str(payload.get("status") or "UNKNOWN").upper()
        created_at = str(payload.get("created_at_utc") or payload.get("created_utc") or "")
        canonical_hypothesis_id = f"rh-{idea_id.lower()}" if idea_id.upper().startswith("EDGE-") else ""
        return {
            "hypothesis_id": hypothesis_id,
            "canonical_hypothesis_id": canonical_hypothesis_id,
            "source_idea_id": idea_id,
            "title": title,
            "hypothesis_summary": str(payload.get("hypothesis") or payload.get("thesis") or title),
            "status": status,
            "classification": _research_status_class(status),
            "edge_family": payload.get("edge_family") or payload.get("edge_type") or "UNKNOWN",
            "source": payload.get("source") or "UNKNOWN",
            "created_at_utc": created_at,
            "updated_at_utc": str(payload.get("updated_at_utc") or payload.get("updated_utc") or ""),
            "schema_type": "edge_hypothesis.v1",
            "legacy_source": True,
            "orphaned": True,
            "active": status in {"PROPOSED", "VALIDATED", "TEST_QUEUED", "QUEUED"},
            "queued": "QUEUED" in status,
            "source_artifact": str(path),
            "source_hash": _file_hash(path),
            "freshness_status": "CURRENT" if day_utc in path.parts or created_at.startswith(day_utc) else "HISTORICAL",
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
        }
    hypothesis_id = str(payload.get("hypothesis_id") or path.parent.name)
    status = str(payload.get("status") or payload.get("lifecycle_state") or "UNKNOWN").upper()
    title = str(payload.get("title") or payload.get("hypothesis_summary") or hypothesis_id)
    source_idea_id = str(payload.get("source_idea_id") or payload.get("source_edge_id") or "")
    if not source_idea_id and hypothesis_id.startswith("rh-edge-"):
        source_idea_id = hypothesis_id.replace("rh-", "", 1).upper()
    created_at = str(payload.get("created_at_utc") or payload.get("generated_at_utc") or payload.get("generated_at") or "")
    return {
        "hypothesis_id": hypothesis_id,
        "canonical_hypothesis_id": hypothesis_id,
        "source_idea_id": source_idea_id,
        "title": title,
        "hypothesis_summary": str(payload.get("hypothesis_summary") or title),
        "status": status,
        "classification": _research_status_class(status),
        "edge_family": payload.get("edge_family") or "UNKNOWN",
        "source": payload.get("source") or "UNKNOWN",
        "created_at_utc": created_at,
        "updated_at_utc": str(payload.get("updated_at_utc") or payload.get("updated_utc") or ""),
        "schema_type": "research_hypothesis.v1",
        "legacy_source": False,
        "orphaned": False,
        "active": status in {"IDEA", "RESEARCH_QUEUED", "DATA_NEEDED", "BACKTEST_READY", "PAPER_TEST_CANDIDATE", "SLEEVE_REVIEW_CANDIDATE"},
        "queued": status in {"RESEARCH_QUEUED", "DATA_NEEDED", "BACKTEST_READY"},
        "source_artifact": str(path),
        "source_hash": _file_hash(path),
        "freshness_status": "CURRENT" if day_utc in path.parts or created_at.startswith(day_utc) else "HISTORICAL",
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def _research_status_class(status: str) -> str:
    normalized = str(status or "UNKNOWN").upper()
    if normalized in {"IDEA", "PROPOSED", "VALIDATED"}:
        return "ACTIVE"
    if normalized in {"RESEARCH_QUEUED", "DATA_NEEDED", "BACKTEST_READY", "TEST_QUEUED", "QUEUED"}:
        return "QUEUED"
    if normalized in {"BACKTEST_COMPLETE", "RESULT_REVIEWED", "COMPLETED", "TESTED"}:
        return "COMPLETED"
    if normalized in {"REJECTED", "AUTO_REJECTED", "RETIRED", "INVALIDATED"}:
        return "REJECTED"
    if normalized in {"AUTO_ARCHIVED", "ARCHIVED"}:
        return "COMPLETED"
    return "UNKNOWN"


def _governance_section(governance: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    sections = {"awaiting_approval": [], "approved": [], "rejected": [], "deferred": []}
    for rec in governance.get("recommendations", []) if isinstance(governance.get("recommendations"), list) else []:
        status = str(rec.get("approval_status") or "PROPOSED")
        item = {"recommendation_id": rec.get("recommendation_id"), "type": rec.get("type"), "target": rec.get("target"), "approval_status": status, "human_approval_required": True}
        if status in {"PROPOSED", "NEEDS_MORE_EVIDENCE"}:
            sections["awaiting_approval"].append(item)
        elif status == "APPROVED_BY_HUMAN":
            sections["approved"].append(item)
        elif status == "REJECTED_BY_HUMAN":
            sections["rejected"].append(item)
        elif status == "DEFERRED":
            sections["deferred"].append(item)
    return sections


def _regime_section(regime_memory: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "AVAILABLE" if regime_memory else "MISSING",
        "evidence_quality": regime_memory.get("evidence_quality", "UNKNOWN"),
        "candidate_outcomes_by_regime": regime_memory.get("candidate_outcomes_by_regime") or [],
        "sleeve_outcomes_by_regime": regime_memory.get("sleeve_outcomes_by_regime") or [],
        "unknowns": regime_memory.get("unknowns") or ["UNKNOWN"],
    }


def _event_triggers(triggered: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "triggered_run_id": row.get("triggered_run_id"),
            "trigger_id": row.get("trigger_id"),
            "status": row.get("status"),
            "selected_sleeve_ids": row.get("selected_sleeve_ids") or [],
            "candidate_count": row.get("candidate_count", 0),
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
        }
        for row in triggered.get("runs", [])
        if isinstance(row, dict)
    ]


def _actions_required(*, runtime: dict[str, Any], candidates: dict[str, list[dict[str, Any]]], governance: dict[str, list[dict[str, Any]]], sleeves: dict[str, list[dict[str, Any]]], research: dict[str, list[dict[str, Any]]], missing: list[dict[str, Any]], sources: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    actions = []
    runtime_class = str(runtime.get("runtime_truth_classification") or "UNKNOWN")
    runtime_available = runtime.get("status") == "AVAILABLE"
    if not runtime_available or runtime_class in {"", "UNKNOWN", "BLOCKED", "MISSING"}:
        actions.append(_action("runtime", "CRITICAL", "RUNTIME_REVIEW", "Runtime truth requires attention.", sources["runtime_truth"]))
    for row in candidates["awaiting_decision"]:
        actions.append(_action(f"candidate-review:{row.get('candidate_id')}", "HIGH", "CANDIDATE_REVIEW", f"Candidate {row.get('candidate_id')} needs operator review.", sources["candidate_lifecycle"], required=True, command="npm run aegis:record-candidate-review -- --help"))
    for row in candidates["awaiting_outcome"]:
        actions.append(_action(f"candidate-outcome:{row.get('candidate_id')}", "HIGH", "CANDIDATE_OUTCOME", f"Candidate {row.get('candidate_id')} needs outcome update.", sources["candidate_lifecycle"], required=True, command="npm run aegis:update-candidate-outcomes"))
    for row in governance["awaiting_approval"]:
        actions.append(_action(f"governance:{row.get('recommendation_id')}", "MEDIUM", "GOVERNANCE_APPROVAL", f"Recommendation {row.get('recommendation_id')} awaits approval.", sources["intelligence_governance"], required=True, command="npm run aegis:record-governance-decision -- --help"))
    for row in sleeves["challenged"] + sleeves["watch"]:
        actions.append(_action(f"sleeve:{row.get('sleeve_id')}", "MEDIUM", "SLEEVE_REVIEW", f"Sleeve {row.get('sleeve_id')} is {row.get('recommendation')}.", sources["sleeve_challenger"], required=False, command="npm run aegis:sleeve-challenger"))
    for row in research["review_required"]:
        actions.append(_action(f"research:{row.get('research_task_id') or row.get('sleeve_review_candidate_id')}", "LOW", "RESEARCH_REVIEW", "Research item requires review.", sources["research_lab"], required=False, command="npm run aegis:research-lab-loop"))
    for row in missing:
        priority = "HIGH" if row.get("critical") else "LOW"
        actions.append(_action(f"missing:{row['source']}", priority, "MISSING_SOURCE", f"Refresh missing source artifact: {row['source']}.", sources[row["source"]], required=bool(row.get("critical"))))
    return sorted(actions, key=lambda row: (_priority(row["priority"]), row["type"], row["action_id"]))


def _action(action_id: str, priority: str, action_type: str, title: str, source: dict[str, Any], *, required: bool = True, command: str = "") -> dict[str, Any]:
    return {
        "action_id": action_id,
        "type": action_type,
        "priority": priority,
        "title": title,
        "reason": title,
        "source_artifact": source.get("path", ""),
        "source_hash": source.get("hash", ""),
        "required_by_operator": required,
        "suggested_command": command,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def _conflicts(*, payloads: dict[str, dict[str, Any]], candidates: dict[str, list[dict[str, Any]]], ranking_by_id: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    conflicts = []
    lifecycle_ids = {row.get("candidate_id") for rows in candidates.values() for row in rows}
    for candidate_id, rank in sorted(ranking_by_id.items()):
        if candidate_id not in lifecycle_ids:
            conflicts.append(_conflict("ranking_without_lifecycle", f"candidate:{candidate_id}", ["candidate_ranking", "candidate_lifecycle"], "candidate_lifecycle", "Candidate Lifecycle wins for candidate state."))
        else:
            lifecycle_row = next((row for rows in candidates.values() for row in rows if row.get("candidate_id") == candidate_id), {})
            rank_decision = rank.get("current_operator_decision")
            if rank_decision and lifecycle_row.get("current_operator_decision") and rank_decision != lifecycle_row.get("current_operator_decision"):
                conflicts.append(_conflict("candidate_decision_disagreement", f"candidate:{candidate_id}:current_operator_decision", ["candidate_ranking", "candidate_lifecycle"], "candidate_lifecycle", "Candidate Lifecycle wins for candidate state."))
    runtime = payloads.get("runtime_truth") or {}
    if runtime and runtime.get("broker_submit_required") is True:
        conflicts.append(_conflict("broker_policy_disagreement", "broker_submit_required", ["runtime_truth", "canonical_policy"], "runtime_truth", "Runtime Truth wins for permissions; canonical projection still cannot enable execution."))
    return conflicts


def _conflict(conflict_id: str, field: str, sources: list[str], chosen_source: str, reason: str) -> dict[str, Any]:
    return {"conflict_id": conflict_id, "field": field, "sources": sources, "chosen_source": chosen_source, "reason": reason, "operator_visibility": True}


def _warnings(*, runtime: dict[str, Any], missing: list[dict[str, Any]], sleeves: dict[str, list[dict[str, Any]]], conflicts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    warnings = []
    runtime_class = str(runtime.get("runtime_truth_classification") or "UNKNOWN")
    if runtime.get("status") != "AVAILABLE" or runtime_class in {"", "UNKNOWN", "BLOCKED", "MISSING"}:
        warnings.append({"warning_id": "runtime_unavailable", "priority": "CRITICAL", "status": runtime_class, "message": "Runtime truth is missing or blocked.", "source_artifact": ""})
    elif runtime_class == "PARTIAL_CONTEXT":
        warnings.append({"warning_id": "runtime_partial_context", "priority": "LOW", "status": runtime_class, "message": "Runtime evidence is partial; advisory and capture gates remain conservative.", "source_artifact": ""})
    for row in missing:
        warnings.append({"warning_id": f"missing:{row['source']}", "priority": "HIGH" if row.get("critical") else "LOW", "status": "MISSING", "message": f"Missing source artifact: {row['source']}", "source_artifact": ""})
    for row in sleeves.get("challenged", []):
        warnings.append({"warning_id": f"sleeve:{row.get('sleeve_id')}", "priority": "MEDIUM", "status": row.get("recommendation"), "message": f"Sleeve challenge: {row.get('sleeve_id')}", "source_artifact": ""})
    for row in conflicts:
        warnings.append({"warning_id": f"conflict:{row['conflict_id']}", "priority": "MEDIUM", "status": "CONFLICT", "message": row["reason"], "source_artifact": ""})
    return warnings


def _drilldown_index(sources: dict[str, dict[str, Any]], candidates: dict[str, list[dict[str, Any]]], top_candidates: list[dict[str, Any]], sleeves: dict[str, list[dict[str, Any]]], research: dict[str, list[dict[str, Any]]], governance: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    rows = [{"id": key, "type": "source_artifact", "path": row.get("path", ""), "hash": row.get("hash", "")} for key, row in sorted(sources.items()) if row.get("path")]
    rows.extend({"id": str(row.get("candidate_id")), "type": "candidate", "path": row.get("source_artifact", "")} for row in top_candidates)
    rows.extend({"id": str(row.get("sleeve_id")), "type": "sleeve", "path": sources.get("sleeve_challenger", {}).get("path", "")} for bucket in sleeves.values() for row in bucket)
    research_rows = []
    for key in ("captured_hypotheses", "priority_tasks", "new_tasks", "review_required", "completed", "rejected", "orphaned"):
        research_rows.extend(row for row in research.get(key, []) if isinstance(row, dict))
    rows.extend({"id": str(row.get("hypothesis_id") or row.get("research_task_id") or row.get("sleeve_review_candidate_id")), "type": "research", "path": row.get("source_artifact") or sources.get("research_lab", {}).get("path", ""), "hash": row.get("source_hash", "")} for row in research_rows)
    rows.extend({"id": str(row.get("recommendation_id")), "type": "governance", "path": sources.get("intelligence_governance", {}).get("path", "")} for bucket in governance.values() for row in bucket)
    return rows


def _no_action_now(actions: list[dict[str, Any]], top_candidates: list[dict[str, Any]], warnings: list[dict[str, Any]]) -> list[str]:
    if not actions and not top_candidates and not warnings:
        return ["No candidate, governance, research, or runtime action is required by the canonical projection."]
    out = []
    if not top_candidates:
        out.append("No ranked advisory candidates are available now.")
    return out


def _generated_at(payload: dict[str, Any]) -> str:
    for key in ("generated_at_utc", "generated_at", "timestamp_utc"):
        if payload.get(key):
            return str(payload[key])
    return ""


def _freshness(*, path: Path | None, generated_at: str, day_utc: str) -> str:
    if not path:
        return "MISSING"
    if not generated_at:
        return "UNKNOWN"
    return "CURRENT" if generated_at.startswith(day_utc) else "STALE"


def _deterministic_generated_at(sources: dict[str, dict[str, Any]], day_utc: str) -> str:
    values = sorted(str(row.get("generated_at") or "") for row in sources.values() if row.get("generated_at"))
    return values[-1] if values else f"{day_utc}T00:00:00Z"


def _file_hash(path: Path | None) -> str:
    if not path:
        return ""
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _priority(priority: str) -> int:
    return {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3, "INFO": 4}.get(str(priority), 9)
