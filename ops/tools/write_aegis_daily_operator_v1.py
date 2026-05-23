#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.intelligence_common_v1 import intelligence_summaries_v1, latest_json_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT, build_runtime_truth_kernel_v1, render_recovery_plan_v1, write_runtime_truth_kernel_reports_v1  # noqa: E402
from ops.tools.write_aegis_operational_readiness_v1 import build_operational_readiness_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="write_aegis_daily_operator_v1")
    parser.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--generated_at_utc", default="")
    args = parser.parse_args(argv)

    root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day)
    kernel = build_runtime_truth_kernel_v1(truth_root=root, day_utc=day, generated_at_utc=args.generated_at_utc or None)
    paths = write_runtime_truth_kernel_reports_v1(truth_root=root, payload=kernel)
    operational = build_operational_readiness_v1(kernel)
    payload = build_daily_operator_v1(kernel=kernel, operational=operational, kernel_paths=paths, truth_root=root, day_utc=day)

    out_dir = root / "reports" / "aegis_daily_operator_v1" / day
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "daily_operator.v1.json"
    summary_path = out_dir / "daily_operator.summary.txt"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(render_daily_operator_summary_v1(payload), encoding="utf-8")
    print(json.dumps({"path": str(json_path), "summary_path": str(summary_path), "broker_submit_required": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


def build_daily_operator_v1(*, kernel: dict[str, Any], operational: dict[str, Any], kernel_paths: dict[str, str], truth_root: Path, day_utc: str) -> dict[str, Any]:
    disabled = {
        "live_broker_trading": kernel.get("live_broker_trading_policy") or "DISABLED_BY_DESIGN",
        "autonomous_execution": kernel.get("autonomous_execution_policy") or "DISABLED_BY_DESIGN",
        "broker_submit_transmit": kernel.get("broker_submit_transmit_policy") or "DISABLED_BY_DESIGN",
    }
    manual = kernel.get("manual_capture_status") if isinstance(kernel.get("manual_capture_status"), dict) else {}
    commands = {
        "truth_kernel": "npm run aegis:truth-kernel",
        "readiness": "npm run aegis:readiness",
        "operational_readiness": "npm run aegis:operational-readiness",
        "recovery_plan": "npm run aegis:recovery-plan",
        "audit": "npm run aegis:audit",
        "replay_state": "npm run aegis:replay-state",
        "state_diff": "npm run aegis:state-diff",
        "capture_manual_trade": "npm run aegis:capture-manual-trade -- --symbol SPY --side BUY --quantity 10 --price 500 --trade-date YYYY-MM-DD --operator-attestation true",
        "record_candidate_decision": "npm run aegis:record-candidate-decision -- --candidate-id CANDIDATE_ID --decision TRADED_MANUALLY --intended-shares 25 --risk-bucket SMALL --operator OPERATOR --reason REVIEW_REASON",
        "correct_candidate_decision": "npm run aegis:correct-candidate-decision -- --candidate-id CANDIDATE_ID --field intended_shares --new-value 20 --operator OPERATOR --reason CORRECTION_REASON",
        "update_candidate_outcomes": "npm run aegis:update-candidate-outcomes",
        "candidate_ranking": "npm run aegis:candidate-ranking",
        "research_lab_loop": "npm run aegis:research-lab-loop",
        "sleeve_challenger": "npm run aegis:sleeve-challenger",
    }
    actions = _operator_actions(kernel)
    candidate_path, candidate_lifecycle = latest_json_v1(truth_root, "aegis_candidate_lifecycle_v1", day_utc, "candidate_lifecycle.v1.json")
    ranking_path, ranking = latest_json_v1(truth_root, "aegis_candidate_ranking_v1", day_utc, "candidate_ranking.v1.json")
    research_lab_path, research_lab = latest_json_v1(truth_root, "aegis_research_lab_execution_loop_v1", day_utc, "research_lab_execution_loop.v1.json")
    challenger_path, challenger = latest_json_v1(truth_root, "aegis_sleeve_challenger_v1", day_utc, "sleeve_challenger.v1.json")
    canonical_path, canonical = latest_json_v1(truth_root, "aegis_canonical_operator_state_v1", day_utc, "canonical_operator_state.v1.json")
    brief_path, brief = latest_json_v1(truth_root, "aegis_operator_brief_v1", day_utc, "operator_brief.v1.json")
    candidate_workflow = _candidate_workflow(candidate_lifecycle)
    return {
        "schema_id": "aegis_daily_operator",
        "schema_version": "v1",
        "artifact_id": "aegis_daily_operator_v1",
        "day_utc": day_utc,
        "generated_at_utc": kernel.get("generated_at_utc"),
        "truth_root": str(truth_root),
        "target_operating_mode": kernel.get("target_operating_mode"),
        "target_mode_ready": bool(kernel.get("human_approved_advisory_runtime_ready")),
        "runtime_truth_classification": kernel.get("runtime_truth_classification"),
        "highest_readiness_layer": kernel.get("highest_readiness_layer"),
        "advisory_status": kernel.get("advisory_status"),
        "advisory_state": kernel.get("advisory_state") or {},
        "manual_capture_available": bool(kernel.get("manual_trade_capture_allowed")),
        "manual_capture_status": manual,
        "manual_receipts_today": int(manual.get("manual_trade_receipt_count") or 0),
        "blocked_capabilities": kernel.get("blocked_capabilities") or [],
        "disabled_by_policy": disabled,
        "policy_disabled_capabilities": kernel.get("policy_disabled_capabilities") or [],
        "optional_not_required_capabilities": kernel.get("optional_not_required_capabilities") or [],
        "missing_or_stale_source_count": kernel.get("missing_or_stale_source_count"),
        "operator_action_required": bool(kernel.get("operator_action_required")),
        "operator_action_reason": kernel.get("operator_action_reason") or "",
        "operator_next_actions": actions,
        "candidate_workflow": candidate_workflow,
        "candidate_ranking_summary": {
            "status": "AVAILABLE" if ranking_path else "NOT_FOUND",
            "path": str(ranking_path or ""),
            "ranked_candidate_count": len(ranking.get("ranked_candidates") or []),
        },
        "research_lab_summary": {
            "status": "AVAILABLE" if research_lab_path else "NOT_FOUND",
            "path": str(research_lab_path or ""),
            "research_task_count": len(research_lab.get("research_tasks") or []),
        },
        "sleeve_challenger_summary": {
            "status": "AVAILABLE" if challenger_path else "NOT_FOUND",
            "path": str(challenger_path or ""),
            "challenge_count": len(challenger.get("challenges") or []),
        },
        "canonical_operator_state": {
            "status": "AVAILABLE" if canonical_path else "NOT_FOUND",
            "path": str(canonical_path or ""),
            "action_count": len(canonical.get("actions_required") or []),
        },
        "operator_brief": {
            "status": "AVAILABLE" if brief_path else "NOT_FOUND",
            "path": str(brief_path or ""),
            "action_count": len(brief.get("action_required") or []),
        },
        "commands_for_today": commands,
        "kernel_report_paths": kernel_paths,
        "recovery_plan_text": render_recovery_plan_v1(kernel),
        "operational_readiness": {
            "operational_completeness_percentage": operational.get("operational_completeness_percentage"),
            "safety_guarantees_preserved": operational.get("safety_guarantees_preserved") or {},
        },
        "intelligence_summaries": intelligence_summaries_v1(truth_root, day_utc),
        "safety": {
            "broker_submit_required": False,
            "autonomous_execution_allowed": False,
            "live_trade_ready": False,
            "manual_capture_is_journaling_only": True,
        },
    }


def render_daily_operator_summary_v1(payload: dict[str, Any]) -> str:
    disabled = payload.get("disabled_by_policy") if isinstance(payload.get("disabled_by_policy"), dict) else {}
    commands = payload.get("commands_for_today") if isinstance(payload.get("commands_for_today"), dict) else {}
    lines = [
        "AEGIS DAILY OPERATOR v1",
        f"day_utc: {payload.get('day_utc')}",
        f"target_mode_ready: {str(payload.get('target_mode_ready')).lower()}",
        f"target_operating_mode: {payload.get('target_operating_mode')}",
        f"highest_readiness_layer: {payload.get('highest_readiness_layer')}",
        f"advisory_status: {payload.get('advisory_status')}",
        f"manual_capture_available: {str(payload.get('manual_capture_available')).lower()}",
        f"manual_receipts_today: {payload.get('manual_receipts_today')}",
        f"blocked_capabilities: {', '.join(payload.get('blocked_capabilities') or []) or 'NONE'}",
        f"disabled_by_policy: live_broker_trading={disabled.get('live_broker_trading')} autonomous_execution={disabled.get('autonomous_execution')} broker_submit_transmit={disabled.get('broker_submit_transmit')}",
        f"operator_action_required: {str(payload.get('operator_action_required')).lower()}",
        f"operator_action_reason: {payload.get('operator_action_reason') or 'NONE'}",
        "",
        "operator_next_actions:",
    ]
    lines.extend(f"- {item}" for item in payload.get("operator_next_actions") or ["NONE"])
    workflow = payload.get("candidate_workflow") if isinstance(payload.get("candidate_workflow"), dict) else {}
    lines.extend(
        [
            "",
            "candidate_workflow:",
            f"- candidates_generated: {workflow.get('candidates_generated', 0)}",
            f"- awaiting_decision: {workflow.get('awaiting_decision', 0)}",
            f"- ignored_candidates: {workflow.get('ignored_candidates', 0)}",
            f"- candidates_with_decisions: {workflow.get('candidates_with_decisions', 0)}",
            f"- corrected_decisions: {workflow.get('corrected_decisions', 0)}",
            f"- latest_intended_shares: {workflow.get('latest_intended_shares') or 'NONE'}",
            f"- traded_awaiting_outcome: {workflow.get('traded_awaiting_outcome', 0)}",
            f"- deferred_candidates: {workflow.get('deferred_candidates', 0)}",
            f"- canonical_operator_state: {(payload.get('canonical_operator_state') or {}).get('status', 'UNKNOWN')}",
            f"- operator_brief: {(payload.get('operator_brief') or {}).get('status', 'UNKNOWN')}",
        ]
    )
    lines.extend(["", "commands_for_today:"])
    for key in sorted(commands):
        lines.append(f"- {key}: {commands[key]}")
    lines.extend(["", "intelligence_summaries:"])
    summaries = payload.get("intelligence_summaries") if isinstance(payload.get("intelligence_summaries"), dict) else {}
    for key in sorted(summaries):
        row = summaries[key] if isinstance(summaries[key], dict) else {}
        lines.append(f"- {key}: {row.get('status') or 'UNKNOWN'}")
    lines.append("")
    return "\n".join(lines)


def _operator_actions(kernel: dict[str, Any]) -> list[str]:
    actions = [
        "Review advisory state before taking any external action.",
        "If manually trading externally, capture the receipt with npm run aegis:capture-manual-trade.",
        "Run end-of-day audit with npm run aegis:audit.",
        "Do not expect Aegis to submit/transmit broker orders; broker execution is disabled by design.",
    ]
    if not bool(kernel.get("human_approved_advisory_runtime_ready")):
        actions.insert(0, "Resolve target-mode readiness blockers before relying on Monday advisory runtime.")
    if kernel.get("advisory_status") == "NO_ACTIONABLE_CANDIDATES":
        actions.insert(1, "No actionable candidates are available now; remain in review/monitor mode.")
    return actions


def _candidate_workflow(lifecycle: dict[str, Any]) -> dict[str, Any]:
    candidates = lifecycle.get("candidates") if isinstance(lifecycle.get("candidates"), list) else []
    awaiting = [row for row in candidates if row.get("operator_decision") in {None, "", "GENERATED", "REVIEWED"}]
    traded_pending = [
        row
        for row in candidates
        if row.get("operator_decision") == "TRADED_MANUALLY" and row.get("outcome_status") in {"OUTCOME_PENDING", "OUTCOME_UNKNOWN", None, ""}
    ]
    return {
        "status": "AVAILABLE" if candidates else "NO_CANDIDATES_FOUND",
        "candidates_generated": len(candidates),
        "awaiting_decision": len(awaiting),
        "candidates_with_decisions": sum(1 for row in candidates if row.get("current_operator_decision") not in {None, "", "GENERATED", "REVIEWED"}),
        "corrected_decisions": sum(1 for row in candidates if int(row.get("correction_count") or 0) > 0),
        "correction_history_available": any(row.get("audit_history") for row in candidates),
        "latest_intended_shares": next((row.get("current_intended_shares") for row in candidates if row.get("current_intended_shares") is not None), None),
        "latest_risk_bucket": next((row.get("current_risk_bucket") for row in candidates if row.get("current_risk_bucket")), ""),
        "ignored_candidates": sum(1 for row in candidates if row.get("operator_decision") == "IGNORED"),
        "traded_candidates": sum(1 for row in candidates if row.get("operator_decision") == "TRADED_MANUALLY"),
        "traded_awaiting_outcome": len(traded_pending),
        "deferred_candidates": sum(1 for row in candidates if row.get("operator_decision") == "DEFERRED"),
        "next_action": "Record decisions with npm run aegis:record-candidate-decision." if awaiting else "Update outcomes with npm run aegis:update-candidate-outcomes when evidence is available.",
    }


if __name__ == "__main__":
    raise SystemExit(main())
