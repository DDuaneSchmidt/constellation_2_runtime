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

from ops.aegis.intelligence_common_v1 import intelligence_summaries_v1, latest_json_v1, write_json_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT, build_runtime_truth_kernel_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="write_aegis_operator_inbox_v1")
    parser.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    truth_root = Path(args.truth_root).expanduser().resolve()
    payload = build_operator_inbox_v1(truth_root=truth_root, day_utc=str(args.day))
    out_dir = truth_root / "reports" / "aegis_operator_inbox_v1" / str(args.day)
    json_path = write_json_v1(out_dir / "operator_inbox.v1.json", payload)
    summary_path = out_dir / "operator_inbox.summary.txt"
    summary_path.write_text(render_operator_inbox_summary_v1(payload), encoding="utf-8")
    print(json.dumps({"path": str(json_path), "summary_path": str(summary_path), "attention_count": payload["attention_count"], "broker_submit_required": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


def build_operator_inbox_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    kernel = build_runtime_truth_kernel_v1(truth_root=truth_root, day_utc=day_utc)
    summaries = intelligence_summaries_v1(truth_root, day_utc)
    risk_path, risk = latest_json_v1(truth_root, "aegis_risk_governance_v1", day_utc, "risk_governance.v1.json")
    sleeve_path, sleeve = latest_json_v1(truth_root, "aegis_sleeve_attribution_v1", day_utc, "sleeve_attribution.v1.json")
    research_path, research = latest_json_v1(truth_root, "aegis_research_priorities_v1", day_utc, "prioritized_research_queue.v1.json")
    lifecycle_path, lifecycle = latest_json_v1(truth_root, "aegis_candidate_lifecycle_v1", day_utc, "candidate_lifecycle.v1.json")
    ranking_path, ranking = latest_json_v1(truth_root, "aegis_candidate_ranking_v1", day_utc, "candidate_ranking.v1.json")
    challenger_path, challenger = latest_json_v1(truth_root, "aegis_sleeve_challenger_v1", day_utc, "sleeve_challenger.v1.json")
    canonical_path, canonical = latest_json_v1(truth_root, "aegis_canonical_operator_state_v1", day_utc, "canonical_operator_state.v1.json")
    brief_path, brief = latest_json_v1(truth_root, "aegis_operator_brief_v1", day_utc, "operator_brief.v1.json")
    attention: list[dict[str, Any]] = []
    if not bool(kernel.get("human_approved_advisory_runtime_ready")):
        attention.append(_item("HIGH", "TARGET_MODE_NOT_READY", kernel.get("operator_action_reason") or "Target-mode readiness is false.", "npm run aegis:daily-operator"))
    if kernel.get("advisory_status") not in {"NO_ACTIONABLE_CANDIDATES", "ADVISORY_CANDIDATES_AVAILABLE"}:
        attention.append(_item("MEDIUM", "ADVISORY_REVIEW", f"Advisory status is {kernel.get('advisory_status')}.", "npm run aegis:truth-kernel"))
    if risk.get("risk_state") in {"ELEVATED", "BLOCKED", "UNKNOWN"}:
        attention.append(_item("HIGH" if risk.get("risk_state") == "BLOCKED" else "MEDIUM", "RISK_GOVERNANCE", f"Risk state is {risk.get('risk_state') or 'UNKNOWN'}.", "npm run aegis:risk-governance"))
    for row in sleeve.get("sleeves", []) if isinstance(sleeve.get("sleeves"), list) else []:
        if row.get("status") in {"WATCH", "DEGRADED", "SUSPEND_CANDIDATE", "UNKNOWN"}:
            attention.append(_item("MEDIUM", "SLEEVE_WARNING", f"{row.get('sleeve_id')}: {row.get('status')}", "npm run aegis:sleeve-attribution"))
    opp_count = int(research.get("opportunity_count") or len(research.get("prioritized_research_queue") or []))
    if opp_count:
        attention.append(_item("LOW", "RESEARCH_PRIORITIES_AVAILABLE", f"{opp_count} research priorities are available.", "npm run aegis:research-priorities"))
    candidate_workflow = _candidate_workflow(lifecycle)
    if candidate_workflow["awaiting_decision"]:
        attention.append(_item("HIGH", "CANDIDATES_AWAITING_DECISION", f"{candidate_workflow['awaiting_decision']} candidates need operator decision.", "npm run aegis:record-candidate-decision -- --help"))
    if candidate_workflow["corrected_decisions"]:
        attention.append(_item("INFO", "CANDIDATE_DECISION_CORRECTIONS", f"{candidate_workflow['corrected_decisions']} candidates have append-only corrections.", "npm run aegis:correct-candidate-decision -- --help"))
    if candidate_workflow["traded_awaiting_outcome"]:
        attention.append(_item("MEDIUM", "CANDIDATE_OUTCOMES_PENDING", f"{candidate_workflow['traded_awaiting_outcome']} traded candidates need outcome update.", "npm run aegis:update-candidate-outcomes"))
    manual = kernel.get("manual_capture_status") if isinstance(kernel.get("manual_capture_status"), dict) else {}
    return {
        "schema_id": "aegis_operator_inbox",
        "schema_version": "v1",
        "artifact_id": "aegis_operator_inbox_v1",
        "day_utc": day_utc,
        "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "truth_root": str(truth_root),
        "what_changed_since_last_run": (kernel.get("runtime_state_history") or {}).get("last_transition_summary") or "SEE_RUNTIME_STATE_TRANSITIONS",
        "operator_attention": attention,
        "attention_count": len(attention),
        "advisory_candidates": kernel.get("advisory_status") == "ADVISORY_CANDIDATES_AVAILABLE",
        "sleeve_warnings": [item for item in attention if item["topic"] == "SLEEVE_WARNING"],
        "research_priorities": {"status": "AVAILABLE" if opp_count else "NOT_FOUND", "count": opp_count, "path": str(research_path or "")},
        "risk_warnings": {"risk_state": risk.get("risk_state") or "UNKNOWN", "path": str(risk_path or "")},
        "manual_trades_to_capture": "UNKNOWN_OPERATOR_DEPENDENT",
        "manual_capture_available": bool(kernel.get("manual_trade_capture_allowed")),
        "manual_receipts_today": int(manual.get("manual_trade_receipt_count") or 0),
        "candidate_workflow": candidate_workflow,
        "candidate_ranking": {
            "status": "AVAILABLE" if ranking_path else "NOT_FOUND",
            "path": str(ranking_path or ""),
            "ranked_candidate_count": len(ranking.get("ranked_candidates") or []),
        },
        "sleeve_challenger": {
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
        "blocked": kernel.get("blocked_capabilities") or [],
        "disabled_by_policy": {
            "live_broker_trading": kernel.get("live_broker_trading_policy") or "DISABLED_BY_DESIGN",
            "autonomous_execution": kernel.get("autonomous_execution_policy") or "DISABLED_BY_DESIGN",
            "broker_submit_transmit": kernel.get("broker_submit_transmit_policy") or "DISABLED_BY_DESIGN",
        },
        "what_operator_should_do_next": _next_actions(kernel, bool(opp_count)),
        "intelligence_summaries": summaries,
        "source_paths": {
            "risk_governance": str(risk_path or ""),
            "sleeve_attribution": str(sleeve_path or ""),
            "research_priorities": str(research_path or ""),
            "candidate_lifecycle": str(lifecycle_path or ""),
            "candidate_ranking": str(ranking_path or ""),
            "sleeve_challenger": str(challenger_path or ""),
            "canonical_operator_state": str(canonical_path or ""),
            "operator_brief": str(brief_path or ""),
        },
        "safety": {"recommendations_are_advisory_only": True, "human_approval_required": True, "broker_submit_required": False, "autonomous_execution_allowed": False},
    }


def render_operator_inbox_summary_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS OPERATOR INBOX v1",
        f"day_utc: {payload.get('day_utc')}",
        f"attention_count: {payload.get('attention_count')}",
        f"advisory_candidates: {str(payload.get('advisory_candidates')).lower()}",
        f"manual_capture_available: {str(payload.get('manual_capture_available')).lower()}",
        f"manual_receipts_today: {payload.get('manual_receipts_today')}",
        f"candidate_awaiting_decision: {(payload.get('candidate_workflow') or {}).get('awaiting_decision', 0)}",
        f"candidate_decisions_recorded: {(payload.get('candidate_workflow') or {}).get('candidates_with_decisions', 0)}",
        f"corrected_decisions: {(payload.get('candidate_workflow') or {}).get('corrected_decisions', 0)}",
        f"traded_candidates_awaiting_outcome: {(payload.get('candidate_workflow') or {}).get('traded_awaiting_outcome', 0)}",
        f"canonical_operator_state: {(payload.get('canonical_operator_state') or {}).get('status', 'UNKNOWN')}",
        f"operator_brief: {(payload.get('operator_brief') or {}).get('status', 'UNKNOWN')}",
        f"risk_state: {(payload.get('risk_warnings') or {}).get('risk_state')}",
        "disabled_by_policy: live broker trading, autonomous execution, broker submit/transmit",
        "",
        "operator_attention:",
    ]
    for item in payload.get("operator_attention") or [{"priority": "INFO", "topic": "NONE", "message": "No operator attention items."}]:
        lines.append(f"- {item.get('priority')}: {item.get('topic')} - {item.get('message')}")
    lines.append("")
    lines.append("next_actions:")
    lines.extend(f"- {item}" for item in payload.get("what_operator_should_do_next") or ["NONE"])
    lines.append("")
    return "\n".join(lines)


def _item(priority: str, topic: str, message: str, command: str) -> dict[str, str]:
    return {"priority": priority, "topic": topic, "message": message, "command": command}


def _next_actions(kernel: dict[str, Any], has_research: bool) -> list[str]:
    actions = ["Review runtime truth before external trading decisions.", "Capture externally executed manual fills with npm run aegis:capture-manual-trade.", "Do not expect Aegis to submit/transmit broker orders."]
    if has_research:
        actions.insert(1, "Review research priorities with npm run aegis:research-priorities.")
    if not bool(kernel.get("human_approved_advisory_runtime_ready")):
        actions.insert(0, "Resolve target-mode readiness blockers before relying on advisory runtime.")
    return actions


def _candidate_workflow(lifecycle: dict[str, Any]) -> dict[str, int | str]:
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
    }


if __name__ == "__main__":
    raise SystemExit(main())
