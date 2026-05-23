#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.intelligence_common_v1 import ai_evidence_v1, latest_json_v1, now_utc_v1, read_json_v1, systemd_timer_inventory_v1
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT


REPORT_FAMILY = "aegis_high_roi_missing_items_review_v1"
VALID_CAPABILITY_STATUSES = {"CONFIRMED", "PARTIAL", "NOT_FOUND", "UNKNOWN"}
VALID_BACKLOG_STATUSES = {"MISSING", "PARTIAL", "WEAK", "CONFIRMED"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="write_aegis_high_roi_missing_items_review_v1")
    parser.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    truth_root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day)
    payload = build_high_roi_missing_items_review_v1(truth_root=truth_root, repo_root=REPO_ROOT, day_utc=day)
    paths = write_high_roi_missing_items_review_v1(truth_root=truth_root, day_utc=day, payload=payload)
    print(
        json.dumps(
            {
                "path": paths["json"],
                "summary_path": paths["summary"],
                "matrix_path": paths["matrix"],
                "backlog_path": paths["backlog"],
                "top_item": payload["highest_roi_missing_items"][0]["name"] if payload["highest_roi_missing_items"] else "NONE",
                "broker_execution_required": False,
                "autonomous_execution_required": False,
            },
            sort_keys=True,
        )
    )
    return 0


def build_high_roi_missing_items_review_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    repo = Path(repo_root).resolve()
    evidence = _evidence_inventory(root, repo, day_utc)
    categories = _review_categories(evidence)
    backlog = _prioritized_backlog(evidence)
    ai_items = [item for item in backlog if item["whether_ai_is_required"] is True or item["name"].lower().startswith("governed ai")]
    non_ai_items = [item for item in backlog if item not in ai_items]
    return {
        "schema_id": "aegis_high_roi_missing_items_review",
        "schema_version": "v1",
        "artifact_id": "aegis_high_roi_missing_items_review_v1",
        "day_utc": day_utc,
        "generated_at_utc": now_utc_v1(),
        "repo_root": str(repo),
        "truth_root": str(root),
        "review_perspective": "USER_OPERATOR",
        "operating_model": {
            "system_role": "Aegis identifies and explains manual trade candidates, challenges sleeves, routes research, and improves operator decision quality.",
            "human_role": "Human operator manually places trades in IB and captures receipts in Aegis.",
            "broker_submit_transmit": "DISABLED_BY_DESIGN",
            "autonomous_execution": "DISABLED_BY_DESIGN",
            "black_box_ai_trade_decisions": "NOT_REQUIRED_AND_NOT_RECOMMENDED",
        },
        "scoring_rubric": _scoring_rubric(),
        "input_evidence": evidence,
        "executive_summary": _executive_summary(evidence, backlog),
        "current_confirmed_strengths": _current_confirmed_strengths(evidence),
        "review_categories": categories,
        "highest_roi_missing_items": backlog,
        "highest_roi_ai_opportunities": ai_items,
        "highest_roi_non_ai_opportunities": non_ai_items,
        "user_workflow_gaps": _category_gaps(categories, "operator_workflow"),
        "sleeve_edge_improvement_gaps": _category_gaps(categories, "sleeve_improvement_loop"),
        "research_lab_gaps": _category_gaps(categories, "research_lab_edge_discovery"),
        "event_ad_hoc_sleeve_gaps": _category_gaps(categories, "regime_event_awareness"),
        "feedback_loop_gaps": _category_gaps(categories, "feedback_loops"),
        "what_not_to_build_next": [
            "broker submit/transmit",
            "autonomous execution",
            "black-box AI trade decisions",
            "unsupported performance claims",
            "more reports without a simpler operator cockpit",
        ],
        "recommended_next_1_day_packet": _next_packet(backlog, effort_allow={"SMALL"}, max_items=3),
        "recommended_next_1_week_packet": _next_packet(backlog, effort_allow={"SMALL", "MEDIUM"}, max_items=5),
        "recommended_next_1_month_roadmap": _next_packet(backlog, effort_allow={"SMALL", "MEDIUM", "LARGE"}, max_items=8),
        "safety_constraints": {
            "broker_execution_required": False,
            "autonomous_execution_required": False,
            "live_trading_required": False,
            "manual_capture_only": True,
        },
    }


def write_high_roi_missing_items_review_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "high_roi_missing_items_review.v1.json"
    summary_path = out_dir / "high_roi_missing_items_review.summary.txt"
    matrix_path = out_dir / "high_roi_missing_items_review.matrix.csv"
    backlog_path = out_dir / "high_roi_missing_items.prioritized_backlog.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(render_high_roi_missing_items_summary_v1(payload), encoding="utf-8")
    matrix_path.write_text(render_high_roi_missing_items_matrix_csv_v1(payload), encoding="utf-8")
    backlog_path.write_text(render_high_roi_missing_items_backlog_md_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path), "matrix": str(matrix_path), "backlog": str(backlog_path)}


def render_high_roi_missing_items_summary_v1(payload: dict[str, Any]) -> str:
    ai = payload["input_evidence"]["ai"]
    lines = [
        "AEGIS HIGH-ROI MISSING ITEMS REVIEW v1",
        f"day_utc: {payload['day_utc']}",
        "",
        "Executive summary",
        f"- {payload['executive_summary']['summary']}",
        f"- Live AI call path: {'CONFIRMED' if ai['live_ai_call_path_found'] else 'NOT_FOUND'}",
        "- Broker submit/transmit: DISABLED_BY_DESIGN and not recommended.",
        "- Autonomous execution: DISABLED_BY_DESIGN and not recommended.",
        "",
        "Current confirmed strengths",
    ]
    lines.extend(f"- {item}" for item in payload["current_confirmed_strengths"])
    lines.extend(["", "Highest-ROI missing or weak items"])
    for item in payload["highest_roi_missing_items"][:10]:
        lines.append(f"{item['rank']}. {item['name']} [{item['status']}] score={item['roi_priority_score']} impact={item['user_impact']} effort={item['implementation_effort']}")
        lines.append(f"   Why it matters: {item['why_it_matters']}")
        lines.append(f"   Recommended implementation: {item['recommended_implementation']}")
    lines.extend(["", "Highest-ROI AI opportunities"])
    for item in payload["highest_roi_ai_opportunities"][:5]:
        lines.append(f"- {item['name']}: {item['recommended_implementation']}")
    lines.extend(["", "Highest-ROI non-AI opportunities"])
    for item in payload["highest_roi_non_ai_opportunities"][:5]:
        lines.append(f"- {item['name']}: {item['recommended_implementation']}")
    lines.extend(["", "Recommended next 1-day packet"])
    lines.extend(f"- {item['name']}" for item in payload["recommended_next_1_day_packet"])
    lines.extend(["", "Recommended next 1-week packet"])
    lines.extend(f"- {item['name']}" for item in payload["recommended_next_1_week_packet"])
    lines.extend(["", "Recommended next 1-month roadmap"])
    lines.extend(f"- {item['name']}" for item in payload["recommended_next_1_month_roadmap"])
    lines.extend(
        [
            "",
            "Required limitations",
            "- This review does not claim AI is operational without a live AI call path.",
            "- This review does not recommend broker execution or autonomous trading.",
            "- This review does not claim investment returns or trading performance.",
            "",
        ]
    )
    return "\n".join(lines)


def render_high_roi_missing_items_matrix_csv_v1(payload: dict[str, Any]) -> str:
    buffer = StringIO()
    fields = [
        "rank",
        "name",
        "status",
        "user_impact",
        "trading_impact_potential",
        "compounding_value",
        "implementation_effort",
        "dependency_risk",
        "roi_priority_score",
        "whether_ai_is_required",
        "whether_human_approval_is_required",
    ]
    writer = csv.DictWriter(buffer, fieldnames=fields)
    writer.writeheader()
    for item in payload["highest_roi_missing_items"]:
        writer.writerow({field: item.get(field) for field in fields})
    return buffer.getvalue()


def render_high_roi_missing_items_backlog_md_v1(payload: dict[str, Any]) -> str:
    lines = [
        "# Aegis High-ROI Missing Items Prioritized Backlog",
        "",
        f"Day UTC: {payload['day_utc']}",
        "",
        "Scoring: `(user_impact * 3) + (trading_impact_potential * 3) + (compounding_value * 2) + effort_inverse + dependency_risk_inverse`.",
        "",
    ]
    for item in payload["highest_roi_missing_items"]:
        lines.extend(
            [
                f"## {item['rank']}. {item['name']}",
                "",
                f"- Status: {item['status']}",
                f"- ROI priority score: {item['roi_priority_score']}",
                f"- User impact: {item['user_impact']}",
                f"- Trading/returns impact potential: {item['trading_impact_potential']}",
                f"- Implementation effort: {item['implementation_effort']}",
                f"- Dependency risk: {item['dependency_risk']}",
                f"- AI required: {str(item['whether_ai_is_required']).lower()}",
                f"- Human approval required: {str(item['whether_human_approval_is_required']).lower()}",
                f"- Why it matters: {item['why_it_matters']}",
                f"- Evidence found: {'; '.join(item['evidence_found']) or 'NONE'}",
                f"- Evidence missing: {'; '.join(item['evidence_missing']) or 'NONE'}",
                f"- Recommended implementation: {item['recommended_implementation']}",
                f"- Validation command/test needed: {item['validation_command_test_needed']}",
                f"- Safety constraints: {'; '.join(item['safety_constraints'])}",
                "",
            ]
        )
    return "\n".join(lines)


def _evidence_inventory(root: Path, repo: Path, day_utc: str) -> dict[str, Any]:
    def ref(name: str, family: str, filename: str) -> dict[str, Any]:
        path, payload = latest_json_v1(root, family, day_utc, filename)
        return {"name": name, "status": "CONFIRMED" if path and payload else "NOT_FOUND", "path": str(path or ""), "payload": payload}

    package = read_json_v1(repo / "package.json")
    scripts = package.get("scripts") if isinstance(package.get("scripts"), dict) else {}
    ui_text = _read_text(repo / "constellation_2" / "phaseL" / "ui" / "server" / "run_ops_dashboard_v1.py") + "\n" + _read_text(repo / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "pages" / "index.js")
    refs = {
        "runtime_truth_kernel": ref("Runtime Truth Kernel", "aegis_runtime_truth_kernel_v1", "runtime_truth_kernel.v1.json"),
        "intelligence_governance_kernel": ref("Intelligence Governance Kernel", "aegis_intelligence_governance_kernel_v1", "intelligence_governance_kernel.v1.json"),
        "automation_ai_inventory": ref("Automation AI inventory", "aegis_automation_ai_inventory_v1", "automation_ai_inventory.v1.json"),
        "daily_operator": ref("Daily operator", "aegis_daily_operator_v1", "daily_operator.v1.json"),
        "operator_inbox": ref("Operator inbox", "aegis_operator_inbox_v1", "operator_inbox.v1.json"),
        "eod": ref("EOD intelligence", "aegis_eod_intelligence_v1", "eod_intelligence.v1.json"),
        "eow": ref("EOW intelligence", "aegis_eow_intelligence_v1", "eow_intelligence.v1.json"),
        "sleeve_performance": ref("Sleeve performance analytics", "aegis_sleeve_performance_analytics_v1", "sleeve_performance_analytics.v1.json"),
        "portfolio_attribution": ref("Portfolio attribution", "aegis_sleeve_performance_analytics_v1", "portfolio_attribution.v1.json"),
        "advisory_quality": ref("Advisory quality", "aegis_sleeve_performance_analytics_v1", "advisory_quality.v1.json"),
        "adaptive_governance": ref("Adaptive governance", "adaptive_governance_v1", "adaptive_governance.v1.json"),
        "research_queue": ref("Research queue optimizer", "research_queue_optimizer_v1", "research_queue_optimizer.v1.json"),
        "research_memory": ref("Research memory graph", "research_memory_graph_v1", "research_memory_graph.v1.json"),
        "regime_context": ref("Regime context", "regime_context_v1", "regime_context.v1.json"),
        "event_sleeve_audit": ref("Event sleeve activation audit", "aegis_event_sleeve_activation_audit_v1", "event_sleeve_activation_audit.v1.json"),
        "candidate_lineage": ref("Candidate lineage", "candidate_lineage_v1", "candidate_lineage.v1.json"),
        "manual_execution_receipt": ref("Manual execution receipt", "manual_execution_receipt_v1", "manual_execution_receipt.v1.json"),
        "sleeve_attribution": ref("Sleeve attribution", "aegis_sleeve_attribution_v1", "sleeve_attribution.v1.json"),
    }
    runtime = refs["runtime_truth_kernel"]["payload"]
    candidate_lineage = refs["candidate_lineage"]["payload"]
    performance = refs["sleeve_performance"]["payload"]
    advisory_quality = refs["advisory_quality"]["payload"].get("advisory_quality") or refs["sleeve_performance"]["payload"].get("advisory_quality") or {}
    ai = ai_evidence_v1(repo)
    return {
        "report_refs": {key: {k: v for k, v in row.items() if k != "payload"} for key, row in refs.items()},
        "runtime_truth": {
            "target_operating_mode": runtime.get("target_operating_mode") or "UNKNOWN",
            "runtime_truth_classification": runtime.get("runtime_truth_classification") or "UNKNOWN",
            "highest_readiness_layer": runtime.get("highest_readiness_layer") or "UNKNOWN",
            "trade_advice_allowed": bool(runtime.get("trade_advice_allowed")),
            "manual_trade_capture_allowed": bool(runtime.get("manual_trade_capture_allowed")),
            "blocked_capabilities": runtime.get("blocked_capabilities") or [],
        },
        "candidate_generation": {
            "candidate_lineage_status": refs["candidate_lineage"]["status"],
            "candidate_count": int(candidate_lineage.get("candidate_count") or 0),
            "trade_advice_allowed": bool(runtime.get("trade_advice_allowed")),
            "advisory_status": runtime.get("advisory_status") or "UNKNOWN",
        },
        "performance_measurement": {
            "engine_status": performance.get("performance_attribution_engine_status") or ("CONFIRMED" if refs["sleeve_performance"]["status"] == "CONFIRMED" else "NOT_FOUND"),
            "metric_count": len(performance.get("metrics") or []),
            "advisory_quality_status": refs["advisory_quality"]["status"],
            "recommendation_accuracy_status": (advisory_quality.get("recommendation_accuracy") or {}).get("metric_status") or "UNKNOWN",
            "false_positive_rate_status": (advisory_quality.get("false_positive_rate") or {}).get("metric_status") or "UNKNOWN",
            "realized_vs_advisory_gap_status": (advisory_quality.get("realized_vs_advisory_gap") or {}).get("metric_status") or "UNKNOWN",
        },
        "ai": {
            "live_ai_call_path_found": bool(ai["live_ai_call_path_found"]),
            "ai_used": bool(ai["ai_used"]),
            "deterministic_fallback": bool(ai["deterministic_fallback"]),
        },
        "scripts": {
            "aegis_scripts": sorted(name for name in scripts if str(name).startswith("aegis:")),
            "has_manual_capture": "aegis:capture-manual-trade" in scripts,
            "has_sleeve_performance": "aegis:sleeve-performance" in scripts,
            "has_research_queue": "aegis:research-queue" in scripts or "aegis:research-queue-optimizer" in scripts,
            "has_event_sleeve_audit": "aegis:event-sleeve-audit" in scripts,
            "has_record_intelligence_approval": "aegis:record-intelligence-approval" in scripts,
        },
        "ui_api": {
            "runtime_truth_page": "CONFIRMED" if "/aegis-runtime-truth" in ui_text else "NOT_FOUND",
            "adaptive_intelligence_page": "CONFIRMED" if "/aegis-adaptive-intelligence" in ui_text else "NOT_FOUND",
            "intelligence_governance_page": "CONFIRMED" if "/aegis-intelligence-governance" in ui_text else "NOT_FOUND",
            "approval_write_ui": "NOT_FOUND" if "record-intelligence-approval" not in ui_text else "PARTIAL",
            "manual_capture_form": "NOT_FOUND" if "capture-manual-trade" not in ui_text else "PARTIAL",
        },
        "systemd_timers": systemd_timer_inventory_v1(repo),
        "tests": {"aegis_test_files": sorted(str(path.relative_to(repo)) for path in (repo / "constellation_2" / "common" / "tests").glob("test_aegis_*.py"))},
    }


def _review_categories(evidence: dict[str, Any]) -> dict[str, Any]:
    candidate_count = evidence["candidate_generation"]["candidate_count"]
    perf = evidence["performance_measurement"]
    ai = evidence["ai"]
    ui = evidence["ui_api"]
    scripts = evidence["scripts"]
    return {
        "trade_candidate_quality": {
            "status": "PARTIAL" if evidence["candidate_generation"]["candidate_lineage_status"] == "CONFIRMED" else "NOT_FOUND",
            "findings": [
                _finding("Are sleeves actually generating actionable manual trade candidates?", "PARTIAL" if candidate_count > 0 else "PARTIAL", f"candidate_count={candidate_count}; advisory_status={evidence['candidate_generation']['advisory_status']}"),
                _finding("Are candidates scored/ranked/explained?", "UNKNOWN" if candidate_count == 0 else "PARTIAL", "Candidate lineage exists, but no current candidate rows prove ranking/explanation quality."),
                _finding("Are false positives tracked?", "PARTIAL" if perf["false_positive_rate_status"] in {"OK", "INSUFFICIENT_DATA"} else "NOT_FOUND", f"false_positive_rate_status={perf['false_positive_rate_status']}"),
                _finding("Is outcome tracked after manual capture?", "PARTIAL", "Manual receipt workflow exists; closed-loop candidate outcome history is not yet proven at scale."),
            ],
            "gaps": ["candidate ranking and explanation layer", "ignored candidate tracking", "candidate-to-outcome loop"],
        },
        "sleeve_improvement_loop": {
            "status": "PARTIAL",
            "findings": [
                _finding("Are sleeves continuously evaluated?", "PARTIAL", f"sleeve_performance_engine={perf['engine_status']}"),
                _finding("Are weak sleeves challenged?", "PARTIAL", "Adaptive governance and failure analysis exist; dedicated sleeve challenger proof is not found."),
                _finding("Are promotions/demotions human-approved?", "CONFIRMED", "Intelligence governance and adaptive governance require human approval."),
                _finding("Are sleeve failures routed into research tasks?", "PARTIAL", "Failure analysis and research queue exist; automatic routing proof is partial."),
            ],
            "gaps": ["dedicated sleeve challenger", "alternative-sleeve comparison", "failure-to-research task routing"],
        },
        "research_lab_edge_discovery": {
            "status": "PARTIAL",
            "findings": [
                _finding("Can the system ingest ideas?", "PARTIAL", "Research queue and memory artifacts exist; full operator idea UX is not proven here."),
                _finding("Can it backtest or validate hypotheses?", "PARTIAL", "Research task scaffolding exists; full backtest result lifecycle is not proven in the current report set."),
                _finding("Can AI challenge hypotheses?", "NOT_FOUND" if not ai["live_ai_call_path_found"] else "PARTIAL", f"live_ai_call_path_found={ai['live_ai_call_path_found']}"),
            ],
            "gaps": ["idea-to-backtest-to-result loop", "research promotion review packet", "AI-assisted hypothesis challenge"],
        },
        "ai_role": {
            "status": "PARTIAL",
            "findings": [
                _finding("Is there a live AI call path?", "NOT_FOUND" if not ai["live_ai_call_path_found"] else "CONFIRMED", f"live_ai_call_path_found={ai['live_ai_call_path_found']}"),
                _finding("Are AI outputs governed?", "CONFIRMED", "Intelligence Governance Kernel governs AI output boundaries."),
                _finding("Can AI summarize evidence into operator guidance?", "NOT_FOUND" if not ai["live_ai_call_path_found"] else "PARTIAL", "No live AI call path is proven."),
            ],
            "gaps": ["governed AI interpretation call path", "AI hypothesis challenger", "AI evidence summarizer"],
        },
        "operator_workflow": {
            "status": "PARTIAL",
            "findings": [
                _finding("Can the user see what to do today?", "CONFIRMED", "Daily operator report exists."),
                _finding("Can the user see candidate trades clearly?", "PARTIAL", "Runtime/adaptive pages exist; one candidate cockpit is not proven."),
                _finding("Can the user approve/reject recommendations in UI?", "NOT_FOUND" if ui["approval_write_ui"] == "NOT_FOUND" else "PARTIAL", f"approval_write_ui={ui['approval_write_ui']}"),
                _finding("Is there one cockpit?", "PARTIAL", "Multiple pages and reports exist; one unified cockpit is not proven."),
            ],
            "gaps": ["one unified operator cockpit", "approval/rejection UX", "manual capture UX"],
        },
        "performance_measurement": {
            "status": "CONFIRMED" if perf["engine_status"] == "FULLY_IMPLEMENTED" else "PARTIAL",
            "findings": [
                _finding("Are required metrics implemented?", "CONFIRMED" if perf["engine_status"] == "FULLY_IMPLEMENTED" else "PARTIAL", f"engine_status={perf['engine_status']}"),
                _finding("Are insufficient data cases handled?", "CONFIRMED", f"recommendation_accuracy_status={perf['recommendation_accuracy_status']}"),
                _finding("Are sleeve outcomes measured after trade capture?", "PARTIAL", "Manual capture and attribution reports exist; outcome samples are still insufficient."),
            ],
            "gaps": ["larger outcome sample", "ignored candidate measurement", "benchmark/regime slices with enough history"],
        },
        "regime_event_awareness": {
            "status": "PARTIAL",
            "findings": [
                _finding("Is 15-minute event monitor confirmed?", "PARTIAL" if evidence["scripts"]["has_event_sleeve_audit"] else "UNKNOWN", "Event sleeve audit command exists; current report classifies details."),
                _finding("Does event detection trigger ad hoc sleeve runs?", "UNKNOWN", "Ad hoc trigger proof must come from event-sleeve audit output."),
                _finding("Are sleeves evaluated by regime?", "PARTIAL", "Regime context and performance attribution fields exist; long-run regime performance is insufficient."),
            ],
            "gaps": ["confirmed ad hoc sleeve trigger path", "event-to-advisory marking", "event failure routing into research"],
        },
        "feedback_loops": {
            "status": "PARTIAL",
            "findings": [
                _finding("Generated candidates feed learning?", "PARTIAL", "Candidate lineage exists; ignored-candidate outcomes are not proven."),
                _finding("Manual trades feed learning?", "PARTIAL", "Manual receipts exist; sample depth is insufficient."),
                _finding("Approval/rejection feeds learning?", "PARTIAL", "Approval ledger CLI exists; operator UX and EOW use need proof."),
            ],
            "gaps": ["ignored candidate feedback", "approval decision analytics", "research outcome feedback"],
        },
        "auditability_and_trust": {
            "status": "CONFIRMED",
            "findings": [
                _finding("Can recommendations be traced?", "CONFIRMED", "Intelligence Governance evidence chains exist."),
                _finding("Can metrics be replayed?", "PARTIAL", "Metric formulas and input hashes exist; dedicated replay for every metric is not complete."),
                _finding("Can human approvals be recorded?", "CONFIRMED", "CLI approval ledger exists."),
            ],
            "gaps": ["metric-level replay command", "approval UX", "sleeve trust history explanation"],
        },
    }


def _prioritized_backlog(evidence: dict[str, Any]) -> list[dict[str, Any]]:
    ai_missing = not evidence["ai"]["live_ai_call_path_found"]
    raw = [
        _item("Closed-loop candidate outcome tracking, including ignored candidates", "PARTIAL", "HIGH", "HIGH", "HIGH", "MEDIUM", "MEDIUM", "Aegis cannot strongly improve candidate quality until generated, ignored, captured, and outcome states are connected.", ["candidate lineage", "manual receipt workflow", "performance attribution engine"], ["ignored candidate ledger", "candidate outcome labels", "candidate-to-receipt joins"], "Add candidate lifecycle table: generated, shown, ignored, manually traded, captured, outcome observed; feed advisory_quality metrics.", "pytest candidate lifecycle + npm run aegis:sleeve-performance", False, False),
        _item("One unified operator cockpit for daily manual trading workflow", "WEAK", "HIGH", "MEDIUM", "HIGH", "MEDIUM", "LOW", "The operator needs one place to see candidates, why/why-not, capture commands, changed-since-yesterday, and approval tasks.", ["daily operator report", "runtime truth UI", "adaptive intelligence UI"], ["single candidate cockpit", "approval UX", "manual capture UX"], "Create a read-only first pass that merges daily operator, candidate lineage, performance attribution, adaptive recommendations, and capture status.", "UI/API smoke test + npm run aegis:daily-operator", False, False),
        _item("Research Lab execution loop from idea to backtest to result to sleeve review", "PARTIAL", "HIGH", "HIGH", "HIGH", "LARGE", "MEDIUM", "The Research Lab is central to discovering better edges, but the current proof is stronger on queues/memory than validated results.", ["research queue", "research memory graph", "adaptive governance"], ["backtest result schema", "result scoring", "promotion review packet", "rejected idea evidence"], "Implement report-only research task runner contracts and result packets; require human promotion review.", "pytest research lifecycle + npm run aegis:research-queue", False, True),
        _item("Candidate ranking and explanation layer: why this trade, why now, why not", "PARTIAL", "HIGH", "HIGH", "MEDIUM", "MEDIUM", "MEDIUM", "Manual trading needs concise reasoning and rejection context, not only candidate existence.", ["candidate lineage artifact", "risk/runtime truth artifacts"], ["rank score", "why-now narrative", "why-not blockers", "evidence citations"], "Add candidate_explanation.v1.json with rank, evidence, risk, regime, sleeve reason, counterarguments, and do-not-trade reasons.", "pytest candidate explanation + audit handoff includes top candidates", False, False),
        _item("Governed AI call path for operator brief, hypothesis challenge, and failure interpretation", "MISSING" if ai_missing else "PARTIAL", "HIGH", "MEDIUM", "HIGH", "MEDIUM", "MEDIUM", "The highest-value AI role is synthesis and challenge over fixed evidence, not trade execution.", ["AI governance scaffolding", "deterministic fallback labels"], ["live AI provider call path", "prompt/output hashes", "citations", "human approval gate"], "Add optional AI interpreter that only consumes evidence chains and emits governed interpretations/recommendations.", "pytest AI governance + no AI facts/actions", True, True),
        _item("Sleeve challenger engine: why should this sleeve still exist?", "PARTIAL", "HIGH", "MEDIUM", "HIGH", "MEDIUM", "LOW", "Weak sleeves must be challenged before they pollute candidate quality.", ["sleeve performance analytics", "failure analysis", "adaptive governance"], ["explicit challenger report", "stale sleeve thresholds", "alternative sleeve comparison"], "Generate challenger reports that ask keep/watch/research/retire-review with evidence and human approval.", "npm run aegis:adaptive-governance + pytest challenger report", False, True),
        _item("Human approval/rejection UX for adaptive governance", "PARTIAL", "MEDIUM", "MEDIUM", "HIGH", "SMALL", "LOW", "The CLI ledger exists, but governance improves faster when approval/rejection is easy and reviewed weekly.", ["record-intelligence-approval CLI", "approval ledger"], ["safe UI workflow", "approval review page", "EOW approval analytics"], "Add read-only list plus guarded CLI examples first; only add write UI with CSRF/safety tests.", "pytest approval ledger + UI route no broker controls", False, True),
        _item("Event-driven ad hoc sleeve run confirmation and hardening", "PARTIAL", "MEDIUM", "MEDIUM", "MEDIUM", "MEDIUM", "MEDIUM", "Event-driven candidates need proven trigger lineage to avoid unclear why-now behavior.", ["event sleeve audit command", "event validity gate"], ["confirmed ad hoc trigger proof", "event-to-candidate link", "no-event no-action trace"], "Harden event sleeve audit to prove whether event monitor invokes advisory sleeve runs and records trigger lineage.", "npm run aegis:event-sleeve-audit + pytest no execution", False, False),
        _item("Regime memory connected directly to sleeve trust and research priorities", "PARTIAL", "MEDIUM", "MEDIUM", "HIGH", "MEDIUM", "MEDIUM", "Regime awareness only compounds when outcomes and sleeve trust are segmented by regime.", ["regime context", "research memory", "performance attribution regime fields"], ["enough regime-tagged outcomes", "regime trust deltas", "research tasks from regime failures"], "Persist regime-tagged candidate/outcome rows and feed them into sleeve trust and research queue scoring.", "pytest regime outcome linkage + npm run aegis:sleeve-performance", False, False),
        _item("AI-assisted edge discovery and duplicate/overfit detection", "MISSING" if ai_missing else "PARTIAL", "MEDIUM", "MEDIUM", "HIGH", "MEDIUM", "MEDIUM", "AI can help challenge hypotheses once facts and metrics are fixed, but cannot create proof.", ["research memory graph", "AI governance"], ["live AI path", "duplicate detection prompts", "overfit challenge reports"], "Add governed AI hypothesis challenger with citations to research memory and performance evidence.", "pytest no AI facts + research memory citations", True, True),
        _item("Operator behavior and discipline feedback from manual captures and overrides", "PARTIAL", "MEDIUM", "MEDIUM", "MEDIUM", "SMALL", "LOW", "Manual trading quality depends on whether the operator captures, overrides, or ignores recommendations consistently.", ["manual capture workflow", "daily operator report"], ["override reasons", "capture latency", "missed receipt warnings"], "Add operator_behavior_feedback.v1.json from captures, ignored candidates, late receipts, and overrides.", "pytest operator behavior report + daily operator summary", False, False),
        _item("Simplify reports into one actionable daily view", "WEAK", "HIGH", "LOW", "MEDIUM", "SMALL", "LOW", "Aegis has many reports; user value depends on a short daily decision surface.", ["daily operator", "operator inbox", "multiple UI pages"], ["one prioritized view", "hide low-value unknowns", "top candidate/research/approval actions"], "Merge daily operator, inbox, and top adaptive items into a concise operator cockpit summary.", "UI snapshot/smoke + npm run aegis:daily-operator", False, False),
    ]
    scored = [_score_item(item) for item in raw]
    scored.sort(key=lambda item: (-item["roi_priority_score"], item["implementation_effort"], item["name"]))
    for idx, item in enumerate(scored, 1):
        item["rank"] = idx
    return scored


def _item(
    name: str,
    status: str,
    user_impact: str,
    trading_impact: str,
    compounding: str,
    effort: str,
    risk: str,
    why: str,
    found: list[str],
    missing: list[str],
    implementation: str,
    validation: str,
    ai_required: bool,
    approval_required: bool,
) -> dict[str, Any]:
    return {
        "rank": 0,
        "name": name,
        "status": status,
        "user_impact": user_impact,
        "trading_impact_potential": trading_impact,
        "compounding_value": compounding,
        "implementation_effort": effort,
        "dependency_risk": risk,
        "why_it_matters": why,
        "evidence_found": found,
        "evidence_missing": missing,
        "recommended_implementation": implementation,
        "validation_command_test_needed": validation,
        "whether_ai_is_required": ai_required,
        "whether_human_approval_is_required": approval_required,
        "safety_constraints": ["no broker submit/transmit", "no autonomous execution", "human manual trade capture remains separate"],
    }


def _score_item(item: dict[str, Any]) -> dict[str, Any]:
    out = dict(item)
    out["roi_priority_score"] = (
        _impact_weight(item["user_impact"]) * 3
        + _impact_weight(item["trading_impact_potential"]) * 3
        + _impact_weight(item["compounding_value"]) * 2
        + _effort_inverse(item["implementation_effort"])
        + _risk_inverse(item["dependency_risk"])
    )
    return out


def _scoring_rubric() -> dict[str, Any]:
    return {
        "formula": "ROI_PRIORITY_SCORE = (user_impact_weight * 3) + (trading_impact_potential_weight * 3) + (compounding_value_weight * 2) + implementation_effort_inverse + dependency_risk_inverse",
        "impact_weights": {"HIGH": 3, "MEDIUM": 2, "LOW": 1, "UNKNOWN": 0},
        "implementation_effort_inverse": {"SMALL": 3, "MEDIUM": 2, "LARGE": 1},
        "dependency_risk_inverse": {"LOW": 3, "MEDIUM": 2, "HIGH": 1},
        "integer_scores_only": True,
    }


def _executive_summary(evidence: dict[str, Any], backlog: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "summary": "Aegis has strong governance scaffolding for a human-approved manual-trade advisory system, but the highest ROI gaps are candidate outcome closure, candidate explanation, research execution proof, one operator cockpit, and governed AI synthesis.",
        "top_missing_item": backlog[0]["name"] if backlog else "UNKNOWN",
        "ai_status": "NOT_FOUND" if not evidence["ai"]["live_ai_call_path_found"] else "CONFIRMED",
        "operator_model": "Aegis identifies and explains candidates; the human trades manually in IB and captures receipts.",
    }


def _current_confirmed_strengths(evidence: dict[str, Any]) -> list[str]:
    strengths = []
    if evidence["runtime_truth"]["runtime_truth_classification"] != "UNKNOWN":
        strengths.append("Runtime Truth Kernel governs operational truth and permissions.")
    if evidence["runtime_truth"]["manual_trade_capture_allowed"]:
        strengths.append("Manual trade capture is available as journaling/audit only.")
    if evidence["performance_measurement"]["engine_status"] == "FULLY_IMPLEMENTED":
        strengths.append("Performance Attribution Engine emits source-hashed, thresholded metric records.")
    if evidence["scripts"]["has_research_queue"]:
        strengths.append("Research queue infrastructure exists.")
    if evidence["ui_api"]["intelligence_governance_page"] == "CONFIRMED":
        strengths.append("Read-only intelligence governance UI/API exists.")
    strengths.append("Broker submit/transmit and autonomous execution remain disabled by design.")
    return strengths


def _category_gaps(categories: dict[str, Any], key: str) -> list[str]:
    row = categories.get(key) if isinstance(categories.get(key), dict) else {}
    return list(row.get("gaps") or [])


def _next_packet(backlog: list[dict[str, Any]], *, effort_allow: set[str], max_items: int) -> list[dict[str, Any]]:
    return [
        {"rank": item["rank"], "name": item["name"], "recommended_implementation": item["recommended_implementation"]}
        for item in backlog
        if item["implementation_effort"] in effort_allow
    ][:max_items]


def _finding(question: str, status: str, evidence: str) -> dict[str, str]:
    return {"question": question, "status": status if status in VALID_CAPABILITY_STATUSES else "UNKNOWN", "evidence": evidence}


def _impact_weight(value: str) -> int:
    return {"HIGH": 3, "MEDIUM": 2, "LOW": 1, "UNKNOWN": 0}.get(str(value), 0)


def _effort_inverse(value: str) -> int:
    return {"SMALL": 3, "MEDIUM": 2, "LARGE": 1}.get(str(value), 0)


def _risk_inverse(value: str) -> int:
    return {"LOW": 3, "MEDIUM": 2, "HIGH": 1}.get(str(value), 0)


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


if __name__ == "__main__":
    raise SystemExit(main())
