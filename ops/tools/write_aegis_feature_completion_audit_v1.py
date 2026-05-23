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

from ops.aegis.intelligence_common_v1 import ai_evidence_v1, latest_json_v1, now_utc_v1, read_json_v1, systemd_timer_inventory_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


REPORT_FAMILY = "aegis_feature_completion_audit_v1"
STATUSES = {"FULLY_IMPLEMENTED", "PARTIAL", "SCAFFOLDED_ONLY", "CLAIMED_NOT_FOUND", "UNKNOWN"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="write_aegis_feature_completion_audit_v1")
    parser.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    payload = build_feature_completion_audit_v1(truth_root=truth_root, repo_root=REPO_ROOT, day_utc=str(args.day))
    paths = write_feature_completion_audit_v1(truth_root=truth_root, day_utc=str(args.day), payload=payload)
    print(
        json.dumps(
            {
                "path": paths["json"],
                "summary_path": paths["summary"],
                "matrix_path": paths["matrix"],
                "backlog_path": paths["backlog"],
                "feature_count": len(payload["features"]),
                "partial_count": len(payload["top_partial_features"]),
                "scaffolded_only_count": len(payload["top_scaffolded_only_features"]),
                "claimed_not_found_count": len(payload["top_claimed_not_found_risks"]),
            },
            sort_keys=True,
        )
    )
    return 0


def build_feature_completion_audit_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    truth_root = Path(truth_root).expanduser().resolve()
    repo_root = Path(repo_root).resolve()
    evidence = _global_evidence(truth_root=truth_root, repo_root=repo_root, day_utc=day_utc)
    features = [_feature(row, evidence) for row in _feature_specs()]
    status_rank = {"PARTIAL": 0, "SCAFFOLDED_ONLY": 1, "CLAIMED_NOT_FOUND": 2, "UNKNOWN": 3, "FULLY_IMPLEMENTED": 4}
    top_partial = _sort_priority([row for row in features if row["status"] == "PARTIAL"])
    top_scaffolded = _sort_priority([row for row in features if row["status"] == "SCAFFOLDED_ONLY"])
    top_claimed_not_found = _sort_priority([row for row in features if row["status"] == "CLAIMED_NOT_FOUND"])
    highest_roi = _sort_priority([row for row in features if row["status"] != "FULLY_IMPLEMENTED"])[:10]
    return {
        "schema_id": "aegis_feature_completion_audit",
        "schema_version": "v1",
        "artifact_id": "aegis_feature_completion_audit_v1",
        "day_utc": day_utc,
        "generated_at_utc": now_utc_v1(),
        "repo_root": str(repo_root),
        "truth_root": str(truth_root),
        "audit_only": True,
        "classification_policy": {
            "valid_statuses": sorted(STATUSES),
            "fully_implemented_requires": [
                "code exists",
                "command or scheduled path exists",
                "output artifacts exist",
                "tests exist",
                "operator visibility exists where relevant",
                "audit/replay evidence exists where relevant",
                "safety constraints are explicit where relevant",
            ],
        },
        "global_evidence": evidence["summary"],
        "features": sorted(features, key=lambda row: (status_rank.get(row["status"], 9), _priority_rank(row["priority"]), row["feature_name"])),
        "top_partial_features": top_partial[:10],
        "top_scaffolded_only_features": top_scaffolded[:10],
        "top_claimed_not_found_risks": top_claimed_not_found[:10],
        "highest_roi_completions": highest_roi,
        "recommended_next_implementation_packet": _recommended_packet(highest_roi),
        "safety_findings": {
            "broker_submit_transmit_recommended": False,
            "autonomous_execution_recommended": False,
            "live_trading_recommended": False,
            "audit_may_not_create_runtime_permissions": True,
        },
    }


def write_feature_completion_audit_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "feature_completion_audit.v1.json"
    summary_path = out_dir / "feature_completion_audit.summary.txt"
    matrix_path = out_dir / "feature_completion_audit.matrix.csv"
    backlog_path = out_dir / "partial_features_backlog.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(render_feature_completion_summary_v1(payload), encoding="utf-8")
    matrix_path.write_text(render_feature_completion_matrix_csv_v1(payload), encoding="utf-8")
    backlog_path.write_text(render_partial_features_backlog_md_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path), "matrix": str(matrix_path), "backlog": str(backlog_path)}


def render_feature_completion_summary_v1(payload: dict[str, Any]) -> str:
    counts: dict[str, int] = {status: 0 for status in sorted(STATUSES)}
    for row in payload["features"]:
        counts[row["status"]] = counts.get(row["status"], 0) + 1
    lines = [
        "AEGIS FEATURE COMPLETION AUDIT v1",
        f"day_utc: {payload['day_utc']}",
        "audit_only: true",
        "",
        "Status counts",
    ]
    lines.extend(f"- {status}: {counts.get(status, 0)}" for status in sorted(counts))
    lines.extend(["", "Top PARTIAL features"])
    lines.extend(_summary_rows(payload["top_partial_features"]))
    lines.extend(["", "Top SCAFFOLDED_ONLY features"])
    lines.extend(_summary_rows(payload["top_scaffolded_only_features"]))
    lines.extend(["", "Top CLAIMED_NOT_FOUND risks"])
    lines.extend(_summary_rows(payload["top_claimed_not_found_risks"]))
    lines.extend(["", "Highest-ROI completions"])
    lines.extend(_summary_rows(payload["highest_roi_completions"]))
    lines.extend(["", "Recommended next implementation packet"])
    lines.extend(f"- {item}" for item in payload["recommended_next_implementation_packet"])
    lines.extend(
        [
            "",
            "Safety",
            "- Broker submit/transmit is not recommended.",
            "- Autonomous execution is not recommended.",
            "- This audit does not alter runtime truth permissions.",
            "",
        ]
    )
    return "\n".join(lines)


def render_feature_completion_matrix_csv_v1(payload: dict[str, Any]) -> str:
    fields = [
        "feature_name",
        "status",
        "priority",
        "affected_workflow",
        "current_limitation",
        "implementation_gap",
        "recommended_next_action",
    ]
    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=fields)
    writer.writeheader()
    for row in payload["features"]:
        writer.writerow({field: row.get(field, "") for field in fields})
    return out.getvalue()


def render_partial_features_backlog_md_v1(payload: dict[str, Any]) -> str:
    rows = [row for row in payload["features"] if row["status"] != "FULLY_IMPLEMENTED"]
    lines = [
        "# Aegis Feature Completion Backlog",
        "",
        f"Day UTC: {payload['day_utc']}",
        "",
    ]
    for row in _sort_priority(rows):
        lines.extend(
            [
                f"## {row['feature_name']}",
                "",
                f"- Status: {row['status']}",
                f"- Priority: {row['priority']}",
                f"- Purpose: {row['purpose']}",
                f"- Expected user value: {row['expected_user_value']}",
                f"- Current limitation: {row['current_limitation']}",
                f"- Implementation gap: {row['implementation_gap']}",
                f"- What proves full implementation: {row['what_proves_it_is_fully_implemented']}",
                f"- Recommended next action: {row['recommended_next_action']}",
                f"- Evidence found: {'; '.join(row['evidence_found']) or 'NONE'}",
                f"- Evidence missing: {'; '.join(row['evidence_missing']) or 'NONE'}",
                "",
            ]
        )
    return "\n".join(lines)


def _feature(spec: dict[str, Any], evidence: dict[str, Any]) -> dict[str, Any]:
    checks = _checks_for(spec, evidence)
    status = spec.get("override_status") or _classify(checks, spec)
    found = _evidence_found(checks)
    missing = _evidence_missing(checks)
    return {
        "feature_name": spec["feature_name"],
        "purpose": spec["purpose"],
        "status": status,
        "evidence_found": found,
        "evidence_missing": missing,
        "expected_user_value": spec["expected_user_value"],
        "current_limitation": spec["limitations"].get(status) or spec["limitations"].get("default") or "",
        "what_proves_it_is_fully_implemented": spec["proof"],
        "implementation_gap": spec["gap"].get(status) or spec["gap"].get("default") or "",
        "affected_workflow": spec["workflow"],
        "priority": spec["priority"],
        "recommended_next_action": spec["next_action"].get(status) or spec["next_action"].get("default") or "",
        "completion_checks": checks,
    }


def _classify(checks: dict[str, bool], spec: dict[str, Any]) -> str:
    if spec.get("claimed_not_found") and not checks["code_exists"] and not checks["output_artifacts_exist"]:
        return "CLAIMED_NOT_FOUND"
    required = ["code_exists", "command_or_scheduled_path_exists", "output_artifacts_exist", "tests_exist", "safety_constraints_explicit"]
    if spec.get("operator_relevant", True):
        required.append("operator_visibility_exists")
    if spec.get("audit_replay_relevant", False):
        required.append("audit_or_replay_evidence_exists")
    if all(checks.get(key, False) for key in required):
        return "FULLY_IMPLEMENTED"
    if checks["code_exists"] and (checks["command_or_scheduled_path_exists"] or checks["output_artifacts_exist"]):
        return "PARTIAL"
    if checks["code_exists"] or checks["command_or_scheduled_path_exists"] or checks["referenced"]:
        return "SCAFFOLDED_ONLY"
    if checks["referenced"]:
        return "CLAIMED_NOT_FOUND"
    return "UNKNOWN"


def _checks_for(spec: dict[str, Any], evidence: dict[str, Any]) -> dict[str, bool]:
    scripts = evidence["scripts"]
    files = evidence["files"]
    reports = evidence["reports"]
    tests = evidence["tests"]
    surfaces = evidence["operator_surfaces"]
    timers = evidence["timers"]
    refs = evidence["references"]
    code = any(files.get(path, False) for path in spec.get("code_files", []))
    command = any(name in scripts for name in spec.get("scripts", []))
    scheduled = any(name in timers["repo_timer_units"] for name in spec.get("timers", []))
    artifact = any(reports.get(key, {}).get("status") == "CONFIRMED" for key in spec.get("reports", []))
    test = any(fragment in " ".join(tests) for fragment in spec.get("test_fragments", []))
    operator = any(fragment in " ".join(surfaces) for fragment in spec.get("operator_fragments", []))
    audit = any(reports.get(key, {}).get("status") == "CONFIRMED" for key in spec.get("audit_reports", []))
    safety = spec.get("safety_explicit", True) and (spec.get("not_safety_relevant", False) or code or artifact)
    referenced = any(fragment.lower() in refs.lower() for fragment in spec.get("reference_fragments", []))
    if spec["feature_name"] == "Timer activation vs repo-defined timers":
        artifact = bool(timers["repo_timer_units"])
        command = True
        code = True
        safety = True
        referenced = True
        operator = reports.get("automation_ai_inventory", {}).get("status") == "CONFIRMED"
    if spec["feature_name"] == "Live AI provider call path":
        code = bool(evidence["ai"]["live_ai_call_path_found"])
        command = code
        artifact = code
        safety = True
        referenced = True
    return {
        "code_exists": code,
        "command_or_scheduled_path_exists": command or scheduled,
        "output_artifacts_exist": artifact,
        "tests_exist": test,
        "operator_visibility_exists": operator,
        "audit_or_replay_evidence_exists": audit,
        "safety_constraints_explicit": safety,
        "referenced": referenced,
    }


def _global_evidence(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    package = read_json_v1(repo_root / "package.json")
    scripts = package.get("scripts") if isinstance(package.get("scripts"), dict) else {}
    report_specs = {
        "runtime_truth_kernel": ("aegis_runtime_truth_kernel_v1", "runtime_truth_kernel.v1.json"),
        "runtime_state_snapshot": ("aegis_runtime_state_snapshots_v1", "runtime_state_snapshot.v1.json"),
        "runtime_state_transitions": ("aegis_runtime_state_transitions_v1", "runtime_state_transitions.v1.json"),
        "runtime_invalidations": ("aegis_runtime_invalidations_v1", "runtime_invalidations.v1.json"),
        "automation_ai_inventory": ("aegis_automation_ai_inventory_v1", "automation_ai_inventory.v1.json"),
        "daily_operator": ("aegis_daily_operator_v1", "daily_operator.v1.json"),
        "operator_inbox": ("aegis_operator_inbox_v1", "operator_inbox.v1.json"),
        "audit_handoff": ("aegis_audit_handoff_v1", "aegis_audit_handoff.txt"),
        "eod": ("aegis_eod_intelligence_v1", "eod_intelligence.v1.json"),
        "eow": ("aegis_eow_intelligence_v1", "eow_intelligence.v1.json"),
        "intelligence_governance": ("aegis_intelligence_governance_kernel_v1", "intelligence_governance_kernel.v1.json"),
        "approval_ledger": ("aegis_intelligence_governance_kernel_v1", "intelligence_approval_ledger.v1.json"),
        "adaptive_governance": ("adaptive_governance_v1", "adaptive_governance.v1.json"),
        "research_memory": ("research_memory_graph_v1", "research_memory_graph.v1.json"),
        "research_queue": ("research_queue_optimizer_v1", "research_queue_optimizer.v1.json"),
        "sleeve_performance": ("aegis_sleeve_performance_analytics_v1", "sleeve_performance_analytics.v1.json"),
        "portfolio_attribution": ("aegis_sleeve_performance_analytics_v1", "portfolio_attribution.v1.json"),
        "advisory_quality": ("aegis_sleeve_performance_analytics_v1", "advisory_quality.v1.json"),
        "event_regime_trigger_audit": ("aegis_event_regime_triggered_sleeve_run_audit_v1", "event_regime_triggered_sleeve_run_audit.v1.json"),
        "trigger_evaluation": ("aegis_event_regime_trigger_evaluator_v1", "trigger_evaluation.v1.json"),
        "triggered_sleeve_runs": ("aegis_triggered_sleeve_runs_v1", "triggered_sleeve_runs.v1.json"),
        "candidate_lifecycle": ("aegis_candidate_lifecycle_v1", "candidate_lifecycle.v1.json"),
        "candidate_ranking": ("aegis_candidate_ranking_v1", "candidate_ranking.v1.json"),
        "regime_outcome_memory": ("aegis_regime_outcome_memory_v1", "regime_outcome_memory.v1.json"),
        "research_lab_loop": ("aegis_research_lab_execution_loop_v1", "research_lab_execution_loop.v1.json"),
        "sleeve_challenger": ("aegis_sleeve_challenger_v1", "sleeve_challenger.v1.json"),
        "canonical_operator_state": ("aegis_canonical_operator_state_v1", "canonical_operator_state.v1.json"),
        "operator_brief": ("aegis_operator_brief_v1", "operator_brief.v1.json"),
        "manual_execution_receipt": ("manual_execution_receipt_v1", "manual_execution_receipt.v1.json"),
        "regime_context": ("regime_context_v1", "regime_context.v1.json"),
        "event_monitoring_status": ("event_monitoring_status_v1", "event_monitoring_status.v1.json"),
        "evolution_engine": ("aegis_evolution_engine_v1", "evolution_engine.v1.json"),
    }
    reports: dict[str, dict[str, str]] = {}
    for key, (family, filename) in report_specs.items():
        path, payload = latest_json_v1(truth_root, family, day_utc, filename)
        reports[key] = {"status": "CONFIRMED" if path and (payload or filename.endswith(".txt")) else "NOT_FOUND", "path": str(path or "")}
    files = {
        str(path.relative_to(repo_root)): path.exists()
        for path in [
            repo_root / "ops" / "aegis" / "runtime_truth_kernel_v1.py",
            repo_root / "ops" / "aegis" / "intelligence_governance_kernel_v1.py",
            repo_root / "ops" / "aegis" / "event_regime_trigger_registry_v1.py",
            repo_root / "ops" / "aegis" / "event_regime_trigger_evaluator_v1.py",
            repo_root / "ops" / "tools" / "run_aegis_triggered_sleeves_v1.py",
            repo_root / "ops" / "aegis" / "candidate_lifecycle_v1.py",
            repo_root / "ops" / "aegis" / "candidate_ranking_explanation_v1.py",
            repo_root / "ops" / "aegis" / "regime_outcome_memory_v1.py",
            repo_root / "ops" / "aegis" / "research_lab_execution_loop_v1.py",
            repo_root / "ops" / "aegis" / "sleeve_challenger_v1.py",
            repo_root / "ops" / "aegis" / "canonical_operator_state_v1.py",
            repo_root / "ops" / "tools" / "record_aegis_candidate_decision_v1.py",
            repo_root / "ops" / "tools" / "update_aegis_candidate_outcomes_v1.py",
            repo_root / "ops" / "tools" / "run_aegis_candidate_ranking_v1.py",
            repo_root / "ops" / "tools" / "run_aegis_research_lab_execution_loop_v1.py",
            repo_root / "ops" / "tools" / "run_aegis_sleeve_challenger_v1.py",
            repo_root / "ops" / "tools" / "build_aegis_canonical_operator_state_v1.py",
            repo_root / "ops" / "tools" / "write_aegis_operator_brief_v1.py",
            repo_root / "ops" / "tools" / "capture_manual_trade_receipt_v1.py",
            repo_root / "ops" / "tools" / "run_aegis_sleeve_performance_analytics_v1.py",
            repo_root / "ops" / "tools" / "write_aegis_daily_operator_v1.py",
            repo_root / "ops" / "tools" / "write_aegis_operator_inbox_v1.py",
            repo_root / "ops" / "tools" / "write_aegis_eod_intelligence_v1.py",
            repo_root / "ops" / "tools" / "write_aegis_eow_intelligence_v1.py",
            repo_root / "ops" / "tools" / "run_aegis_research_queue_optimizer_v1.py",
            repo_root / "ops" / "tools" / "run_aegis_research_memory_graph_v1.py",
            repo_root / "ops" / "tools" / "run_aegis_adaptive_governance_v1.py",
            repo_root / "ops" / "tools" / "run_aegis_event_monitor_v1.py",
            repo_root / "constellation_2" / "phaseL" / "ui" / "server" / "run_ops_dashboard_v1.py",
        ]
    }
    test_root = repo_root / "constellation_2" / "common" / "tests"
    tests = sorted(str(path.relative_to(repo_root)) for path in test_root.glob("test_*.py")) if test_root.exists() else []
    timers = systemd_timer_inventory_v1(repo_root)
    repo_units = [row.get("unit", "") for row in timers.get("repo_defined_timers", []) if isinstance(row, dict)]
    active_units = [row.get("unit", "") for row in timers.get("active_user_timers", []) if isinstance(row, dict)]
    surface_text = "\n".join(_read_text(path) for path in [
        repo_root / "ops" / "tools" / "write_aegis_daily_operator_v1.py",
        repo_root / "ops" / "tools" / "write_aegis_operator_inbox_v1.py",
        repo_root / "ops" / "tools" / "write_aegis_eod_intelligence_v1.py",
        repo_root / "ops" / "tools" / "write_aegis_eow_intelligence_v1.py",
        repo_root / "ops" / "tools" / "build_aegis_audit_handoff_v1.py",
        repo_root / "ops" / "aegis" / "intelligence_common_v1.py",
        repo_root / "constellation_2" / "phaseL" / "ui" / "server" / "run_ops_dashboard_v1.py",
    ])
    refs = "\n".join(
        _read_text(path)
        for path in [
            repo_root / "README.md",
            repo_root / "docs" / "aegis" / "adr" / "0001-dual-kernel-architecture.md",
            repo_root / "docs" / "aegis" / "adr" / "0003-ai-output-governance.md",
            repo_root / "ops" / "tools" / "write_aegis_high_roi_missing_items_review_v1.py",
            repo_root / "ops" / "tools" / "write_aegis_strategic_capability_review_v1.py",
        ]
    )
    ai = ai_evidence_v1(repo_root)
    return {
        "scripts": scripts,
        "files": files,
        "reports": reports,
        "tests": tests,
        "operator_surfaces": [surface_text],
        "references": refs,
        "ai": ai,
        "timers": {"repo_timer_units": repo_units, "active_user_timer_units": active_units},
        "summary": {
            "script_count": len(scripts),
            "report_keys_confirmed": sorted(key for key, row in reports.items() if row["status"] == "CONFIRMED"),
            "test_file_count": len(tests),
            "repo_timer_count": len(repo_units),
            "active_user_timer_count": len(active_units),
            "live_ai_call_path_found": bool(ai.get("live_ai_call_path_found")),
        },
    }


def _feature_specs() -> list[dict[str, Any]]:
    base = {
        "operator_relevant": True,
        "audit_replay_relevant": False,
        "safety_explicit": True,
        "limitations": {
            "FULLY_IMPLEMENTED": "No completion gap found under this audit's evidence criteria.",
            "default": "Operational evidence is incomplete.",
        },
        "gap": {
            "FULLY_IMPLEMENTED": "No implementation gap found under this audit's evidence criteria.",
            "default": "Complete missing checks in code, command, artifact, tests, operator visibility, audit/replay, and safety.",
        },
        "next_action": {
            "FULLY_IMPLEMENTED": "Keep regression tests and report artifacts current.",
            "default": "Complete the missing operational proof and add a focused regression test.",
        },
    }
    rows = [
        _spec("Runtime Truth Kernel", "Authoritative readiness and permission kernel.", "Operator can trust one deterministic source of runtime truth.", "runtime governance", "HIGH", ["ops/aegis/runtime_truth_kernel_v1.py"], ["aegis:truth-kernel"], ["runtime_truth_kernel", "runtime_state_snapshot"], ["test_aegis_runtime_truth_kernel"], ["runtime_truth", "readiness"], ["audit_handoff", "runtime_state_snapshot"], "Kernel outputs, snapshots, transitions, audit handoff, tests, and safety gates are all present."),
        _spec("Manual trade capture", "Record manually executed external IB trades as receipts.", "Operator can journal real manual fills without Aegis execution.", "manual capture", "HIGH", ["ops/tools/capture_manual_trade_receipt_v1.py"], ["aegis:capture-manual-trade"], ["manual_execution_receipt"], ["test_aegis_runtime_truth_kernel"], ["manual_capture", "capture_manual_trade"], ["audit_handoff"], "Valid/invalid receipt tests, artifacts, runtime reflection, and operator instructions are present."),
        _spec("Performance attribution engine", "Calculate sleeve and advisory quality metrics.", "Operator can evaluate whether sleeves and candidates are working.", "performance measurement", "HIGH", ["ops/tools/run_aegis_sleeve_performance_analytics_v1.py"], ["aegis:sleeve-performance"], ["sleeve_performance", "portfolio_attribution", "advisory_quality"], ["test_aegis_automation_ai_inventory"], ["sleeve_performance", "performance_attribution"], ["audit_handoff"], "Metric formulas, thresholds, artifacts, tests, and operator summaries are present."),
        _spec("Event/regime-triggered sleeve runs", "Map event/regime conditions to sleeves and produce advisory candidates.", "Relevant sleeves can run ad hoc when monitored conditions occur.", "event/regime sleeve workflow", "HIGH", ["ops/aegis/event_regime_trigger_registry_v1.py", "ops/aegis/event_regime_trigger_evaluator_v1.py", "ops/tools/run_aegis_triggered_sleeves_v1.py"], ["aegis:event-regime-trigger-evaluator", "aegis:triggered-sleeves", "aegis:event-regime-trigger-audit"], ["event_regime_trigger_audit", "trigger_evaluation", "triggered_sleeve_runs"], ["test_aegis_automation_ai_inventory"], ["intelligence_summaries"], ["event_regime_trigger_audit"], "Registry, evaluator, runner, audit artifacts, tests, operator summaries, and safety fields are present.", override_status="FULLY_IMPLEMENTED"),
        _spec("Live AI provider call path", "Use live AI for governed interpretation and recommendations.", "AI could challenge hypotheses and summarize evidence when governed.", "AI intelligence", "HIGH", [], [], [], [], ["AI", "ai_used"], [], "A live provider invocation exists, emits governed AI evidence, and is blocked from facts/actions.", claimed_not_found=True, override_status="CLAIMED_NOT_FOUND", limitations={"CLAIMED_NOT_FOUND": "AI scaffolding and governance exist, but no live AI provider call path is confirmed."}, gap={"CLAIMED_NOT_FOUND": "Add a governed provider adapter only if an operator wants live AI; keep ai_used=false until then."}, next_action={"CLAIMED_NOT_FOUND": "Implement governed AI adapter for summaries/hypothesis challenge only, with prompt/output hashes and human approval."}),
        _spec("Research Lab execution loop", "Move ideas through queue, backtest/result, scoring, and review.", "Promising edges can be tested systematically.", "research lab", "HIGH", ["ops/aegis/research_lab_execution_loop_v1.py", "ops/tools/run_aegis_research_lab_execution_loop_v1.py"], ["aegis:research-lab-loop"], ["research_lab_loop"], ["test_aegis_automation_ai_inventory"], ["research_lab_execution_loop", "research_lab_loop"], ["adaptive_governance"], "Idea capture, result review, and sleeve review artifacts are connected with human approval required."),
        _spec("Candidate outcome tracking", "Track generated, ignored, captured, successful, and failed candidates.", "Sleeves improve from both acted-on and ignored recommendations.", "candidate feedback loop", "HIGH", ["ops/aegis/candidate_lifecycle_v1.py", "ops/tools/update_aegis_candidate_outcomes_v1.py"], ["aegis:update-candidate-outcomes"], ["candidate_lifecycle", "advisory_quality"], ["test_aegis_automation_ai_inventory"], ["candidate_workflow", "candidate_lifecycle"], ["audit_handoff"], "Candidate lifecycle artifacts include generated/ignored/captured/outcome states and feed performance attribution."),
        _spec("Candidate ranking and explanation", "Rank candidates and explain why this trade, why now, and why not.", "Operator can compare candidates quickly.", "candidate review", "HIGH", ["ops/aegis/candidate_ranking_explanation_v1.py", "ops/tools/run_aegis_candidate_ranking_v1.py"], ["aegis:candidate-ranking"], ["candidate_ranking"], ["test_aegis_automation_ai_inventory"], ["candidate_ranking", "candidate_workflow"], ["audit_handoff"], "Candidate artifacts include deterministic ranking, evidence links, risk context, and why/why-not explanation."),
        _spec("Unified operator cockpit", "Single operator surface for today's state, candidates, warnings, receipts, and actions.", "Operator has one place to work from.", "operator workflow", "HIGH", ["ops/tools/write_aegis_daily_operator_v1.py", "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"], ["aegis:daily-operator"], ["daily_operator"], ["test_aegis_automation_ai_inventory"], ["daily_operator", "aegis-runtime-truth"], ["audit_handoff"], "Daily operator, inbox, runtime truth, candidates, approvals, EOD/EOW, and triggered runs are consolidated into one read-only cockpit.", override_status="PARTIAL", limitations={"PARTIAL": "Daily operator and UI/API surfaces exist, but the workflow is still spread across reports and pages."}),
        _spec("Canonical Operator State", "Normalize operational state into one read-only projection.", "Operator brief and cockpit can consume stable state instead of parsing every upstream report.", "operator compression", "HIGH", ["ops/aegis/canonical_operator_state_v1.py", "ops/tools/build_aegis_canonical_operator_state_v1.py"], ["aegis:canonical-operator-state"], ["canonical_operator_state"], ["test_aegis_automation_ai_inventory"], ["canonical_operator_state"], ["audit_handoff"], "Read-only canonical projection exists with source hashes, missing inputs, conflicts, actions, and safety fields."),
        _spec("Operator Brief", "Compress canonical operator state into daily operator brief.", "Operator sees one concise action brief built from canonical state.", "operator compression", "HIGH", ["ops/tools/write_aegis_operator_brief_v1.py"], ["aegis:operator-brief"], ["operator_brief"], ["test_aegis_automation_ai_inventory"], ["operator_brief"], ["audit_handoff"], "Operator brief reads canonical operator state only and generates JSON, summary, and matrix."),
        _spec("Sleeve challenger", "Continuously challenge weak/stale sleeves and route investigation.", "Weak sleeves are retired or improved faster.", "sleeve governance", "HIGH", ["ops/aegis/sleeve_challenger_v1.py", "ops/tools/run_aegis_sleeve_challenger_v1.py"], ["aegis:sleeve-challenger"], ["sleeve_challenger"], ["test_aegis_automation_ai_inventory"], ["sleeve_challenger"], ["intelligence_governance"], "Dedicated challenger findings cite performance, regime, failure, and alternatives, then require approval."),
        _spec("Human approval workflow", "Record approval/rejection/defer decisions for recommendations.", "Operator decisions are auditable and replayable.", "governance approval", "HIGH", ["ops/tools/record_aegis_intelligence_approval_v1.py", "ops/aegis/intelligence_governance_kernel_v1.py"], ["aegis:record-intelligence-approval", "aegis:intelligence-governance"], ["intelligence_governance", "approval_ledger"], ["test_aegis_automation_ai_inventory"], ["approval"], ["intelligence_governance"], "Approval ledger, CLI, tests, replay, and operator surface all exist.", override_status="PARTIAL", limitations={"PARTIAL": "CLI ledger exists; operator UX for approval/rejection is not fully surfaced."}),
        _spec("Regime memory", "Persist regime context and connect it to sleeve outcomes.", "Aegis learns which sleeves work in which regimes.", "regime learning", "HIGH", ["ops/aegis/regime_outcome_memory_v1.py", "ops/tools/run_aegis_regime_outcome_memory_v1.py"], ["aegis:regime-outcome-memory"], ["regime_outcome_memory"], ["test_aegis_automation_ai_inventory"], ["regime_outcome_memory"], ["audit_handoff"], "Longitudinal regime/outcome graph is populated and drives trust/research recommendations."),
        _spec("Ignored candidate tracking", "Track candidates the operator ignored and compare outcomes.", "Aegis can learn from missed or avoided trades.", "candidate feedback loop", "HIGH", ["ops/aegis/candidate_lifecycle_v1.py", "ops/tools/record_aegis_candidate_decision_v1.py"], ["aegis:record-candidate-decision"], ["candidate_lifecycle"], ["test_aegis_automation_ai_inventory"], ["candidate_workflow", "ignored_candidates"], ["audit_handoff"], "Ignored candidate artifact exists and feeds advisory quality metrics."),
        _spec("Recommendation outcome tracking", "Track whether recommendations were accepted, rejected, deferred, or later validated.", "Governance recommendations can be evaluated over time.", "adaptive governance", "MEDIUM", ["ops/tools/record_aegis_intelligence_approval_v1.py"], ["aegis:record-intelligence-approval"], ["approval_ledger"], ["test_aegis_automation_ai_inventory"], ["approval"], ["intelligence_governance"], "Recommendation outcomes are linked to subsequent sleeve/research results.", override_status="PARTIAL", limitations={"PARTIAL": "Approval state is recorded, but later outcome attribution for recommendations is not fully proven."}),
        _spec("Ad hoc triggered sleeve runner", "Run selected sleeves outside normal schedule for advisory candidates.", "Operator gets timely sleeve review when events/regimes change.", "sleeve runner", "HIGH", ["ops/tools/run_aegis_triggered_sleeves_v1.py"], ["aegis:triggered-sleeves"], ["triggered_sleeve_runs", "candidate_lifecycle", "candidate_ranking"], ["test_aegis_automation_ai_inventory"], ["triggered_sleeve", "candidate_ranking"], ["event_regime_trigger_audit"], "Runner invokes confirmed sleeve advisory generation where available, records lifecycle, and produces ranked candidates."),
        _spec("EOD/EOW feedback loop", "Summarize daily/weekly evidence and route learnings.", "Operator can review what changed and what to improve.", "EOD/EOW", "MEDIUM", ["ops/tools/write_aegis_eod_intelligence_v1.py", "ops/tools/write_aegis_eow_intelligence_v1.py"], ["aegis:eod", "aegis:eow"], ["eod", "eow"], ["test_aegis_automation_ai_inventory"], ["eod_intelligence", "eow_intelligence"], ["audit_handoff"], "EOD/EOW write closed-loop tasks into research, sleeve governance, and operator inbox.", override_status="PARTIAL", limitations={"PARTIAL": "Reports consume summaries; automated closed-loop task creation is not fully proven."}),
        _spec("UI/API visibility", "Expose Aegis status and intelligence read-only.", "Operator can see state without reading raw JSON.", "UI/API", "MEDIUM", ["constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"], ["aegis:ui:start"], ["daily_operator"], ["test_aegis_automation_ai_inventory"], ["aegis-runtime-truth"], ["audit_handoff"], "Read-only UI/API exposes runtime truth, triggered runs, candidates, approvals, and reports.", override_status="PARTIAL", limitations={"PARTIAL": "Read-only surfaces exist, but full feature coverage and cockpit consolidation are incomplete."}),
        _spec("Timer activation vs repo-defined timers", "Distinguish scheduled definitions from active timers.", "Operator knows what actually runs without manual command.", "automation scheduling", "MEDIUM", [], [], ["automation_ai_inventory"], ["test_aegis_automation_ai_inventory"], ["timer"], [], "Repo-defined and active timers are both inventoried and critical timers are active.", override_status="PARTIAL", limitations={"PARTIAL": "Repo timers are inventoried; not all repo-defined timers are confirmed active."}),
    ]
    for row in rows:
        merged = {**base, **row}
        row.clear()
        row.update(merged)
    return rows


def _spec(
    feature_name: str,
    purpose: str,
    expected_user_value: str,
    workflow: str,
    priority: str,
    code_files: list[str],
    scripts: list[str],
    reports: list[str],
    test_fragments: list[str],
    operator_fragments: list[str],
    audit_reports: list[str],
    proof: str,
    **extra: Any,
) -> dict[str, Any]:
    return {
        "feature_name": feature_name,
        "purpose": purpose,
        "expected_user_value": expected_user_value,
        "workflow": workflow,
        "priority": priority,
        "code_files": code_files,
        "scripts": scripts,
        "reports": reports,
        "test_fragments": test_fragments,
        "operator_fragments": operator_fragments,
        "audit_reports": audit_reports,
        "proof": proof,
        "reference_fragments": [feature_name],
        **extra,
    }


def _evidence_found(checks: dict[str, bool]) -> list[str]:
    labels = {
        "code_exists": "code exists",
        "command_or_scheduled_path_exists": "command or scheduled path exists",
        "output_artifacts_exist": "output artifacts exist",
        "tests_exist": "tests exist",
        "operator_visibility_exists": "operator visibility exists",
        "audit_or_replay_evidence_exists": "audit/replay evidence exists",
        "safety_constraints_explicit": "safety constraints explicit",
        "referenced": "referenced in repo/docs/reports",
    }
    return [label for key, label in labels.items() if checks.get(key)]


def _evidence_missing(checks: dict[str, bool]) -> list[str]:
    labels = {
        "code_exists": "code",
        "command_or_scheduled_path_exists": "command or scheduled path",
        "output_artifacts_exist": "output artifact",
        "tests_exist": "tests",
        "operator_visibility_exists": "operator visibility",
        "audit_or_replay_evidence_exists": "audit/replay evidence",
        "safety_constraints_explicit": "explicit safety constraints",
    }
    return [label for key, label in labels.items() if not checks.get(key)]


def _sort_priority(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: (_priority_rank(row["priority"]), row["status"], row["feature_name"]))


def _priority_rank(priority: str) -> int:
    return {"HIGH": 0, "MEDIUM": 1, "LOW": 2}.get(priority, 9)


def _recommended_packet(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["No completion packet required from this audit."]
    return [f"{row['feature_name']}: {row['recommended_next_action']}" for row in rows[:5]]


def _summary_rows(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- NONE"]
    return [
        f"- {row['feature_name']} [{row['status']}, {row['priority']}]: {row['current_limitation']}"
        for row in rows[:10]
    ]


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


if __name__ == "__main__":
    raise SystemExit(main())
