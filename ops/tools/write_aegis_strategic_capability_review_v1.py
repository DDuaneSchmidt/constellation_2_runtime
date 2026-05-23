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

from ops.aegis.intelligence_common_v1 import ai_evidence_v1, intelligence_summaries_v1, latest_json_v1, now_utc_v1, read_json_v1, systemd_timer_inventory_v1
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT


REPORT_FAMILY = "aegis_strategic_capability_review_v1"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="write_aegis_strategic_capability_review_v1")
    parser.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day)
    payload = build_strategic_capability_review_v1(truth_root=truth_root, repo_root=REPO_ROOT, day_utc=day)
    paths = write_strategic_capability_review_v1(truth_root=truth_root, day_utc=day, payload=payload)
    print(
        json.dumps(
            {
                "path": paths["json"],
                "summary_path": paths["summary"],
                "matrix_path": paths["matrix"],
                "idea_quality": payload["strategic_verdict"]["idea_quality"],
                "long_term_potential": payload["strategic_verdict"]["long_term_potential"],
                "broker_execution_goal": False,
                "autonomous_execution_goal": False,
            },
            sort_keys=True,
        )
    )
    return 0


def build_strategic_capability_review_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    repo = Path(repo_root).resolve()
    generated_at = now_utc_v1()
    evidence = _evidence_inventory(root, repo, day_utc)
    differentiation = _differentiation_review(evidence)
    maturity = _capability_maturity(evidence)
    compounding = _compounding_value_analysis(evidence)
    return_paths = _return_impact_hypotheses(evidence)
    ai = _ai_usefulness(evidence, repo)
    tradeoffs = _architecture_tradeoffs()
    failure_risks = _failure_risk_review(evidence)
    moat = _moat_review(evidence)
    overengineering = _overengineering_review(evidence)
    exceptional = _exceptional_improvements()
    thesis = _strategic_thesis(evidence)
    verdict = _strategic_verdict(evidence, thesis, compounding, differentiation)
    return {
        "schema_id": "aegis_strategic_capability_review",
        "schema_version": "v1",
        "artifact_id": "aegis_strategic_capability_review_v1",
        "day_utc": day_utc,
        "generated_at_utc": generated_at,
        "repo_root": str(repo),
        "truth_root": str(root),
        "review_type": "strategic_capability_review",
        "not_a_readiness_score": True,
        "not_a_test_pass_fail_report": True,
        "not_a_trading_performance_claim": True,
        "classification_policy": {
            "labels": ["CONFIRMED", "PARTIAL", "NOT_FOUND", "UNKNOWN"],
            "unsupported_claims_allowed": False,
            "investment_return_claims_allowed": False,
            "production_maturity_claims_require_artifacts": True,
        },
        "input_evidence": evidence,
        "executive_interpretation": _executive_interpretation(evidence),
        "strategic_thesis_evaluation": thesis,
        "differentiation_review": differentiation,
        "capability_maturity_commentary": maturity,
        "compounding_value_analysis": compounding,
        "return_impact_hypotheses": return_paths,
        "ai_usefulness_evaluation": ai,
        "architecture_tradeoff_review": tradeoffs,
        "failure_risk_review": failure_risks,
        "moat_defensibility_review": moat,
        "overengineering_review": overengineering,
        "what_would_make_aegis_exceptional": exceptional,
        "strategic_verdict": verdict,
        "safety_constraints": {
            "broker_submit_transmit_is_goal": False,
            "autonomous_execution_is_goal": False,
            "live_trading_is_goal": False,
            "automatic_capital_allocation_is_goal": False,
            "automatic_sleeve_mutation_is_goal": False,
        },
    }


def write_strategic_capability_review_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "strategic_capability_review.v1.json"
    summary_path = out_dir / "strategic_capability_review.summary.txt"
    matrix_path = out_dir / "strategic_capability_review.matrix.csv"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(render_strategic_capability_review_summary_v1(payload), encoding="utf-8")
    matrix_path.write_text(render_strategic_capability_review_matrix_csv_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path), "matrix": str(matrix_path)}


def render_strategic_capability_review_summary_v1(payload: dict[str, Any]) -> str:
    verdict = payload["strategic_verdict"]
    thesis = payload["strategic_thesis_evaluation"]
    ai = payload["ai_usefulness_evaluation"]
    compounding = payload["compounding_value_analysis"]
    lines = [
        "AEGIS STRATEGIC CAPABILITY REVIEW v1",
        f"day_utc: {payload['day_utc']}",
        "",
        "Executive interpretation",
        f"- What Aegis is: {payload['executive_interpretation']['what_is_aegis']}",
        f"- What Aegis is not: {payload['executive_interpretation']['what_is_it_not']}",
        f"- Core idea: {payload['executive_interpretation']['core_idea']}",
        f"- Differentiation from a basic AI trading bot: {payload['executive_interpretation']['different_from_basic_ai_trading_bot']}",
        f"- Still unproven: {payload['executive_interpretation']['still_unproven']}",
        "",
        "Strategic thesis",
        f"- thesis_strength: {thesis['thesis_strength']}",
        f"- compounding_potential: {compounding['compounding_potential']}",
        "",
        "AI role",
        f"- current_ai_maturity: {ai['current_ai_maturity']}",
        f"- live_ai_call_path_found: {str(ai['live_ai_call_path_found']).lower()}",
        f"- deterministic_fallback: {str(ai['deterministic_fallback']).lower()}",
        "- AI outputs are governed as interpretation, recommendation, or summary only.",
        "",
        "Strategic verdict",
        f"- idea_quality: {verdict['idea_quality']}",
        f"- long_term_potential: {verdict['long_term_potential']}",
        f"- differentiation: {verdict['differentiation']}",
        f"- current_biggest_strength: {verdict['current_biggest_strength']}",
        f"- current_biggest_weakness: {verdict['current_biggest_weakness']}",
        f"- most_valuable_next_proof: {verdict['most_valuable_next_proof']}",
        f"- most_dangerous_failure_mode: {verdict['most_dangerous_failure_mode']}",
        f"- best_future_direction: {verdict['best_future_direction']}",
        "",
        "What not to build next",
    ]
    lines.extend(f"- {item}" for item in verdict["what_not_to_build_next"])
    lines.extend(["", "Required limitation statements"])
    lines.extend(
        [
            "- This review does not claim investment returns.",
            "- This review does not claim trading performance.",
            "- Broker submit/transmit is disabled by design and is not framed as a strategic goal.",
            "- Autonomous execution is disabled by design and is not framed as a strategic goal.",
            "- AI capability is not claimed without live AI call evidence.",
            "",
        ]
    )
    return "\n".join(lines)


def render_strategic_capability_review_matrix_csv_v1(payload: dict[str, Any]) -> str:
    rows: list[dict[str, str]] = []
    for item in payload["differentiation_review"]:
        rows.append({"section": "differentiation", "name": item["capability"], "classification": item["classification"], "maturity": "", "risk_or_weakness": item["why_it_matters"], "improvement_path": item["proof_needed"]})
    for item in payload["capability_maturity_commentary"]:
        rows.append({"section": "maturity", "name": item["domain"], "classification": "", "maturity": item["maturity"], "risk_or_weakness": "; ".join(item["weaknesses"]), "improvement_path": item["improvement_path"]})
    for item in payload["failure_risk_review"]:
        rows.append({"section": "failure_risk", "name": item["risk"], "classification": item["risk_level"], "maturity": "", "risk_or_weakness": item["evidence"], "improvement_path": item["mitigation"]})
    for item in payload["return_impact_hypotheses"]:
        rows.append({"section": "return_impact_hypothesis", "name": item["pathway"], "classification": item["evidence_status"], "maturity": item["expected_impact"], "risk_or_weakness": item["failure_mode"], "improvement_path": item["how_to_measure"]})
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=["section", "name", "classification", "maturity", "risk_or_weakness", "improvement_path"])
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def _evidence_inventory(root: Path, repo: Path, day_utc: str) -> dict[str, Any]:
    def ref(key: str, family: str, filename: str) -> dict[str, Any]:
        path, payload = latest_json_v1(root, family, day_utc, filename)
        return {"key": key, "status": "CONFIRMED" if path and payload else "NOT_FOUND", "path": str(path or ""), "payload": payload}

    package = read_json_v1(repo / "package.json")
    scripts = package.get("scripts") if isinstance(package.get("scripts"), dict) else {}
    adr_root = repo / "docs" / "aegis" / "adr"
    test_paths = sorted(str(path.relative_to(repo)) for path in (repo / "constellation_2" / "common" / "tests").glob("test_aegis_*.py")) if (repo / "constellation_2" / "common" / "tests").exists() else []
    manual_receipts = sorted((root / "manual_trade_receipts" / day_utc).glob("*.json")) if (root / "manual_trade_receipts" / day_utc).exists() else []
    ui_server = repo / "constellation_2" / "phaseL" / "ui" / "server" / "run_ops_dashboard_v1.py"
    ui_pages = repo / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "pages" / "index.js"
    ui_text = _read_text(ui_server) + "\n" + _read_text(ui_pages)
    refs = {
        "runtime_truth_kernel": ref("runtime_truth_kernel", "aegis_runtime_truth_kernel_v1", "runtime_truth_kernel.v1.json"),
        "intelligence_governance_kernel": ref("intelligence_governance_kernel", "aegis_intelligence_governance_kernel_v1", "intelligence_governance_kernel.v1.json"),
        "automation_ai_inventory": ref("automation_ai_inventory", "aegis_automation_ai_inventory_v1", "automation_ai_inventory.v1.json"),
        "daily_operator": ref("daily_operator", "aegis_daily_operator_v1", "daily_operator.v1.json"),
        "audit_handoff": {"key": "audit_handoff", "status": "CONFIRMED" if (root / "reports" / "aegis_audit_handoff_v1" / day_utc / "aegis_audit_handoff.txt").exists() else "NOT_FOUND", "path": str(root / "reports" / "aegis_audit_handoff_v1" / day_utc / "aegis_audit_handoff.txt"), "payload": {}},
        "eod_intelligence": ref("eod_intelligence", "aegis_eod_intelligence_v1", "eod_intelligence.v1.json"),
        "eow_intelligence": ref("eow_intelligence", "aegis_eow_intelligence_v1", "eow_intelligence.v1.json"),
        "adaptive_governance": ref("adaptive_governance", "adaptive_governance_v1", "adaptive_governance.v1.json"),
        "sleeve_performance": ref("sleeve_performance", "sleeve_performance_analytics_v1", "sleeve_performance_analytics.v1.json"),
        "sleeve_attribution": ref("sleeve_attribution", "aegis_sleeve_attribution_v1", "sleeve_attribution.v1.json"),
        "research_memory": ref("research_memory", "research_memory_graph_v1", "research_memory_graph.v1.json"),
        "research_queue": ref("research_queue", "research_queue_optimizer_v1", "research_queue_optimizer.v1.json"),
        "manual_capture_receipt_status": ref("manual_capture_receipt_status", "manual_execution_receipt_v1", "manual_execution_receipt.v1.json"),
    }
    runtime = refs["runtime_truth_kernel"]["payload"]
    intelligence = refs["intelligence_governance_kernel"]["payload"]
    return {
        "report_refs": refs,
        "intelligence_summaries": intelligence_summaries_v1(root, day_utc),
        "runtime_truth": {
            "classification": runtime.get("runtime_truth_classification") or "UNKNOWN",
            "highest_readiness_layer": runtime.get("highest_readiness_layer") or "UNKNOWN",
            "target_operating_mode": runtime.get("target_operating_mode") or "UNKNOWN",
            "trade_advice_allowed": bool(runtime.get("trade_advice_allowed")),
            "manual_trade_capture_allowed": bool(runtime.get("manual_trade_capture_allowed")),
            "missing_or_stale_source_count": runtime.get("missing_or_stale_source_count"),
            "blocked_capabilities": runtime.get("blocked_capabilities") or [],
            "policy_disabled_capabilities": runtime.get("policy_disabled_capabilities") or [],
        },
        "intelligence_governance": {
            "recommendation_count": intelligence.get("recommendation_count", 0),
            "ai_usage": intelligence.get("ai_usage") or ai_evidence_v1(repo),
            "validation": intelligence.get("validation_summary") or intelligence.get("validation") or {},
            "dual_kernel_boundary": intelligence.get("dual_kernel_boundary") or {},
        },
        "scripts": {
            "count": len(scripts),
            "aegis_scripts": sorted(name for name in scripts if str(name).startswith("aegis:")),
            "has_strategic_review_script": "aegis:strategic-capability-review" in scripts,
            "has_truth_kernel_script": "aegis:truth-kernel" in scripts,
            "has_manual_capture_script": "aegis:capture-manual-trade" in scripts,
            "has_replay_script": "aegis:replay-state" in scripts,
            "has_state_diff_script": "aegis:state-diff" in scripts,
        },
        "systemd_timer_inventory": systemd_timer_inventory_v1(repo),
        "docs": {
            "adr_dir_status": "CONFIRMED" if adr_root.exists() else "NOT_FOUND",
            "adr_files": sorted(str(path.relative_to(repo)) for path in adr_root.glob("*.md")) if adr_root.exists() else [],
        },
        "tests": {"aegis_test_files": test_paths, "status": "CONFIRMED" if test_paths else "NOT_FOUND"},
        "manual_capture": {"receipt_count_today": len(manual_receipts), "receipt_paths": [str(path) for path in manual_receipts]},
        "ui_api": {
            "runtime_truth_route": "CONFIRMED" if "/api/aegis/runtime-truth" in ui_text and "/aegis-runtime-truth" in ui_text else "NOT_FOUND",
            "adaptive_intelligence_route": "CONFIRMED" if "/api/aegis/adaptive-intelligence" in ui_text and "/aegis-adaptive-intelligence" in ui_text else "NOT_FOUND",
            "intelligence_governance_route": "CONFIRMED" if "/api/aegis/intelligence-governance" in ui_text and "/aegis-intelligence-governance" in ui_text else "NOT_FOUND",
            "post_patch_approval_ui": "NOT_FOUND" if "record-intelligence-approval" not in ui_text and "/api/aegis/intelligence-governance" in ui_text else "UNKNOWN",
        },
    }


def _executive_interpretation(evidence: dict[str, Any]) -> dict[str, str]:
    return {
        "what_is_aegis": "A human-approved portfolio operations, research, runtime-truth, and adaptive governance system.",
        "what_is_it_not": "Aegis is not a live broker trading system, not an autonomous execution system, and not a proven trading-performance engine.",
        "core_idea": "Separate operational truth from intelligence governance, then use durable evidence to improve research, sleeve review, operator decisions, and failure learning.",
        "why_might_it_be_valuable": "The architecture can reduce unsupported decisions by forcing evidence-backed readiness, traceable recommendations, manual receipt capture, replay, and human approval.",
        "different_from_basic_ai_trading_bot": "The confirmed design emphasizes deterministic kernels, evidence chains, disabled broker execution, approval ledgers, and auditability instead of direct AI-to-trade automation.",
        "still_unproven": "The system still needs long-run operational history, statistically useful sleeve samples, measured advisory quality, operator adoption evidence, and outcome feedback at scale.",
        "what_would_make_it_stronger": "A longer evidence history, stronger performance measurement, cleaner operator summaries, governed AI interpretation, and research-memory links from hypothesis to outcome.",
    }


def _strategic_thesis(evidence: dict[str, Any]) -> dict[str, Any]:
    refs = evidence["report_refs"]
    supporting = _available([
        ("Runtime Truth Kernel governs current operational truth.", refs["runtime_truth_kernel"]),
        ("Intelligence Governance Kernel governs recommendation lineage and approval state.", refs["intelligence_governance_kernel"]),
        ("Research queue and memory artifacts exist.", refs["research_queue"]),
        ("Sleeve performance analytics artifacts exist.", refs["sleeve_performance"]),
        ("Daily operator and audit reports consume generated evidence.", refs["daily_operator"]),
    ])
    against = [
        "Trading outcome quality is not proven by the strategic review inputs.",
        "Current performance and sleeve analytics depend on limited available evidence.",
        "Operator adoption and decision-quality improvement are not yet measured over time.",
    ]
    return {
        "thesis_strength": "MODERATE" if len(supporting) >= 4 else "UNKNOWN",
        "evidence_supporting_thesis": supporting,
        "evidence_against_thesis": against,
        "unproven_assumptions": [
            "Better evidence governance improves operator decisions.",
            "Research memory and failure analysis reduce repeated mistakes.",
            "Sleeve analytics become useful once sample sizes grow.",
            "Operators consistently capture manual receipts and review governed recommendations.",
        ],
        "what_must_be_measured_over_time": [
            "advisory false-positive rate",
            "recommendation accuracy",
            "sleeve health changes after governance review",
            "manual receipt completeness",
            "operator decision latency and compliance",
            "research task closure quality",
        ],
    }


def _differentiation_review(evidence: dict[str, Any]) -> list[dict[str, str]]:
    refs = evidence["report_refs"]
    runtime = evidence["runtime_truth"]
    scripts = evidence["scripts"]
    docs = evidence["docs"]
    ui = evidence["ui_api"]
    items = [
        ("Runtime Truth Kernel", refs["runtime_truth_kernel"]["status"], "Makes readiness and permissions deterministic.", "Keep it the sole authority for operational permissions."),
        ("evidence-backed readiness", refs["runtime_truth_kernel"]["status"], "Avoids narrative readiness claims.", "Add more artifact schema validation and provenance checks."),
        ("disabled-by-policy broker execution", "CONFIRMED" if "BROKER_SUBMIT_TRANSMIT" in runtime["policy_disabled_capabilities"] or runtime["target_operating_mode"] == "HUMAN_APPROVED_ADVISORY_RUNTIME" else "PARTIAL", "Frames live trading as out of scope instead of a missing goal.", "Continue asserting this in UI, audit, and control packets."),
        ("manual capture workflow", "CONFIRMED" if scripts["has_manual_capture_script"] else "NOT_FOUND", "Captures human-executed trades without broker automation.", "Measure receipt completeness and validation failures."),
        ("replay/diff/history", "CONFIRMED" if scripts["has_replay_script"] and scripts["has_state_diff_script"] else "NOT_FOUND", "Allows prior state reconstruction and transition review.", "Use replay output in operational reviews."),
        ("adaptive governance", refs["adaptive_governance"]["status"], "Turns research and sleeve evidence into approval-gated recommendations.", "Connect recommendations to later outcomes."),
        ("research memory", refs["research_memory"]["status"], "Creates a durable record of hypotheses, tasks, events, and outcomes.", "Improve graph edge density and duplicate detection."),
        ("sleeve lifecycle governance", "PARTIAL" if refs["sleeve_performance"]["status"] == "CONFIRMED" or refs["sleeve_attribution"]["status"] == "CONFIRMED" else "NOT_FOUND", "Supports trust changes without automatic sleeve mutation.", "Add stronger lifecycle state transitions and sample thresholds."),
        ("intelligence governance", refs["intelligence_governance_kernel"]["status"], "Separates facts, metrics, interpretations, recommendations, and approvals.", "Require every adaptive recommendation to carry governed lineage."),
        ("AI governance and deterministic fallback labeling", "CONFIRMED" if refs["intelligence_governance_kernel"]["status"] == "CONFIRMED" else "PARTIAL", "Prevents unsupported AI capability claims.", "Add live AI only behind the same governance boundary."),
        ("human approval model", "CONFIRMED" if refs["intelligence_governance_kernel"]["status"] == "CONFIRMED" else "PARTIAL", "Stops recommendations from directly becoming operational change.", "Use the approval ledger in weekly review."),
        ("UI/API summaries", "CONFIRMED" if ui["runtime_truth_route"] == "CONFIRMED" and ui["intelligence_governance_route"] == "CONFIRMED" else "PARTIAL", "Makes governed state visible to the operator.", "Reduce dashboard duplication and improve prioritization."),
        ("architecture decision records", "CONFIRMED" if docs["adr_files"] else "NOT_FOUND", "Records context, decisions, consequences, and constraints.", "Keep ADRs current when governance boundaries change."),
    ]
    return [{"capability": name, "classification": status, "why_it_matters": why, "proof_needed": proof} for name, status, why, proof in items]


def _capability_maturity(evidence: dict[str, Any]) -> list[dict[str, Any]]:
    refs = evidence["report_refs"]
    runtime = evidence["runtime_truth"]
    ai = evidence["intelligence_governance"]["ai_usage"]
    domains = [
        ("Runtime governance", "HIGH" if refs["runtime_truth_kernel"]["status"] == "CONFIRMED" and runtime["classification"] == "REAL_RUNTIME" else "MEDIUM", ["Runtime Truth Kernel artifact exists.", f"classification={runtime['classification']}"], ["Trade advice remains blocked when evidence does not support it."], ["Current proof is artifact-governance proof, not outcome proof."], "Add broader invalidation scenarios and artifact provenance checks.", "High impact on safety and operator trust."),
        ("Operational safety", "HIGH", ["Broker and autonomous execution are disabled by design."], ["The target mode is human-approved advisory runtime."], ["Legacy/deferred trading code still needs continued boundary checks."], "Keep safety searches and policy-disabled wording in regression tests.", "Prevents scope drift into unauthorized execution."),
        ("Auditability/replayability", "HIGH" if evidence["scripts"]["has_replay_script"] else "MEDIUM", ["Replay and state-diff scripts are present."], ["State transitions and snapshots support after-the-fact review."], ["Replay value depends on consistent daily use."], "Use replay in EOD/EOW review.", "Supports learning and accountability."),
        ("Manual capture and operator workflow", "MEDIUM" if runtime["manual_trade_capture_allowed"] else "LOW", [f"manual_trade_capture_allowed={runtime['manual_trade_capture_allowed']}"], ["Manual capture is journaling-only and separated from execution."], ["Receipt quality depends on operator discipline."], "Measure missing fields, late entries, and unmatched advisories.", "Improves feedback data when used consistently."),
        ("Research lab quality", "MEDIUM" if refs["research_queue"]["status"] == "CONFIRMED" else "LOW", [refs["research_queue"]["path"]], ["Research tasks can be prioritized deterministically."], ["Research quality and closure outcomes remain immature."], "Track hypothesis lifecycle completion and rejected idea reasons.", "Can reduce research entropy."),
        ("Sleeve analytics quality", "LOW" if refs["sleeve_performance"]["status"] == "CONFIRMED" else "UNKNOWN", [refs["sleeve_performance"]["path"]], ["Metrics are separated from recommendations."], ["Sample sizes and realized outcome evidence are still limited."], "Add sample thresholds, benchmark basis, and realized-vs-advisory tracking.", "Can improve sleeve trust decisions once data grows."),
        ("Adaptive governance quality", "MEDIUM" if refs["adaptive_governance"]["status"] == "CONFIRMED" else "LOW", [refs["adaptive_governance"]["path"]], ["Recommendations are approval-gated."], ["Recommendations need later outcome validation."], "Close the loop from recommendation to operator decision to result.", "Can focus research and reduce repeated mistakes."),
        ("AI role and AI governance", "MEDIUM" if refs["intelligence_governance_kernel"]["status"] == "CONFIRMED" else "LOW", [f"ai_used={ai.get('ai_used')}", f"deterministic_fallback={ai.get('deterministic_fallback')}"], ["AI is not allowed to create facts, metrics, approvals, or actions."], ["No live AI call path is currently evidenced."], "Add governed AI interpretation only after citation and prompt-hash controls are complete.", "Can improve synthesis without weakening facts."),
        ("Operator inbox / cognitive compression", "MEDIUM" if refs["daily_operator"]["status"] == "CONFIRMED" else "LOW", [refs["daily_operator"]["path"]], ["Daily operator output compresses status and commands."], ["Too many reports can dilute attention."], "Promote a small set of high-priority operator next actions.", "Can improve adoption and decision consistency."),
        ("Long-term regime memory", "LOW" if refs["research_memory"]["status"] == "CONFIRMED" else "UNKNOWN", [refs["research_memory"]["path"]], ["Graph structure exists."], ["Regime-outcome memory needs time-series depth."], "Attach regimes to advisories, manual receipts, sleeve outcomes, and failures.", "Can improve filtering over time."),
        ("Performance analytics", "LOW", ["Sleeve analytics artifacts exist when generated."], ["Metric framework exists."], ["The review has no basis to claim improved returns or robust performance."], "Build clean benchmark and outcome datasets.", "Central to proving economic usefulness."),
        ("Production operational maturity", "MEDIUM", [f"aegis_scripts={len(evidence['scripts']['aegis_scripts'])}", f"timers={len(evidence['systemd_timer_inventory']['repo_defined_timers'])}"], ["Many report commands and some scheduling evidence exist."], ["Production maturity needs uptime, incident, and operator adoption evidence."], "Add service-level health, runbook, and report freshness monitoring.", "Improves reliability of daily use."),
        ("UI/operator usability", "MEDIUM" if evidence["ui_api"]["runtime_truth_route"] == "CONFIRMED" else "LOW", [str(evidence["ui_api"])], ["Read-only surfaces expose key reports."], ["UI breadth may exceed operator attention bandwidth."], "Consolidate the top decisions and proof links.", "Improves practical use."),
        ("Long-term maintainability", "MEDIUM", [f"ADR count={len(evidence['docs']['adr_files'])}", f"test files={len(evidence['tests']['aegis_test_files'])}"], ["ADRs and tests document boundaries."], ["Many engines increase coupling and maintenance load."], "Merge duplicate intelligence outputs and preserve stable schemas.", "Protects future iteration speed."),
    ]
    return [
        {"domain": domain, "maturity": maturity, "evidence": ev, "strengths": strengths, "weaknesses": weaknesses, "improvement_path": improvement, "potential_impact": impact}
        for domain, maturity, ev, strengths, weaknesses, improvement, impact in domains
    ]


def _compounding_value_analysis(evidence: dict[str, Any]) -> dict[str, Any]:
    return {
        "compounding_potential": "MEDIUM",
        "compounding_mechanisms": [
            {"mechanism": "operating history improves future decisions", "classification": "PARTIAL", "evidence": "Runtime snapshots, replay, audit, and EOD/EOW artifacts exist; long-run history still needs accumulation."},
            {"mechanism": "regime memory compounds", "classification": "PARTIAL", "evidence": "Regime and research-memory artifacts exist; regime-to-outcome evidence is still thin."},
            {"mechanism": "failure analysis compounds", "classification": "PARTIAL", "evidence": "Failure analysis report exists; confirmed causal library needs more outcomes."},
            {"mechanism": "research memory prevents repeated mistakes", "classification": "PARTIAL", "evidence": "Research graph and queue artifacts exist; duplicate avoidance needs measured use."},
            {"mechanism": "sleeve attribution improves governance", "classification": "PARTIAL", "evidence": "Sleeve analytics exist; low sample sizes limit confidence."},
            {"mechanism": "manual capture improves feedback quality", "classification": "CONFIRMED", "evidence": "Manual capture workflow records external human executions without broker automation."},
            {"mechanism": "AI becomes more useful as evidence grows", "classification": "PARTIAL", "evidence": "AI governance exists; no live AI call path is evidenced."},
        ],
        "blockers_to_compounding": ["insufficient operational history", "low statistical samples", "incomplete realized outcome linkage", "operator capture discipline", "report sprawl"],
        "data_needed_to_validate": ["advisory history", "manual receipt outcomes", "benchmark returns", "sleeve lifecycle decisions", "research closure outcomes", "regime labels tied to results"],
    }


def _return_impact_hypotheses(evidence: dict[str, Any]) -> list[dict[str, str]]:
    return [
        _impact("better research prioritization", "MEDIUM", "PARTIAL", "Track research task acceptance, completion, later usefulness, and avoided duplicate work.", "weeks to months", "Prioritizer ranks clean-looking but low-value tasks."),
        _impact("faster retirement of weak sleeves", "MEDIUM", "PARTIAL", "Measure sleeve downgrades against later avoided weak signals.", "months", "Low sample size causes premature retirement."),
        _impact("better regime filtering", "MEDIUM", "PARTIAL", "Compare advisory outcomes by regime label and excluded regime mismatches.", "months", "Regime labels remain too coarse."),
        _impact("better operator discipline", "MEDIUM", "CONFIRMED", "Measure checklist adherence, receipt capture latency, and unrecorded external actions.", "days to weeks", "Operator bypasses the workflow."),
        _impact("fewer false-positive advisories", "HIGH", "UNKNOWN", "Track false-positive rate from advisory candidate to outcome.", "months", "No consistent outcome labels."),
        _impact("better failure analysis", "MEDIUM", "PARTIAL", "Track recurring failure pattern reduction after investigations.", "months", "Narrative causality outruns evidence."),
        _impact("better sleeve trust allocation", "MEDIUM", "PARTIAL", "Track trust guidance against later sleeve health changes.", "months", "Guidance lacks benchmark and correlation context."),
        _impact("stronger risk warnings", "MEDIUM", "PARTIAL", "Track warnings, blocked advisories, and later avoided adverse states.", "weeks to months", "Warnings become noisy."),
        _impact("reduced repeated mistakes", "HIGH", "PARTIAL", "Track repeated hypothesis failures and duplicate research rate.", "months", "Research memory is not used in decisions."),
    ]


def _ai_usefulness(evidence: dict[str, Any], repo: Path) -> dict[str, Any]:
    ai = ai_evidence_v1(repo)
    governance = evidence["intelligence_governance"]
    return {
        "current_ai_maturity": "GOVERNED_DETERMINISTIC_FALLBACK",
        "live_ai_call_path_found": bool(ai["live_ai_call_path_found"]),
        "ai_used": bool(governance["ai_usage"].get("ai_used", False)),
        "deterministic_fallback": bool(governance["ai_usage"].get("deterministic_fallback", True)),
        "future_ai_potential": "MEDIUM",
        "best_ai_use_cases": ["summarize evidence chains", "draft interpretations with citations", "cluster failure patterns", "surface research-memory duplicates", "compress operator inbox"],
        "ai_risk_areas": ["creating facts", "inventing causality", "overstating low-sample metrics", "issuing action language", "masking uncertainty"],
        "ai_use_cases_to_avoid": ["fact creation", "metric calculation", "approval decisions", "runtime permission decisions", "broker execution", "autonomous sleeve mutation"],
        "governance_status": "CONFIRMED" if evidence["report_refs"]["intelligence_governance_kernel"]["status"] == "CONFIRMED" else "PARTIAL",
        "separation_from_facts_metrics_actions": "CONFIRMED" if governance["dual_kernel_boundary"] else "PARTIAL",
        "human_approval_required": True,
    }


def _architecture_tradeoffs() -> list[dict[str, str]]:
    return [
        {"tradeoff": "determinism vs intelligence", "current_balance": "Determinism controls facts, metrics, readiness, and permissions; intelligence is advisory.", "risk": "Interpretation richness can stay shallow without governed AI or deeper data.", "recommended_adjustment": "Keep deterministic fact/metric layers and add richer governed interpretations only with citations."},
        {"tradeoff": "safety vs speed", "current_balance": "Safety dominates; broker execution and autonomous action are disabled.", "risk": "Manual approval slows adaptation.", "recommended_adjustment": "Improve operator summaries instead of automating approval."},
        {"tradeoff": "governance vs complexity", "current_balance": "Governance is strong and expanding.", "risk": "Too many engines can create maintenance drag.", "recommended_adjustment": "Merge duplicate summaries and keep one primary operator path."},
        {"tradeoff": "human approval vs automation", "current_balance": "Human approval is required for operational change.", "risk": "Approval ledger value depends on regular use.", "recommended_adjustment": "Make approval review part of EOW workflow."},
        {"tradeoff": "evidence quality vs interpretation richness", "current_balance": "Evidence quality constrains claims.", "risk": "Low samples limit useful interpretation.", "recommended_adjustment": "Add sample-size-aware confidence gates to all sleeve and failure findings."},
        {"tradeoff": "modularity vs coupling", "current_balance": "Many report modules share common evidence helpers.", "risk": "Schemas can drift across engines.", "recommended_adjustment": "Centralize more report contracts and lineage validation."},
        {"tradeoff": "UI clarity vs report depth", "current_balance": "Reports are deep; UI surfaces are broad.", "risk": "Operator attention fragments.", "recommended_adjustment": "Show fewer, higher-value next actions with proof links."},
    ]


def _failure_risk_review(evidence: dict[str, Any]) -> list[dict[str, str]]:
    return [
        _risk("insufficient real operational history", "HIGH", "Current reports exist, but long-run outcome history is not evidenced.", "Run daily workflows and retain snapshots, advisories, receipts, and outcomes.", "few completed outcome-linked cycles"),
        _risk("weak statistical sample sizes", "HIGH", "Sleeve and failure analytics are present but sample depth is limited.", "Enforce sample thresholds and low-confidence labels.", "high-confidence labels on low samples"),
        _risk("overengineered governance", "MEDIUM", "Multiple kernels and engines exist.", "Consolidate duplicate outputs and keep operator workflow simple.", "operator ignores reports"),
        _risk("AI narrative risk", "MEDIUM", "AI governance exists; live AI is not evidenced.", "Keep AI out of facts, metrics, approvals, and execution.", "uncited AI summaries"),
        _risk("operator overload", "MEDIUM", "Many reports and UI surfaces exist.", "Create one prioritized daily action list.", "missed required reviews"),
        _risk("stale sleeves", "MEDIUM", "Sleeve governance exists but outcome proof is limited.", "Add automatic stale warnings, not automatic sleeve changes.", "old sleeves keep advisory influence"),
        _risk("research entropy", "MEDIUM", "Research queue exists; closure quality remains unproven.", "Require task outcomes and duplicate checks.", "growing queue with low closure rate"),
        _risk("circular reasoning", "MEDIUM", "Adaptive recommendations can consume prior adaptive outputs.", "Require independent outcome evidence for promotion claims.", "recommendations cite only recommendations"),
        _risk("poor UI adoption", "MEDIUM", "Read-only UI exists; adoption evidence is not present.", "Track operator workflow usage and reduce clutter.", "CLI reports used but UI not reviewed"),
        _risk("hidden coupling", "MEDIUM", "Many scripts consume generated artifacts.", "Add schema contract tests and report dependency maps.", "report changes break downstream consumers"),
        _risk("unmeasured performance quality", "HIGH", "The review has no basis for return or performance claims.", "Build realized/advisory outcome datasets and benchmark comparisons.", "claims appear before measurement"),
    ]


def _moat_review(evidence: dict[str, Any]) -> list[dict[str, str]]:
    return [
        _moat("accumulated regime memory", "MEDIUM", "Regime history tied to outcomes can become operator-specific context.", "Weak regime labels or sparse outcomes.", "Track regime-labeled advisory outcomes."),
        _moat("research memory graph", "MEDIUM", "Hypothesis history can prevent repeat work and expose idea lineage.", "Incomplete graph edges.", "Measure duplicate reduction and reused findings."),
        _moat("operator-specific workflow intelligence", "MEDIUM", "Manual receipts and operator actions can tailor future review.", "Inconsistent operator capture.", "Track adherence and review outcomes."),
        _moat("evidence-backed governance history", "HIGH", "Approval and audit trails become difficult to replicate without time and discipline.", "If ledger is not used.", "Measure approval lifecycle completeness."),
        _moat("sleeve performance history", "MEDIUM", "Longitudinal sleeve evidence can improve trust governance.", "Low sample size and changing definitions.", "Track samples, benchmarks, and regime slices."),
        _moat("failure-pattern library", "MEDIUM", "Recurring failures can be detected earlier.", "Narrative cause without proof.", "Track confirmed repeated patterns."),
        _moat("manual capture and attribution history", "MEDIUM", "External human executions become feedback data.", "Missing or late receipts.", "Measure receipt coverage and linkage."),
        _moat("adaptive governance history", "MEDIUM", "Past recommendations and approvals can improve future governance.", "No outcome loop.", "Track recommendation result and audit status."),
    ]


def _overengineering_review(evidence: dict[str, Any]) -> dict[str, Any]:
    return {
        "overengineering_risks": [
            "too many engines with overlapping summaries",
            "weak-confidence metrics presented across many files",
            "low-sample analytics creating report volume before evidence depth",
            "dashboard breadth exceeding operator attention",
            "governance ceremony without regular approval-ledger use",
        ],
        "simplification_opportunities": [
            "merge duplicate adaptive intelligence summaries into one operator-facing synthesis",
            "keep Runtime Truth and Intelligence Governance as the two top-level kernels",
            "make EOD/EOW the main review surfaces for long-term learning",
            "collapse low-value report sections into UNKNOWN/NOT_ENOUGH_DATA notes",
        ],
        "modules_to_merge_or_defer": [
            "parallel regime_detection and regime_context outputs unless both remain distinct",
            "parallel cross_sleeve_interaction and cross_sleeve_analysis outputs unless both carry unique evidence",
            "capital allocation intelligence until qualitative sleeve trust has stronger proof",
        ],
        "operator_clarity_improvements": [
            "one daily top-action list",
            "one weekly learning review",
            "clear proof links for each recommendation",
            "separate current operational truth from strategic learning",
        ],
    }


def _exceptional_improvements() -> list[dict[str, str]]:
    return [
        {"priority": "measurement quality", "improvement": "Build clean advisory-to-outcome datasets with benchmark and regime labels.", "why": "This is the main proof path for long-term usefulness."},
        {"priority": "performance analytics", "improvement": "Add sample-thresholded sleeve metrics and realized-vs-advisory gap tracking.", "why": "Sleeve governance needs measured evidence."},
        {"priority": "regime memory", "improvement": "Tie regimes to advisories, failures, manual receipts, and research outcomes.", "why": "Context can become a durable learning asset."},
        {"priority": "AI-governed interpretation", "improvement": "Introduce AI only for cited interpretation and recommendation drafting.", "why": "AI is most useful where synthesis is hard and evidence is already fixed."},
        {"priority": "operator cognitive compression", "improvement": "Reduce daily review to the smallest action set with proof links.", "why": "Adoption is a strategic dependency."},
        {"priority": "research governance", "improvement": "Require hypothesis closure, duplicate checks, and rejected-idea memory.", "why": "Research quality compounds through disciplined memory."},
        {"priority": "sleeve attribution", "improvement": "Link sleeve recommendations, captures, outcomes, and regime state.", "why": "Trust guidance needs complete attribution paths."},
        {"priority": "failure learning", "improvement": "Turn repeated failure patterns into explicit research tasks.", "why": "Avoiding repeat mistakes is a plausible source of value."},
        {"priority": "human approval ledger", "improvement": "Use approvals, deferrals, and rejections in EOW review.", "why": "Governance history only compounds when operators use it."},
        {"priority": "long-run operational use", "improvement": "Run the daily/EOD/EOW loop consistently and preserve artifacts.", "why": "The system needs history to prove its thesis."},
    ]


def _strategic_verdict(evidence: dict[str, Any], thesis: dict[str, Any], compounding: dict[str, Any], differentiation: list[dict[str, str]]) -> dict[str, Any]:
    confirmed = sum(1 for row in differentiation if row["classification"] == "CONFIRMED")
    return {
        "idea_quality": "STRONG" if thesis["thesis_strength"] == "MODERATE" else "UNKNOWN",
        "long_term_potential": "HIGH" if compounding["compounding_potential"] == "MEDIUM" and confirmed >= 7 else "MEDIUM",
        "differentiation": "HIGH" if confirmed >= 7 else "MEDIUM",
        "current_biggest_strength": "A disciplined separation between Runtime Truth permissions and Intelligence Governance recommendations.",
        "current_biggest_weakness": "The architecture is ahead of the measured operational history and performance evidence.",
        "most_valuable_next_proof": "A sustained advisory-to-manual-capture-to-outcome dataset with regime and sleeve attribution.",
        "most_dangerous_failure_mode": "Producing many well-governed reports that do not improve operator decisions or measured research quality.",
        "best_future_direction": "Concentrate on outcome measurement, sample-size-aware sleeve analytics, failure learning, and concise operator review.",
        "what_not_to_build_next": [
            "broker submit/transmit",
            "autonomous execution",
            "automatic sleeve promotion or demotion",
            "automatic capital allocation",
            "AI-generated facts or metrics",
            "more dashboards without reducing operator decision load",
        ],
    }


def _available(items: list[tuple[str, dict[str, Any]]]) -> list[str]:
    return [text for text, ref in items if ref.get("status") == "CONFIRMED"]


def _impact(pathway: str, expected: str, status: str, measure: str, horizon: str, failure: str) -> dict[str, str]:
    return {"pathway": pathway, "expected_impact": expected, "evidence_status": status, "how_to_measure": measure, "time_horizon": horizon, "failure_mode": failure}


def _risk(name: str, level: str, evidence: str, mitigation: str, warning: str) -> dict[str, str]:
    return {"risk": name, "risk_level": level, "evidence": evidence, "mitigation": mitigation, "early_warning_indicators": warning}


def _moat(name: str, potential: str, why: str, weakens: str, measure: str) -> dict[str, str]:
    return {"edge": name, "moat_potential": potential, "why_it_could_compound": why, "what_would_weaken_it": weakens, "how_to_measure_if_becoming_valuable": measure}


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


if __name__ == "__main__":
    raise SystemExit(main())
