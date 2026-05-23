from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.write_aegis_automation_ai_inventory_v1 import (
    build_automation_ai_inventory_v1,
    render_automation_ai_inventory_summary_v1,
    write_automation_ai_inventory_v1,
)
from ops.tools.run_aegis_event_sleeve_activation_audit_v1 import build_event_sleeve_activation_audit_v1
from ops.tools.run_aegis_event_regime_triggered_sleeve_run_audit_v1 import (
    build_event_regime_triggered_sleeve_run_audit_v1,
    write_event_regime_triggered_sleeve_run_audit_v1,
)
from ops.aegis.event_regime_trigger_registry_v1 import build_event_regime_trigger_registry_v1
from ops.aegis.event_regime_trigger_evaluator_v1 import (
    build_event_regime_trigger_evaluation_v1,
    write_event_regime_trigger_evaluation_v1,
)
from ops.tools.run_aegis_triggered_sleeves_v1 import build_triggered_sleeve_runs_v1, write_triggered_sleeve_runs_v1
from ops.tools.run_aegis_evolution_engine_v1 import main as run_evolution_engine_main
from ops.tools.run_aegis_research_prioritizer_v1 import main as run_research_prioritizer_main
from ops.tools.run_aegis_sleeve_attribution_v1 import main as run_sleeve_attribution_main
from ops.tools.run_aegis_risk_governance_v1 import main as run_risk_governance_main
from ops.tools.write_aegis_operator_inbox_v1 import main as write_operator_inbox_main
from ops.tools.write_aegis_eod_intelligence_v1 import build_eod_intelligence_v1, main as write_eod_intelligence_main
from ops.tools.write_aegis_eow_intelligence_v1 import build_eow_intelligence_v1, main as write_eow_intelligence_main
from ops.tools.run_aegis_regime_detection_v1 import main as run_regime_detection_main
from ops.tools.run_aegis_capital_allocation_intelligence_v1 import main as run_capital_allocation_intelligence_main
from ops.tools.run_aegis_failure_analysis_v1 import main as run_failure_analysis_main
from ops.tools.run_aegis_research_memory_graph_v1 import main as run_research_memory_graph_main
from ops.tools.run_aegis_cross_sleeve_interaction_v1 import main as run_cross_sleeve_interaction_main
from ops.tools.run_aegis_research_queue_optimizer_v1 import main as run_research_queue_optimizer_main
from ops.tools.run_aegis_event_interpretation_v1 import main as run_event_interpretation_main
from ops.tools.run_aegis_adaptive_governance_v1 import main as run_adaptive_governance_main
from ops.tools.run_aegis_regime_context_v1 import main as run_regime_context_main
from ops.tools.run_aegis_sleeve_performance_analytics_v1 import main as run_sleeve_performance_analytics_main
from ops.tools.run_aegis_cross_sleeve_analysis_v1 import main as run_cross_sleeve_analysis_main
from ops.aegis.candidate_lifecycle_v1 import append_candidate_decision_correction_v1, append_candidate_decision_v1, build_candidate_lifecycle_v1, read_candidate_decisions_v1, update_candidate_outcomes_v1
from ops.aegis.candidate_ranking_explanation_v1 import build_candidate_ranking_v1
from ops.aegis.canonical_operator_state_v1 import build_canonical_operator_state_v1, write_canonical_operator_state_v1
from ops.aegis.research_hypothesis_classification_v1 import build_research_hypothesis_classification_v1, write_research_hypothesis_classification_v1
from ops.aegis.regime_outcome_memory_v1 import build_regime_outcome_memory_v1
from ops.aegis.research_lab_execution_loop_v1 import build_research_lab_execution_loop_v1, write_research_lab_execution_loop_v1
from ops.aegis.sleeve_challenger_v1 import build_sleeve_challenger_v1, write_sleeve_challenger_v1
from ops.tools.run_aegis_candidate_ranking_v1 import main as run_candidate_ranking_main
from ops.tools.run_aegis_regime_outcome_memory_v1 import main as run_regime_outcome_memory_main
from ops.tools.run_aegis_research_lab_execution_loop_v1 import main as run_research_lab_loop_main
from ops.tools.run_aegis_sleeve_challenger_v1 import main as run_sleeve_challenger_main
from ops.tools.write_aegis_daily_operator_v1 import build_daily_operator_v1
from ops.tools.build_aegis_audit_handoff_v1 import build_handoff_text_v1
from ops.tools.write_aegis_operator_brief_v1 import build_operator_brief_v1, write_operator_brief_v1
from ops.aegis.intelligence_governance.ai_output_governance_v1 import govern_ai_output_v1
from ops.aegis.intelligence_governance.evidence_chain_v1 import evidence_chain_item_v1, validate_evidence_chain_v1
from ops.aegis.intelligence_governance.metric_rules_v1 import metric_result_v1
from ops.aegis.intelligence_governance_kernel_v1 import replay_intelligence_governance_v1
from ops.aegis.journal.journal_event_v1 import build_journal_timeline_v1, write_journal_timeline_v1
from ops.tools.run_aegis_intelligence_governance_kernel_v1 import main as run_intelligence_governance_main
from ops.tools.record_aegis_intelligence_approval_v1 import main as record_intelligence_approval_main
from ops.tools.write_aegis_strategic_capability_review_v1 import (
    build_strategic_capability_review_v1,
    render_strategic_capability_review_summary_v1,
    write_strategic_capability_review_v1,
)
from ops.tools.write_aegis_high_roi_missing_items_review_v1 import (
    build_high_roi_missing_items_review_v1,
    render_high_roi_missing_items_summary_v1,
    write_high_roi_missing_items_review_v1,
)
from ops.tools.write_aegis_feature_completion_audit_v1 import build_feature_completion_audit_v1


DAY = "2026-05-16"


def test_automation_ai_inventory_report_files_are_generated(tmp_path: Path) -> None:
    payload = build_automation_ai_inventory_v1(
        repo_root=REPO_ROOT,
        truth_root=tmp_path,
        day_utc=DAY,
        include_systemctl=False,
    )

    paths = write_automation_ai_inventory_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)

    json_path = Path(paths["json"])
    summary_path = Path(paths["summary"])
    matrix_path = Path(paths["matrix"])
    assert json_path.exists()
    assert summary_path.exists()
    assert matrix_path.exists()
    loaded = json.loads(json_path.read_text(encoding="utf-8"))
    assert loaded["schema_id"] == "aegis_automation_ai_inventory"
    assert loaded["automation_entry_points"]["npm_scripts"]
    assert loaded["scheduled_automation"]["repo_defined_timers"]
    assert loaded["ai_usage"]["classification"] in {"NO_LIVE_AI_CALL_PATH_FOUND", "LIVE_AI_CALL_PATH_FOUND"}
    assert loaded["research_lab_automation"]["pipeline_stages"]
    assert loaded["feedback_loops"]


def test_inventory_classifies_ai_paths_without_inference(tmp_path: Path) -> None:
    payload = build_automation_ai_inventory_v1(
        repo_root=REPO_ROOT,
        truth_root=tmp_path,
        day_utc=DAY,
        include_systemctl=False,
    )

    assert payload["ai_usage"]["live_ai_call_path_found"] is False
    assert payload["ai_usage"]["classification"] == "NO_LIVE_AI_CALL_PATH_FOUND"
    assert payload["ai_usage"]["deterministic_fallback_evidence"]
    assert all(row["uses_ai"] is False for row in payload["classification_matrix"] if row["ai_role"] != "LIVE_AI_CALL_PATH_FOUND")


def test_inventory_scheduled_manual_and_unknowns_are_explicit(tmp_path: Path) -> None:
    payload = build_automation_ai_inventory_v1(
        repo_root=REPO_ROOT,
        truth_root=tmp_path,
        day_utc=DAY,
        include_systemctl=False,
    )

    scheduled = [row for row in payload["classification_matrix"] if row["trigger_type"].startswith("scheduled")]
    assert scheduled
    assert all(row["category"] == "systemd_timer" for row in scheduled)
    assert payload["manual_commands"]
    statuses = {row["status"] for row in payload["not_implemented_or_unknown"]}
    assert "NOT_IMPLEMENTED" in statuses or "NOT_FOUND" in statuses or "UNKNOWN" in statuses


def test_inventory_trade_execution_marking_requires_submit_evidence(tmp_path: Path) -> None:
    payload = build_automation_ai_inventory_v1(
        repo_root=REPO_ROOT,
        truth_root=tmp_path,
        day_utc=DAY,
        include_systemctl=False,
    )

    executing = [row for row in payload["classification_matrix"] if row["can_execute_trades"] is True]
    assert executing
    assert all(row["safety_class"] == "LEGACY_GUARDED_PAPER_SUBMIT" for row in executing)
    assert all("run_aegis_paper_submit_v1.py" in row["file"] or "run_aegis_paper_submit_v1.py" in row["command"] for row in executing)


def test_inventory_summary_and_csv_are_operator_safe(tmp_path: Path) -> None:
    payload = build_automation_ai_inventory_v1(
        repo_root=REPO_ROOT,
        truth_root=tmp_path,
        day_utc=DAY,
        include_systemctl=False,
    )

    summary = render_automation_ai_inventory_summary_v1(payload)
    lowered = summary.lower()
    assert "probably" not in lowered
    assert "likely" not in lowered
    assert "possibly" not in lowered
    assert "CONFIRMED: NO LIVE AI CALL PATH FOUND." in summary
    assert "Do not expect Aegis to submit or transmit broker orders" in summary

    paths = write_automation_ai_inventory_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    rows = list(csv.DictReader(Path(paths["matrix"]).read_text(encoding="utf-8").splitlines()))
    assert rows
    assert {"name", "trigger_type", "uses_ai", "can_execute_trades"}.issubset(rows[0])


def test_tier0_tier1_tier2_reports_are_generated(tmp_path: Path) -> None:
    _seed_intelligence_inputs(tmp_path)

    assert run_research_prioritizer_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_sleeve_attribution_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_evolution_engine_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_risk_governance_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert write_operator_inbox_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert write_eod_intelligence_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert write_eow_intelligence_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0

    expected = [
        tmp_path / "reports" / "aegis_research_priorities_v1" / DAY / "prioritized_research_queue.v1.json",
        tmp_path / "reports" / "aegis_sleeve_attribution_v1" / DAY / "sleeve_attribution.v1.json",
        tmp_path / "reports" / "aegis_evolution_engine_v1" / DAY / "evolution_engine.v1.json",
        tmp_path / "reports" / "aegis_operator_inbox_v1" / DAY / "operator_inbox.v1.json",
        tmp_path / "reports" / "aegis_risk_governance_v1" / DAY / "risk_governance.v1.json",
        tmp_path / "reports" / "aegis_eod_intelligence_v1" / DAY / "eod_intelligence.v1.json",
        tmp_path / "reports" / "aegis_eow_intelligence_v1" / DAY / "eow_intelligence.v1.json",
    ]
    assert all(path.exists() for path in expected)


def test_research_priorities_do_not_claim_ai_without_live_evidence(tmp_path: Path) -> None:
    _seed_intelligence_inputs(tmp_path)
    assert run_research_prioritizer_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0

    payload = _read(tmp_path / "reports" / "aegis_research_priorities_v1" / DAY / "prioritized_research_queue.v1.json")
    assert payload["ai_used"] is False
    assert payload["deterministic_fallback"] is True
    assert payload["prioritized_research_queue"]
    assert all(row["recommendation_is_advisory_only"] is True for row in payload["prioritized_research_queue"])


def test_sleeve_attribution_marks_missing_performance_unknown_not_invented(tmp_path: Path) -> None:
    assert run_sleeve_attribution_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0

    payload = _read(tmp_path / "reports" / "aegis_sleeve_attribution_v1" / DAY / "sleeve_attribution.v1.json")
    assert payload["sleeves"]
    row = payload["sleeves"][0]
    assert row["status"] == "UNKNOWN"
    assert row["return"] == "UNKNOWN"
    assert row["hit_rate"] == "UNKNOWN"
    assert row["missing_performance_data"] is True


def test_evolution_recommendations_are_advisory_and_human_approved(tmp_path: Path) -> None:
    _seed_intelligence_inputs(tmp_path)
    assert run_evolution_engine_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0

    payload = _read(tmp_path / "reports" / "aegis_evolution_engine_v1" / DAY / "evolution_engine.v1.json")
    assert payload["safety"]["automatic_sleeve_mutation_allowed"] is False
    assert payload["safety"]["human_approval_required_for_promotion_demotion"] is True
    assert payload["recommendations"]
    assert all(row["advisory_only"] is True for row in payload["recommendations"])
    assert all(row["human_approval_required"] is True for row in payload["recommendations"])


def test_event_sleeve_activation_audit_distinguishes_statuses_and_does_not_execute(tmp_path: Path) -> None:
    payload = build_event_sleeve_activation_audit_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)

    assert payload["event_monitor_cadence_status"] in {"CONFIRMED", "PARTIAL", "NOT_FOUND"}
    assert payload["ad_hoc_sleeve_trigger_status"] in {"CONFIRMED", "PARTIAL", "NOT_FOUND"}
    assert payload["can_execute_trades"] is False
    assert payload["broker_submit_required"] is False
    assert payload["autonomous_execution_allowed"] is False


def test_event_regime_triggered_sleeve_run_audit_report_is_generated(tmp_path: Path) -> None:
    payload = build_event_regime_triggered_sleeve_run_audit_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    paths = write_event_regime_triggered_sleeve_run_audit_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)

    assert Path(paths["json"]).exists()
    assert Path(paths["summary"]).exists()
    assert Path(paths["matrix"]).exists()
    loaded = _read(Path(paths["json"]))
    assert loaded["schema_id"] == "aegis_event_regime_triggered_sleeve_run_audit"
    assert loaded["event_or_regime_triggered_sleeve_runs_implemented"] in {"YES", "PARTIAL", "NO"}
    assert loaded["safety"]["broker_execution_enabled"] is False
    assert loaded["safety"]["autonomous_execution_enabled"] is False


def test_event_regime_trigger_audit_inspects_monitor_regime_mapping_and_trigger_path(tmp_path: Path) -> None:
    payload = build_event_regime_triggered_sleeve_run_audit_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)

    assert payload["recurring_event_monitor"]["status"] in {"CONFIRMED", "PARTIAL", "NOT_FOUND", "UNKNOWN"}
    assert payload["recurring_event_monitor"]["timer_name"] == "aegis-event-monitor-v1.timer"
    assert payload["regime_detection"]["status"] in {"CONFIRMED", "PARTIAL", "NOT_FOUND", "UNKNOWN"}
    assert isinstance(payload["regime_detection"]["regimes_detected"], list)
    assert payload["trigger_mapping_registry"]["status"] in {"CONFIRMED", "PARTIAL", "NOT_FOUND", "UNKNOWN"}
    assert isinstance(payload["trigger_mapping_registry"]["mappings"], list)
    assert payload["ad_hoc_sleeve_triggering"]["status"] in {"CONFIRMED", "PARTIAL", "NOT_FOUND", "UNKNOWN"}
    assert isinstance(payload["ad_hoc_sleeve_triggering"]["code_path"], list)


def test_event_regime_trigger_audit_does_not_claim_yes_without_full_path(tmp_path: Path) -> None:
    payload = build_event_regime_triggered_sleeve_run_audit_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)

    if payload["event_or_regime_triggered_sleeve_runs_implemented"] == "YES":
        assert payload["trigger_mapping_registry"]["status"] == "CONFIRMED"
        assert payload["ad_hoc_sleeve_triggering"]["status"] == "CONFIRMED"
        assert payload["triggered_run_auditability"]["status"] == "CONFIRMED"
    else:
        assert payload["missing_items"]
    assert payload["event_or_regime_triggered_sleeve_runs_implemented"] == "PARTIAL"
    assert any("Triggered sleeve run audit field missing" in item for item in payload["missing_items"])


def test_event_regime_trigger_audit_reports_operator_visibility_and_missing_items(tmp_path: Path) -> None:
    payload = build_event_regime_triggered_sleeve_run_audit_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)

    assert payload["triggered_run_auditability"]["status"] in {"CONFIRMED", "PARTIAL", "NOT_FOUND", "UNKNOWN"}
    assert payload["triggered_run_auditability"]["fields_missing"]
    assert payload["operator_visibility"]["status"] in {"CONFIRMED", "PARTIAL", "NOT_FOUND", "UNKNOWN"}
    assert payload["operator_visibility"]["surfaces"]
    assert payload["missing_items"]
    assert payload["recommended_next_implementation"]


def test_event_regime_trigger_registry_loads_confirmed_mappings() -> None:
    registry = build_event_regime_trigger_registry_v1(repo_root=REPO_ROOT, day_utc=DAY)

    assert registry["confirmed_sleeve_ids"] == ["PRIMARY"]
    assert registry["mapping_status"] == "CONFIRMED"
    assert len(registry["mappings"]) >= 12
    assert all(row["advisory_only"] is True for row in registry["mappings"])
    assert all(row["broker_execution_allowed"] is False for row in registry["mappings"])
    assert all(row["autonomous_execution_allowed"] is False for row in registry["mappings"])


def test_event_regime_trigger_unmapped_event_produces_no_mapping(tmp_path: Path) -> None:
    fake_repo = tmp_path / "repo"
    _seed_runtime_truth_ready(tmp_path)
    _seed_event_monitor_trigger(tmp_path, "PANIC_EXHAUSTION")

    payload = build_event_regime_trigger_evaluation_v1(
        truth_root=tmp_path,
        repo_root=fake_repo,
        day_utc=DAY,
        triggered_sleeves_enabled=True,
    )
    row = next(item for item in payload["decisions"] if item["trigger_id"] == "event:PANIC_EXHAUSTION")
    assert row["decision"] == "NO_MAPPING"
    assert row["selected_sleeve_ids"] == []
    assert row["reason_skipped"] == "MISSING_SLEEVE_MAPPING"


def test_event_regime_trigger_missing_event_evidence_is_insufficient(tmp_path: Path) -> None:
    _seed_runtime_truth_ready(tmp_path)

    payload = build_event_regime_trigger_evaluation_v1(
        truth_root=tmp_path,
        repo_root=REPO_ROOT,
        day_utc=DAY,
        triggered_sleeves_enabled=True,
    )
    row = next(item for item in payload["decisions"] if item["trigger_id"] == "event:PANIC_EXHAUSTION")
    assert row["decision"] == "INSUFFICIENT_EVIDENCE"
    assert row["reason_skipped"] == "EVENT_OR_REGIME_EVIDENCE_MISSING_OR_STALE"


def test_event_regime_trigger_confirmed_condition_selects_expected_sleeve(tmp_path: Path) -> None:
    _seed_runtime_truth_ready(tmp_path)
    _seed_event_monitor_trigger(tmp_path, "PANIC_EXHAUSTION")

    payload = build_event_regime_trigger_evaluation_v1(
        truth_root=tmp_path,
        repo_root=REPO_ROOT,
        day_utc=DAY,
        triggered_sleeves_enabled=True,
    )
    row = next(item for item in payload["decisions"] if item["trigger_id"] == "event:PANIC_EXHAUSTION")
    assert row["decision"] == "RUN_SLEEVES"
    assert row["selected_sleeve_ids"] == ["PRIMARY"]
    assert row["broker_execution_allowed"] is False
    assert row["autonomous_execution_allowed"] is False


def test_event_regime_trigger_cooldown_max_runs_blocks_repeated_trigger(tmp_path: Path) -> None:
    _seed_runtime_truth_ready(tmp_path)
    _seed_event_monitor_trigger(tmp_path, "PANIC_EXHAUSTION")
    _seed_triggered_run_history(tmp_path, trigger_id="event:PANIC_EXHAUSTION", count=3)

    payload = build_event_regime_trigger_evaluation_v1(
        truth_root=tmp_path,
        repo_root=REPO_ROOT,
        day_utc=DAY,
        triggered_sleeves_enabled=True,
    )
    row = next(item for item in payload["decisions"] if item["trigger_id"] == "event:PANIC_EXHAUSTION")
    assert row["decision"] == "SKIP"
    assert row["max_runs_per_day_status"] == "BLOCKED_MAX_RUNS_PER_DAY"


def test_triggered_sleeve_runner_generates_advisory_candidates_only(tmp_path: Path) -> None:
    _seed_runtime_truth_ready(tmp_path)
    _seed_event_monitor_trigger(tmp_path, "PANIC_EXHAUSTION")
    evaluation = build_event_regime_trigger_evaluation_v1(
        truth_root=tmp_path,
        repo_root=REPO_ROOT,
        day_utc=DAY,
        triggered_sleeves_enabled=True,
    )
    write_event_regime_trigger_evaluation_v1(truth_root=tmp_path, day_utc=DAY, payload=evaluation)

    payload = build_triggered_sleeve_runs_v1(
        truth_root=tmp_path,
        repo_root=REPO_ROOT,
        day_utc=DAY,
        trigger_id="event:PANIC_EXHAUSTION",
        sleeve_ids=["PRIMARY"],
        dry_run=True,
    )

    assert payload["candidate_count"] == 1
    run = payload["runs"][0]
    assert run["selected_sleeve_ids"] == ["PRIMARY"]
    assert run["safety"]["broker_execution_allowed"] is False
    assert run["safety"]["autonomous_execution_allowed"] is False
    candidate = _read(Path(run["advisory_candidate_artifacts"][0]))
    assert candidate["candidates"][0]["advisory_only"] is True
    assert candidate["candidates"][0]["broker_submit_transmit_called"] is False


def test_triggered_run_audit_includes_required_fields_and_audit_can_return_yes(tmp_path: Path) -> None:
    _seed_runtime_truth_ready(tmp_path)
    _seed_event_monitor_trigger(tmp_path, "PANIC_EXHAUSTION")
    evaluation = build_event_regime_trigger_evaluation_v1(
        truth_root=tmp_path,
        repo_root=REPO_ROOT,
        day_utc=DAY,
        triggered_sleeves_enabled=True,
    )
    write_event_regime_trigger_evaluation_v1(truth_root=tmp_path, day_utc=DAY, payload=evaluation)
    runs = build_triggered_sleeve_runs_v1(
        truth_root=tmp_path,
        repo_root=REPO_ROOT,
        day_utc=DAY,
        trigger_id="event:PANIC_EXHAUSTION",
        sleeve_ids=["PRIMARY"],
        dry_run=True,
    )
    write_triggered_sleeve_runs_v1(truth_root=tmp_path, day_utc=DAY, payload=runs)

    payload = build_event_regime_triggered_sleeve_run_audit_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)

    assert payload["event_or_regime_triggered_sleeve_runs_implemented"] == "YES"
    assert payload["triggered_run_auditability"]["status"] == "CONFIRMED"
    assert payload["triggered_sleeve_runner"]["candidate_count"] == 1
    assert payload["safety"]["broker_execution_enabled"] is False
    assert payload["safety"]["autonomous_execution_enabled"] is False


def test_event_regime_trigger_disabled_config_emits_skipped_config_disabled(tmp_path: Path) -> None:
    _seed_runtime_truth_ready(tmp_path)
    _seed_event_monitor_trigger(tmp_path, "PANIC_EXHAUSTION")

    payload = build_event_regime_trigger_evaluation_v1(
        truth_root=tmp_path,
        repo_root=REPO_ROOT,
        day_utc=DAY,
        triggered_sleeves_enabled=False,
    )
    row = next(item for item in payload["decisions"] if item["trigger_id"] == "event:PANIC_EXHAUSTION")
    assert row["decision"] == "SKIP"
    assert row["reason_skipped"] == "SKIPPED_CONFIG_DISABLED"


def test_eod_eow_consume_new_intelligence_summaries(tmp_path: Path) -> None:
    _seed_intelligence_inputs(tmp_path)
    assert run_research_prioritizer_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_sleeve_attribution_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_evolution_engine_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_risk_governance_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0

    eod = build_eod_intelligence_v1(truth_root=tmp_path, day_utc=DAY)
    eow = build_eow_intelligence_v1(truth_root=tmp_path, day_utc=DAY)

    assert eod["consumed_summaries"]["research_priorities"]["status"] == "AVAILABLE"
    assert eod["consumed_summaries"]["sleeve_attribution"]["status"] == "AVAILABLE"
    assert eow["consumed_summaries"]["evolution_engine"]["status"] == "AVAILABLE"
    assert eod["safety"]["broker_submit_required"] is False
    assert eow["safety"]["autonomous_execution_allowed"] is False


def test_new_intelligence_reports_preserve_no_execution_safety(tmp_path: Path) -> None:
    _seed_intelligence_inputs(tmp_path)
    assert run_evolution_engine_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_sleeve_attribution_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_risk_governance_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0

    for path in [
        tmp_path / "reports" / "aegis_evolution_engine_v1" / DAY / "evolution_engine.v1.json",
        tmp_path / "reports" / "aegis_sleeve_attribution_v1" / DAY / "sleeve_attribution.v1.json",
        tmp_path / "reports" / "aegis_risk_governance_v1" / DAY / "risk_governance.v1.json",
    ]:
        text = path.read_text(encoding="utf-8")
        assert '"broker_submit_required": false' in text
        assert '"autonomous_execution_allowed": false' in text
        assert "order_submitted" not in text
        assert "transmit" not in text.lower()


def test_adaptive_intelligence_engines_generate_required_reports(tmp_path: Path) -> None:
    _seed_intelligence_inputs(tmp_path)
    _seed_event_validity_no_packet(tmp_path)

    assert run_regime_detection_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_capital_allocation_intelligence_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_failure_analysis_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_research_memory_graph_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_cross_sleeve_interaction_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_research_queue_optimizer_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_event_interpretation_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_adaptive_governance_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0

    specs = [
        ("regime_detection_v1", "regime_detection.v1.json", "regime_detection.summary.txt"),
        ("capital_allocation_intelligence_v1", "capital_allocation_intelligence.v1.json", "capital_allocation_intelligence.summary.txt"),
        ("failure_analysis_v1", "failure_analysis.v1.json", "failure_analysis.summary.txt"),
        ("research_memory_graph_v1", "research_memory_graph.v1.json", "research_memory_graph.summary.txt"),
        ("cross_sleeve_interaction_v1", "cross_sleeve_interaction.v1.json", "cross_sleeve_interaction.summary.txt"),
        ("research_queue_optimizer_v1", "research_queue_optimizer.v1.json", "research_queue_optimizer.summary.txt"),
        ("event_interpretation_v1", "event_interpretation.v1.json", "event_interpretation.summary.txt"),
        ("adaptive_governance_v1", "adaptive_governance.v1.json", "adaptive_governance.summary.txt"),
    ]
    for family, filename, summary in specs:
        json_path = tmp_path / "reports" / family / DAY / filename
        summary_path = tmp_path / "reports" / family / DAY / summary
        assert json_path.exists()
        assert summary_path.exists()
        payload = _read(json_path)
        for key in ("engine_name", "generated_at", "day_utc", "ai_used", "deterministic_fallback", "input_artifacts", "input_artifact_status", "evidence_quality", "conclusions", "recommendations", "human_approval_required", "execution_allowed", "broker_submit_transmit_allowed", "autonomous_execution_allowed", "unknowns", "next_operator_actions"):
            assert key in payload
        assert payload["human_approval_required"] is True
        assert payload["execution_allowed"] is False
        assert payload["broker_submit_transmit_allowed"] is False
        assert payload["autonomous_execution_allowed"] is False


def test_adaptive_engines_preserve_ai_and_unknown_semantics(tmp_path: Path) -> None:
    assert run_regime_detection_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_cross_sleeve_interaction_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0

    regime = _read(tmp_path / "reports" / "regime_detection_v1" / DAY / "regime_detection.v1.json")
    cross = _read(tmp_path / "reports" / "cross_sleeve_interaction_v1" / DAY / "cross_sleeve_interaction.v1.json")

    assert regime["ai_used"] is False
    assert regime["deterministic_fallback"] is True
    assert regime["evidence_quality"] in {"UNKNOWN", "LOW"}
    assert regime["regime_classifications"]["volatility_regime"]["classification"] == "UNKNOWN"
    assert cross["concentration_risk"] == "UNKNOWN"
    assert cross["redundant_sleeves"] == []
    assert "PAIRWISE_CORRELATION_DATA" in cross["unknowns"]


def test_event_interpretation_no_event_packet_does_not_produce_trade_advice(tmp_path: Path) -> None:
    _seed_event_validity_no_packet(tmp_path)

    assert run_event_interpretation_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    payload = _read(tmp_path / "reports" / "event_interpretation_v1" / DAY / "event_interpretation.v1.json")

    assert payload["event_importance"] == "NONE"
    assert payload["expected_market_impact"] == "NONE"
    assert payload["trade_advice_generated"] is False
    assert payload["advisory_implication"] == "NO_EVENT_DRIVEN_ACTION"


def test_research_queue_optimizer_is_deterministic(tmp_path: Path) -> None:
    _seed_intelligence_inputs(tmp_path)
    assert run_research_prioritizer_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_research_queue_optimizer_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    first = _read(tmp_path / "reports" / "research_queue_optimizer_v1" / DAY / "research_queue_optimizer.v1.json")

    assert run_research_queue_optimizer_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    second = _read(tmp_path / "reports" / "research_queue_optimizer_v1" / DAY / "research_queue_optimizer.v1.json")

    assert first["priority_formula"] == second["priority_formula"]
    assert first["optimized_research_queue"] == second["optimized_research_queue"]
    assert first["optimized_research_queue"][0]["human_approval_required"] is True


def test_adaptive_governance_requires_human_approval_and_no_mutation(tmp_path: Path) -> None:
    _seed_intelligence_inputs(tmp_path)
    _seed_event_validity_no_packet(tmp_path)
    assert run_research_prioritizer_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_sleeve_attribution_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_regime_detection_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_failure_analysis_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_research_memory_graph_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_event_interpretation_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_research_queue_optimizer_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_capital_allocation_intelligence_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_cross_sleeve_interaction_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_adaptive_governance_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0

    payload = _read(tmp_path / "reports" / "adaptive_governance_v1" / DAY / "adaptive_governance.v1.json")
    assert payload["automated_sleeve_mutation_allowed"] is False
    assert payload["recommendations"]
    assert all(row["human_approval_required"] is True for row in payload["recommendations"])
    assert all(row["automated_change_allowed"] is False for row in payload["recommendations"])


def test_adaptive_summaries_are_consumed_by_daily_eod_eow(tmp_path: Path) -> None:
    _seed_intelligence_inputs(tmp_path)
    assert run_regime_detection_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_event_interpretation_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_adaptive_governance_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0

    eod = build_eod_intelligence_v1(truth_root=tmp_path, day_utc=DAY)
    eow = build_eow_intelligence_v1(truth_root=tmp_path, day_utc=DAY)

    assert eod["consumed_summaries"]["regime_detection"]["status"] == "AVAILABLE"
    assert eod["consumed_summaries"]["event_interpretation"]["status"] == "AVAILABLE"
    assert eow["consumed_summaries"]["adaptive_governance"]["status"] == "AVAILABLE"


def test_adaptive_governance_evidence_model_separates_chain(tmp_path: Path) -> None:
    _seed_intelligence_inputs(tmp_path)
    assert run_regime_context_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_sleeve_performance_analytics_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_failure_analysis_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_research_memory_graph_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_research_prioritizer_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_research_queue_optimizer_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_cross_sleeve_analysis_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_adaptive_governance_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0

    payload = _read(tmp_path / "reports" / "adaptive_governance_v1" / DAY / "adaptive_governance.v1.json")
    for key in ("facts", "metrics", "interpretations", "recommendations", "actions"):
        assert key in payload
    assert payload["facts"]
    assert payload["metrics"]
    assert payload["interpretations"]
    assert payload["actions"] == []
    assert all(row["human_approval_required"] is True for row in payload["recommendations"])
    assert all(row["automated_change_allowed"] is False for row in payload["recommendations"])
    assert payload["broker_execution_allowed"] is False
    assert payload["autonomous_execution_allowed"] is False


def test_sleeve_performance_missing_metrics_are_insufficient_data(tmp_path: Path) -> None:
    assert run_sleeve_performance_analytics_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0

    payload = _read(tmp_path / "reports" / "sleeve_performance_analytics_v1" / DAY / "sleeve_performance_analytics.v1.json")
    assert payload["sleeve_metrics"]
    assert payload["sleeve_metrics"][0]["return"] == "INSUFFICIENT_DATA"
    assert payload["sleeve_metrics"][0]["sharpe"] == "INSUFFICIENT_DATA"
    assert payload["ai_used"] is False
    assert payload["deterministic_fallback"] is True


def test_performance_attribution_engine_generates_all_report_files(tmp_path: Path) -> None:
    assert run_sleeve_performance_analytics_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0

    report_dir = tmp_path / "reports" / "aegis_sleeve_performance_analytics_v1" / DAY
    assert (report_dir / "sleeve_performance_analytics.v1.json").exists()
    assert (report_dir / "portfolio_attribution.v1.json").exists()
    assert (report_dir / "advisory_quality.v1.json").exists()
    assert (report_dir / "performance_attribution.summary.txt").exists()
    assert (report_dir / "performance_attribution.matrix.csv").exists()
    payload = _read(report_dir / "sleeve_performance_analytics.v1.json")
    assert payload["performance_attribution_engine_status"] == "FULLY_IMPLEMENTED"
    assert payload["broker_execution_allowed"] is False
    assert payload["autonomous_execution_allowed"] is False


def test_performance_attribution_metric_formulas_exist() -> None:
    catalog = metric_result_v1("sharpe", [0.01] * 20)["formula_version"]
    assert catalog
    for name in ["sharpe", "sortino", "max_drawdown", "win_rate", "expectancy", "profit_factor"]:
        result = metric_result_v1(name, [0.01, -0.005, 0.002, 0.003, -0.001] * 4)
        assert result["formula"]
        assert result["formula_version"]
        assert result["metric_status"] in {"OK", "INVALID_INPUT"}


def test_performance_attribution_metrics_include_sources_hashes_and_thresholds(tmp_path: Path) -> None:
    _seed_intelligence_inputs(tmp_path)
    assert run_sleeve_attribution_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_regime_context_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_sleeve_performance_analytics_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0

    payload = _read(tmp_path / "reports" / "aegis_sleeve_performance_analytics_v1" / DAY / "sleeve_performance_analytics.v1.json")
    metric = payload["sleeve_metrics"][0]["metrics"]["sharpe"]
    assert metric["metric_status"] == "INSUFFICIENT_DATA"
    assert metric["sample_size"] < metric["minimum_sample_size"]
    assert metric["confidence"] != "HIGH"
    assert metric["input_artifacts"]
    assert metric["input_hashes"]
    assert len(metric["input_artifacts"]) == len(metric["input_hashes"])
    assert metric["evidence_quality"] in {"HIGH", "MEDIUM", "LOW", "UNKNOWN"}


def test_performance_attribution_advisory_quality_and_gap_are_generated(tmp_path: Path) -> None:
    assert run_sleeve_performance_analytics_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0

    advisory = _read(tmp_path / "reports" / "aegis_sleeve_performance_analytics_v1" / DAY / "advisory_quality.v1.json")
    quality = advisory["advisory_quality"]
    assert "recommendation_accuracy" in quality
    assert "false_positive_rate" in quality
    assert "realized_vs_advisory_gap" in quality
    assert quality["recommendation_accuracy"]["metric_status"] == "INSUFFICIENT_DATA"
    assert quality["false_positive_rate"]["metric_status"] == "INSUFFICIENT_DATA"
    assert quality["realized_vs_advisory_gap"]["metric_status"] == "INSUFFICIENT_DATA"


def test_performance_attribution_regime_adjusted_metrics_are_insufficient_without_history(tmp_path: Path) -> None:
    assert run_sleeve_performance_analytics_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0

    payload = _read(tmp_path / "reports" / "aegis_sleeve_performance_analytics_v1" / DAY / "sleeve_performance_analytics.v1.json")
    regime_metric = payload["sleeve_metrics"][0]["metrics"]["regime_adjusted_performance"]
    advisory_regime = payload["advisory_quality"]["regime_adjusted_recommendation_accuracy"]
    assert regime_metric["metric_status"] == "INSUFFICIENT_DATA"
    assert advisory_regime["metric_status"] == "INSUFFICIENT_DATA"
    assert regime_metric["confidence"] in {"LOW", "UNKNOWN"}
    assert advisory_regime["confidence"] in {"LOW", "UNKNOWN"}


def test_performance_attribution_adaptive_governance_consumes_summary(tmp_path: Path) -> None:
    _seed_intelligence_inputs(tmp_path)
    assert run_sleeve_attribution_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_sleeve_performance_analytics_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    assert run_adaptive_governance_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0

    adaptive = _read(tmp_path / "reports" / "adaptive_governance_v1" / DAY / "adaptive_governance.v1.json")
    assert adaptive["input_artifact_status"]["sleeve_performance"]["status"] == "AVAILABLE"
    assert adaptive["input_artifacts"]["sleeve_performance"].endswith("sleeve_performance_analytics.v1.json")


def test_intelligence_governance_kernel_report_and_ledger_are_generated(tmp_path: Path) -> None:
    _seed_governed_intelligence_inputs(tmp_path)

    assert run_intelligence_governance_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0

    report_dir = tmp_path / "reports" / "aegis_intelligence_governance_kernel_v1" / DAY
    kernel_path = report_dir / "intelligence_governance_kernel.v1.json"
    assert kernel_path.exists()
    assert (report_dir / "intelligence_governance_kernel.summary.txt").exists()
    assert (report_dir / "intelligence_recommendations.v1.json").exists()
    assert (report_dir / "intelligence_approval_ledger.v1.json").exists()
    ledger_jsonl = tmp_path / "reports" / "aegis_intelligence_approval_ledger_v1" / DAY / "intelligence_approval_ledger.v1.jsonl"
    assert ledger_jsonl.exists()

    payload = _read(kernel_path)
    assert payload["dual_kernel_boundary"]["runtime_truth_kernel_is_sole_authority_for_readiness_permissions"] is True
    assert payload["dual_kernel_boundary"]["may_mutate_runtime_truth"] is False
    assert payload["safety"]["broker_execution_allowed"] is False
    assert payload["safety"]["autonomous_execution_allowed"] is False
    assert payload["recommendations"]
    assert payload["ai_usage"]["ai_used"] is False
    assert payload["ai_usage"]["deterministic_fallback"] is True


def test_intelligence_evidence_chain_enforces_layer_rules() -> None:
    fact_without_source = evidence_chain_item_v1(
        conclusion_id="bad_fact",
        layer="FACT",
        source_artifacts=[],
        repo_root=REPO_ROOT,
    )
    metric_without_formula = evidence_chain_item_v1(
        conclusion_id="bad_metric",
        layer="METRIC",
        source_artifacts=["/tmp/source.json"],
        repo_root=REPO_ROOT,
    )
    low_sample = evidence_chain_item_v1(
        conclusion_id="low_sample_metric",
        layer="METRIC",
        source_artifacts=["/tmp/source.json"],
        formula_or_rule="mean(return)",
        sample_size=1,
        minimum_sample_size=20,
        evidence_quality="HIGH",
        confidence="HIGH",
        repo_root=REPO_ROOT,
    )

    issues = validate_evidence_chain_v1([fact_without_source, metric_without_formula])
    assert "bad_fact:FACT_REQUIRES_SOURCE_ARTIFACT" in issues
    assert "bad_metric:METRIC_REQUIRES_FORMULA" in issues
    assert "bad_metric:METRIC_REQUIRES_SAMPLE_SIZE" in issues
    assert low_sample["confidence"] == "LOW"


def test_metric_rules_are_deterministic_and_conservative() -> None:
    empty_sharpe = metric_result_v1("sharpe", [])
    win_rate = metric_result_v1("win_rate", [1, 0, -1])

    assert empty_sharpe["metric_status"] in {"MISSING_INPUT", "INSUFFICIENT_DATA"}
    assert empty_sharpe["value"] is None
    assert win_rate["metric_status"] == "INSUFFICIENT_DATA"
    assert win_rate["sample_size"] == 3
    assert win_rate["minimum_sample_size"] >= 3
    assert win_rate["formula_version"]


def test_ai_output_governance_blocks_ai_facts_metrics_and_approvals() -> None:
    for output_type in ["FACT", "METRIC", "APPROVAL", "RUNTIME_PERMISSION", "EXECUTION_INSTRUCTION"]:
        governed = govern_ai_output_v1(
            repo_root=REPO_ROOT,
            output_type=output_type,
            prompt="test prompt",
            input_artifacts=[],
            output_payload={"claim": "test"},
            evidence_citations=[],
        )
        assert governed["prohibited_claim_check"] == "FAIL"
        assert governed["ai_used"] is False
        assert governed["deterministic_fallback"] is True
        assert governed["automated_change_allowed"] is False


def test_intelligence_approval_ledger_is_append_only_and_cli_only(tmp_path: Path) -> None:
    _seed_governed_intelligence_inputs(tmp_path)
    assert run_intelligence_governance_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    kernel = _read(tmp_path / "reports" / "aegis_intelligence_governance_kernel_v1" / DAY / "intelligence_governance_kernel.v1.json")
    recommendation_id = kernel["recommendations"][0]["recommendation_id"]
    ledger_path = tmp_path / "reports" / "aegis_intelligence_approval_ledger_v1" / DAY / "intelligence_approval_ledger.v1.jsonl"
    before = ledger_path.read_text(encoding="utf-8").splitlines()

    assert record_intelligence_approval_main(
        [
            "--truth_root",
            str(tmp_path),
            "--day",
            DAY,
            "--recommendation-id",
            recommendation_id,
            "--decision",
            "DEFERRED",
            "--reason",
            "test review later",
            "--operator",
            "pytest",
        ]
    ) == 0

    after = ledger_path.read_text(encoding="utf-8").splitlines()
    assert len(after) == len(before) + 1
    latest = json.loads(after[-1])
    assert latest["event_type"] == "DEFERRED"
    assert latest["recommendation_id"] == recommendation_id


def test_intelligence_governance_replay_reconstructs_recommendation_lineage(tmp_path: Path) -> None:
    _seed_governed_intelligence_inputs(tmp_path)
    assert run_intelligence_governance_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0

    replay = replay_intelligence_governance_v1(truth_root=tmp_path, day_utc=DAY)

    assert replay["schema_id"] == "aegis_intelligence_governance_replay"
    assert replay["recommendations"]
    assert replay["approval_events"]
    assert replay["lineage"]
    assert replay["lineage"][0]["recommendation_id"]
    assert replay["lineage"][0]["evidence_chain"]
    assert all(item["runtime_truth_mutation_allowed"] is False for row in replay["lineage"] for item in row["evidence_chain"])


def test_strategic_capability_review_report_files_are_generated(tmp_path: Path) -> None:
    payload = build_strategic_capability_review_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    paths = write_strategic_capability_review_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)

    json_path = Path(paths["json"])
    summary_path = Path(paths["summary"])
    matrix_path = Path(paths["matrix"])
    assert json_path.exists()
    assert summary_path.exists()
    assert matrix_path.exists()
    loaded = _read(json_path)
    assert loaded["schema_id"] == "aegis_strategic_capability_review"
    assert loaded["not_a_readiness_score"] is True
    assert loaded["not_a_test_pass_fail_report"] is True
    assert loaded["not_a_trading_performance_claim"] is True
    assert loaded["strategic_verdict"]["idea_quality"] in {"STRONG", "MODERATE", "WEAK", "UNKNOWN"}


def test_strategic_capability_review_contains_required_sections(tmp_path: Path) -> None:
    payload = build_strategic_capability_review_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)

    required = {
        "executive_interpretation",
        "strategic_thesis_evaluation",
        "differentiation_review",
        "capability_maturity_commentary",
        "compounding_value_analysis",
        "return_impact_hypotheses",
        "ai_usefulness_evaluation",
        "architecture_tradeoff_review",
        "failure_risk_review",
        "moat_defensibility_review",
        "overengineering_review",
        "what_would_make_aegis_exceptional",
        "strategic_verdict",
    }
    assert required.issubset(payload.keys())
    assert payload["differentiation_review"]
    assert payload["capability_maturity_commentary"]
    assert payload["return_impact_hypotheses"]


def test_strategic_capability_review_summary_avoids_unsupported_return_claims(tmp_path: Path) -> None:
    payload = build_strategic_capability_review_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    summary = render_strategic_capability_review_summary_v1(payload)
    lowered = summary.lower()

    assert "likely" not in lowered
    assert "probably" not in lowered
    assert "possibly" not in lowered
    assert "will generate returns" not in lowered
    assert "will improve returns" not in lowered
    assert "beat the market" not in lowered
    assert "does not claim investment returns" in lowered
    assert "does not claim trading performance" in lowered


def test_strategic_capability_review_unknowns_are_explicit(tmp_path: Path) -> None:
    payload = build_strategic_capability_review_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    text = json.dumps(payload, sort_keys=True)

    assert "UNKNOWN" in text
    assert any(row["classification"] in {"NOT_FOUND", "UNKNOWN", "PARTIAL"} for row in payload["differentiation_review"])
    assert payload["input_evidence"]["runtime_truth"]["classification"] == "UNKNOWN"


def test_strategic_capability_review_does_not_claim_ai_without_evidence(tmp_path: Path) -> None:
    payload = build_strategic_capability_review_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    ai = payload["ai_usefulness_evaluation"]

    assert ai["live_ai_call_path_found"] is False
    assert ai["ai_used"] is False
    assert ai["deterministic_fallback"] is True
    assert ai["current_ai_maturity"] == "GOVERNED_DETERMINISTIC_FALLBACK"
    assert "fact creation" in ai["ai_use_cases_to_avoid"]
    assert "broker execution" in ai["ai_use_cases_to_avoid"]


def test_strategic_capability_review_does_not_frame_broker_or_autonomous_as_goal(tmp_path: Path) -> None:
    payload = build_strategic_capability_review_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    summary = render_strategic_capability_review_summary_v1(payload)
    safety = payload["safety_constraints"]

    assert safety["broker_submit_transmit_is_goal"] is False
    assert safety["autonomous_execution_is_goal"] is False
    assert safety["live_trading_is_goal"] is False
    assert "Broker submit/transmit is disabled by design and is not framed as a strategic goal." in summary
    assert "Autonomous execution is disabled by design and is not framed as a strategic goal." in summary
    assert "broker submit/transmit" in payload["strategic_verdict"]["what_not_to_build_next"]
    assert "autonomous execution" in payload["strategic_verdict"]["what_not_to_build_next"]


def test_strategic_capability_review_matrix_csv_is_generated(tmp_path: Path) -> None:
    payload = build_strategic_capability_review_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    paths = write_strategic_capability_review_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)

    rows = list(csv.DictReader(Path(paths["matrix"]).read_text(encoding="utf-8").splitlines()))
    assert rows
    assert {"section", "name", "classification", "maturity", "risk_or_weakness", "improvement_path"}.issubset(rows[0])
    assert any(row["section"] == "differentiation" for row in rows)
    assert any(row["section"] == "failure_risk" for row in rows)


def test_high_roi_missing_items_review_report_files_are_generated(tmp_path: Path) -> None:
    payload = build_high_roi_missing_items_review_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    paths = write_high_roi_missing_items_review_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)

    assert Path(paths["json"]).exists()
    assert Path(paths["summary"]).exists()
    assert Path(paths["matrix"]).exists()
    assert Path(paths["backlog"]).exists()
    loaded = _read(Path(paths["json"]))
    assert loaded["schema_id"] == "aegis_high_roi_missing_items_review"
    assert loaded["review_perspective"] == "USER_OPERATOR"
    assert loaded["highest_roi_missing_items"]


def test_high_roi_missing_items_review_includes_required_categories(tmp_path: Path) -> None:
    payload = build_high_roi_missing_items_review_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    required = {
        "trade_candidate_quality",
        "sleeve_improvement_loop",
        "research_lab_edge_discovery",
        "ai_role",
        "operator_workflow",
        "performance_measurement",
        "regime_event_awareness",
        "feedback_loops",
        "auditability_and_trust",
    }
    assert required.issubset(payload["review_categories"].keys())
    assert payload["user_workflow_gaps"]
    assert payload["sleeve_edge_improvement_gaps"]
    assert payload["research_lab_gaps"]


def test_high_roi_missing_items_status_vocabularies_are_explicit(tmp_path: Path) -> None:
    payload = build_high_roi_missing_items_review_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    category_statuses = {row["status"] for row in payload["review_categories"].values()}
    backlog_statuses = {row["status"] for row in payload["highest_roi_missing_items"]}

    assert category_statuses <= {"CONFIRMED", "PARTIAL", "NOT_FOUND", "UNKNOWN"}
    assert backlog_statuses <= {"MISSING", "PARTIAL", "WEAK", "CONFIRMED"}
    assert any(status in backlog_statuses for status in {"MISSING", "PARTIAL", "WEAK"})


def test_high_roi_missing_items_does_not_recommend_broker_or_autonomous_as_required(tmp_path: Path) -> None:
    payload = build_high_roi_missing_items_review_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    text = json.dumps(payload, sort_keys=True).lower()

    assert payload["safety_constraints"]["broker_execution_required"] is False
    assert payload["safety_constraints"]["autonomous_execution_required"] is False
    assert "broker submit/transmit" in payload["what_not_to_build_next"]
    assert "autonomous execution" in payload["what_not_to_build_next"]
    assert "broker_execution_required\": true" not in text
    assert "autonomous_execution_required\": true" not in text


def test_high_roi_missing_items_does_not_claim_ai_call_path_without_evidence(tmp_path: Path) -> None:
    payload = build_high_roi_missing_items_review_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    summary = render_high_roi_missing_items_summary_v1(payload)

    assert payload["input_evidence"]["ai"]["live_ai_call_path_found"] is False
    assert payload["input_evidence"]["ai"]["ai_used"] is False
    assert payload["input_evidence"]["ai"]["deterministic_fallback"] is True
    assert "Live AI call path: NOT_FOUND" in summary
    assert "does not claim AI is operational without a live AI call path" in summary


def test_high_roi_missing_items_ranking_is_deterministic(tmp_path: Path) -> None:
    first = build_high_roi_missing_items_review_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    second = build_high_roi_missing_items_review_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)

    first_rows = [(row["rank"], row["name"], row["roi_priority_score"]) for row in first["highest_roi_missing_items"]]
    second_rows = [(row["rank"], row["name"], row["roi_priority_score"]) for row in second["highest_roi_missing_items"]]
    assert first_rows == second_rows
    scores = [row["roi_priority_score"] for row in first["highest_roi_missing_items"]]
    assert scores == sorted(scores, reverse=True)


def test_high_roi_missing_items_summary_is_user_perspective(tmp_path: Path) -> None:
    payload = build_high_roi_missing_items_review_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    summary = render_high_roi_missing_items_summary_v1(payload)

    assert "Highest-ROI missing or weak items" in summary
    assert "Recommended next 1-day packet" in summary
    assert "Recommended next 1-week packet" in summary
    assert "Recommended next 1-month roadmap" in summary
    assert "candidate outcome" in summary.lower()
    assert "manual" in payload["operating_model"]["human_role"].lower()


def test_candidate_lifecycle_records_ignored_candidate(tmp_path: Path) -> None:
    candidate_id = _seed_triggered_candidate(tmp_path)

    append_candidate_decision_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=candidate_id, decision="IGNORED", reason="No manual trade", operator="pytest")
    lifecycle = build_candidate_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)

    candidate = next(row for row in lifecycle["candidates"] if row["candidate_id"] == candidate_id)
    assert candidate["operator_decision"] == "IGNORED"
    assert candidate["state"] == "IGNORED"
    assert candidate["feeds_attribution"] is True
    assert candidate["feeds_research"] is True
    assert candidate["safety"]["broker_execution_allowed"] is False


def test_candidate_lifecycle_traded_candidate_links_manual_receipt_and_outcome_without_broker(tmp_path: Path) -> None:
    candidate_id = _seed_triggered_candidate(tmp_path)

    append_candidate_decision_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        candidate_id=candidate_id,
        decision="TRADED_MANUALLY",
        reason="Manually executed outside Aegis",
        operator="pytest",
        manual_trade_receipt_id="receipt-001",
    )
    update_candidate_outcomes_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        candidate_id=candidate_id,
        outcome_status="OUTCOME_WON",
        outcome_window="1d",
        outcome_metrics={"return": 0.02},
    )
    lifecycle = build_candidate_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)
    candidate = next(row for row in lifecycle["candidates"] if row["candidate_id"] == candidate_id)

    assert candidate["manual_trade_receipt_id"] == "receipt-001"
    assert candidate["operator_decision"] == "TRADED_MANUALLY"
    assert candidate["outcome_status"] == "OUTCOME_WON"
    assert candidate["outcome_metrics"]["return"] == 0.02
    assert candidate["safety"]["broker_submit_transmit_called"] is False


def test_candidate_decision_correction_preserves_original_and_updates_latest_state(tmp_path: Path) -> None:
    candidate_id = _seed_triggered_candidate(tmp_path)
    original = append_candidate_decision_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        candidate_id=candidate_id,
        decision="TRADED_MANUALLY",
        reason="Approved for manual IB execution",
        operator="pytest",
        intended_shares=50,
        risk_bucket="SMALL",
    )

    correction = append_candidate_decision_correction_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        candidate_id=candidate_id,
        field="intended_shares",
        new_value="25",
        reason="Typed wrong share count",
        operator="pytest",
    )
    events = read_candidate_decisions_v1(truth_root=tmp_path, day_utc=DAY)
    lifecycle = build_candidate_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)
    candidate = next(row for row in lifecycle["candidates"] if row["candidate_id"] == candidate_id)

    assert len(events) == 2
    assert events[0]["decision_event_id"] == original["decision_event_id"]
    assert events[1]["correction_id"] == correction["correction_id"]
    assert candidate["current_intended_shares"] == 25
    assert candidate["decision_history_count"] == 1
    assert candidate["correction_count"] == 1
    assert candidate["latest_correction_id"] == correction["correction_id"]
    assert len(candidate["audit_history"]) == 2
    assert candidate["audit_history"][0]["decision"] == "TRADED_MANUALLY"


def test_candidate_decision_correction_can_change_decision_and_requires_reason_operator(tmp_path: Path) -> None:
    candidate_id = _seed_triggered_candidate(tmp_path)
    append_candidate_decision_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        candidate_id=candidate_id,
        decision="TRADED_MANUALLY",
        reason="Approved",
        operator="pytest",
        intended_shares=10,
    )

    with pytest.raises(ValueError):
        append_candidate_decision_correction_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=candidate_id, field="decision", new_value="IGNORED", reason="", operator="pytest")
    with pytest.raises(ValueError):
        append_candidate_decision_correction_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=candidate_id, field="decision", new_value="IGNORED", reason="Changed", operator="")
    with pytest.raises(ValueError):
        append_candidate_decision_correction_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=candidate_id, field="not_a_field", new_value="x", reason="Changed", operator="pytest")

    append_candidate_decision_correction_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        candidate_id=candidate_id,
        field="decision",
        new_value="IGNORED",
        reason="Decided not to trade after review",
        operator="pytest",
    )
    append_candidate_decision_correction_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        candidate_id=candidate_id,
        field="decision_reason",
        new_value="No longer meets setup",
        reason="Corrected note",
        operator="pytest",
    )
    lifecycle = build_candidate_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)
    candidate = next(row for row in lifecycle["candidates"] if row["candidate_id"] == candidate_id)

    assert candidate["current_operator_decision"] == "IGNORED"
    assert candidate["operator_decision"] == "IGNORED"
    assert candidate["current_decision_reason"] == "No longer meets setup"
    assert candidate["correction_count"] == 2
    assert candidate["safety"]["broker_execution_allowed"] is False
    assert candidate["safety"]["autonomous_execution_allowed"] is False


def test_candidate_outcome_missing_data_remains_pending_or_unknown(tmp_path: Path) -> None:
    candidate_id = _seed_triggered_candidate(tmp_path)

    update_candidate_outcomes_v1(truth_root=tmp_path, day_utc=DAY)
    lifecycle = build_candidate_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)
    candidate = next(row for row in lifecycle["candidates"] if row["candidate_id"] == candidate_id)

    assert candidate["outcome_status"] in {"OUTCOME_PENDING", "OUTCOME_UNKNOWN"}
    assert candidate["outcome_metrics"] == {}


def test_advisory_quality_consumes_candidate_lifecycle(tmp_path: Path) -> None:
    candidate_id = _seed_triggered_candidate(tmp_path)
    append_candidate_decision_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=candidate_id, decision="IGNORED", reason="No setup", operator="pytest")
    update_candidate_outcomes_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=candidate_id, outcome_status="OUTCOME_UNKNOWN")

    assert run_sleeve_performance_analytics_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    advisory = _read(tmp_path / "reports" / "aegis_sleeve_performance_analytics_v1" / DAY / "advisory_quality.v1.json")["advisory_quality"]

    assert advisory["lifecycle_candidate_count"] >= 1
    assert advisory["ignored_candidate_count"] == 1
    assert "candidate_hit_rate" in advisory
    assert "ignored_candidate_opportunity_cost" in advisory
    assert advisory["candidate_hit_rate"]["metric_status"] == "INSUFFICIENT_DATA"


def test_performance_attribution_consumes_corrected_latest_candidate_state(tmp_path: Path) -> None:
    candidate_id = _seed_triggered_candidate(tmp_path)
    append_candidate_decision_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        candidate_id=candidate_id,
        decision="TRADED_MANUALLY",
        reason="Approved",
        operator="pytest",
        intended_shares=50,
    )
    append_candidate_decision_correction_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        candidate_id=candidate_id,
        field="decision",
        new_value="IGNORED",
        reason="Corrected decision",
        operator="pytest",
    )

    assert run_sleeve_performance_analytics_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    advisory = _read(tmp_path / "reports" / "aegis_sleeve_performance_analytics_v1" / DAY / "advisory_quality.v1.json")["advisory_quality"]

    assert advisory["lifecycle_candidate_count"] == 1
    assert advisory["ignored_candidate_count"] == 1
    assert advisory["traded_candidate_count"] == 0


def test_candidate_ranking_explains_why_now_and_why_not(tmp_path: Path) -> None:
    _seed_triggered_candidate(tmp_path)

    assert run_candidate_ranking_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    ranking = build_candidate_ranking_v1(truth_root=tmp_path, day_utc=DAY)
    row = ranking["ranked_candidates"][0]

    assert row["why_this_trade"]
    assert row["why_now"]
    assert row["why_not"]
    assert row["human_review_required"] is True
    assert row["broker_execution_allowed"] is False


def test_triggered_runner_feeds_candidate_lifecycle_and_does_not_fabricate_missing_generators(tmp_path: Path) -> None:
    _seed_runtime_truth_ready(tmp_path)
    _seed_event_monitor_trigger(tmp_path, "PANIC_EXHAUSTION")
    evaluation = build_event_regime_trigger_evaluation_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY, triggered_sleeves_enabled=True)
    write_event_regime_trigger_evaluation_v1(truth_root=tmp_path, day_utc=DAY, payload=evaluation)

    generated = build_triggered_sleeve_runs_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY, trigger_id="event:PANIC_EXHAUSTION", sleeve_ids=["PRIMARY"], dry_run=True)
    unsupported = build_triggered_sleeve_runs_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY, trigger_id="event:PANIC_EXHAUSTION", sleeve_ids=["UNKNOWN"], dry_run=True)
    lifecycle = build_candidate_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)

    assert generated["candidate_count"] == 1
    assert lifecycle["candidate_count"] >= 1
    assert unsupported["runs"][0]["candidate_count"] == 0
    assert unsupported["runs"][0]["warnings"] == ["NO_SELECTED_SLEEVES_AFTER_FILTER"]


def test_candidate_and_governance_decisions_require_reason_operator_and_do_not_mutate(tmp_path: Path) -> None:
    candidate_id = _seed_triggered_candidate(tmp_path)

    with pytest.raises(ValueError):
        append_candidate_decision_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=candidate_id, decision="IGNORED", reason="", operator="pytest")
    with pytest.raises(ValueError):
        append_candidate_decision_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=candidate_id, decision="IGNORED", reason="reviewed", operator="")

    append_candidate_decision_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=candidate_id, decision="DEFERRED", reason="review later", operator="pytest")
    lifecycle = build_candidate_lifecycle_v1(truth_root=tmp_path, day_utc=DAY)
    assert lifecycle["safety"]["broker_execution_allowed"] is False
    assert lifecycle["safety"]["autonomous_execution_allowed"] is False


def test_regime_memory_consumes_candidate_outcomes(tmp_path: Path) -> None:
    candidate_id = _seed_triggered_candidate(tmp_path)
    update_candidate_outcomes_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=candidate_id, outcome_status="OUTCOME_LOST", outcome_metrics={"return": -0.01})

    assert run_regime_outcome_memory_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    memory = build_regime_outcome_memory_v1(truth_root=tmp_path, day_utc=DAY)

    assert memory["candidate_outcomes_by_regime"]
    assert memory["candidate_outcomes_by_regime"][0]["outcome_status"] == "OUTCOME_LOST"
    assert memory["broker_execution_allowed"] is False


def test_research_lab_loop_creates_tasks_from_failures_and_requires_approval(tmp_path: Path) -> None:
    candidate_id = _seed_triggered_candidate(tmp_path)
    update_candidate_outcomes_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=candidate_id, outcome_status="OUTCOME_LOST", outcome_metrics={"return": -0.01})
    challenger = build_sleeve_challenger_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    write_sleeve_challenger_v1(truth_root=tmp_path, day_utc=DAY, payload=challenger)

    assert run_research_lab_loop_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    loop = build_research_lab_execution_loop_v1(truth_root=tmp_path, day_utc=DAY)
    paths = write_research_lab_execution_loop_v1(truth_root=tmp_path, day_utc=DAY, payload=loop)

    assert Path(paths["main"]).exists()
    assert loop["research_tasks"]
    assert all(row["human_approval_required"] is True for row in loop["research_tasks"])
    assert loop["automatic_sleeve_activation_allowed"] is False


def test_sleeve_challenger_emits_approval_gated_recommendations(tmp_path: Path) -> None:
    _seed_triggered_candidate(tmp_path)

    assert run_sleeve_challenger_main(["--truth_root", str(tmp_path), "--day", DAY]) == 0
    challenger = build_sleeve_challenger_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)

    assert challenger["challenges"]
    assert all(row["human_approval_required"] is True for row in challenger["challenges"])
    assert all(row["automated_change_allowed"] is False for row in challenger["challenges"])
    assert challenger["broker_execution_allowed"] is False
    assert challenger["autonomous_execution_allowed"] is False


def test_daily_operator_surfaces_pending_candidate_actions(tmp_path: Path) -> None:
    _seed_triggered_candidate(tmp_path)
    kernel = {
        "generated_at_utc": "2026-05-16T12:00:00Z",
        "target_operating_mode": "HUMAN_APPROVED_ADVISORY_RUNTIME",
        "human_approved_advisory_runtime_ready": True,
        "runtime_truth_classification": "REAL_RUNTIME",
        "highest_readiness_layer": "HUMAN_APPROVED_ADVISORY_RUNTIME_READY",
        "advisory_status": "ADVISORY_CANDIDATES_AVAILABLE",
        "manual_trade_capture_allowed": True,
        "manual_capture_status": {"manual_trade_receipt_count": 0},
        "blocked_capabilities": [],
        "missing_or_stale_source_count": 0,
    }
    payload = build_daily_operator_v1(kernel=kernel, operational={}, kernel_paths={}, truth_root=tmp_path, day_utc=DAY)

    assert payload["candidate_workflow"]["awaiting_decision"] >= 1
    assert payload["candidate_workflow"]["next_action"].startswith("Record decisions")
    assert payload["safety"]["broker_submit_required"] is False
    assert payload["safety"]["autonomous_execution_allowed"] is False


def test_daily_operator_and_inbox_show_corrected_latest_state(tmp_path: Path) -> None:
    candidate_id = _seed_triggered_candidate(tmp_path)
    append_candidate_decision_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        candidate_id=candidate_id,
        decision="TRADED_MANUALLY",
        reason="Approved",
        operator="pytest",
        intended_shares=50,
    )
    append_candidate_decision_correction_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        candidate_id=candidate_id,
        field="intended_shares",
        new_value="25",
        reason="Corrected shares",
        operator="pytest",
    )
    kernel = {
        "generated_at_utc": "2026-05-16T12:00:00Z",
        "target_operating_mode": "HUMAN_APPROVED_ADVISORY_RUNTIME",
        "human_approved_advisory_runtime_ready": True,
        "runtime_truth_classification": "REAL_RUNTIME",
        "highest_readiness_layer": "HUMAN_APPROVED_ADVISORY_RUNTIME_READY",
        "advisory_status": "ADVISORY_CANDIDATES_AVAILABLE",
        "manual_trade_capture_allowed": True,
        "manual_capture_status": {"manual_trade_receipt_count": 0},
        "blocked_capabilities": [],
        "missing_or_stale_source_count": 0,
    }
    daily = build_daily_operator_v1(kernel=kernel, operational={}, kernel_paths={}, truth_root=tmp_path, day_utc=DAY)
    inbox = write_operator_inbox_main(["--truth_root", str(tmp_path), "--day", DAY])
    inbox_payload = _read(tmp_path / "reports" / "aegis_operator_inbox_v1" / DAY / "operator_inbox.v1.json")

    assert inbox == 0
    assert daily["candidate_workflow"]["corrected_decisions"] == 1
    assert daily["candidate_workflow"]["latest_intended_shares"] == 25
    assert inbox_payload["candidate_workflow"]["corrected_decisions"] == 1
    assert inbox_payload["candidate_workflow"]["latest_intended_shares"] == 25


def test_audit_handoff_shows_candidate_correction_history(tmp_path: Path) -> None:
    candidate_id = _seed_triggered_candidate(tmp_path)
    append_candidate_decision_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=candidate_id, decision="TRADED_MANUALLY", reason="Approved", operator="pytest", intended_shares=50)
    append_candidate_decision_correction_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=candidate_id, field="intended_shares", new_value="25", reason="Corrected shares", operator="pytest")
    packet = {
        "next_operator_actions": [],
        "current_actionable_items": [],
        "blocked_items": [],
    }
    kernel = {
        "runtime_truth_classification": "REAL_RUNTIME",
        "highest_readiness_layer": "HUMAN_APPROVED_ADVISORY_RUNTIME_READY",
        "target_operating_mode": "HUMAN_APPROVED_ADVISORY_RUNTIME",
        "human_approved_advisory_runtime_ready": True,
        "advisory_status": "NO_ACTIONABLE_CANDIDATES",
        "layers": {},
        "do_not_claim": [],
        "dependency_graph": {},
        "missing_or_stale_source_count": 0,
    }
    transition_path = tmp_path / "runtime_state_transitions.v1.json"
    invalidations_path = tmp_path / "runtime_invalidations.v1.json"
    transition_path.write_text(json.dumps({"summary": {}, "transition_count": 0}), encoding="utf-8")
    invalidations_path.write_text(json.dumps({"invalidation_count": 0}), encoding="utf-8")
    text = build_handoff_text_v1(
        packet=packet,
        packet_path=tmp_path / "packet.json",
        truth_root=tmp_path,
        day_utc=DAY,
        kernel=kernel,
        kernel_paths={
            "runtime_truth_kernel": str(tmp_path / "runtime_truth_kernel.v1.json"),
            "runtime_state_snapshot": str(tmp_path / "runtime_state_snapshot.v1.json"),
            "runtime_state_transitions": str(transition_path),
            "runtime_invalidations": str(invalidations_path),
        },
    )

    assert "CANDIDATE DECISION / CORRECTION HISTORY" in text
    assert '"corrected_decisions": 1' in text
    assert '"current_intended_shares": 25' in text


def test_canonical_operator_state_generates_with_all_inputs_present(tmp_path: Path) -> None:
    _seed_canonical_inputs(tmp_path)

    payload = build_canonical_operator_state_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    paths = write_canonical_operator_state_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)

    assert Path(paths["json"]).exists()
    assert Path(paths["summary"]).exists()
    assert Path(paths["matrix"]).exists()
    assert payload["schema_id"] == "aegis_canonical_operator_state"
    assert payload["missing_inputs"] == []
    assert payload["runtime"]["runtime_truth_classification"] == "REAL_RUNTIME"
    assert payload["candidates"]["awaiting_decision"]
    assert payload["source_artifacts"]
    assert payload["source_hashes"]["runtime_truth"]
    assert payload["safety"]["broker_execution_allowed"] is False
    assert payload["safety"]["autonomous_execution_allowed"] is False


def test_canonical_operator_state_discovers_historical_and_legacy_hypotheses(tmp_path: Path) -> None:
    _seed_canonical_inputs(tmp_path)
    old_day = "2026-05-15"
    canonical_dir = tmp_path / "research_lab" / "research_hypothesis_v1" / old_day / "rh-edge-2026-9999"
    canonical_dir.mkdir(parents=True, exist_ok=True)
    canonical_path = canonical_dir / "research_hypothesis.v1.json"
    canonical_path.write_text(
        json.dumps(
            {
                "schema_id": "research_hypothesis",
                "artifact_id": "research_hypothesis_v1",
                "hypothesis_id": "rh-edge-2026-9999",
                "title": "Historical event reversal hypothesis",
                "hypothesis_summary": "Test reversal after event dislocation.",
                "status": "IDEA",
                "edge_family": "MEAN_REVERSION",
                "source": "MANUAL",
                "created_at_utc": f"{old_day}T12:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    legacy_dir = tmp_path / "research_lab" / "ideas" / "proposed"
    legacy_dir.mkdir(parents=True, exist_ok=True)
    legacy_path = legacy_dir / "EDGE-LEGACY-001.edge_hypothesis.v1.json"
    legacy_path.write_text(
        json.dumps(
            {
                "schema_version": "edge_hypothesis.v1",
                "idea_id": "EDGE-LEGACY-001",
                "hypothesis": "Legacy breadth collapse reversal idea",
                "status": "PROPOSED",
                "edge_type": "EVENT_REVERSION",
                "source": "AI_RESEARCH",
                "created_utc": f"{old_day}T13:00:00Z",
            }
        ),
        encoding="utf-8",
    )

    payload = build_canonical_operator_state_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    hypotheses = payload["research"]["captured_hypotheses"]

    assert {row["hypothesis_id"] for row in hypotheses} >= {"rh-edge-2026-9999"}
    historical = next(row for row in hypotheses if row["hypothesis_id"] == "rh-edge-2026-9999")
    legacy = next(row for row in payload["research"]["needs_classification"] if row["hypothesis_id"] == "EDGE-LEGACY-001")
    assert historical["freshness_status"] == "HISTORICAL"
    assert historical["source_artifact"] == str(canonical_path)
    assert historical["source_hash"]
    assert historical["classification"] == "REAL_OPERATOR_HYPOTHESIS"
    assert legacy["legacy_source"] is True
    assert legacy["orphaned"] is True
    assert legacy["classification"] == "NEEDS_MANUAL_CLASSIFICATION"
    assert payload["research"]["needs_classification"]
    assert str(canonical_path) in payload["source_artifacts"]
    assert any(key.startswith("research_hypothesis:") for key in payload["source_hashes"])
    assert payload["safety"]["broker_execution_allowed"] is False
    assert payload["safety"]["autonomous_execution_allowed"] is False


def test_research_hypothesis_classification_excludes_fixtures_and_links_duplicates(tmp_path: Path) -> None:
    _seed_canonical_inputs(tmp_path)
    old_day = "2026-05-15"
    canonical_dir = tmp_path / "research_lab" / "research_hypothesis_v1" / old_day / "rh-edge-2026-0101"
    canonical_dir.mkdir(parents=True, exist_ok=True)
    canonical_path = canonical_dir / "research_hypothesis.v1.json"
    canonical_path.write_text(
        json.dumps(
            {
                "schema_id": "research_hypothesis",
                "hypothesis_id": "rh-edge-2026-0101",
                "title": "Real edge record",
                "status": "IDEA",
                "source": "MANUAL",
                "created_at_utc": f"{old_day}T12:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    legacy_dir = tmp_path / "research_lab" / "ideas" / "validated"
    legacy_dir.mkdir(parents=True, exist_ok=True)
    legacy_path = legacy_dir / "EDGE-2026-0101.edge_hypothesis.v1.json"
    legacy_path.write_text(
        json.dumps(
            {
                "schema_version": "edge_hypothesis.v1",
                "idea_id": "EDGE-2026-0101",
                "hypothesis": "Legacy duplicate edge record",
                "status": "VALIDATED",
                "source": "AI_RESEARCH",
                "created_utc": f"{old_day}T13:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    fixture_path = tmp_path / "research_lab" / "ideas" / "proposed" / "fixture_idea_v1.edge_hypothesis.v1.json"
    fixture_path.parent.mkdir(parents=True, exist_ok=True)
    fixture_path.write_text(
        json.dumps(
            {
                "schema_version": "edge_hypothesis.v1",
                "idea_id": "fixture_idea_v1",
                "hypothesis": "Mean reversion after large down days",
                "status": "PROPOSED",
                "source": "AI_RESEARCH",
                "created_utc": f"{old_day}T14:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    unknown_path = tmp_path / "research_lab" / "ideas" / "proposed" / "EDGE-ORPHAN-001.edge_hypothesis.v1.json"
    unknown_path.write_text(
        json.dumps(
            {
                "schema_version": "edge_hypothesis.v1",
                "idea_id": "EDGE-ORPHAN-001",
                "hypothesis": "Unknown orphan edge record",
                "status": "PROPOSED",
                "source": "AI_RESEARCH",
                "created_utc": f"{old_day}T15:00:00Z",
            }
        ),
        encoding="utf-8",
    )

    report = build_research_hypothesis_classification_v1(truth_root=tmp_path, day_utc=DAY)
    paths = write_research_hypothesis_classification_v1(truth_root=tmp_path, day_utc=DAY, payload=report)
    assert Path(paths["json"]).exists()
    rows = {str(row["path"]): row for row in report["hypotheses"]}

    assert rows[str(canonical_path)]["classification"] == "REAL_OPERATOR_HYPOTHESIS"
    assert rows[str(legacy_path)]["classification"] == "LEGACY_DUPLICATE"
    assert rows[str(legacy_path)]["duplicate_of"] == "rh-edge-2026-0101"
    assert rows[str(fixture_path)]["classification"] == "TEST_FIXTURE"
    assert rows[str(fixture_path)]["appears_in_research_ui"] is False
    assert rows[str(unknown_path)]["classification"] == "NEEDS_MANUAL_CLASSIFICATION"

    canonical = build_canonical_operator_state_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    research = canonical["research"]
    assert any(row["hypothesis_id"] == "rh-edge-2026-0101" for row in research["active_hypotheses"])
    assert not any(row["hypothesis_id"] == "fixture_idea_v1" for row in research["active_hypotheses"])
    assert any(row["hypothesis_id"] == "fixture_idea_v1" for row in research["test_fixtures_excluded"])
    assert any(row["hypothesis_id"] == "EDGE-2026-0101" for row in research["duplicates"])
    assert any(row["hypothesis_id"] == "EDGE-ORPHAN-001" for row in research["needs_classification"])
    assert any(row["source_hash"] for row in research["test_fixtures_excluded"])
    assert canonical["safety"]["broker_execution_allowed"] is False
    assert canonical["safety"]["autonomous_execution_allowed"] is False


def test_canonical_operator_state_generates_with_missing_inputs_and_labels_them(tmp_path: Path) -> None:
    payload = build_canonical_operator_state_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)

    assert payload["missing_inputs"]
    assert any(row["source"] == "runtime_truth" for row in payload["missing_inputs"])
    assert payload["runtime"]["runtime_truth_classification"] == "UNKNOWN"
    assert any(action["type"] == "MISSING_SOURCE" for action in payload["actions_required"])


def test_partial_runtime_context_is_not_operator_decision_blocker_when_kernel_exists(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "reports" / "aegis_runtime_truth_kernel_v1" / DAY
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "runtime_truth_kernel.v1.json").write_text(
        json.dumps(
            {
                "schema_id": "aegis_runtime_truth_kernel",
                "generated_at_utc": f"{DAY}T12:00:00Z",
                "runtime_truth_classification": "PARTIAL_CONTEXT",
                "highest_readiness_layer": "ADVISORY_ONLY",
                "target_operating_mode": "HUMAN_APPROVED_ADVISORY_RUNTIME",
                "operator_action_required": True,
                "operator_action_reason": "Refresh evidence before relying on trade advice.",
                "broker_submit_required": False,
                "autonomous_execution_allowed": False,
                "trade_advice_allowed": False,
            }
        ),
        encoding="utf-8",
    )
    lifecycle_dir = tmp_path / "reports" / "aegis_candidate_lifecycle_v1" / DAY
    lifecycle_dir.mkdir(parents=True, exist_ok=True)
    (lifecycle_dir / "candidate_lifecycle.v1.json").write_text(json.dumps({"schema_id": "aegis_candidate_lifecycle", "candidates": []}), encoding="utf-8")

    payload = build_canonical_operator_state_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)

    assert payload["runtime"]["runtime_truth_classification"] == "PARTIAL_CONTEXT"
    assert not any(action["type"] == "RUNTIME_REVIEW" for action in payload["actions_required"])
    assert any(row["warning_id"] == "runtime_partial_context" and row["priority"] == "LOW" for row in payload["warnings"])
    assert payload["safety"]["broker_submit_transmit_allowed"] is False


def test_canonical_operator_state_copies_authoritative_values_and_corrections(tmp_path: Path) -> None:
    candidate_id = _seed_canonical_inputs(tmp_path)
    append_candidate_decision_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=candidate_id, decision="TRADED_MANUALLY", reason="Approved", operator="pytest", intended_shares=50, risk_bucket="SMALL")
    append_candidate_decision_correction_v1(truth_root=tmp_path, day_utc=DAY, candidate_id=candidate_id, field="intended_shares", new_value="25", reason="Corrected shares", operator="pytest")

    payload = build_canonical_operator_state_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    candidate = payload["candidates"]["approved_or_traded"][0]

    assert payload["runtime"]["runtime_truth_classification"] == "REAL_RUNTIME"
    assert candidate["current_operator_decision"] == "TRADED_MANUALLY"
    assert candidate["current_intended_shares"] == 25
    assert payload["candidates"]["corrected"][0]["latest_correction_id"]
    assert payload["performance"]["performance_attribution_engine_status"] == "FULLY_IMPLEMENTED"
    assert payload["performance"]["advisory_quality"]


def test_canonical_operator_state_records_conflicts_explicitly(tmp_path: Path) -> None:
    _seed_canonical_inputs(tmp_path)
    ranking_path = tmp_path / "reports" / "aegis_candidate_ranking_v1" / DAY / "candidate_ranking.v1.json"
    ranking = _read(ranking_path)
    ranking["ranked_candidates"].append({"candidate_id": "missing-from-lifecycle", "rank": 1})
    ranking_path.write_text(json.dumps(ranking), encoding="utf-8")

    payload = build_canonical_operator_state_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)

    assert payload["conflicts"]
    assert payload["conflicts"][0]["operator_visibility"] is True
    assert payload["conflicts"][0]["chosen_source"] == "candidate_lifecycle"


def test_operator_brief_reads_canonical_state_only_and_generates_reports(tmp_path: Path) -> None:
    _seed_canonical_inputs(tmp_path)
    canonical = build_canonical_operator_state_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    paths = write_canonical_operator_state_v1(truth_root=tmp_path, day_utc=DAY, payload=canonical)
    brief = build_operator_brief_v1(canonical=canonical, canonical_path=Path(paths["json"]))
    brief_paths = write_operator_brief_v1(truth_root=tmp_path, day_utc=DAY, payload=brief)

    assert Path(brief_paths["json"]).exists()
    assert Path(brief_paths["summary"]).exists()
    assert Path(brief_paths["matrix"]).exists()
    assert brief["reads_only_canonical_operator_state"] is True
    assert brief["canonical_operator_state_path"] == paths["json"]
    assert brief["safety"]["broker_execution_allowed"] is False


def test_canonical_operator_state_is_deterministic_and_does_not_modify_sources(tmp_path: Path) -> None:
    _seed_canonical_inputs(tmp_path)
    source = tmp_path / "reports" / "aegis_candidate_lifecycle_v1" / DAY / "candidate_lifecycle.v1.json"
    before = source.read_bytes()

    first = build_canonical_operator_state_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    second = build_canonical_operator_state_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)

    assert first == second
    assert source.read_bytes() == before


def test_canonical_operator_state_preserves_safety_and_ai_semantics(tmp_path: Path) -> None:
    _seed_canonical_inputs(tmp_path)
    payload = build_canonical_operator_state_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)

    assert payload["ai_used"] is False
    assert payload["deterministic_fallback"] is True
    assert payload["projection_semantics"]["read_only"] is True
    assert payload["projection_semantics"]["may_mutate_runtime_truth"] is False
    assert payload["projection_semantics"]["may_mutate_candidates"] is False
    assert payload["safety"]["broker_submit_transmit_allowed"] is False


def test_feature_completion_audit_recognizes_canonical_state_and_operator_brief(tmp_path: Path) -> None:
    _seed_canonical_inputs(tmp_path)
    canonical = build_canonical_operator_state_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    canonical_paths = write_canonical_operator_state_v1(truth_root=tmp_path, day_utc=DAY, payload=canonical)
    brief = build_operator_brief_v1(canonical=canonical, canonical_path=Path(canonical_paths["json"]))
    write_operator_brief_v1(truth_root=tmp_path, day_utc=DAY, payload=brief)

    payload = build_feature_completion_audit_v1(truth_root=tmp_path, repo_root=REPO_ROOT, day_utc=DAY)
    statuses = {row["feature_name"]: row["status"] for row in payload["features"]}

    assert statuses["Canonical Operator State"] == "FULLY_IMPLEMENTED"
    assert statuses["Operator Brief"] == "FULLY_IMPLEMENTED"


def _seed_triggered_candidate(root: Path) -> str:
    _seed_runtime_truth_ready(root)
    _seed_event_monitor_trigger(root, "PANIC_EXHAUSTION")
    evaluation = build_event_regime_trigger_evaluation_v1(
        truth_root=root,
        repo_root=REPO_ROOT,
        day_utc=DAY,
        triggered_sleeves_enabled=True,
    )
    write_event_regime_trigger_evaluation_v1(truth_root=root, day_utc=DAY, payload=evaluation)
    runs = build_triggered_sleeve_runs_v1(
        truth_root=root,
        repo_root=REPO_ROOT,
        day_utc=DAY,
        trigger_id="event:PANIC_EXHAUSTION",
        sleeve_ids=["PRIMARY"],
        dry_run=True,
    )
    write_triggered_sleeve_runs_v1(truth_root=root, day_utc=DAY, payload=runs)
    assert runs["candidate_count"] == 1
    return runs["runs"][0]["candidate_ids"][0]


def _seed_canonical_inputs(root: Path) -> str:
    candidate_id = _seed_triggered_candidate(root)
    assert run_sleeve_performance_analytics_main(["--truth_root", str(root), "--day", DAY]) == 0
    assert run_regime_outcome_memory_main(["--truth_root", str(root), "--day", DAY]) == 0
    assert run_sleeve_challenger_main(["--truth_root", str(root), "--day", DAY]) == 0
    assert run_research_lab_loop_main(["--truth_root", str(root), "--day", DAY]) == 0
    governance_dir = root / "reports" / "aegis_intelligence_governance_kernel_v1" / DAY
    governance_dir.mkdir(parents=True, exist_ok=True)
    (governance_dir / "intelligence_governance_kernel.v1.json").write_text(
        json.dumps(
            {
                "schema_id": "aegis_intelligence_governance_kernel",
                "generated_at_utc": f"{DAY}T12:00:00Z",
                "recommendations": [
                    {
                        "recommendation_id": "REC-001",
                        "type": "WATCH_SLEEVE",
                        "target": "PRIMARY",
                        "approval_status": "PROPOSED",
                        "human_approval_required": True,
                    }
                ],
                "approval_summary": {"awaiting_approval": 1},
                "ai_usage": {"ai_used": False, "deterministic_fallback": True},
                "safety": {"broker_execution_allowed": False, "autonomous_execution_allowed": False},
            }
        ),
        encoding="utf-8",
    )
    audit_dir = root / "reports" / "aegis_audit_handoff_v1" / DAY
    audit_dir.mkdir(parents=True, exist_ok=True)
    (audit_dir / "aegis_audit_handoff.txt").write_text("canonical_operator_state\noperator_brief\n", encoding="utf-8")
    journal_payload = build_journal_timeline_v1(truth_root=root, day_utc=DAY)
    write_journal_timeline_v1(truth_root=root, day_utc=DAY, payload=journal_payload)
    return candidate_id


def _seed_intelligence_inputs(root: Path) -> None:
    research_dir = root / "research_lab" / "hypotheses" / DAY
    research_dir.mkdir(parents=True, exist_ok=True)
    (research_dir / "research_hypothesis.v1.json").write_text(
        json.dumps(
            {
                "schema_id": "research_hypothesis",
                "hypothesis_id": "HYP-001",
                "title": "Volatility event mean reversion",
                "source": "operator",
                "hypothesis_summary": "Test post-event reversal behavior.",
                "edge_family": "EVENT_REVERSION",
                "failure_conditions": ["No edge after costs"],
            }
        ),
        encoding="utf-8",
    )
    queue_dir = root / "research_lab" / "research_task_queue_v1" / DAY
    queue_dir.mkdir(parents=True, exist_ok=True)
    (queue_dir / "research_task_queue.v1.json").write_text(
        json.dumps({"schema_id": "research_task_queue", "tasks": [{"task_id": "TASK-001", "hypothesis_id": "HYP-001", "status": "QUEUED", "task_type": "BACKTEST"}]}),
        encoding="utf-8",
    )
    sleeve_dir = root / "reports" / "sleeve_performance_report_v1" / DAY
    sleeve_dir.mkdir(parents=True, exist_ok=True)
    (sleeve_dir / "sleeve_performance_report.v1.json").write_text(
        json.dumps({"schema_id": "sleeve_performance_report", "trade_rows": [{"sleeve_id": "core", "return_pct": 0.02, "outcome_status": "WIN", "drawdown": -0.01}]}),
        encoding="utf-8",
    )
    receipt_dir = root / "manual_trade_receipts" / DAY
    receipt_dir.mkdir(parents=True, exist_ok=True)
    (receipt_dir / "manual_trade_receipt.v1.json").write_text(
        json.dumps({"schema_id": "manual_trade_receipt", "receipt_id": "R-001", "strategy_or_sleeve": "core", "broker_submission_by_aegis": False, "autonomous_execution": False}),
        encoding="utf-8",
    )


def _seed_governed_intelligence_inputs(root: Path) -> None:
    _seed_intelligence_inputs(root)
    _seed_event_validity_no_packet(root)
    assert run_regime_context_main(["--truth_root", str(root), "--day", DAY]) == 0
    assert run_sleeve_performance_analytics_main(["--truth_root", str(root), "--day", DAY]) == 0
    assert run_failure_analysis_main(["--truth_root", str(root), "--day", DAY]) == 0
    assert run_research_memory_graph_main(["--truth_root", str(root), "--day", DAY]) == 0
    assert run_research_prioritizer_main(["--truth_root", str(root), "--day", DAY]) == 0
    assert run_research_queue_optimizer_main(["--truth_root", str(root), "--day", DAY]) == 0
    assert run_cross_sleeve_analysis_main(["--truth_root", str(root), "--day", DAY]) == 0
    assert run_adaptive_governance_main(["--truth_root", str(root), "--day", DAY]) == 0


def _seed_event_validity_no_packet(root: Path) -> None:
    event_dir = root / "reports" / "event_validity_gate_v1" / DAY / "run"
    event_dir.mkdir(parents=True, exist_ok=True)
    (event_dir / "event_validity_gate.v1.json").write_text(
        json.dumps(
            {
                "schema_id": "event_validity_gate",
                "generated_at": "2026-05-16T12:00:00Z",
                "day_utc": DAY,
                "evaluated": True,
                "event_packet_present": False,
                "event_packet_path": None,
                "validity_status": "NO_EVENT_PACKET",
                "reason": "No event packet exists.",
            }
        ),
        encoding="utf-8",
    )


def _seed_runtime_truth_ready(root: Path) -> None:
    report_dir = root / "reports" / "aegis_runtime_truth_kernel_v1" / DAY
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "runtime_truth_kernel.v1.json").write_text(
        json.dumps(
            {
                "schema_id": "aegis_runtime_truth_kernel",
                "runtime_truth_classification": "REAL_RUNTIME",
                "human_approved_advisory_runtime_ready": True,
                "target_operating_mode": "HUMAN_APPROVED_ADVISORY_RUNTIME",
                "broker_submit_required": False,
                "autonomous_execution_allowed": False,
            }
        ),
        encoding="utf-8",
    )


def _seed_event_monitor_trigger(root: Path, event_type: str) -> None:
    event_dir = root / "reports" / "event_monitoring_status_v1" / DAY / "run"
    event_dir.mkdir(parents=True, exist_ok=True)
    (event_dir / "event_monitoring_status.v1.json").write_text(
        json.dumps(
            {
                "schema_id": "event_monitoring_status",
                "monitor_run_id": f"event-monitor:{DAY}:pytest",
                "day_utc": DAY,
                "timestamp_utc": "2026-05-16T12:00:00Z",
                "event_rule_ids_evaluated": [f"event_rule:{event_type}:v1"],
                "thresholds_evaluated": ["pytest_threshold PASS"],
                "triggered_events": [f"event:{event_type}:{DAY}:pytest"],
                "blocked_events": [],
                "tactical_packets_created": [],
                "market_snapshot_freshness_status": "FRESH",
                "broker_submit_required": False,
                "runtime_mutation_allowed": False,
            }
        ),
        encoding="utf-8",
    )


def _seed_triggered_run_history(root: Path, *, trigger_id: str, count: int) -> None:
    report_dir = root / "reports" / "aegis_triggered_sleeve_runs_v1" / DAY
    report_dir.mkdir(parents=True, exist_ok=True)
    runs = []
    for idx in range(count):
        runs.append(
            {
                "triggered_run_id": f"history:{idx}",
                "trigger_id": trigger_id,
                "trigger_type": "EVENT",
                "detected_condition": trigger_id.split(":", 1)[-1],
                "selected_sleeve_ids": ["PRIMARY"],
                "command_executed": "pytest",
                "output_artifacts": [],
                "candidate_count": 1,
                "status": "SUCCESS",
                "safety": {
                    "advisory_only": True,
                    "broker_execution_allowed": False,
                    "autonomous_execution_allowed": False,
                    "broker_submit_transmit_called": False,
                },
            }
        )
    (report_dir / "triggered_sleeve_runs.v1.json").write_text(
        json.dumps({"schema_id": "aegis_triggered_sleeve_runs", "runs": runs}),
        encoding="utf-8",
    )


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))
