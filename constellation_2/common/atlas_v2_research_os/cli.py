from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

from .artifact_store import ArtifactStore, DEFAULT_STORE_ROOT
from .backlog_seed_reports import build_backlog_seeding_report, write_seed_report
from .backlog_seeding import generate_seed_backlog_items
from .backlog_seeding_models import BacklogSeedProfile
from .autonomous_research_execution import audit_bounded_research_execution, dry_run_bounded_research, run_bounded_research_once
from .autonomous_research_reports import write_autonomous_research_execution_report
from .candidate_quality_baseline import QUALITY_ROOT, create_candidate_quality_baseline
from .candidate_quality_reports import audit_candidate_quality_reports, write_candidate_quality_report
from .candidate_quality_treatment import create_candidate_quality_treatment
from .certification_reports import build_research_os_certification_report, write_research_os_certification_report
from .artifact_models import ArtifactType
from .governance import validate_no_forbidden_artifacts
from .lineage import validate_lineage_integrity
from .priority_engine import PriorityEngine
from .reports import build_foundation_report, write_foundation_report
from .orchestrator import audit_only as orchestrator_audit_only, dry_run as orchestrator_dry_run, run_once as orchestrator_run_once
from .overnight_research_review import build_overnight_research_summary, dry_run_overnight_research_review, run_overnight_research_review, write_overnight_research_review_report
from .failure_patterns import get_repeated_failures, increment_failure_repetition, list_failure_patterns, record_failure_pattern
from .failure_observatory import list_unresolved_failures
from .failure_observatory_governance import validate_failure_observatory_allowed
from .failure_observatory_reports import summarize_failures_for_day, write_failure_report
from .mechanism_clusters import create_mechanism_cluster, list_mechanism_clusters
from .mechanism_hypothesis_generator import generate_mechanism_search_1000, generate_mechanism_search_small
from .mechanism_search_reports import write_mechanism_search_report
from .mechanism_deep_trial import run_mechanism_deep_trial, write_mechanism_deep_trial_report
from .memory_index import add_memory_object, list_memory_objects, memory_root, validate_memory_integrity
from .memory_models import MemoryEvidenceMaturity, MemoryType, create_memory_object
from .memory_reports import write_memory_report
from .regime_context import create_regime_context, list_regime_contexts
from .semantic_deduplication import find_potential_duplicates
from .research_backlog import ResearchBacklog
from .worker_adapters import register_default_worker_adapters
from .worker_registry import list_workers
from .worker_reports import build_worker_connection_report, build_worker_interface_report, write_worker_connection_report, write_worker_interface_report
from .worker_connection_audit import build_worker_connection_audit, write_worker_connection_audit
from .worker_execution_records import write_worker_execution_report
from .learning_feedback_engine import run_learning_feedback_demo
from .learning_feedback_reports import audit_learning_feedback, write_learning_feedback_report
from .family_learning_engine import run_family_learning
from .learning_validation_reports import audit_learning_validation_report, build_learning_validation_report, write_learning_validation_report
from .paper_trade_test_plan import create_paper_trade_test_plan
from .paper_trading_queue import approve_for_paper_test, enqueue_paper_trade_candidate
from .paper_trading_queue_reports import audit_paper_trading_queue_report, build_paper_trading_queue_report, write_paper_trading_queue_report
from .historical_replay_engine import create_historical_replay_request, run_historical_replay
from .historical_replay_reports import audit_historical_replay_report, demo_historical_replay_results, write_historical_replay_report
from .historical_replay_results import route_historical_replay_backlog_items
from .paper_forward_outcome_reports import audit_paper_forward_outcome_report, demo_paper_forward_outcomes, write_paper_forward_outcome_report
from .throughput_review import run_throughput_repair_and_review
from .methodology_evaluation import run_methodology_evaluation
from .methodology_trial import run_methodology_trial_100
from .bulk_observation_import import import_observations, seed_demo_observations
from .observation_import_reports import audit_observation_import_report, write_observation_import_report
from .paper_forward_observation_reports import write_paper_forward_observation_report
from .candidate_observation_readiness import write_candidate_observation_readiness_report
from .observation_cluster_splitting import run_observation_cluster_split_experiment
from .candidate_backtests import run_candidate_backtest_report
from .candidate_family_discovery import run_candidate_family_discovery
from .replay_sample_yield import run_replay_sample_yield
from .paper_forward_campaign import write_paper_forward_campaign_report
from .observation_source_breakdown import write_observation_source_breakdown_report
from .backtest_aware_final_qualification import run_backtest_aware_final_qualification
from .final_candidate_ranking import run_final_candidate_ranking
from .candidate_data_validation_plan import write_candidate_data_validation_plan, write_paper_forward_approval_checklist
from .candidate_symbol_attribution import run_candidate_symbol_attribution
from .direct_candidate_data_validation import run_direct_candidate_data_validation
from .direct_replay_zero_sample_diagnosis import run_direct_replay_zero_sample_diagnosis
from .direct_replay_attrition_audit import run_direct_replay_attrition_audit
from .portfolio_relevance_estimate import run_portfolio_relevance_estimate
from .holdout_aware_family_ranking import run_holdout_aware_family_ranking
from .family_stability_analysis import run_family_stability_analysis
from .manual_fundamental_claim_intake import run_manual_pegy_claim_intake
from .pegy_ratio_spec import run_pegy_ratio_spec_report
from .market_data_readiness import run_market_data_readiness
from .local_market_data_import import validate_local_market_data, run_market_data_import_report
from .market_data_coverage_report import run_market_data_coverage_report
from .market_data_acquisition_plan import run_market_data_acquisition_dry_run, run_market_data_acquisition_plan
from .safe_historical_intraday_downloader import download_intraday_csv_if_explicitly_enabled, dry_run_intraday_download
from .manual_intraday_csv_intake import run_manual_intraday_csv_intake
from .manual_intraday_data_sourcing_pack import write_manual_intraday_data_sourcing_pack
from .priority1_intraday_file_creation import run_priority1_intraday_file_creation
from .tiingo_intraday_diagnostic import run_tiingo_intraday_diagnostic
from .research_adversary_review import build_research_adversary_review, write_research_adversary_review
from .research_adversary_evaluation import build_research_adversary_evaluation, write_research_adversary_evaluation_report
from .expanded_search_trial import write_expanded_search_trial_report
from .expanded_search_narrowing import run_expanded_search_narrowing
from .expanded_search_gate_audit import write_expanded_search_gate_audit_report
from .focused_expanded_search_trial import write_focused_expanded_search_trial_report
from .search_overfit_guardrail import run_search_overfit_guardrail
from .holdout_replay_validation import run_holdout_replay_validation
from .holdout_readiness_audit import run_holdout_readiness_audit
from .holdout_event_row_builder_design import run_holdout_event_row_builder_design
from .holdout_event_row_materializer import run_holdout_event_row_materializer
from .holdout_event_outcome_backfill_plan import run_holdout_event_outcome_backfill_plan
from .holdout_event_backfill_materializer import run_holdout_event_backfill_materializer
from .holdout_reconstruction_from_databento import run_holdout_reconstruction_from_databento
from .holdout_event_row_readiness_recheck import run_holdout_event_row_readiness_recheck
from .holdout_readiness_after_backfill import run_holdout_readiness_after_backfill
from .holdout_replay_dry_run import run_holdout_replay_dry_run
from .holdout_replay import run_holdout_replay
from .governed_holdout_outcome_data_acquisition_manifest import run_governed_holdout_outcome_data_acquisition_manifest
from .holdout_data_feasibility_audit import run_holdout_data_feasibility_audit
from .confirmed_reversal_family_expansion import run_confirmed_reversal_family_expansion
from .reversal_trending_exact_coverage_plan import run_reversal_trending_exact_coverage_plan
from .exact_coverage_import_specification import run_exact_coverage_import_specification
from .exact_replay_without_fallback import run_exact_replay_without_fallback
from .exact_coverage_import_validator import run_exact_coverage_import_validator
from .exact_intraday_data_acquisition_manifest import run_exact_intraday_data_acquisition_manifest
from .exact_data_repair_loop import run_exact_data_repair_loop
from .net_of_cost_evidence import run_net_of_cost_evidence
from .cost_robustness_expansion import run_cost_robustness_expansion
from .mechanism_expansion_program import run_mechanism_expansion_program
from .mechanism_survivor_audit import run_mechanism_survivor_audit
from .controlled_surface_expansion_gate import run_controlled_surface_expansion_gate
from .execution_realism_economic_viability import run_execution_realism_economic_viability
from .execution_cost_failure_analysis import run_execution_cost_failure_analysis
from .final_evidence_synthesis import run_final_evidence_synthesis
from .evidence_review_board import run_evidence_review_board
from .verified_runtime_graph_evidence_coverage_repair import run_verified_runtime_graph_evidence_coverage_repair
from .forward_observation_starter import run_forward_observation_starter
from .forward_observation_scoreboard import run_forward_observation_scoreboard
from .forward_observation_outcome_measurement import run_forward_observation_outcome_measurement
from .forward_observation_loop import run_forward_observation_loop
from .final_research_verdict import run_final_research_verdict
from .timeframe_neighborhood_expansion import run_timeframe_neighborhood_expansion
from .temporal_robustness_decay import run_temporal_robustness_decay
from .trade_readiness_gate import run_trade_readiness_gate
from .generalization_edge_magnitude_assessment import run_generalization_edge_magnitude_assessment
from .narrow_edge_deep_dive import run_narrow_edge_deep_dive
from .holdout_final_attempt import run_holdout_final_attempt
from .reversal_neighborhood_expansion import run_reversal_neighborhood_expansion
from .large_cap_growth_expansion import run_large_cap_growth_expansion
from .tsla_full_surface_expansion import run_tsla_full_surface_expansion
from .walk_forward_validation import run_walk_forward_validation
from .expansion_validation_survivor_density import run_expansion_validation_survivor_density
from .regime_expansion import write_regime_expansion_report
from .regime_vocabulary_bridge import run_regime_vocabulary_bridge_report
from .regime_boundary_expansion import run_regime_boundary_expansion
from .volatility_profile_expansion import run_volatility_profile_expansion
from .null_model_randomized_control import run_null_model_randomized_control
from .evidence_lineage_graph import write_evidence_lineage_graph
from .determinism_repair_audit import write_determinism_repair_audit

NOW = "2026-06-04T00:00:00Z"


def _main_research_adversary_review(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Generate offline Research Adversary review artifacts")
    parser.add_argument("--observation", action="append", type=Path, default=[])
    parser.add_argument("--claim", action="append", type=Path, default=[])
    parser.add_argument("--hypothesis", action="append", type=Path, default=[])
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    report = build_research_adversary_review(
        observations=args.observation,
        claims=args.claim,
        hypotheses=args.hypothesis,
        generated_at=NOW,
    )
    paths = write_research_adversary_review(report, args.out)
    print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
    return 0


def _main_research_adversary_evaluation(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Evaluate Research Adversary review artifacts without authority expansion")
    parser.add_argument("--reviews", type=Path, required=True)
    parser.add_argument("--failures", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    report = build_research_adversary_evaluation(reviews=args.reviews, failures=args.failures, created_at=NOW)
    paths = write_research_adversary_evaluation_report(report, args.out)
    print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
    return 0


def seed_demo_backlog(root: Path) -> dict[str, object]:
    store = ArtifactStore(root)
    backlog = ResearchBacklog(root)
    if not store.exists("q-demo-opening-range"):
        store.create_artifact(
            artifact_id="q-demo-opening-range",
            artifact_type=ArtifactType.QUESTION.value,
            created_at=NOW,
            created_by="atlas_research_os_cli",
            confidence=0.4,
            evidence_level="GENERATED_ONLY",
            labels=["generated"],
            metadata={"question": "Does opening range segmentation improve research learning?"},
            is_root=True,
        )
    if not backlog.get("bi-demo-opening-range", required=False):
        backlog.create_backlog_item(
            backlog_item_id="bi-demo-opening-range",
            item_type="RESEARCH_QUESTION",
            title="Review opening range segmentation evidence",
            description="Foundation demo backlog item only; no autonomous run.",
            created_at=NOW,
            created_by="atlas_research_os_cli",
            source_artifact_ids=["q-demo-opening-range"],
            state="READY",
            expected_learning_value=0.6,
            novelty_score=0.2,
            evidence_gap_score=0.3,
            cost_estimate=0.1,
        )
    return {"seeded": True, "root": str(root)}



def seed_demo_memory(root: Path) -> dict[str, object]:
    store = ArtifactStore(root)
    if not store.exists("q-demo-memory-opening-range"):
        store.create_artifact(
            artifact_id="q-demo-memory-opening-range",
            artifact_type=ArtifactType.QUESTION.value,
            created_at=NOW,
            created_by="atlas_research_os_cli",
            confidence=0.4,
            evidence_level="GENERATED_ONLY",
            labels=["generated"],
            metadata={"question": "Opening range claims may need volatility context."},
            is_root=True,
        )
    if not any(row["regime_context_id"] == "regime-demo-opening" for row in list_regime_contexts(root)):
        create_regime_context(root=root, regime_context_id="regime-demo-opening", labels=["OPENING_SESSION", "UNKNOWN"], description="Explicit demo context; not validation.", confidence=0.2, source_artifact_ids=["q-demo-memory-opening-range"], created_at=NOW)
    existing_memory_ids = {row["memory_id"] for row in list_memory_objects(root)}
    if "mem-demo-opening-range" not in existing_memory_ids:
        add_memory_object(create_memory_object(memory_id="mem-demo-opening-range", memory_type=MemoryType.CLAIM_CLUSTER.value, created_at=NOW, source_artifact_ids=["q-demo-memory-opening-range"], mechanism_tags=["OPENING_RANGE"], regime_context_ids=["regime-demo-opening"], evidence_level=MemoryEvidenceMaturity.GENERATED_ONLY.value, confidence=0.3, labels=["generated_only"], metadata={"canonical_text": "Opening range claims may need volatility context."}), root, artifact_store=store)
    if "mem-demo-opening-range-variant" not in existing_memory_ids:
        add_memory_object(create_memory_object(memory_id="mem-demo-opening-range-variant", memory_type=MemoryType.CLAIM_CLUSTER.value, created_at=NOW, source_artifact_ids=["q-demo-memory-opening-range"], mechanism_tags=["OPENING_RANGE"], regime_context_ids=["regime-demo-opening"], evidence_level=MemoryEvidenceMaturity.GENERATED_ONLY.value, confidence=0.3, labels=["generated_only"], metadata={"canonical_text": "Opening drive claims may need volatility context."}), root, artifact_store=store)
    if not any(row["cluster_id"] == "cluster-demo-opening-range" for row in list_mechanism_clusters(root)):
        create_mechanism_cluster(root=root, cluster_id="cluster-demo-opening-range", mechanism_tags=["OPENING_RANGE"], name="Opening Range", description="Demo research cluster only.", source_artifact_ids=["q-demo-memory-opening-range"], created_at=NOW)
    if not any(row["failure_id"] == "failure-demo-opening-range" for row in list_failure_patterns(root)):
        failure = record_failure_pattern(root=root, failure_id="failure-demo-opening-range", failure_type="REGIME_MISMATCH", source_artifact_ids=["q-demo-memory-opening-range"], mechanism_tags=["OPENING_RANGE"], regime_context_ids=["regime-demo-opening"], reason="Demo failure memory; not validation.", evidence_level=MemoryEvidenceMaturity.GENERATED_ONLY.value, first_seen_at=NOW)
        increment_failure_repetition(root, failure["failure_id"], seen_at=NOW)
    retired_path = memory_root(root) / "retired_knowledge.json"
    retired_path.parent.mkdir(parents=True, exist_ok=True)
    if not retired_path.exists():
        retired_path.write_text("[]\n", encoding="utf-8")
    return {"seeded": True, "root": str(root)}


def seed_demo_candidate_quality(root: Path = QUALITY_ROOT) -> dict[str, object]:
    window = {"start": "2026-06-01", "end": "2026-06-04", "mode": "FIXED_REPLAY"}
    try:
        baseline = create_candidate_quality_baseline(
            root=root,
            baseline_id="cq-baseline-demo",
            created_at=NOW,
            created_by="atlas_research_os_cli",
            source_artifact_ids=["q-demo-memory-opening-range"],
            raw_signals=100,
            generated_candidates=20,
            rejected_candidates=55,
            gate_suppressions=25,
            portfolio_scoring_rejections=12,
            portfolio_scoring_passes=8,
            evidence_levels=["HISTORICAL_REPLAY", "PAPER_FORWARD_OBSERVATION"],
            hypotheses_tested=10,
            hypotheses_not_falsified=5,
            repeated_failures=10,
            failure_categories=["REGIME_MISMATCH"] * 6 + ["DUPLICATE_SIGNAL"] * 4,
            measurement_window=window,
            signal_universe_id="demo-fixed-replay-universe",
            candidate_factory_version="candidate-factory-demo-v1",
        )
    except Exception:
        from .candidate_quality_baseline import load_candidate_quality_baseline
        baseline = load_candidate_quality_baseline("cq-baseline-demo", root)
    try:
        create_candidate_quality_treatment(
            root=root,
            treatment_id="cq-treatment-demo",
            created_at=NOW,
            created_by="atlas_research_os_cli",
            source_artifact_ids=["q-demo-memory-opening-range"],
            raw_signals=100,
            generated_candidates=28,
            rejected_candidates=45,
            gate_suppressions=20,
            portfolio_scoring_rejections=8,
            portfolio_scoring_passes=14,
            evidence_levels=["HISTORICAL_REPLAY", "PAPER_FORWARD_OBSERVATION"],
            hypotheses_tested=10,
            hypotheses_not_falsified=7,
            repeated_failures=6,
            failure_categories=["REGIME_MISMATCH"] * 3 + ["DUPLICATE_SIGNAL"] * 3,
            measurement_window=window,
            signal_universe_id="demo-fixed-replay-universe",
            candidate_factory_version="candidate-factory-demo-v1",
            learning_input_ids=["mem-demo-opening-range"],
            research_os_memory_ids=["mem-demo-opening-range"],
            baseline=baseline,
        )
    except Exception:
        pass
    return {"seeded": True, "root": str(root), "baseline_id": "cq-baseline-demo", "treatment_id": "cq-treatment-demo"}


def demo_learning_validation_snapshots() -> list[dict[str, object]]:
    return [
        {
            "snapshot_id": "learning-validation-demo-before",
            "observed_at": "2026-06-01T00:00:00Z",
            "repeated_failures": 10,
            "duplicate_ideas": 6,
            "regime_gaps": 4,
            "evidence_maturity_score": 0.25,
            "hypothesis_survival_rate": 0.40,
            "candidate_quality_score": 0.45,
            "metadata": {"measurement_only": True, "authority": "NONE"},
        },
        {
            "snapshot_id": "learning-validation-demo-after",
            "observed_at": "2026-06-05T00:00:00Z",
            "repeated_failures": 6,
            "duplicate_ideas": 3,
            "regime_gaps": 2,
            "evidence_maturity_score": 0.55,
            "hypothesis_survival_rate": 0.62,
            "candidate_quality_score": 0.67,
            "metadata": {"measurement_only": True, "authority": "NONE"},
        },
    ]


def seed_demo_paper_trading_queue(root: Path) -> dict[str, object]:
    plan = create_paper_trade_test_plan(
        test_plan_id="paper-plan-demo-opening-range",
        candidate_id="paper-candidate-demo-opening-range",
        created_at=NOW,
        mechanism_tags=["OPENING_RANGE", "VOLATILITY_CONTEXT"],
        regime_context="OPENING_SESSION_LOW_VOL",
        entry_condition_description="Observe paper entry only after the research signal crosses its fixed threshold in the paper window.",
        exit_condition_description="Observe paper exit only at mean reversion, invalidation, or observation-window expiry.",
        invalidating_conditions=["Regime context becomes UNKNOWN", "Required observation data is unavailable"],
        paper_observation_window={"days": 30, "mode": "PAPER_OBSERVATION_ONLY"},
        minimum_sample_size=20,
        success_metrics=["win_rate", "expectancy", "sample_size", "hypothesis_survival"],
        failure_metrics=["max_drawdown", "sample_size", "regime_specific_performance"],
        risk_notes=["No real-capital sizing; paper observation only."],
        expected_failure_modes=["Regime mismatch", "Duplicate signal decay"],
        source_artifact_ids=["q-demo-memory-opening-range"],
        human_review_required=True,
        metadata={"demo": True},
    )
    item = enqueue_paper_trade_candidate(
        candidate_id=plan["candidate_id"],
        test_plan=plan,
        queue_item_id="paper-queue-demo-opening-range",
        priority=0.75,
        created_at=NOW,
        root=root,
        metadata={"demo": True},
    )
    return {"seeded": True, "test_plan": plan, "queue_item": item}


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "research-adversary-review":
        return _main_research_adversary_review(argv[1:])
    if argv and argv[0] == "research-adversary-evaluation":
        return _main_research_adversary_evaluation(argv[1:])

    parser = argparse.ArgumentParser(description="Atlas V2 Research OS foundation CLI")
    parser.add_argument("--root", type=Path, default=DEFAULT_STORE_ROOT)
    parser.add_argument("--audit", action="store_true")
    parser.add_argument("--report", action="store_true")
    parser.add_argument("--seed-demo-backlog", action="store_true")
    parser.add_argument("--select-next", action="store_true")
    parser.add_argument("--memory-audit", action="store_true")
    parser.add_argument("--memory-report", action="store_true")
    parser.add_argument("--seed-demo-memory", action="store_true")
    parser.add_argument("--find-duplicates", action="store_true")
    parser.add_argument("--list-repeated-failures", action="store_true")
    parser.add_argument("--failure-report", action="store_true")
    parser.add_argument("--failure-summary", action="store_true")
    parser.add_argument("--list-unresolved-failures", action="store_true")
    parser.add_argument("--failure-audit", action="store_true")
    parser.add_argument("--candidate-quality-report", action="store_true")
    parser.add_argument("--seed-demo-candidate-quality", action="store_true")
    parser.add_argument("--candidate-quality-audit", action="store_true")
    parser.add_argument("--orchestrator-run-once", action="store_true")
    parser.add_argument("--orchestrator-dry-run", action="store_true")
    parser.add_argument("--orchestrator-audit-only", action="store_true")
    parser.add_argument("--autonomous-research-run-once", action="store_true")
    parser.add_argument("--autonomous-research-dry-run", action="store_true")
    parser.add_argument("--autonomous-research-audit", action="store_true")
    parser.add_argument("--autonomous-research-report", action="store_true")
    parser.add_argument("--overnight-research-review", action="store_true")
    parser.add_argument("--overnight-research-dry-run", action="store_true")
    parser.add_argument("--overnight-research-summary", action="store_true")
    parser.add_argument("--research-os-certify", action="store_true")
    parser.add_argument("--research-os-certification-report", action="store_true")
    parser.add_argument("--worker-audit", action="store_true")
    parser.add_argument("--worker-report", action="store_true")
    parser.add_argument("--worker-connection-audit", action="store_true")
    parser.add_argument("--worker-connection-report", action="store_true")
    parser.add_argument("--worker-execution-report", action="store_true")
    parser.add_argument("--list-workers", action="store_true")
    parser.add_argument("--learning-feedback-demo", action="store_true")
    parser.add_argument("--learning-feedback-report", action="store_true")
    parser.add_argument("--learning-feedback-audit", action="store_true")
    parser.add_argument("--family-learning-report", action="store_true")
    parser.add_argument("--learning-validation-report", action="store_true")
    parser.add_argument("--learning-validation-audit", action="store_true")
    parser.add_argument("--learning-validation-horizon", default="30_day", choices=["7_day", "30_day", "90_day", "lifetime"])
    parser.add_argument("--paper-trading-queue-demo", action="store_true")
    parser.add_argument("--paper-trading-queue-report", action="store_true")
    parser.add_argument("--paper-trading-queue-audit", action="store_true")
    parser.add_argument("--paper-trading-queue-demo-approve", action="store_true")
    parser.add_argument("--seed-research-backlog", action="store_true")
    parser.add_argument("--seed-research-backlog-small", action="store_true")
    parser.add_argument("--seed-research-backlog-deep", action="store_true")
    parser.add_argument("--backlog-seeding-report", action="store_true")
    parser.add_argument("--throughput-review", action="store_true")
    parser.add_argument("--methodology-evaluation", action="store_true")
    parser.add_argument("--methodology-trial-100", action="store_true")
    parser.add_argument("--paper-forward-observation-report", action="store_true")
    parser.add_argument("--candidate-observation-readiness-report", action="store_true")
    parser.add_argument("--candidate-backtest-report", action="store_true")
    parser.add_argument("--candidate-family-discovery", action="store_true")
    parser.add_argument("--expanded-search-trial", action="store_true")
    parser.add_argument("--expanded-search-narrowing", action="store_true")
    parser.add_argument("--expanded-search-gate-audit", action="store_true")
    parser.add_argument("--focused-expanded-search-trial", action="store_true")
    parser.add_argument("--search-overfit-guardrail", action="store_true")
    parser.add_argument("--holdout-replay-validation", action="store_true")
    parser.add_argument("--holdout-readiness-audit", action="store_true")
    parser.add_argument("--holdout-event-row-builder-design", action="store_true")
    parser.add_argument("--holdout-event-row-materializer", action="store_true")
    parser.add_argument("--holdout-event-outcome-backfill-plan", action="store_true")
    parser.add_argument("--holdout-event-backfill-materializer", action="store_true")
    parser.add_argument("--holdout-reconstruction-from-databento", action="store_true")
    parser.add_argument("--holdout-event-row-readiness-recheck", action="store_true")
    parser.add_argument("--holdout-readiness-after-backfill", action="store_true")
    parser.add_argument("--holdout-replay-dry-run", action="store_true")
    parser.add_argument("--holdout-replay", action="store_true")
    parser.add_argument("--governed-holdout-outcome-data-acquisition-manifest", action="store_true")
    parser.add_argument("--holdout-data-feasibility-audit", action="store_true")
    parser.add_argument("--confirmed-reversal-family-expansion", action="store_true")
    parser.add_argument("--reversal-trending-exact-coverage-plan", action="store_true")
    parser.add_argument("--exact-coverage-import-specification", action="store_true")
    parser.add_argument("--exact-data-repair-loop", action="store_true")
    parser.add_argument("--exact-replay-without-fallback", action="store_true")
    parser.add_argument("--exact-coverage-import-validator", action="store_true")
    parser.add_argument("--exact-intraday-data-acquisition-manifest", action="store_true")
    parser.add_argument("--net-of-cost-evidence", action="store_true")
    parser.add_argument("--cost-robustness-expansion", action="store_true")
    parser.add_argument("--mechanism-expansion-program", action="store_true")
    parser.add_argument("--mechanism-survivor-audit", action="store_true")
    parser.add_argument("--controlled-surface-expansion-gate", action="store_true")
    parser.add_argument("--final-evidence-synthesis", action="store_true")
    parser.add_argument("--execution-realism-economic-viability", action="store_true")
    parser.add_argument("--execution-cost-failure-analysis", action="store_true")
    parser.add_argument("--evidence-review-board", action="store_true")
    parser.add_argument("--verified-runtime-graph-evidence-coverage-repair", action="store_true")
    parser.add_argument("--forward-observation-starter", action="store_true")
    parser.add_argument("--forward-observation-scoreboard", action="store_true")
    parser.add_argument("--forward-observation-outcome-measurement", action="store_true")
    parser.add_argument("--forward-observation-loop", action="store_true")
    parser.add_argument("--final-research-verdict", action="store_true")
    parser.add_argument("--timeframe-neighborhood-expansion", action="store_true")
    parser.add_argument("--temporal-robustness-decay", action="store_true")
    parser.add_argument("--trade-readiness-gate", action="store_true")
    parser.add_argument("--generalization-edge-magnitude-assessment", action="store_true")
    parser.add_argument("--narrow-edge-deep-dive", action="store_true")
    parser.add_argument("--holdout-final-attempt", action="store_true")
    parser.add_argument("--reversal-neighborhood-expansion", action="store_true")
    parser.add_argument("--large-cap-growth-expansion", action="store_true")
    parser.add_argument("--tsla-full-surface-expansion", action="store_true")
    parser.add_argument("--walk-forward-validation", action="store_true")
    parser.add_argument("--expansion-validation-survivor-density", action="store_true")
    parser.add_argument("--regime-expansion-report", action="store_true")
    parser.add_argument("--regime-vocabulary-bridge-report", action="store_true")
    parser.add_argument("--regime-boundary-expansion", action="store_true")
    parser.add_argument("--volatility-profile-expansion", action="store_true")
    parser.add_argument("--null-model-randomized-control", action="store_true")
    parser.add_argument("--evidence-lineage-graph", action="store_true")
    parser.add_argument("--determinism-repair-audit", action="store_true")
    parser.add_argument("--replay-sample-yield", action="store_true")
    parser.add_argument("--paper-forward-campaign-report", action="store_true")
    parser.add_argument("--observation-source-breakdown-report", action="store_true")
    parser.add_argument("--observation-cluster-split-experiment", action="store_true")
    parser.add_argument("--backtest-aware-final-qualification", action="store_true")
    parser.add_argument("--final-candidate-ranking", action="store_true")
    parser.add_argument("--candidate-data-validation-plan", action="store_true")
    parser.add_argument("--candidate-symbol-attribution", action="store_true")
    parser.add_argument("--paper-forward-approval-checklist", action="store_true")
    parser.add_argument("--direct-candidate-data-validation", action="store_true")
    parser.add_argument("--direct-replay-zero-sample-diagnosis", action="store_true")
    parser.add_argument("--direct-replay-attrition-audit", action="store_true")
    parser.add_argument("--portfolio-relevance-estimate", action="store_true")
    parser.add_argument("--holdout-aware-family-ranking", action="store_true")
    parser.add_argument("--family-stability-analysis", action="store_true")
    parser.add_argument("--manual-pegy-claim-intake", action="store_true")
    parser.add_argument("--pegy-ratio-spec-report", action="store_true")
    parser.add_argument("--market-data-readiness", action="store_true")
    parser.add_argument("--market-data-import-report", action="store_true")
    parser.add_argument("--market-data-coverage-report", action="store_true")
    parser.add_argument("--validate-local-market-data", action="store_true")
    parser.add_argument("--market-data-acquisition-plan", action="store_true")
    parser.add_argument("--market-data-acquisition-dry-run", action="store_true")
    parser.add_argument("--historical-intraday-download-dry-run", action="store_true")
    parser.add_argument("--execute-historical-intraday-download", action="store_true")
    parser.add_argument("--manual-intraday-csv-intake", action="store_true")
    parser.add_argument("--manual-intraday-data-sourcing-pack", action="store_true")
    parser.add_argument("--priority1-intraday-file-creation", action="store_true")
    parser.add_argument("--tiingo-intraday-diagnostic", action="store_true")
    parser.add_argument("--provider", choices=["tiingo", "alpha_vantage"], default="")
    parser.add_argument("--mechanism-search-small", action="store_true")
    parser.add_argument("--mechanism-search-1000", action="store_true")
    parser.add_argument("--mechanism-search-report", action="store_true")
    parser.add_argument("--mechanism-deep-trial-report", action="store_true")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--historical-replay", action="store_true")
    parser.add_argument("--historical-replay-report", action="store_true")
    parser.add_argument("--historical-replay-audit", action="store_true")
    parser.add_argument("--historical-replay-window", default="1y", choices=["30d", "90d", "180d", "1y", "3y", "5y"])
    parser.add_argument("--paper-forward-outcomes", action="store_true")
    parser.add_argument("--paper-forward-outcome-report", action="store_true")
    parser.add_argument("--paper-forward-outcome-audit", action="store_true")
    parser.add_argument("--import-observations", type=Path)
    parser.add_argument("--import-observations-small", type=Path)
    parser.add_argument("--observation-import-report", action="store_true")
    parser.add_argument("--observation-import-audit", action="store_true")
    parser.add_argument("--seed-demo-observations", action="store_true")
    args = parser.parse_args(argv)

    if args.mechanism_search_small:
        result = generate_mechanism_search_small(args.root, created_at=NOW)
        print(json.dumps(result, sort_keys=True))
        return 0 if result["governance_result"]["status"] == "PASS" else 2
    if args.mechanism_search_1000:
        result = generate_mechanism_search_1000(args.root, created_at=NOW)
        print(json.dumps({"run_id": result["run_id"], "emitted_count": result["emitted_count"], "pipeline_result_count": len(result.get("pipeline_results", [])), "governance_result": result["governance_result"]}, sort_keys=True))
        return 0 if result["governance_result"]["status"] == "PASS" else 2
    if args.mechanism_search_report:
        run = generate_mechanism_search_small(args.root, created_at=NOW)
        paths = write_mechanism_search_report(args.root, run=run)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0

    if args.mechanism_deep_trial_report:
        report = run_mechanism_deep_trial(root=args.root)
        paths = write_mechanism_deep_trial_report(report)
        print(json.dumps({"summary": report.get("summary", {}), "paths": {key: str(value) for key, value in paths.items()}}, sort_keys=True))
        return 0

    if args.candidate_backtest_report:
        report = run_candidate_backtest_report(root=args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "candidate_backtests" / "latest.json")}, sort_keys=True))
        return 0
    if args.candidate_family_discovery:
        report = run_candidate_family_discovery(root=args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "candidate_family_discovery" / "latest.json")}, sort_keys=True))
        return 0
    if args.expanded_search_trial:
        paths = write_expanded_search_trial_report(root=args.root, report_root=Path("reports") / "atlas_v2_research_os" / "expanded_search_trial", day=date.today().isoformat(), created_at=NOW)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0
    if args.expanded_search_narrowing:
        report = run_expanded_search_narrowing(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "expanded_search_narrowing" / "latest.json")}, sort_keys=True))
        return 0
    if args.expanded_search_gate_audit:
        paths = write_expanded_search_gate_audit_report(root=args.root, report_root=Path("reports") / "atlas_v2_research_os" / "expanded_search_gate_audit", day=date.today().isoformat(), created_at=NOW)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0
    if args.focused_expanded_search_trial:
        paths = write_focused_expanded_search_trial_report(root=args.root, report_root=Path("reports") / "atlas_v2_research_os" / "focused_expanded_search_trial", day=date.today().isoformat(), created_at=NOW)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0
    if args.search_overfit_guardrail:
        report = run_search_overfit_guardrail(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "search_overfit_guardrail" / "latest.json")}, sort_keys=True))
        return 0
    if args.holdout_replay_validation:
        report = run_holdout_replay_validation(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "holdout_replay_validation" / "latest.json")}, sort_keys=True))
        return 0
    if args.holdout_readiness_audit:
        report = run_holdout_readiness_audit(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "holdout_readiness_audit" / "latest.json")}, sort_keys=True))
        return 0
    if args.holdout_event_row_builder_design:
        report = run_holdout_event_row_builder_design(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "target_families": summary.get("target_families", 0),
            "candidate_requirements": summary.get("candidate_requirements", 0),
            "compatible_import_rows": summary.get("compatible_import_rows", 0),
            "blocked_requirements": summary.get("blocked_requirements", 0),
            "confidence_impact": summary.get("confidence_impact"),
        }, sort_keys=True))
        return 0
    if args.holdout_event_row_materializer:
        report = run_holdout_event_row_materializer(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "families_targeted": summary.get("families_targeted"),
            "source_artifacts_scanned": summary.get("source_artifacts_scanned"),
            "rows_materialized": summary.get("rows_materialized"),
            "complete_rows": summary.get("complete_rows"),
            "incomplete_rows": summary.get("incomplete_rows"),
            "blocked_requirements": summary.get("blocked_requirements"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "holdout_event_row_materializer" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.holdout_event_outcome_backfill_plan:
        report = run_holdout_event_outcome_backfill_plan(args.root, created_at=NOW)
        summary = report.get("summary", {})
        print(json.dumps({
            "artifacts_scanned": summary.get("artifacts_scanned"),
            "fields_inventoried": summary.get("fields_inventoried"),
            "direct_recovery": summary.get("direct_recovery"),
            "indirect_recovery": summary.get("indirect_recovery"),
            "partial_recovery": summary.get("partial_recovery"),
            "no_recovery": summary.get("no_recovery"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "holdout_event_outcome_backfill_plan" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.holdout_event_backfill_materializer:
        report = run_holdout_event_backfill_materializer(args.root, created_at=NOW)
        summary = report.get("summary", {})
        print(json.dumps({
            "rows_backfilled": summary.get("rows_backfilled"),
            "fields_backfilled": summary.get("fields_backfilled"),
            "complete_rows": summary.get("complete_rows"),
            "incomplete_rows": summary.get("incomplete_rows"),
            "top_remaining_blockers": summary.get("top_remaining_blockers"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "holdout_event_backfill_materializer" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.holdout_reconstruction_from_databento:
        report = run_holdout_reconstruction_from_databento(args.root, created_at=NOW)
        summary = report.get("summary", {})
        print(json.dumps({
            "rows_reviewed": summary.get("rows_reviewed"),
            "rows_reconstructed_complete": summary.get("rows_reconstructed_complete"),
            "rows_still_blocked": summary.get("rows_still_blocked"),
            "returns_computed": summary.get("returns_computed"),
            "split_date_blockers": summary.get("split_date_blockers"),
            "lookahead_risk_blockers": summary.get("lookahead_risk_blockers"),
            "holdout_replay_now_possible": summary.get("holdout_replay_now_possible"),
            "report": str(args.root / "holdout_reconstruction_from_databento" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.holdout_event_row_readiness_recheck:
        report = run_holdout_event_row_readiness_recheck(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "families_reviewed": summary.get("families_reviewed"),
            "candidates_reviewed": summary.get("candidates_reviewed"),
            "rows_reviewed": summary.get("rows_reviewed"),
            "ready_count": summary.get("ready_count"),
            "blocked_count": summary.get("blocked_count"),
            "top_blockers": summary.get("top_blockers"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "holdout_event_row_readiness_recheck" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.holdout_readiness_after_backfill:
        report = run_holdout_readiness_after_backfill(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "ready_families": summary.get("ready_family_count"),
            "blocked_families": summary.get("blocked_family_count"),
            "complete_rows": summary.get("complete_rows"),
            "remaining_blockers": summary.get("remaining_blockers"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "holdout_readiness_after_backfill" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.holdout_replay_dry_run:
        report = run_holdout_replay_dry_run(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "rows_tested": summary.get("rows_tested"),
            "parser_ready_count": summary.get("parser_ready_count"),
            "blocked_count": summary.get("blocked_count"),
            "blockers": summary.get("blockers"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "holdout_replay_dry_run" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.holdout_replay:
        report = run_holdout_replay(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "families_tested": summary.get("families_tested"),
            "survived": summary.get("survived"),
            "weakened": summary.get("weakened"),
            "failed": summary.get("failed"),
            "blocked": summary.get("blocked"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "holdout_replay" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.confirmed_reversal_family_expansion:
        report = run_confirmed_reversal_family_expansion(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "confirmed_reversal_family_expansion" / "latest.json")}, sort_keys=True))
        return 0
    if args.reversal_trending_exact_coverage_plan:
        report = run_reversal_trending_exact_coverage_plan(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "families_reviewed": summary.get("families_reviewed"),
            "candidates_reviewed": summary.get("candidates_reviewed"),
            "exact_coverage_available": summary.get("exact_coverage_available"),
            "fallback_only_rows": summary.get("fallback_only_rows"),
            "missing_data_rows": summary.get("missing_data_rows"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "reversal_trending_exact_coverage_plan" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.exact_coverage_import_specification:
        report = run_exact_coverage_import_specification(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "required_files": summary.get("required_files"),
            "P0_files": summary.get("P0_files"),
            "P1_files": summary.get("P1_files"),
            "families_blocked": summary.get("families_blocked"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "exact_coverage_import_specification" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.exact_data_repair_loop:
        report = run_exact_data_repair_loop(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "files_reviewed": summary.get("files_reviewed"),
            "repairable_files": summary.get("repairable_files"),
            "unrepaired_files": summary.get("unrepaired_files"),
            "revalidated_ready": summary.get("revalidated_ready"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "exact_data_repair_loop" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.exact_replay_without_fallback:
        report = run_exact_replay_without_fallback(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "families_reviewed": summary.get("families_reviewed"),
            "candidates_reviewed": summary.get("candidates_reviewed"),
            "exact_replays_run": summary.get("exact_replays_run"),
            "exact_confirmed_strong": summary.get("exact_confirmed_strong"),
            "exact_confirmed_weak": summary.get("exact_confirmed_weak"),
            "exact_failed": summary.get("exact_failed"),
            "exact_blocked": summary.get("exact_blocked"),
            "confidence_impact": summary.get("confidence_impact"),
        }, sort_keys=True))
        return 0
    if args.exact_coverage_import_validator:
        report = run_exact_coverage_import_validator(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "required_files": summary.get("required_files"),
            "files_found": summary.get("files_found"),
            "valid_ready": summary.get("valid_ready"),
            "valid_with_warnings": summary.get("valid_with_warnings"),
            "rejected": summary.get("rejected"),
            "missing": summary.get("missing"),
            "confidence_impact": summary.get("confidence_impact"),
        }, sort_keys=True))
        return 0
    if args.exact_intraday_data_acquisition_manifest:
        report = run_exact_intraday_data_acquisition_manifest(args.root, created_at=NOW)
        summary = report.get("summary", {})
        print(json.dumps({
            "unique_files_required": summary.get("unique_files_required"),
            "P0_files": summary.get("P0_files"),
            "P1_files": summary.get("P1_files"),
            "top_symbols_timeframes": summary.get("top_symbols_timeframes"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "exact_intraday_data_acquisition_manifest" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.net_of_cost_evidence:
        report = run_net_of_cost_evidence(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "rows_evaluated": summary.get("rows_evaluated"),
            "net_survives_strong": summary.get("net_survives_strong"),
            "net_survives_weak": summary.get("net_survives_weak"),
            "cost_eroded": summary.get("cost_eroded"),
            "net_blocked": summary.get("net_blocked"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "net_of_cost_evidence" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.cost_robustness_expansion:
        report = run_cost_robustness_expansion(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "break_even_cost_bps": summary.get("break_even_cost_bps"),
            "strongest_family": summary.get("strongest_family"),
            "cost_robust_family_count": summary.get("cost_robust_family_count"),
            "cost_sensitive_family_count": summary.get("cost_sensitive_family_count"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "cost_robustness_expansion" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.mechanism_expansion_program:
        report = run_mechanism_expansion_program(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "families_tested": summary.get("families_tested"),
            "exact_replays_run": summary.get("exact_replays_run"),
            "blocked_tests": summary.get("blocked_tests"),
            "best_mechanism": summary.get("best_mechanism"),
            "best_family": summary.get("best_family"),
            "report": str(args.root / "mechanism_expansion_program" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.mechanism_survivor_audit:
        report = run_mechanism_survivor_audit(args.root, created_at=NOW)
        print(json.dumps({
            "families_audited": report.get("families_audited"),
            "strong_survivors_before_audit": report.get("strong_survivors_before_audit"),
            "strong_survivors_after_audit": report.get("strong_survivors_after_audit"),
            "duplicate_artifact_risk_count": report.get("duplicate_artifact_risk_count"),
            "leakage_risk_count": report.get("leakage_risk_count"),
            "final_decision": report.get("final_decision"),
            "confidence_impact": report.get("confidence_impact"),
            "report": str(args.root / "mechanism_survivor_audit" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.controlled_surface_expansion_gate:
        report = run_controlled_surface_expansion_gate(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "decision": summary.get("decision"),
            "target_family_id": summary.get("target_family_id"),
            "approved_surface_count": summary.get("approved_surface_count"),
            "blocked_surface_count": summary.get("blocked_surface_count"),
            "high_risk_count": summary.get("high_risk_count"),
            "confidence_impact": summary.get("confidence_impact"),
            "candidate_promotion": summary.get("candidate_promotion"),
            "trading_authority": summary.get("trading_authority"),
            "report": str(args.root / "controlled_surface_expansion_gate" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.final_evidence_synthesis:
        report = run_final_evidence_synthesis(args.root)
        final = report.get("final_report", {})
        print(json.dumps({
            "overall_conclusion": final.get("overall_conclusion"),
            "strongest_family": final.get("strongest_family"),
            "weakest_family": final.get("weakest_family"),
            "confidence_impact": final.get("confidence_impact"),
            "report": str(args.root / "final_evidence_synthesis" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.execution_realism_economic_viability:
        report = run_execution_realism_economic_viability(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "overall_classification": summary.get("overall_classification"),
            "liquidity": summary.get("liquidity_classification"),
            "slippage": summary.get("slippage_classification"),
            "capacity": summary.get("capacity_classification"),
            "execution_robustness": summary.get("execution_classification"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "execution_realism_economic_viability" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.execution_cost_failure_analysis:
        report = run_execution_cost_failure_analysis(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "overall_classification": summary.get("overall_classification"),
            "rows_evaluated": summary.get("rows_evaluated"),
            "rows_cost_eroded_10bps": summary.get("rows_cost_eroded_10bps"),
            "rows_net_failed_10bps": summary.get("rows_net_failed_10bps"),
            "non_surviving_nonblocked_10bps": summary.get("non_surviving_nonblocked_10bps"),
            "tsla_viability": (summary.get("tsla_viability") or {}).get("classification"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "execution_cost_failure_analysis" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.generalization_edge_magnitude_assessment:
        report = run_generalization_edge_magnitude_assessment(args.root)
        summary = report.get("summary", {})
        edge = (report.get("edge_magnitude_assessment") or [{}])[0]
        print(json.dumps({
            "target_family_id": report.get("target_family_id"),
            "overall_classification": report.get("overall_classification"),
            "confidence_impact": report.get("confidence_impact"),
            "rows_evaluated": summary.get("rows_evaluated"),
            "surviving_symbol_count": summary.get("surviving_symbol_count"),
            "edge_magnitude_classification": edge.get("edge_magnitude_classification"),
            "report": str(args.root / "generalization_edge_magnitude_assessment" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.narrow_edge_deep_dive:
        report = run_narrow_edge_deep_dive(args.root)
        summary = report.get("summary", {})
        isolation = (report.get("isolation_risk_report") or [{}])[0]
        print(json.dumps({
            "target_family_id": summary.get("target_family_id"),
            "target_symbol": summary.get("target_symbol"),
            "classification": summary.get("classification"),
            "isolation_risk": isolation.get("isolation_risk"),
            "net_surviving_symbols": summary.get("net_surviving_symbols"),
            "target_rows_evaluated": summary.get("target_rows_evaluated"),
            "report": str(args.root / "narrow_edge_deep_dive" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.holdout_final_attempt:
        report = run_holdout_final_attempt(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "target_family_id": summary.get("target_family_id"),
            "final_classification": summary.get("final_classification"),
            "target_holdout_rows_found": summary.get("target_holdout_rows_found"),
            "recoverable_rows": summary.get("recoverable_rows"),
            "unrecoverable_rows": summary.get("unrecoverable_rows"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "holdout_final_attempt" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.reversal_neighborhood_expansion:
        report = run_reversal_neighborhood_expansion(args.root)
        summary = report.get("summary", {})
        top = (report.get("robustness_ranking") or [{}])[0]
        print(json.dumps({
            "target_family_id": summary.get("target_family_id"),
            "variants_tested": summary.get("variants_tested"),
            "top_variant": summary.get("top_variant"),
            "top_classification": summary.get("top_classification"),
            "classification_counts": summary.get("classification_counts"),
            "top_robustness_score": top.get("robustness_score"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "reversal_neighborhood_expansion" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.large_cap_growth_expansion:
        report = run_large_cap_growth_expansion(args.root)
        summary = report.get("summary", {})
        top = (report.get("cross_symbol_ranking") or [{}])[0]
        print(json.dumps({
            "target_family_id": summary.get("target_family"),
            "surface": summary.get("surface"),
            "symbols_requested": summary.get("symbols_requested"),
            "symbols_with_exact_data": summary.get("symbols_with_exact_data"),
            "symbols_confirmed": summary.get("symbols_confirmed"),
            "symbols_weak": summary.get("symbols_weak"),
            "symbols_failed": summary.get("symbols_failed"),
            "top_symbol": top.get("symbol"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "large_cap_growth_expansion" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.tsla_full_surface_expansion:
        report = run_tsla_full_surface_expansion(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "symbol": summary.get("symbol"),
            "surfaces_tested": summary.get("surfaces_tested"),
            "blocked_surfaces": summary.get("blocked_surfaces"),
            "overall_classification": summary.get("overall_classification"),
            "best_mechanism": (summary.get("best_mechanism") or {}).get("group"),
            "best_timeframe": (summary.get("best_timeframe") or {}).get("group"),
            "best_regime": (summary.get("best_regime") or {}).get("group"),
            "report": str(args.root / "tsla_full_surface_expansion" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.walk_forward_validation:
        report = run_walk_forward_validation(args.root)
        summary = report.get("summary", {})
        consistency = (report.get("consistency_report") or [{}])[0]
        print(json.dumps({
            "target_family_id": summary.get("target_family_id"),
            "classification": summary.get("classification"),
            "windows_generated": summary.get("windows_generated"),
            "windows_measured": summary.get("windows_measured"),
            "positive_test_windows": consistency.get("positive_test_windows"),
            "average_test_expectancy": consistency.get("average_test_expectancy"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "walk_forward_validation" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.expansion_validation_survivor_density:
        report = run_expansion_validation_survivor_density(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "total_surfaces_reviewed": summary.get("total_surfaces_reviewed"),
            "exact_survivors": summary.get("exact_survivors"),
            "net_survivors": summary.get("net_survivors"),
            "cost_robust_survivors": summary.get("cost_robust_survivors"),
            "survivor_density": summary.get("survivor_density"),
            "final_decision": summary.get("final_decision"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "expansion_validation_survivor_density" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.evidence_review_board:
        report = run_evidence_review_board(args.root)
        final = report.get("final_report", {})
        print(json.dumps({
            "overall_conclusion": final.get("overall_conclusion"),
            "strongest_family": final.get("strongest_family"),
            "weakest_family": final.get("weakest_family"),
            "recommended_next_phase": final.get("recommended_next_phase"),
            "report": str(args.root / "evidence_review_board" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.governed_holdout_outcome_data_acquisition_manifest:
        report = run_governed_holdout_outcome_data_acquisition_manifest(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "families_requiring_outcome_data": summary.get("families_requiring_outcome_data"),
            "candidates_requiring_outcome_data": summary.get("candidates_requiring_outcome_data"),
            "top_missing_fields": summary.get("top_missing_fields"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "governed_holdout_outcome_data_acquisition_manifest" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.holdout_data_feasibility_audit:
        report = run_holdout_data_feasibility_audit(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "rows_evaluated": summary.get("rows_evaluated"),
            "families_evaluated": summary.get("families_evaluated"),
            "holdout_feasibility": summary.get("holdout_feasibility"),
            "classification_counts": summary.get("classification_counts"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "holdout_data_feasibility_audit" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.verified_runtime_graph_evidence_coverage_repair:
        report = run_verified_runtime_graph_evidence_coverage_repair(args.root, day_utc=date.today().isoformat(), created_at=NOW)
        summary = report.get("summary", {})
        print(json.dumps({
            "blockers_inventoried": summary.get("blockers_inventoried"),
            "blockers_repaired": summary.get("blockers_repaired"),
            "remaining_blockers": summary.get("remaining_blockers"),
            "authority_changed": summary.get("authority_changed"),
            "evidence_fabricated": summary.get("evidence_fabricated"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "verified_runtime_graph_evidence_coverage_repair" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.forward_observation_starter:
        report = run_forward_observation_starter(args.root, created_at=NOW, now=NOW)
        summary = report.get("summary", {})
        print(json.dumps({
            "target_family": summary.get("target_family"),
            "candidates_enabled": summary.get("candidates_enabled"),
            "observations_created": summary.get("observations_created"),
            "pending_outcomes": summary.get("pending_outcomes"),
            "ready_for_measurement": summary.get("ready_for_measurement"),
            "completed_outcomes": summary.get("completed_outcomes"),
            "confidence_impact": summary.get("confidence_impact"),
        }, sort_keys=True))
        return 0
    if args.forward_observation_scoreboard:
        report = run_forward_observation_scoreboard(args.root, created_at=NOW)
        summary = report.get("summary", {})
        print(json.dumps({
            "strongest_family": summary.get("strongest_family"),
            "weakest_family": summary.get("weakest_family"),
            "families_improving": summary.get("families_improving"),
            "families_weakening": summary.get("families_weakening"),
            "classification_counts": summary.get("classification_counts"),
            "confidence_impact": summary.get("confidence_impact"),
        }, sort_keys=True))
        return 0
    if args.forward_observation_outcome_measurement:
        report = run_forward_observation_outcome_measurement(args.root, created_at=NOW, now=NOW)
        summary = report.get("summary", {})
        print(json.dumps({
            "observations_measured": summary.get("observations_measured"),
            "observations_still_pending": summary.get("observations_still_pending"),
            "expectancy": summary.get("expectancy"),
            "win_rate": summary.get("win_rate"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "forward_observation_outcome_measurement" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.forward_observation_loop:
        report = run_forward_observation_loop(args.root, created_at=NOW, now=NOW)
        summary = report.get("summary", {})
        print(json.dumps({
            "target_family": summary.get("target_family"),
            "new_signals": summary.get("new_signals"),
            "pending_observations": summary.get("pending_observations"),
            "measured_outcomes": summary.get("measured_outcomes"),
            "forward_classification": summary.get("forward_classification"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "forward_observation_loop" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.final_research_verdict:
        report = run_final_research_verdict(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "final_verdict": summary.get("final_verdict"),
            "confidence_impact": summary.get("confidence_impact"),
            "signal_likely_exists": summary.get("signal_likely_exists"),
            "edge_economically_meaningful": summary.get("edge_economically_meaningful"),
            "edge_robust": summary.get("edge_robust"),
            "edge_likely_exploitable": summary.get("edge_likely_exploitable"),
            "research_continue": summary.get("research_continue"),
            "remaining_blockers": summary.get("remaining_blockers"),
            "report": str(args.root / "final_research_verdict" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.timeframe_neighborhood_expansion:
        report = run_timeframe_neighborhood_expansion(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "target_symbol": summary.get("target_symbol"),
            "timeframes_tested": summary.get("timeframes_tested"),
            "exact_replays_run": summary.get("exact_replays_run"),
            "classification_counts": summary.get("classification_counts"),
            "key_question_answer": summary.get("key_question_answer"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "timeframe_neighborhood_expansion" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.temporal_robustness_decay:
        report = run_temporal_robustness_decay(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "target_family_id": summary.get("target_family_id"),
            "target_symbol": summary.get("target_symbol"),
            "target_timeframe": summary.get("target_timeframe"),
            "candidate_weighted_signal_count": summary.get("candidate_weighted_signal_count"),
            "months_evaluated": summary.get("months_evaluated"),
            "classification": summary.get("classification"),
            "key_question_answer": summary.get("key_question_answer"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "temporal_robustness_decay" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.trade_readiness_gate:
        report = run_trade_readiness_gate(args.root)
        summary = report.get("summary", {})
        print(json.dumps({
            "survivors_evaluated": summary.get("survivors_evaluated"),
            "paper_spec_ready_count": summary.get("paper_spec_ready_count"),
            "near_ready_count": summary.get("near_ready_count"),
            "rejected_count": summary.get("rejected_count"),
            "false_discovery_risk": summary.get("false_discovery_risk"),
            "atlas_practical_utility": summary.get("atlas_practical_utility"),
            "report": str(args.root / "trade_readiness_gate" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.generalization_edge_magnitude_assessment:
        report = run_generalization_edge_magnitude_assessment(args.root, created_at=NOW)
        summary = report.get("summary", {})
        edge = (report.get("edge_magnitude_assessment") or [{}])[0]
        print(json.dumps({
            "family_id": summary.get("family_id"),
            "overall_classification": summary.get("overall_classification"),
            "edge_magnitude_classification": edge.get("edge_magnitude_classification"),
            "confidence_impact": summary.get("confidence_impact"),
            "report": str(args.root / "generalization_edge_magnitude_assessment" / "latest.json"),
        }, sort_keys=True))
        return 0
    if args.regime_expansion_report:
        paths = write_regime_expansion_report(root=args.root, created_at=NOW)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0
    if args.regime_vocabulary_bridge_report:
        report = run_regime_vocabulary_bridge_report(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "regime_vocabulary_bridge" / "latest.json")}, sort_keys=True))
        return 0
    if args.regime_boundary_expansion:
        report = run_regime_boundary_expansion(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "overall_classification": report.get("overall_classification"), "trending_necessary": report.get("trending_necessary"), "report": str(args.root / "regime_boundary_expansion" / "latest.json")}, sort_keys=True))
        return 0
    if args.volatility_profile_expansion:
        report = run_volatility_profile_expansion(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "overall_classification": report.get("overall_classification"), "report": str(args.root / "volatility_profile_expansion" / "latest.json")}, sort_keys=True))
        return 0
    if args.null_model_randomized_control:
        report = run_null_model_randomized_control(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "overall_classification": report.get("overall_classification"), "signal_not_better_than_null": report.get("signal_not_better_than_null"), "report": str(args.root / "null_model_randomized_control" / "latest.json")}, sort_keys=True))
        return 0
    if args.evidence_lineage_graph:
        paths = write_evidence_lineage_graph(args.root, created_at=NOW)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0
    if args.determinism_repair_audit:
        paths = write_determinism_repair_audit(root=args.root)
        payload = json.loads(paths["json"].read_text(encoding="utf-8"))
        print(json.dumps({"findings": payload["summary"]["findings"], "fixed": payload["summary"]["fixed"], "suspected": payload["summary"]["suspected"], "remaining_blockers": payload["summary"]["remaining_blockers"], "confidence_impact": payload["confidence_impact"], "authority_status": "research_only_no_authority_change", "paths": {key: str(value) for key, value in paths.items()}}, sort_keys=True))
        return 0
    if args.replay_sample_yield:
        report = run_replay_sample_yield(root=args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "replay_sample_yield" / "latest.json")}, sort_keys=True))
        return 0
    if args.paper_forward_campaign_report:
        paths = write_paper_forward_campaign_report(args.root)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0
    if args.observation_source_breakdown_report:
        paths = write_observation_source_breakdown_report(args.root)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0

    if args.observation_cluster_split_experiment:
        report = run_observation_cluster_split_experiment(args.root)
        print(json.dumps({"pre_split": report.get("pre_split", {}), "post_split": report.get("post_split", {}), "comparison": report.get("comparison", {}), "report": str(args.root / "observation_cluster_split_experiment" / "latest.json")}, sort_keys=True))
        return 0
    if args.backtest_aware_final_qualification:
        report = run_backtest_aware_final_qualification(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "backtest_aware_final_qualification" / "latest.json")}, sort_keys=True))
        return 0
    if args.final_candidate_ranking:
        result = run_final_candidate_ranking(args.root)
        print(json.dumps({"ranking_summary": result["ranking"].get("summary", {}), "campaign_summary": result["campaign"].get("summary", {}), "paths": result.get("paths", {})}, sort_keys=True))
        return 0
    if args.candidate_data_validation_plan:
        paths = write_candidate_data_validation_plan(args.root)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0
    if args.candidate_symbol_attribution:
        report = run_candidate_symbol_attribution(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "candidate_symbol_attribution" / "latest.json")}, sort_keys=True))
        return 0
    if args.paper_forward_approval_checklist:
        paths = write_paper_forward_approval_checklist(args.root)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0
    if args.direct_candidate_data_validation:
        report = run_direct_candidate_data_validation(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "direct_candidate_data_validation" / "latest.json")}, sort_keys=True))
        return 0
    if args.direct_replay_zero_sample_diagnosis:
        report = run_direct_replay_zero_sample_diagnosis(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "direct_replay_zero_sample_diagnosis" / "latest.json")}, sort_keys=True))
        return 0
    if args.direct_replay_attrition_audit:
        report = run_direct_replay_attrition_audit(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "direct_replay_attrition_audit" / "latest.json")}, sort_keys=True))
        return 0
    if args.portfolio_relevance_estimate:
        report = run_portfolio_relevance_estimate(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "portfolio_relevance_estimate" / "latest.json")}, sort_keys=True))
        return 0
    if args.holdout_aware_family_ranking:
        report = run_holdout_aware_family_ranking(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "holdout_aware_family_ranking" / "latest.json")}, sort_keys=True))
        return 0
    if args.family_stability_analysis:
        report = run_family_stability_analysis(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "family_stability_analysis" / "latest.json")}, sort_keys=True))
        return 0
    if args.manual_pegy_claim_intake:
        report = run_manual_pegy_claim_intake(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "manual_fundamental_claims" / "pegy_ratio" / "latest.json")}, sort_keys=True))
        return 0
    if args.pegy_ratio_spec_report:
        report = run_pegy_ratio_spec_report(args.root)
        print(json.dumps({"metric_spec": report.get("metric_spec", {}).get("metric_name"), "report": str(args.root / "manual_fundamental_claims" / "pegy_ratio" / "latest.json")}, sort_keys=True))
        return 0
    if args.family_learning_report:
        report = run_family_learning(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "family_learning" / "latest.json")}, sort_keys=True))
        return 0
    if args.market_data_readiness:
        report = run_market_data_readiness(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "market_data_readiness" / "latest.json")}, sort_keys=True))
        return 0
    if args.market_data_import_report:
        report = run_market_data_import_report(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "market_data_import" / "latest.json")}, sort_keys=True))
        return 0
    if args.market_data_coverage_report:
        report = run_market_data_coverage_report(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "market_data_import" / "latest_coverage.json")}, sort_keys=True))
        return 0
    if args.validate_local_market_data:
        report = validate_local_market_data(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "market_data_import" / "latest.json")}, sort_keys=True))
        return 0
    if args.market_data_acquisition_plan:
        report = run_market_data_acquisition_plan(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "market_data_acquisition_plan" / "latest.json")}, sort_keys=True))
        return 0
    if args.market_data_acquisition_dry_run:
        report = run_market_data_acquisition_dry_run(args.root)
        print(json.dumps(report, sort_keys=True))
        return 0
    if args.historical_intraday_download_dry_run:
        report = dry_run_intraday_download(args.root, provider=args.provider or None)
        print(json.dumps({"summary": report.get("summary", {}), "provider_detection": report.get("provider_detection", []), "report": str(args.root / "historical_intraday_download" / "latest.json")}, sort_keys=True))
        return 0
    if args.execute_historical_intraday_download:
        if not args.provider:
            print(json.dumps({"error": "--provider is required for --execute-historical-intraday-download", "allowed_providers": ["tiingo", "alpha_vantage"]}, sort_keys=True))
            return 2
        report = download_intraday_csv_if_explicitly_enabled(args.root, provider=args.provider)
        print(json.dumps({"summary": report.get("summary", {}), "post_download_refresh": report.get("post_download_refresh", {}), "report": str(args.root / "historical_intraday_download" / "latest.json")}, sort_keys=True))
        return 0 if not report.get("summary", {}).get("failed_count") else 2
    if args.manual_intraday_csv_intake:
        report = run_manual_intraday_csv_intake(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "candidate_1_readiness": report.get("candidate_1_readiness", {}), "candidate_2_readiness": report.get("candidate_2_readiness", {}), "report": str(args.root / "manual_intraday_csv_intake" / "latest.json")}, sort_keys=True))
        return 0
    if args.manual_intraday_data_sourcing_pack:
        paths = write_manual_intraday_data_sourcing_pack(args.root)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0
    if args.priority1_intraday_file_creation:
        report = run_priority1_intraday_file_creation(args.root)
        print(json.dumps({"status": report.get("status"), "files_created": report.get("files_created", []), "remaining_missing_files": report.get("remaining_missing_files", []), "candidate_1": report.get("candidate_1_validation_result", {}).get("classification"), "candidate_2": report.get("candidate_2_validation_result", {}).get("classification"), "report": str(args.root / "priority1_intraday_file_creation" / "latest.json")}, sort_keys=True))
        return 0
    if args.tiingo_intraday_diagnostic:
        report = run_tiingo_intraday_diagnostic(args.root)
        print(json.dumps({"summary": report.get("summary", {}), "report": str(args.root / "tiingo_intraday_diagnostic" / "latest.json")}, sort_keys=True))
        return 0 if report.get("summary", {}).get("classification") == "SUCCESS" else 2
    if args.import_observations:
        report = import_observations(args.import_observations, root=args.root, workload_profile="BATCH_IMPORT")
        print(json.dumps(report, sort_keys=True))
        return 0
    if args.import_observations_small:
        report = import_observations(args.import_observations_small, root=args.root, workload_profile="SMALL_IMPORT")
        print(json.dumps(report, sort_keys=True))
        return 0
    if args.seed_demo_observations:
        result = seed_demo_observations(root=args.root, created_at=NOW)
        print(json.dumps(result, sort_keys=True))
        return 0
    if args.observation_import_report:
        latest_path = Path("reports/atlas_v2_research_os/observation_import/latest.json")
        if latest_path.exists():
            report = json.loads(latest_path.read_text(encoding="utf-8"))
            paths = write_observation_import_report(report)
        else:
            result = seed_demo_observations(root=args.root, created_at=NOW)
            report = result["import_report"]
            paths = write_observation_import_report(report)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0
    if args.observation_import_audit:
        audit = audit_observation_import_report()
        print(json.dumps(audit, sort_keys=True))
        return 0 if audit["observation_import_audit_ok"] else 2

    if args.paper_forward_outcomes:
        outcomes = demo_paper_forward_outcomes()
        print(json.dumps(outcomes, sort_keys=True))
        return 0
    if args.paper_forward_outcome_report:
        paths = write_paper_forward_outcome_report(demo_paper_forward_outcomes(), feedback_root=args.root)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0
    if args.paper_forward_outcome_audit:
        audit = audit_paper_forward_outcome_report()
        print(json.dumps(audit, sort_keys=True))
        return 0 if audit["paper_forward_outcome_audit_ok"] else 2

    if args.historical_replay:
        request = create_historical_replay_request(
            hypothesis_id="hyp-cli-historical-replay-demo",
            mechanism_tags=["OPENING_RANGE", "VOLATILITY_EXPANSION"],
            regime_context={"label": "OPENING_SESSION_HIGH_VOLATILITY"},
            time_window=args.historical_replay_window,
            source_artifact_ids=["artifact-cli-historical-replay-demo"],
            replay_id="hist-replay-cli-demo",
            created_at=NOW,
        )
        samples = [{"return": value, "regime": "OPENING_SESSION_HIGH_VOLATILITY"} for value in [0.018, 0.012, -0.004, 0.021, 0.009, 0.016, -0.003, 0.014, 0.011, 0.020]]
        result = run_historical_replay(request, samples, created_at=NOW)
        result["routed_backlog_items"] = route_historical_replay_backlog_items(result, root=args.root, created_at=NOW)
        print(json.dumps(result, sort_keys=True))
        return 0
    if args.historical_replay_report:
        results = demo_historical_replay_results()
        paths = write_historical_replay_report(results, backlog_root=args.root)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0
    if args.historical_replay_audit:
        audit = audit_historical_replay_report()
        print(json.dumps(audit, sort_keys=True))
        return 0 if audit["historical_replay_audit_ok"] else 2

    if args.failure_report:
        paths = write_failure_report(args.root)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0
    if args.failure_summary:
        print(json.dumps(summarize_failures_for_day(args.root), sort_keys=True))
        return 0
    if args.list_unresolved_failures:
        print(json.dumps(list_unresolved_failures(args.root), sort_keys=True))
        return 0
    if args.failure_audit:
        audit = validate_failure_observatory_allowed({"authority": "record and summarize failures only"})
        print(json.dumps(audit, sort_keys=True))
        return 0 if audit["status"] == "PASS" else 2

    if args.learning_feedback_demo:
        print(json.dumps(run_learning_feedback_demo(args.root), sort_keys=True))
        return 0
    if args.learning_feedback_report:
        paths = write_learning_feedback_report(args.root)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0
    if args.learning_feedback_audit:
        audit = audit_learning_feedback(args.root)
        print(json.dumps(audit, sort_keys=True))
        return 0 if audit["learning_feedback_audit_ok"] else 2
    if args.learning_validation_report:
        paths = write_learning_validation_report(demo_learning_validation_snapshots(), horizon=args.learning_validation_horizon)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0
    if args.learning_validation_audit:
        report = build_learning_validation_report(demo_learning_validation_snapshots(), horizon=args.learning_validation_horizon)
        audit = audit_learning_validation_report(report)
        print(json.dumps(audit, sort_keys=True))
        return 0 if audit["learning_validation_audit_ok"] else 2

    if args.seed_research_backlog or args.seed_research_backlog_small or args.seed_research_backlog_deep:
        if args.seed_research_backlog_small:
            profile = BacklogSeedProfile.SMALL_REVIEW.value
        elif args.seed_research_backlog_deep:
            profile = BacklogSeedProfile.DEEP_RESEARCH.value
        else:
            profile = BacklogSeedProfile.OVERNIGHT_RESEARCH.value
        result = generate_seed_backlog_items(args.root, profile=profile)
        print(json.dumps(result, sort_keys=True))
        return 0 if (result.get("governance_result", {}) or {}).get("status") == "PASS" else 2
    if args.backlog_seeding_report:
        report = build_backlog_seeding_report(args.root)
        paths = write_seed_report(report, args.root, day=report.get("day"))
        print(json.dumps({"report": report, "paths": {key: str(value) for key, value in paths.items()}}, sort_keys=True))
        return 0

    if args.throughput_review:
        report = run_throughput_repair_and_review(args.root)
        print(json.dumps(report, sort_keys=True))
        return 0
    if args.methodology_evaluation:
        report = run_methodology_evaluation(args.root)
        print(json.dumps(report, sort_keys=True))
        return 0
    if args.methodology_trial_100:
        report = run_methodology_trial_100(args.root)
        print(json.dumps(report, sort_keys=True))
        return 0
    if args.paper_forward_observation_report:
        paths = write_paper_forward_observation_report(args.root)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0
    if args.candidate_observation_readiness_report:
        paths = write_candidate_observation_readiness_report(args.root)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0

    if args.paper_trading_queue_demo:
        result = seed_demo_paper_trading_queue(args.root)
        if args.paper_trading_queue_demo_approve:
            result["approved_queue_item"] = approve_for_paper_test("paper-queue-demo-opening-range", reviewer="atlas_research_os_cli", root=args.root)
        print(json.dumps(result, sort_keys=True))
        return 0
    if args.paper_trading_queue_report:
        seed_demo_paper_trading_queue(args.root)
        paths = write_paper_trading_queue_report(args.root)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0
    if args.paper_trading_queue_audit:
        report = build_paper_trading_queue_report(args.root)
        audit = audit_paper_trading_queue_report(report)
        print(json.dumps(audit, sort_keys=True))
        return 0 if audit["paper_trading_queue_audit_ok"] else 2

    if args.research_os_certify:
        report = build_research_os_certification_report(args.root)
        print(json.dumps(report, sort_keys=True))
        return 0 if report["status"] not in {"FAIL", "BLOCKED"} else 2
    if args.research_os_certification_report:
        paths = write_research_os_certification_report(args.root)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0

    if args.list_workers:
        register_default_worker_adapters(replace=True)
        print(json.dumps(list_workers(), sort_keys=True))
        return 0
    if args.worker_audit:
        audit = build_worker_interface_report()
        print(json.dumps(audit, sort_keys=True))
        return 0 if audit["contract_audit_result"] == "PASS" else 2
    if args.worker_report:
        paths = write_worker_interface_report()
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0
    if args.worker_connection_audit:
        audit = build_worker_connection_audit()
        paths = write_worker_connection_audit(args.root)
        print(json.dumps({"audit": audit, "paths": {key: str(value) for key, value in paths.items()}}, sort_keys=True))
        return 0 if audit["contract_audit_result"] == "PASS" else 2
    if args.worker_connection_report:
        report = build_worker_connection_report(args.root)
        paths = write_worker_connection_report(args.root)
        print(json.dumps({"report": report, "paths": {key: str(value) for key, value in paths.items()}}, sort_keys=True))
        return 0 if report["contract_audit_result"] == "PASS" else 2
    if args.worker_execution_report:
        paths = write_worker_execution_report(args.root)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0
    if args.orchestrator_run_once:
        print(json.dumps(orchestrator_run_once(args.root), sort_keys=True))
        return 0
    if args.orchestrator_dry_run:
        print(json.dumps(orchestrator_dry_run(args.root), sort_keys=True))
        return 0
    if args.orchestrator_audit_only:
        print(json.dumps(orchestrator_audit_only(args.root), sort_keys=True))
        return 0
    if args.autonomous_research_run_once:
        print(json.dumps(run_bounded_research_once(args.root), sort_keys=True))
        return 0
    if args.autonomous_research_dry_run:
        print(json.dumps(dry_run_bounded_research(args.root), sort_keys=True))
        return 0
    if args.autonomous_research_audit:
        print(json.dumps(audit_bounded_research_execution(args.root), sort_keys=True))
        return 0
    if args.autonomous_research_report:
        result = audit_bounded_research_execution(args.root)
        paths = write_autonomous_research_execution_report(result, args.root)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0
    if args.overnight_research_review:
        report = run_overnight_research_review(args.root)
        print(json.dumps(report, sort_keys=True))
        return 0 if report.get("stop_reason") not in {"GOVERNANCE_FAILURE", "CERTIFICATION_FAILURE", "FORBIDDEN_ARTIFACT_ATTEMPT", "CONSECUTIVE_FAILURE_LIMIT_REACHED"} else 2
    if args.overnight_research_dry_run:
        report = dry_run_overnight_research_review(args.root)
        print(json.dumps(report, sort_keys=True))
        return 0 if report.get("stop_reason") not in {"GOVERNANCE_FAILURE", "CERTIFICATION_FAILURE", "FORBIDDEN_ARTIFACT_ATTEMPT", "CONSECUTIVE_FAILURE_LIMIT_REACHED"} else 2
    if args.overnight_research_summary:
        report = build_overnight_research_summary(args.root)
        paths = write_overnight_research_review_report(report, args.root)
        print(json.dumps({"report": report, "paths": {key: str(value) for key, value in paths.items()}}, sort_keys=True))
        return 0
    if args.seed_demo_candidate_quality:
        print(json.dumps(seed_demo_candidate_quality(), sort_keys=True))
        return 0
    if args.candidate_quality_report:
        seed_demo_candidate_quality()
        paths = write_candidate_quality_report(baseline_id="cq-baseline-demo", treatment_id="cq-treatment-demo")
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0
    if args.candidate_quality_audit:
        audit = audit_candidate_quality_reports()
        print(json.dumps(audit, sort_keys=True))
        return 0 if audit["candidate_quality_audit_ok"] else 2
    if args.seed_demo_memory:
        print(json.dumps(seed_demo_memory(args.root), sort_keys=True))
        return 0
    if args.memory_audit:
        ok, failures = validate_memory_integrity(args.root, artifact_store=ArtifactStore(args.root))
        print(json.dumps({"memory_integrity_ok": ok, "memory_integrity_failures": failures}, sort_keys=True))
        return 0 if ok else 2
    if args.memory_report:
        paths = write_memory_report(args.root)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0
    if args.find_duplicates:
        print(json.dumps(find_potential_duplicates(args.root), sort_keys=True))
        return 0
    if args.list_repeated_failures:
        print(json.dumps(get_repeated_failures(args.root, 2), sort_keys=True))
        return 0
    if args.seed_demo_backlog:
        print(json.dumps(seed_demo_backlog(args.root), sort_keys=True))
        return 0
    if args.audit:
        store = ArtifactStore(args.root)
        lineage_ok, lineage_failures = validate_lineage_integrity(store)
        forbidden_ok, forbidden_failures = validate_no_forbidden_artifacts(args.root)
        print(json.dumps({"lineage_ok": lineage_ok, "lineage_failures": lineage_failures, "forbidden_ok": forbidden_ok, "forbidden_failures": forbidden_failures}, sort_keys=True))
        return 0 if lineage_ok and forbidden_ok else 2
    if args.report:
        paths = write_foundation_report(args.root)
        print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
        return 0
    if args.select_next:
        selected = PriorityEngine(ResearchBacklog(args.root), ArtifactStore(args.root)).select_next_items(limit=args.limit, seed=args.seed)
        print(json.dumps(selected, sort_keys=True))
        return 0
    print(json.dumps(build_foundation_report(args.root), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
