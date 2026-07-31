from __future__ import annotations

import argparse
import json
from pathlib import Path

from .backtest_mvp import DEFAULT_REPORT_ROOT, run_backtest_mvp
from .data_import_validator import (
    REPORT_DIRS,
    run_backtest_evidence_review,
    run_backtest_integrity_audit,
    run_benchmark_backtest,
    run_data_import_validator,
    run_factor_score_builder,
    run_historical_portfolio_backtest,
    run_monthly_portfolio_constructor,
    run_opportunity_score_builder,
    run_portfolio_benchmark_comparison,
    run_total_return_builder,
)
from .data_import_package import (
    REPORT_DIRS as IMPORT_PACKAGE_REPORT_DIRS,
    run_benchmark_import_validator,
    run_data_file_naming_contract,
    run_fundamental_import_validator,
    run_import_folder_structure,
    run_normalized_data_cache,
    run_portfolio_data_import_readiness,
    run_price_import_validator,
    run_raw_data_import_scanner,
    run_sector_industry_map_validator,
    run_universe_import_validator,
    run_vendor_request_templates,
)
from .data_unlock_program import (
    REPORT_DIRS as DATA_UNLOCK_REPORT_DIRS,
    run_benchmark_recovery,
    run_databento_baseline_feasibility,
    run_data_unlock_program,
    run_data_unlock_program_d021_d050,
    run_existing_data_recovery,
    run_first_backtest_readiness_audit,
    run_fundamental_data_strategy,
    run_portfolio_atlas_data_unlock_review,
)
from .data_acquisition_control_center import (
    REPORT_DIRS as DATA_ACQUISITION_REPORT_DIRS,
    run_data_acquisition_control_center,
    run_data_acquisition_dry_run,
    run_data_acquisition_faq,
    run_data_acquisition_program,
    run_data_acquisition_program_review,
    run_data_dictionary,
    run_data_import_checklist,
    run_data_purchase_decision,
    run_full_model_data_path,
    run_minimum_viable_dataset,
    run_price_only_baseline_path,
    run_vendor_evaluation_matrix,
)
from .price_only_baseline_pb001 import REPORT_DIR as PB001_REPORT_DIR, run_price_only_baseline_pb001
from .price_only_universe_expansion import AGGREGATE_REPORT_DIR as PB002_REPORT_DIR, run_price_only_universe_expansion
from .full_model_transition import REPORT_DIR as FULL_MODEL_TRANSITION_REPORT_DIR, run_full_model_transition
from .price_only_robustness_validation import REPORT_DIR as ROBUSTNESS_REPORT_DIR, run_price_only_robustness_validation
from .size_decomposition import run_size_decomposition
from .first_source_backed_backtest import REPORT_DIRS as FIRST_SOURCE_REPORT_DIRS, run_first_source_backed_backtest
from .data_source_inventory import (
    REPORT_DIRS as DATA_READINESS_REPORT_DIRS,
    run_benchmark_data_contract,
    run_data_acquisition_plan,
    run_data_readiness_scorecard,
    run_data_source_inventory,
    run_fundamental_data_contract,
    run_historical_price_return_contract,
    run_point_in_time_universe_contract,
    run_post_import_validation_plan,
)
from .decision_grade_validation import run_decision_grade_validation
from .validation_campaign_002 import (
    run_behavioral_stress_test,
    run_complexity_benefit_review,
    run_implementation_funding_decision,
    run_monte_carlo_retirement_engine,
    run_oak_harvest_replacement_standard,
    run_oos_holdout_validation,
    run_portfolio_atlas_kill_test,
    run_portfolio_explainability,
    run_regime_performance_analysis,
    run_retirement_simulation_inputs,
    run_shadow_portfolio_design,
    run_tax_turnover_proxy,
    run_validation_campaign_002,
    run_validation_campaign_002_summary,
    run_walk_forward_portfolio_validation,
    run_weekly_review_package,
)
from .validation_framework import run_validation_framework


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Portfolio Atlas research-only CLI")
    parser.add_argument("--backtest-mvp", action="store_true", help="Build Portfolio Atlas P004-P007 backtest MVP reports")
    parser.add_argument("--validation-framework", action="store_true", help="Build Portfolio Atlas P008-P012 validation framework reports")
    parser.add_argument("--data-source-inventory", action="store_true", help="Build Portfolio Atlas P013 data source inventory reports")
    parser.add_argument("--point-in-time-universe-contract", action="store_true", help="Build Portfolio Atlas P014 PIT universe contract reports")
    parser.add_argument("--historical-price-return-contract", action="store_true", help="Build Portfolio Atlas P015 price/return contract reports")
    parser.add_argument("--fundamental-data-contract", action="store_true", help="Build Portfolio Atlas P016 fundamental data contract reports")
    parser.add_argument("--benchmark-data-contract", action="store_true", help="Build Portfolio Atlas P017 benchmark data contract reports")
    parser.add_argument("--data-readiness-scorecard", action="store_true", help="Build Portfolio Atlas P018 data readiness scorecard reports")
    parser.add_argument("--data-acquisition-plan", action="store_true", help="Build Portfolio Atlas P019 data acquisition plan reports")
    parser.add_argument("--post-import-validation-plan", action="store_true", help="Build Portfolio Atlas P020 post-import validation plan reports")
    parser.add_argument("--import-folder-structure", action="store_true", help="Build Portfolio Atlas P046 import folder structure reports")
    parser.add_argument("--vendor-request-templates", action="store_true", help="Build Portfolio Atlas P047 vendor request templates")
    parser.add_argument("--data-file-naming-contract", action="store_true", help="Build Portfolio Atlas P048 data file naming contract")
    parser.add_argument("--raw-data-import-scanner", action="store_true", help="Build Portfolio Atlas P049 raw data import scanner")
    parser.add_argument("--universe-import-validator", action="store_true", help="Build Portfolio Atlas P050 universe import validator")
    parser.add_argument("--price-import-validator", action="store_true", help="Build Portfolio Atlas P051 price import validator")
    parser.add_argument("--fundamental-import-validator", action="store_true", help="Build Portfolio Atlas P052 fundamental import validator")
    parser.add_argument("--benchmark-import-validator", action="store_true", help="Build Portfolio Atlas P053 benchmark import validator")
    parser.add_argument("--sector-industry-map-validator", action="store_true", help="Build Portfolio Atlas P054 sector/industry map validator")
    parser.add_argument("--portfolio-data-import-readiness", action="store_true", help="Build Portfolio Atlas P055 data import readiness")
    parser.add_argument("--normalized-data-cache", action="store_true", help="Build Portfolio Atlas P056-P060 normalized data cache")
    parser.add_argument("--portfolio-data-import-package", action="store_true", help="Build all Portfolio Atlas P046-P060 import package reports")
    parser.add_argument("--data-acquisition-control-center", action="store_true", help="Build Portfolio Atlas D001 data acquisition control center")
    parser.add_argument("--vendor-evaluation-matrix", action="store_true", help="Build Portfolio Atlas D002 vendor evaluation matrix")
    parser.add_argument("--data-purchase-decision", action="store_true", help="Build Portfolio Atlas D003 data purchase decision")
    parser.add_argument("--minimum-viable-dataset", action="store_true", help="Build Portfolio Atlas D004 minimum viable dataset")
    parser.add_argument("--price-only-baseline-path", action="store_true", help="Build Portfolio Atlas D005 price-only baseline path")
    parser.add_argument("--full-model-data-path", action="store_true", help="Build Portfolio Atlas D006 full model data path")
    parser.add_argument("--data-import-checklist", action="store_true", help="Build Portfolio Atlas D007 data import checklist")
    parser.add_argument("--data-dictionary", action="store_true", help="Build Portfolio Atlas D009 data dictionary")
    parser.add_argument("--data-acquisition-faq", action="store_true", help="Build Portfolio Atlas D010 data acquisition FAQ")
    parser.add_argument("--data-acquisition-dry-run", action="store_true", help="Build Portfolio Atlas D011-D015 acquisition dry run")
    parser.add_argument("--data-acquisition-program-review", action="store_true", help="Build Portfolio Atlas D016-D020 acquisition program review")
    parser.add_argument("--data-acquisition-program", action="store_true", help="Build all Portfolio Atlas D001-D020 acquisition program reports")
    parser.add_argument("--price-only-baseline-pb001", action="store_true", help="Execute Portfolio Atlas PB001 source-backed price-only baseline")
    parser.add_argument("--price-only-baseline-001", action="store_true", help="Execute Portfolio Atlas PB001 PRICE_ONLY_BASELINE_001 run")
    parser.add_argument("--price-only-universe-expansion", action="store_true", help="Execute Portfolio Atlas PB002-PB020 price-only universe expansion")
    parser.add_argument("--price-only-robustness-validation", action="store_true", help="Build Portfolio Atlas PB021-PB040 price-only robustness validation")
    parser.add_argument("--existing-data-recovery", action="store_true", help="Build Portfolio Atlas D051-D055 existing data recovery reports")
    parser.add_argument("--databento-baseline-feasibility", action="store_true", help="Build Portfolio Atlas D056-D060 Databento baseline feasibility reports")
    parser.add_argument("--benchmark-recovery", action="store_true", help="Build Portfolio Atlas D061-D065 benchmark recovery reports")
    parser.add_argument("--fundamental-data-strategy", action="store_true", help="Build Portfolio Atlas D066-D070 fundamental data strategy reports")
    parser.add_argument("--first-backtest-readiness-audit", action="store_true", help="Build Portfolio Atlas D071-D075 first backtest readiness audit reports")
    parser.add_argument("--portfolio-atlas-data-unlock-review", action="store_true", help="Build Portfolio Atlas D076-D080 data unlock review reports")
    parser.add_argument("--portfolio-data-unlock", action="store_true", help="Build all Portfolio Atlas D051-D080 data unlock reports")
    parser.add_argument("--full-model-transition", action="store_true", help="Build Portfolio Atlas PF001-PF030 full-model transition reports")
    parser.add_argument("--size-decomposition", action="store_true", help="Build Portfolio Atlas size exposure decomposition report")
    parser.add_argument("--data-import-validator", action="store_true", help="Build Portfolio Atlas P021 data import validator reports")
    parser.add_argument("--total-return-builder", action="store_true", help="Build Portfolio Atlas P022 total return reports")
    parser.add_argument("--factor-score-builder", action="store_true", help="Build Portfolio Atlas P023 factor score reports")
    parser.add_argument("--opportunity-score-builder", action="store_true", help="Build Portfolio Atlas P024 opportunity score reports")
    parser.add_argument("--monthly-portfolio-constructor", action="store_true", help="Build Portfolio Atlas P025 monthly portfolio reports")
    parser.add_argument("--historical-portfolio-backtest", action="store_true", help="Build Portfolio Atlas P026 historical backtest reports")
    parser.add_argument("--benchmark-backtest", action="store_true", help="Build Portfolio Atlas P027 benchmark backtest reports")
    parser.add_argument("--portfolio-benchmark-comparison", action="store_true", help="Build Portfolio Atlas P028 comparison reports")
    parser.add_argument("--backtest-integrity-audit", action="store_true", help="Build Portfolio Atlas P029 integrity audit reports")
    parser.add_argument("--backtest-evidence-review", action="store_true", help="Build Portfolio Atlas P030 evidence review reports")
    parser.add_argument("--walk-forward-portfolio-validation", action="store_true", help="Build Portfolio Atlas P031 walk-forward validation reports")
    parser.add_argument("--oos-holdout-validation", action="store_true", help="Build Portfolio Atlas P032 out-of-sample holdout reports")
    parser.add_argument("--regime-performance-analysis", action="store_true", help="Build Portfolio Atlas P033 regime performance reports")
    parser.add_argument("--tax-turnover-proxy", action="store_true", help="Build Portfolio Atlas P034 turnover and tax-drag proxy reports")
    parser.add_argument("--behavioral-stress-test", action="store_true", help="Build Portfolio Atlas P035 behavioral stress reports")
    parser.add_argument("--retirement-simulation-inputs", action="store_true", help="Build Portfolio Atlas P036 retirement input contract reports")
    parser.add_argument("--monte-carlo-retirement-engine", action="store_true", help="Build Portfolio Atlas P037 Monte Carlo retirement engine reports")
    parser.add_argument("--oak-harvest-replacement-standard", action="store_true", help="Build Portfolio Atlas P038 replacement standard reports")
    parser.add_argument("--shadow-portfolio-design", action="store_true", help="Build Portfolio Atlas P039 shadow portfolio design reports")
    parser.add_argument("--weekly-review-package", action="store_true", help="Build Portfolio Atlas P040 weekly review package reports")
    parser.add_argument("--portfolio-explainability", action="store_true", help="Build Portfolio Atlas P041 explainability reports")
    parser.add_argument("--complexity-benefit-review", action="store_true", help="Build Portfolio Atlas P042 complexity vs benefit reports")
    parser.add_argument("--portfolio-atlas-kill-test", action="store_true", help="Build Portfolio Atlas P043 kill test reports")
    parser.add_argument("--validation-campaign-002", action="store_true", help="Build Portfolio Atlas P044 validation campaign 002 reports")
    parser.add_argument("--implementation-funding-decision", action="store_true", help="Build Portfolio Atlas P045 implementation funding decision reports")
    parser.add_argument("--portfolio-validation-campaign-002", action="store_true", help="Build all Portfolio Atlas P031-P045 reports")
    parser.add_argument("--first-source-backed-backtest", action="store_true", help="Build Portfolio Atlas P061-P075 first source-backed backtest reports")
    parser.add_argument("--data-unlock-program", action="store_true", help="Build Portfolio Atlas D021-D050 data unlock reports")
    parser.add_argument("--decision_grade_validation", action="store_true", help="Build Portfolio Atlas P076-P095 decision-grade validation reports")
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    args = parser.parse_args(argv)

    if args.backtest_mvp:
        report = run_backtest_mvp(args.report_root)
        print(
            json.dumps(
                {
                    "status": report["backtest"]["status"],
                    "data_readiness": report["data_contract"]["status"],
                    "portfolio_sizes_supported": report["portfolio_constructor"]["portfolio_sizes_supported"],
                    "report": str(args.report_root / "backtest_mvp" / "latest.json"),
                },
                sort_keys=True,
            )
        )
        return 0

    if args.validation_framework:
        report = run_validation_framework(args.report_root)
        summary = report.get("summary", {})
        print(
            json.dumps(
                {
                    "benchmark_count": summary.get("benchmark_count"),
                    "oak_harvest_assumption_status": summary.get("oak_harvest_assumption_status"),
                    "walk_forward_status": summary.get("walk_forward_status"),
                    "bias_status": summary.get("bias_status"),
                    "validation_decision": summary.get("validation_decision"),
                    "next_phase_recommendation": summary.get("next_phase_recommendation"),
                    "report": str(args.report_root / "validation_framework" / "latest.json"),
                },
                sort_keys=True,
            )
        )
        return 0

    data_readiness_flags = [
        (args.data_source_inventory, run_data_source_inventory, "data_source_inventory"),
        (args.point_in_time_universe_contract, run_point_in_time_universe_contract, "point_in_time_universe_contract"),
        (args.historical_price_return_contract, run_historical_price_return_contract, "historical_price_return_contract"),
        (args.fundamental_data_contract, run_fundamental_data_contract, "fundamental_data_contract"),
        (args.benchmark_data_contract, run_benchmark_data_contract, "benchmark_data_contract"),
        (args.data_readiness_scorecard, run_data_readiness_scorecard, "data_readiness_scorecard"),
        (args.data_acquisition_plan, run_data_acquisition_plan, "data_acquisition_plan"),
        (args.post_import_validation_plan, run_post_import_validation_plan, "post_import_validation_plan"),
    ]
    selected_data_readiness = [(runner, dirname) for enabled, runner, dirname in data_readiness_flags if enabled]
    if selected_data_readiness:
        outputs = []
        for runner, dirname in selected_data_readiness:
            report = runner(args.report_root)
            outputs.append(
                {
                    "report": dirname,
                    "status": report.get("status"),
                    "path": str(args.report_root / DATA_READINESS_REPORT_DIRS[dirname] / "latest.json"),
                }
            )
        print(json.dumps({"reports": outputs, "authority": "Research-only. No live portfolio, no replacement recommendation, no trades, no broker execution."}, sort_keys=True))
        return 0

    import_package_flags = [
        (args.import_folder_structure, run_import_folder_structure, "import_folder_structure"),
        (args.vendor_request_templates, run_vendor_request_templates, "vendor_request_templates"),
        (args.data_file_naming_contract, run_data_file_naming_contract, "data_file_naming_contract"),
        (args.raw_data_import_scanner, run_raw_data_import_scanner, "raw_data_import_scanner"),
        (args.universe_import_validator, run_universe_import_validator, "universe_import_validator"),
        (args.price_import_validator, run_price_import_validator, "price_import_validator"),
        (args.fundamental_import_validator, run_fundamental_import_validator, "fundamental_import_validator"),
        (args.benchmark_import_validator, run_benchmark_import_validator, "benchmark_import_validator"),
        (args.sector_industry_map_validator, run_sector_industry_map_validator, "sector_industry_map_validator"),
        (args.portfolio_data_import_readiness, run_portfolio_data_import_readiness, "portfolio_data_import_readiness"),
        (args.normalized_data_cache, run_normalized_data_cache, "normalized_data_cache"),
    ]
    if args.portfolio_data_import_package:
        selected_import_package = [(runner, dirname) for _, runner, dirname in import_package_flags]
    else:
        selected_import_package = [(runner, dirname) for enabled, runner, dirname in import_package_flags if enabled]
    if selected_import_package:
        outputs = []
        for runner, dirname in selected_import_package:
            report = runner(args.report_root)
            outputs.append(
                {
                    "report": dirname,
                    "status": report.get("status"),
                    "path": str(args.report_root / IMPORT_PACKAGE_REPORT_DIRS[dirname] / "latest.json"),
                }
            )
        print(json.dumps({"reports": outputs, "authority": "Research-only. No live portfolio, no replacement recommendation, no trades, no broker execution."}, sort_keys=True))
        return 0

    if args.data_unlock_program:
        report = run_data_unlock_program_d021_d050(args.report_root)
        print(
            json.dumps(
                {
                    "status": report.get("status"),
                    "classification": report.get("classification"),
                    "can_run_first_real_backtest": report.get("required_answer"),
                    "report": str(args.report_root / "data_unlock_program" / "latest.json"),
                    "authority": "Research-only. No live portfolio. No Oak Harvest replacement recommendation. No broker execution. No trading authority.",
                },
                sort_keys=True,
            )
        )
        return 0

    if args.price_only_baseline_pb001 or args.price_only_baseline_001:
        report = run_price_only_baseline_pb001(args.report_root)
        print(
            json.dumps(
                {
                    "status": report.get("status"),
                    "final_answer": report.get("final_answer", {}).get("answer"),
                    "report": str(args.report_root / PB001_REPORT_DIR / "latest.json"),
                    "authority": "Research-only. No trades. No recommendations. No broker execution.",
                },
                sort_keys=True,
            )
        )
        return 0

    if args.price_only_universe_expansion:
        report = run_price_only_universe_expansion(args.report_root)
        print(
            json.dumps(
                {
                    "status": report.get("status"),
                    "decision": report.get("decision"),
                    "symbols_discovered": report.get("symbols_discovered"),
                    "eligible_symbol_count": report.get("eligible_symbol_count"),
                    "breadth_status": report.get("breadth_status"),
                    "report": str(args.report_root / PB002_REPORT_DIR / "latest.json"),
                    "authority": "Research-only. No trades. No recommendations.",
                },
                sort_keys=True,
            )
        )
        return 0

    if args.price_only_robustness_validation:
        report = run_price_only_robustness_validation(args.report_root)
        print(
            json.dumps(
                {
                    "status": report.get("status"),
                    "decision": report.get("decision"),
                    "report": str(args.report_root / ROBUSTNESS_REPORT_DIR / "latest.json"),
                    "authority": "Research-only. No trades. No recommendations.",
                },
                sort_keys=True,
            )
        )
        return 0

    data_unlock_flags = [
        (args.existing_data_recovery, run_existing_data_recovery, "existing_data_recovery"),
        (args.databento_baseline_feasibility, run_databento_baseline_feasibility, "databento_baseline_feasibility"),
        (args.benchmark_recovery, run_benchmark_recovery, "benchmark_recovery"),
        (args.fundamental_data_strategy, run_fundamental_data_strategy, "fundamental_data_strategy"),
        (args.first_backtest_readiness_audit, run_first_backtest_readiness_audit, "first_backtest_readiness_audit"),
        (args.portfolio_atlas_data_unlock_review, run_portfolio_atlas_data_unlock_review, "portfolio_atlas_data_unlock_review"),
    ]
    if args.portfolio_data_unlock:
        report = run_data_unlock_program(args.report_root)
        review = report["portfolio_atlas_data_unlock_review"]
        print(
            json.dumps(
                {
                    "classification": review["classification"],
                    "can_run_first_source_backed_backtest": review["can_run_first_source_backed_backtest"],
                    "databento_decision": report["databento_baseline_feasibility"]["decision"],
                    "benchmark_status": report["benchmark_recovery"]["status"],
                    "report": str(args.report_root / DATA_UNLOCK_REPORT_DIRS["portfolio_atlas_data_unlock_review"] / "latest.json"),
                    "authority": "Research-only. No portfolio recommendation, no replacement recommendation, no trades, no broker execution.",
                },
                sort_keys=True,
            )
        )
        return 0

    if args.full_model_transition:
        report = run_full_model_transition(args.report_root)
        print(
            json.dumps(
                {
                    "status": report["status"],
                    "decision": report["decision"],
                    "report": str(args.report_root / FULL_MODEL_TRANSITION_REPORT_DIR / "latest.json"),
                    "authority": "Research-only. No trades. No recommendations.",
                },
                sort_keys=True,
            )
        )
        return 0

    if args.size_decomposition:
        report = run_size_decomposition(args.report_root)
        print(
            json.dumps(
                {
                    "status": report["status"],
                    "report": report["output_path"],
                    "authority": "Research-only. No trades. No recommendations.",
                },
                sort_keys=True,
            )
        )
        return 0
    selected_data_unlock = [(runner, dirname) for enabled, runner, dirname in data_unlock_flags if enabled]
    if selected_data_unlock:
        outputs = []
        for runner, dirname in selected_data_unlock:
            report = runner(args.report_root)
            outputs.append(
                {
                    "report": dirname,
                    "status": report.get("status") or report.get("decision") or report.get("classification"),
                    "path": str(args.report_root / DATA_UNLOCK_REPORT_DIRS[dirname] / "latest.json"),
                }
            )
        print(json.dumps({"reports": outputs, "authority": "Research-only. No portfolio recommendation, no replacement recommendation, no trades, no broker execution."}, sort_keys=True))
        return 0

    acquisition_flags = [
        (args.data_acquisition_control_center, run_data_acquisition_control_center, "data_acquisition_control_center"),
        (args.vendor_evaluation_matrix, run_vendor_evaluation_matrix, "vendor_evaluation_matrix"),
        (args.data_purchase_decision, run_data_purchase_decision, "data_purchase_decision"),
        (args.minimum_viable_dataset, run_minimum_viable_dataset, "minimum_viable_dataset"),
        (args.price_only_baseline_path, run_price_only_baseline_path, "price_only_baseline_path"),
        (args.full_model_data_path, run_full_model_data_path, "full_model_data_path"),
        (args.data_import_checklist, run_data_import_checklist, "data_import_checklist"),
        (args.data_dictionary, run_data_dictionary, "data_dictionary"),
        (args.data_acquisition_faq, run_data_acquisition_faq, "data_acquisition_faq"),
        (args.data_acquisition_dry_run, run_data_acquisition_dry_run, "data_acquisition_dry_run"),
        (args.data_acquisition_program_review, run_data_acquisition_program_review, "data_acquisition_program_review"),
    ]
    if args.data_acquisition_program:
        report = run_data_acquisition_program(args.report_root)
        print(
            json.dumps(
                {
                    "status": report.get("status"),
                    "report": str(args.report_root / DATA_ACQUISITION_REPORT_DIRS["data_acquisition_program_review"] / "latest.json"),
                    "authority": "Research-only. No live portfolio, no replacement recommendation, no trades, no broker execution.",
                },
                sort_keys=True,
            )
        )
        return 0
    selected_acquisition = [(runner, dirname) for enabled, runner, dirname in acquisition_flags if enabled]
    if selected_acquisition:
        outputs = []
        for runner, dirname in selected_acquisition:
            report = runner(args.report_root)
            filename = "latest_summary.md" if dirname in {"data_import_checklist", "data_acquisition_faq"} else "latest.json"
            outputs.append(
                {
                    "report": dirname,
                    "status": report.get("status"),
                    "path": str(args.report_root / DATA_ACQUISITION_REPORT_DIRS[dirname] / filename),
                }
            )
        print(json.dumps({"reports": outputs, "authority": "Research-only. No live portfolio, no replacement recommendation, no trades, no broker execution."}, sort_keys=True))
        return 0

    campaign_flags = [
        (args.walk_forward_portfolio_validation, run_walk_forward_portfolio_validation, "walk_forward_portfolio_validation"),
        (args.oos_holdout_validation, run_oos_holdout_validation, "oos_holdout_validation"),
        (args.regime_performance_analysis, run_regime_performance_analysis, "regime_performance_analysis"),
        (args.tax_turnover_proxy, run_tax_turnover_proxy, "tax_turnover_proxy"),
        (args.behavioral_stress_test, run_behavioral_stress_test, "behavioral_stress_test"),
        (args.retirement_simulation_inputs, run_retirement_simulation_inputs, "retirement_simulation_inputs"),
        (args.monte_carlo_retirement_engine, run_monte_carlo_retirement_engine, "monte_carlo_retirement_engine"),
        (args.oak_harvest_replacement_standard, run_oak_harvest_replacement_standard, "oak_harvest_replacement_standard"),
        (args.shadow_portfolio_design, run_shadow_portfolio_design, "shadow_portfolio_design"),
        (args.weekly_review_package, run_weekly_review_package, "weekly_review_package"),
        (args.portfolio_explainability, run_portfolio_explainability, "portfolio_explainability"),
        (args.complexity_benefit_review, run_complexity_benefit_review, "complexity_benefit_review"),
        (args.portfolio_atlas_kill_test, run_portfolio_atlas_kill_test, "portfolio_atlas_kill_test"),
        (args.validation_campaign_002, run_validation_campaign_002_summary, "validation_campaign_002"),
        (args.implementation_funding_decision, run_implementation_funding_decision, "implementation_funding_decision"),
    ]

    if args.portfolio_validation_campaign_002:
        report = run_validation_campaign_002(args.report_root)
        print(
            json.dumps(
                {
                    "validation_campaign_002_decision": report["validation_campaign_002"]["classification"],
                    "funding_decision": report["funding_decision"]["classification"],
                    "authority": "research_only_no_replacement_no_trades",
                    "report": str(args.report_root / "validation_campaign_002" / "latest.json"),
                },
                sort_keys=True,
            )
        )
        return 0

    if args.first_source_backed_backtest:
        report = run_first_source_backed_backtest(args.report_root)
        evidence = report["first_backtest_evidence_review"]
        cont = report["continue_stop_decision"]
        print(
            json.dumps(
                {
                    "status": evidence.get("status"),
                    "continue_stop_decision": cont.get("status"),
                    "report": str(args.report_root / FIRST_SOURCE_REPORT_DIRS["first_backtest_evidence_review"] / "latest.json"),
                    "authority": "Research-only. No live portfolio, no replacement recommendation, no trades.",
                },
                sort_keys=True,
            )
        )
        return 0

    if args.decision_grade_validation:
        report = run_decision_grade_validation(args.report_root)
        print(
            json.dumps(
                {
                    "validation_campaign_003_decision": report["campaign"]["classification"],
                    "funding_decision": report["funding"]["classification"],
                    "phase_decision": report["phase_review"]["classification"],
                    "authority": "research_only_no_live_portfolio_no_replacement_no_trades_no_broker",
                    "report": str(args.report_root / "validation_campaign_003" / "latest.json"),
                },
                sort_keys=True,
            )
        )
        return 0

    selected = [(runner, dirname) for enabled, runner, dirname in campaign_flags if enabled]
    if selected:
        outputs = []
        for runner, dirname in selected:
            report = runner(args.report_root)
            outputs.append({"report": dirname, "classification": report.get("classification"), "path": str(args.report_root / dirname / "latest.json")})
        print(json.dumps({"reports": outputs, "authority": "research_only_no_replacement_no_trades"}, sort_keys=True))
        return 0

    stage_runners = [
        ("data_import_validator", args.data_import_validator, run_data_import_validator),
        ("total_return_builder", args.total_return_builder, run_total_return_builder),
        ("factor_score_builder", args.factor_score_builder, run_factor_score_builder),
        ("opportunity_score_builder", args.opportunity_score_builder, run_opportunity_score_builder),
        ("monthly_portfolio_constructor", args.monthly_portfolio_constructor, run_monthly_portfolio_constructor),
        ("historical_portfolio_backtest", args.historical_portfolio_backtest, run_historical_portfolio_backtest),
        ("benchmark_backtest", args.benchmark_backtest, run_benchmark_backtest),
        ("portfolio_benchmark_comparison", args.portfolio_benchmark_comparison, run_portfolio_benchmark_comparison),
        ("backtest_integrity_audit", args.backtest_integrity_audit, run_backtest_integrity_audit),
        ("backtest_evidence_review", args.backtest_evidence_review, run_backtest_evidence_review),
    ]
    for stage, enabled, runner in stage_runners:
        if enabled:
            report = runner(args.report_root)
            print(
                json.dumps(
                    {
                        "status": report.get("status"),
                        "stage": stage,
                        "report": str(args.report_root / REPORT_DIRS[stage] / "latest.json"),
                        "authority": "Research-only. No live portfolio, no replacement recommendation, no trades.",
                    },
                    sort_keys=True,
                )
            )
            return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

