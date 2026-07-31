#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPORT_FAMILY = "aegis_performance_architecture_review_v1"
REPORT_FILENAME = "performance_architecture_review.v1.json"

SAFETY_FLAGS = {
    "review_only": True,
    "no_runtime_policy_changes": True,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
    "order_routing_allowed": False,
}


def report_path_v1(*, repo_root: Path, day_utc: str) -> Path:
    return repo_root / "reports" / REPORT_FAMILY / day_utc / REPORT_FILENAME


def build_performance_architecture_review_v1(*, repo_root: Path, truth_root: Path, day_utc: str) -> dict[str, Any]:
    repo_root = Path(repo_root).resolve()
    truth_root = Path(truth_root).resolve()
    components = existing_components_v1(repo_root=repo_root, truth_root=truth_root, day_utc=day_utc)
    duplicates = duplicate_source_detection_v1(components)
    canonical = canonical_source_labels_v1(components)
    return {
        "schema_id": "aegis_performance_architecture_review",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day_utc,
        "generated_at_utc": _now_utc(),
        "truth_root": str(truth_root),
        "repo_root": str(repo_root),
        "scope": {
            "review_only": True,
            "areas": [
                "paper outcomes",
                "realized PnL",
                "unrealized PnL",
                "sleeve metrics",
                "exit reviews",
                "trade attribution",
                "candidate attribution",
                "recommendation attribution",
                "performance dashboards",
                "leaderboard/ranking logic",
                "analytics exports",
                "historical ledgers",
            ],
            "out_of_scope": ["new PnL system design implementation", "runtime/trading policy changes"],
        },
        "runtime_truth_context": _runtime_truth_context(truth_root, day_utc),
        "existing_components": components,
        "canonical_vs_duplicate_sources": canonical,
        "duplicate_source_detection": duplicates,
        "event_sourced_vs_derived_systems": event_sourced_vs_derived_v1(components),
        "missing_linkage_points": missing_linkage_points_v1(),
        "stale_legacy_logic": stale_legacy_logic_v1(),
        "overlap_conflicts": overlap_conflicts_v1(duplicates),
        "sleeve_metric_availability": sleeve_metric_availability_v1(),
        "pnl_attribution_quality": pnl_attribution_quality_v1(),
        "unrealized_vs_realized_handling": unrealized_vs_realized_handling_v1(),
        "current_weaknesses": current_weaknesses_v1(),
        "exit_recommendation_quality": exit_recommendation_quality_v1(),
        "pnl_architecture_evaluation": pnl_architecture_evaluation_v1(),
        "sleeve_analytics_readiness": sleeve_analytics_readiness_v1(),
        "architecture_recommendations": architecture_recommendations_v1(),
        "portal_debug_page": {
            "recommended": False,
            "reason": "The requested pass is an offline architecture inventory. A Portal page should wait until the canonical performance truth artifact is selected; otherwise it risks adding another read model before consolidation.",
            "candidate_title": "Aegis Performance Architecture",
        },
        "safety": dict(SAFETY_FLAGS),
        **SAFETY_FLAGS,
    }


def existing_components_v1(*, repo_root: Path, truth_root: Path, day_utc: str) -> list[dict[str, Any]]:
    specs = [
        _component("paper_position_ledger", "paper outcomes", "ops/aegis/paper_position_ledger_v1.py", "reports/aegis_paper_position_ledger_v1/{day}/paper_position_ledger.v1.json", "EVENT_SOURCED", "CANONICAL_FOR_SIMULATED_PAPER_POSITIONS", "Open/closed/invalidated simulated paper position state from append-only paper events plus receipt bootstrap.", ["paper_position_events", "paper_trade_receipts", "market_data"], ["unrealized_pnl", "realized_pnl", "hold_time", "open_position_marks"]),
        _component("paper_trade_outcomes", "paper outcomes", "ops/aegis/human_reviewed_paper_mode_v1.py", "reports/aegis_paper_trade_outcomes_v1/{day}/paper_trade_outcomes.v1.json", "DERIVED_OR_OPERATOR_CAPTURED", "PARTIAL_LEGACY_READ_MODEL", "Operator-confirmed paper outcome tracking used by candidate state and UI.", ["paper_review_decisions", "paper_receipts"], ["paper_outcome_status", "open_trades", "closed_trades"]),
        _component("candidate_lifecycle_outcomes", "candidate attribution", "ops/aegis/candidate_state_v1.py", "reports/aegis_candidate_lifecycle_v1/{day}/candidate_outcomes.v1.json", "DERIVED", "READ_ONLY_LEGACY", "Candidate lifecycle outcome projection for historical candidate state.", ["candidate_review_packet", "paper_trade_outcomes"], ["candidate_outcome_status"]),
        _component("sleeve_trade_fact", "trade attribution", "constellation_2/aegis_truth/sleeve_trade_fact_v1.py", "reports/sleeve_trade_fact_v1/{day}/sleeve_trade_fact.v1.json", "DERIVED_FROM_FACT_LEDGERS", "SHOULD_BE_CANONICAL_FOR_COMPLETED_TRADE_FACTS", "Normalizes completed trade facts from execution/fill evidence for economic truth.", ["post_trade_lifecycle", "fill_ledger", "execution_root"], ["trade_facts", "sleeve_id", "gross_notional", "fees"]),
        _component("sleeve_realized_pnl", "realized PnL", "constellation_2/aegis_truth/sleeve_realized_pnl_v1.py", "reports/sleeve_realized_pnl_v1/{day}/sleeve_realized_pnl.v1.json", "DERIVED", "PARTIAL_SHOULD_NOT_BE_FINAL_CANONICAL_YET", "Computes per-sleeve realized PnL from sleeve trade facts and blocks on broker-backed NAV/cash reconciliation.", ["sleeve_trade_fact", "post_trade_reconciliation", "nav", "cash_ledger"], ["realized_pnl", "fees", "capital_used", "unrealized_pnl_status"]),
        _component("cash_ledger_snapshot", "realized/unrealized PnL", "constellation_2/phaseF/cash_ledger/run/run_cash_ledger_snapshot_day_v1.py", "cash_ledger_v1/snapshots/{day}/cash_ledger_snapshot.v1.json", "BROKER_FACT_SNAPSHOT", "CANONICAL_FOR_BROKER_CASH_NLV_INPUT", "Broker account cash/NLV snapshot used to block PnL if absent or inconsistent.", ["broker_account_summary"], ["cash_total_cents", "nlv_total_cents"]),
        _component("accounting_nav", "realized/unrealized PnL", "accounting_v2/nav", "accounting_v2/nav/{day}/nav.v2.json", "BROKER_ACCOUNTING_SNAPSHOT", "CANONICAL_FOR_PORTFOLIO_NAV_INPUT", "Accounting NAV source consumed by monthly performance and sleeve realized PnL blockers.", ["broker_account_summary", "positions"], ["nav_total", "realized_pnl_to_date", "unrealized_pnl"]),
        _component("monthly_performance_summary", "performance reporting", "ops/tools/run_monthly_performance_summary_v1.py", "reports/monthly_performance_summary_v1/{month}/monthly_performance_summary.v1.json", "DERIVED_HISTORY", "CANONICAL_FOR_MONTHLY_PORTFOLIO_PERFORMANCE_SUMMARY", "Month-level return/drawdown/PnL summary derived only from accounting NAV and fill ledgers.", ["nav_history", "fill_ledger"], ["monthly_return_pct", "realized_pnl", "unrealized_pnl", "drawdown"]),
        _component("sleeve_performance_report", "sleeve metrics", "constellation_2/common/aegis_sleeve_performance_report_v1.py", "reports/sleeve_performance_report_v1/{day}/sleeve_performance_report.v1.json", "DERIVED_JOIN", "PARTIAL_CANONICAL_FOR_MANUAL_PACKET_OUTCOME_JOIN", "Joins manual recommendations, receipts, outcomes, event/alert context, and promoted sleeve metadata.", ["manual_trade_packets", "manual_execution_receipts", "outcome_ledgers", "trade_outcome_attribution"], ["win_rate", "average_return", "regime_performance", "event_performance", "execution_quality"]),
        _component("sleeve_performance_analytics", "sleeve metrics", "ops/aegis/adaptive_governance/sleeve_performance_analytics_v1.py", "reports/aegis_sleeve_performance_analytics_v1/{day}/sleeve_performance_analytics.v1.json", "DERIVED_ANALYTICS", "DUPLICATE_COMPAT_ANALYTICS", "Calculates return, Sharpe, Sortino, win rate, expectancy, profit factor, advisory quality, and stop attribution with sample-size gating.", ["sleeve_attribution", "sleeve_performance_report", "manual_execution_receipt", "candidate_lifecycle", "position_management"], ["win_rate", "expectancy", "profit_factor", "recommendation_accuracy", "regime_adjusted_performance"]),
        _component("sleeve_economic_truth_pipeline", "sleeve metrics", "constellation_2/aegis_truth/sleeve_economic_truth_pipeline_v1.py", "reports/sleeve_economic_truth_pipeline_v1/{day}/sleeve_economic_truth_pipeline.v1.json", "DERIVED_PIPELINE", "SHOULD_BECOME_CANONICAL_ORCHESTRATOR", "Orders trade facts, realized PnL, risk realization, decision-price snapshot, fill quality, effectiveness, and scorecard rollups.", ["sleeve_trade_fact", "sleeve_realized_pnl", "sleeve_risk_realization", "sleeve_fill_quality", "sleeve_effectiveness"], ["daily_scorecard", "weekly_scorecard", "monthly_scorecard"]),
        _component("sleeve_scorecards", "leaderboard/ranking logic", "constellation_2/aegis_truth/sleeve_scorecard_rollup_v1.py", "reports/sleeve_scorecard_daily_v1/{day}/sleeve_scorecard_daily.v1.json", "DERIVED_ROLLUP", "SHOULD_BECOME_CANONICAL_FOR_SLEEVE_RANKING_INPUTS", "Daily/weekly/monthly sleeve scorecards downstream of economic truth pipeline.", ["sleeve_realized_pnl", "sleeve_fill_quality", "sleeve_effectiveness"], ["best_worst_sleeves", "scorecard_status"]),
        _component("exit_policy_registry", "exit reviews", "ops/aegis/trade_lifecycle/exit_policy_registry_v1.py", "in_code_registry", "STATIC_POLICY_REGISTRY", "PARTIAL_CANONICAL_FOR_PAPER_EXIT_RULE_PARAMETERS", "Default plus sleeve-specific stop/take-profit/trailing/time-stop parameters.", [], ["stop_loss_pct", "take_profit_pct", "trailing_stop_pct", "max_hold_days"]),
        _component("exit_recommendations", "exit reviews", "ops/aegis/exit_recommendations_v1.py", "reports/aegis_exit_recommendations_v1/{day}/exit_recommendations.v1.json", "DERIVED_FROM_PAPER_LEDGER", "PARTIAL_RECOMMENDATION_LAYER", "Human-reviewed paper exit recommendations for open simulated paper positions.", ["paper_position_ledger", "exit_policy_registry", "candidate_context"], ["exit_reason_frequency", "unrealized_pnl_by_sleeve", "recommendation_counts"]),
        _component("exit_decision_engine", "exit reviews", "constellation_2/common/exit_decision_engine_v1.py", "positions_v1/exit_decision_v1/{day}/{position_id}/exit_decision.v1.json", "DERIVED_DECISION", "CANONICAL_FOR_POSITION_EXIT_DECISION_WHEN_WRITTEN", "Rule-precedence exit decision engine for normalized positions.", ["position_normalization", "mark", "risk_plan"], ["stop_breach", "structure_break", "time_stop", "regime_invalidated", "partial_profit", "trailing_stop_update"]),
        _component("exit_review_projection", "exit reviews", "ops/aegis/trade_lifecycle/exit_review_projection_v1.py", "reports/exit_review_projection_v1/{day}/exit_review_projection.v1.json", "DERIVED_UI_PROJECTION", "DUPLICATE_READ_MODEL_TO_CONSOLIDATE", "Lifecycle exit review projection combining trade evaluation, daily exit review, thesis state, position management, and exit decisions.", ["trade_lifecycle_ledger", "position_management", "daily_exit_review", "thesis_state"], ["hold_time", "exit_reason", "exit_reason_frequency", "stop_target_status", "closed_outcomes"]),
        _component("trade_outcome_attribution", "trade attribution", "governance/04_DATA/SCHEMAS/C2/REPORTS/trade_outcome_attribution.v1.schema.json", "reports/trade_outcome_attribution_v1/{day}/trade_outcome_attribution.v1.json", "DERIVED_ATTRIBUTION", "READ_ONLY_LEGACY_UNTIL_RECONCILED", "Historical trade outcome attribution schema/report used by sleeve performance report joins.", ["outcome_ledger", "candidate_id"], ["MAE", "MFE", "actual_entry_price", "sleeve_signal_quality"]),
        _component("sleeve_intent_trade_attribution", "recommendation attribution", "constellation_2/common/opportunity_lineage_attribution_v1.py", "reports/sleeve_intent_trade_attribution_v1/{day}/sleeve_intent_trade_attribution.v1.json", "EVENT_LINEAGE_DERIVED_ATTRIBUTION", "SHOULD_BECOME_CANONICAL_FOR_INTENT_TO_FILL_LINEAGE", "Opportunity lineage events and attribution from sleeve run through fill/reject/cancel stages.", ["opportunity_lineage_event", "submit_decision_trace", "order/fill evidence"], ["classification_counts", "opportunity_count", "completeness_status"]),
        _component("outcome_effectiveness_attribution", "recommendation attribution", "constellation_2/common/outcome_effectiveness_attribution_v1.py", "function_only", "RULE_CLASSIFIER", "CANONICAL_FOR_CONSERVATIVE_EFFECTIVENESS_LANGUAGE", "Conservative bounded/unsupported effectiveness attribution classifier.", ["opportunity_payload", "comparison_state"], ["effectiveness_state", "attribution_state"]),
        _component("performance_projection", "performance dashboards", "constellation_2/common/performance_projection_v1.py", "reports/performance_projection_v1/{day}/performance_projection.v1.json", "DERIVED_TIMING_PROJECTION", "NOT_PNL_PERFORMANCE", "Execution journal timing projection; useful operational performance, not trading PnL/performance.", ["execution_journal"], ["stage_durations", "wall_time"]),
        _component("ui_performance_cockpit", "performance dashboards", "constellation_2/phaseL/ui", "portal routes/tests", "READ_MODEL_UI", "DISPLAY_ONLY", "Portal and UI read models expose performance/exit/sleeve data without owning truth.", ["read_models", "reports"], ["dashboard", "cockpit", "debug_views"]),
        _component("nav_history_ledger", "historical ledgers", "ops/tools/gen_nav_history_ledger_v1.py", "monitoring_v1/economic_nav_drawdown_v1/nav_history_ledger/{day}/nav_history_ledger.v1.json", "DERIVED_HISTORY_LEDGER", "CANONICAL_FOR_NAV_HISTORY_ACCELERATION", "Immutable day-indexed NAV history ledger derived only from NAV snapshot truth.", ["nav_snapshot"], ["historical_nav_days"]),
    ]
    out = []
    for spec in specs:
        row = dict(spec)
        artifact = str(row["artifact_pattern"]).replace("{day}", day_utc)
        if artifact.startswith("reports/") or artifact.startswith("accounting_v2/") or artifact.startswith("cash_ledger_v1/") or artifact.startswith("positions_v1/") or artifact.startswith("monitoring_v1/"):
            path = truth_root / artifact
            row["current_day_artifact_path"] = str(path)
            row["current_day_artifact_exists"] = path.exists()
        else:
            path = repo_root / artifact
            row["current_day_artifact_path"] = str(path) if path.exists() else artifact
            row["current_day_artifact_exists"] = path.exists()
        out.append(row)
    return out


def duplicate_source_detection_v1(components: list[dict[str, Any]]) -> list[dict[str, Any]]:
    capability_to_components: dict[str, list[str]] = {}
    for component in components:
        for capability in component.get("capabilities") or []:
            capability_to_components.setdefault(str(capability), []).append(str(component["component_id"]))
    duplicates = []
    for capability, owners in sorted(capability_to_components.items()):
        unique = sorted(set(owners))
        if len(unique) > 1:
            duplicates.append(
                {
                    "capability": capability,
                    "component_ids": unique,
                    "conflict_risk": _conflict_risk(capability),
                    "recommendation": _duplicate_recommendation(capability),
                }
            )
    return duplicates


def canonical_source_labels_v1(components: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "component_id": str(row["component_id"]),
            "domain": str(row["domain"]),
            "canonicality": str(row["canonicality"]),
            "canonical_recommendation": _canonical_recommendation(str(row["component_id"]), str(row["canonicality"])),
        }
        for row in components
    ]


def event_sourced_vs_derived_v1(components: list[dict[str, Any]]) -> dict[str, list[str]]:
    out = {"event_sourced": [], "broker_or_accounting_fact": [], "derived": [], "static_or_function": []}
    for row in components:
        model = str(row.get("truth_model") or "")
        cid = str(row.get("component_id") or "")
        if "EVENT" in model:
            out["event_sourced"].append(cid)
        elif "BROKER" in model:
            out["broker_or_accounting_fact"].append(cid)
        elif "STATIC" in model or "FUNCTION" in model or row.get("artifact_pattern") in {"function_only", "in_code_registry"}:
            out["static_or_function"].append(cid)
        else:
            out["derived"].append(cid)
    return out


def missing_linkage_points_v1() -> list[dict[str, str]]:
    return [
        {"linkage": "paper_position_ledger -> sleeve_realized_pnl", "status": "MISSING", "impact": "Paper realized PnL and broker-backed realized PnL are separate worlds."},
        {"linkage": "exit_recommendation_id -> eventual exit receipt/outcome", "status": "PARTIAL", "impact": "Recommendation hit-rate is reported as insufficient closed attribution."},
        {"linkage": "open-position mark authority -> unrealized PnL by sleeve", "status": "PARTIAL", "impact": "Paper ledger can mark positions from market data, but broker open-position marks are not canonicalized by sleeve."},
        {"linkage": "candidate recommendation -> sleeve intent attribution -> fill/result", "status": "SPLIT", "impact": "Candidate packet joins and opportunity lineage attribution are both useful but not unified."},
        {"linkage": "exit_decision_v1 -> exit_review_projection -> paper_position_ledger", "status": "SPLIT", "impact": "Lifecycle exit engine and paper ledger recommendation layer duplicate exit surfaces."},
        {"linkage": "regime labels -> realized sleeve scorecards", "status": "PARTIAL", "impact": "Regime-specific performance exists in manual sleeve reports but needs economic-truth lineage."},
    ]


def stale_legacy_logic_v1() -> list[dict[str, str]]:
    return [
        {"component_id": "aegis_sleeve_attribution_v1", "classification": "legacy/partial", "reason": "Scans broad report names and manual receipts; useful for advisory health but not strong enough as economic truth."},
        {"component_id": "paper_trade_outcomes", "classification": "legacy/read-only", "reason": "Still used by candidate state, but event-sourced paper_position_ledger is cleaner for paper position state."},
        {"component_id": "trade_outcome_attribution", "classification": "legacy/read-only", "reason": "Schema/report joins historical outcome fields but should be reconciled into opportunity lineage and economic truth."},
        {"component_id": "performance_projection_v1", "classification": "not PnL", "reason": "Name overlaps performance reporting, but it tracks runtime stage durations only."},
        {"component_id": "compat sleeve_performance_analytics_v1", "classification": "compat duplicate", "reason": "Both aegis_sleeve_performance_analytics_v1 and sleeve_performance_analytics_v1 are written."},
    ]


def overlap_conflicts_v1(duplicates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "conflict": "Realized PnL appears in paper ledger, sleeve_realized_pnl, sleeve_performance_report returns, monthly NAV summary, and trade outcome attribution.",
            "severity": "HIGH",
            "affected_duplicate_capabilities": [row["capability"] for row in duplicates if "pnl" in str(row["capability"]).lower() or "return" in str(row["capability"]).lower()],
            "resolution": "Use broker/accounting NAV/cash/fill facts for portfolio/account PnL; use paper_position_ledger only for simulated paper review.",
        },
        {
            "conflict": "Exit decisions exist as exit_decision_v1, exit_review_projection_v1, and exit_recommendations_v1.",
            "severity": "MEDIUM",
            "affected_duplicate_capabilities": [row["capability"] for row in duplicates if "exit" in str(row["capability"]).lower()],
            "resolution": "Keep exit_decision_v1 as per-position decision authority; make projections display-only.",
        },
        {
            "conflict": "Sleeve rankings can be inferred from sleeve_performance_report, sleeve_performance_analytics, and sleeve_scorecards.",
            "severity": "MEDIUM",
            "affected_duplicate_capabilities": [row["capability"] for row in duplicates if "win_rate" in str(row["capability"]) or "expectancy" in str(row["capability"])],
            "resolution": "Make sleeve scorecards the canonical ranking input after they consume canonical economic facts.",
        },
    ]


def sleeve_metric_availability_v1() -> dict[str, Any]:
    return {
        "available_now": ["win_rate", "average_return", "realized_return_pct", "MAE", "MFE", "regime_performance", "event_performance", "expectancy", "profit_factor", "recommendation_accuracy"],
        "partial_or_low_confidence": ["realized_pnl_by_sleeve", "unrealized_pnl_by_sleeve", "exit_reason_frequency", "recommendation_followed_rate", "average_hold_time", "best_worst_sleeves"],
        "missing_or_not_canonical": ["broker-backed unrealized PnL by sleeve", "recommendation hit-rate after exit", "open-position mark authority by sleeve", "regime-specific realized PnL from economic truth"],
        "sample_size_policy": "sleeve_performance_analytics has metric status/sample-size gating; low sample metrics should stay advisory.",
    }


def pnl_attribution_quality_v1() -> dict[str, str]:
    return {
        "realized_pnl": "PARTIAL. sleeve_realized_pnl blocks on broker-backed NAV/cash and derives from sleeve_trade_fact, but the arithmetic is trade-fact simplified and not yet the sole ledger of record.",
        "unrealized_pnl": "NOT_CANONICAL. Paper ledger can compute simulated marks; accounting NAV carries portfolio unrealized_pnl; no canonical broker open-position mark by sleeve exists yet.",
        "historical_attribution": "PARTIAL. Monthly summary and NAV history are stronger for account-level history; sleeve history is split across scorecards and reports.",
        "multiple_ledgers_disagree_risk": "HIGH until paper, manual packet, broker accounting, and opportunity lineage ledgers are explicitly partitioned by authority.",
    }


def unrealized_vs_realized_handling_v1() -> dict[str, str]:
    return {
        "realized": "Closed-trade realized values exist in paper_position_ledger and sleeve_performance_report; broker/account realized appears through NAV/monthly/accounting paths; sleeve_realized_pnl is the emerging economic truth.",
        "unrealized": "Paper open positions have current marks when market data exists; sleeve_realized_pnl intentionally emits unrealized_pnl=null with UNKNOWN_NO_BROKER_MARK for open broker facts.",
        "open_position_marks": "Paper marks are useful but simulated. Broker-backed open marks need a canonical per-sleeve position mark surface before unrealized PnL can be canonical.",
    }


def current_weaknesses_v1() -> list[str]:
    return [
        "Canonical authority boundaries are implicit in code names rather than enforced by one performance truth contract.",
        "Paper/simulated outcomes and broker/accounting outcomes are both called PnL in downstream summaries.",
        "Exit recommendation quality cannot be measured robustly because recommendation IDs are not consistently linked to later exit receipts and outcomes.",
        "Unrealized PnL by sleeve has no broker-backed canonical mark source.",
        "Several performance artifacts compute overlapping win-rate/return/ranking metrics with different input bases.",
        "Regime-specific performance exists but is not yet tied to broker-backed sleeve economic truth.",
        "Performance dashboards should not choose between duplicate ledgers; they need a single read model derived from canonical sources.",
    ]


def exit_recommendation_quality_v1() -> dict[str, Any]:
    per_sleeve = []
    for sleeve_id in ["DEFAULT", "C2_EVENT_DISLOCATION_V1", "C2_MEAN_REVERSION_EQ_V1", "C2_TREND_EQ_PRIMARY_V1"]:
        per_sleeve.append(
            {
                "sleeve_id": sleeve_id,
                "entry_logic_exists": "YES_OUTSIDE_EXIT_REGISTRY",
                "exit_logic_exists": "YES_BASIC_RULES",
                "stop_logic": "YES_PERCENT_AND_RECORDED_STOP",
                "time_stop": "YES_MAX_HOLD_DAYS",
                "trailing_stop": "YES_PERCENT_OR_R_MULTIPLE_REVIEW",
                "invalidation": "PARTIAL_SIGNAL_AND_REGIME_FLAGS",
                "recommendation_attribution": "PARTIAL_RECOMMENDATION_COUNTS_ONLY",
                "pnl_attribution": "PARTIAL_PAPER_LEDGER_SUMMARY",
                "hold_time_metrics": "PARTIAL_HOLDING_DAYS_AND_AVERAGE_CLOSED_HOLD",
                "outcome_metrics": "PARTIAL_WIN_LOSS_AND_REALIZED_PNL",
                "quality": "BASIC_RULES_NOT_BACKTEST_CALIBRATED",
            }
        )
    return {
        "overall": "Exit recommendation quality is operationally useful for human-reviewed paper monitoring, but not yet a canonical or calibrated exit policy.",
        "strengths": ["stop breach", "take profit", "time stop", "trailing stop", "signal/regime invalidation hooks", "operator action safety flags"],
        "gaps": ["certified intraday/current marks", "MFE/high-water evidence", "volatility/ATR stops", "recommendation-to-exit outcome attribution", "sleeve-specific calibration"],
        "per_sleeve": per_sleeve,
    }


def pnl_architecture_evaluation_v1() -> dict[str, Any]:
    return {
        "realized_pnl_is_canonical": "NO_PARTIAL",
        "realized_pnl_best_existing_authority": "sleeve_realized_pnl_v1 should become canonical for sleeve realized PnL after its trade fact arithmetic is reconciled to broker accounting/fill facts.",
        "unrealized_pnl_is_canonical": "NO",
        "open_position_marks_are_canonical": "NO_FOR_SLEEVE_BROKER_MARKS; YES_ONLY_FOR_SIMULATED_PAPER_LEDGER_MARKS",
        "historical_attribution_is_canonical": "PARTIAL_NAV_HISTORY_AND_MONTHLY_ACCOUNT_LEVEL_ONLY",
        "multiple_ledgers_disagree": "POTENTIAL_YES",
        "canonical_now": ["paper_position_ledger for simulated paper positions", "accounting_nav for account NAV inputs", "cash_ledger_snapshot for broker cash/NLV input", "nav_history_ledger for NAV history acceleration"],
        "should_become_canonical": ["sleeve_economic_truth_pipeline as orchestrator", "sleeve_trade_fact for completed trade facts", "sleeve_realized_pnl for broker-backed sleeve realized PnL", "future broker-backed open-position mark by sleeve", "sleeve_scorecards for rankings"],
        "should_be_deprecated": ["broad-scan aegis_sleeve_attribution as performance authority", "compat duplicate sleeve_performance_analytics family", "UI choosing direct historical PnL sources"],
        "read_only_legacy": ["paper_trade_outcomes", "trade_outcome_attribution", "candidate_lifecycle_outcomes", "performance_projection for timing only"],
    }


def sleeve_analytics_readiness_v1() -> dict[str, str]:
    return {
        "win_rate": "AVAILABLE_FROM_sleeve_performance_report_AND_sleeve_performance_analytics_WITH_SAMPLE_LIMITS",
        "expectancy": "AVAILABLE_FROM_sleeve_performance_analytics_WITH_SAMPLE_LIMITS",
        "average_hold_time": "PARTIAL_FROM_exit_recommendations_pnl_hooks_AND_exit_review_projection",
        "realized_pnl_by_sleeve": "PARTIAL_FROM_paper_position_ledger_AND_sleeve_realized_pnl",
        "unrealized_pnl_by_sleeve": "PARTIAL_PAPER_ONLY_NOT_BROKER_CANONICAL",
        "exit_reason_frequency": "PARTIAL_FROM_exit_recommendations_AND_exit_review_projection",
        "recommendation_followed_rate": "NOT_READY_RECOMMENDATION_ID_TO_OUTCOME_LINK_MISSING",
        "best_worst_sleeves": "PARTIAL_FROM_scorecards_OR_ANALYTICS_BUT_CANONICAL_RANKING_INPUT_NOT_DECLARED",
        "regime_specific_performance": "PARTIAL_FROM_sleeve_performance_report_NOT_ECONOMIC_TRUTH_CANONICAL",
    }


def architecture_recommendations_v1() -> dict[str, Any]:
    return {
        "minimal_durable_next_step": "Write a canonical authority map/contract for performance truth that partitions paper simulation, broker/accounting PnL, sleeve economic truth, and UI read models. Then make sleeve_economic_truth_pipeline consume only canonical fact sources and publish one scorecard input.",
        "strongest_long_term_design_before_diminishing_returns": [
            "Canonical fact layer: fill/order/cash/NAV/position mark facts from broker/accounting sources.",
            "Canonical identity layer: stable trade_identity_id/opportunity_id/sleeve_id joins across recommendation, execution, fill, and outcome.",
            "Economic truth reducers: sleeve_trade_fact, sleeve_realized_pnl, broker-backed sleeve_open_mark/unrealized_pnl, sleeve_effectiveness.",
            "Scorecard/read model layer: daily/weekly/monthly sleeve scorecards and one portal performance read model.",
            "Legacy adapters: paper_position_ledger and manual packet reports remain explicitly paper/read-only.",
        ],
        "what_not_to_build": [
            "Do not build another PnL ledger from UI state or candidate packets.",
            "Do not make paper_position_ledger stand in for broker-backed PnL.",
            "Do not add a Portal performance page that directly merges duplicate sources before canonical labels are enforced.",
            "Do not rank sleeves from low-sample advisory analytics without metric-status gating.",
        ],
        "consolidate_first": [
            "Exit surfaces: make exit_decision_v1 the decision authority and keep exit_review_projection/exit_recommendations as read models.",
            "PnL labels: rename or label paper PnL, NAV/account PnL, and sleeve economic PnL distinctly in artifacts.",
            "Recommendation attribution: persist recommendation_id through receipt, exit, and outcome records.",
            "Sleeve ranking: route rankings through sleeve_scorecard rollups after canonical inputs are verified.",
        ],
    }


def write_performance_architecture_review_v1(*, repo_root: Path, truth_root: Path, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    payload = payload or build_performance_architecture_review_v1(repo_root=repo_root, truth_root=truth_root, day_utc=day_utc)
    path = report_path_v1(repo_root=repo_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)
    return path


def _component(component_id: str, domain: str, source: str, artifact_pattern: str, truth_model: str, canonicality: str, description: str, inputs: list[str], capabilities: list[str]) -> dict[str, Any]:
    return {
        "component_id": component_id,
        "domain": domain,
        "source": source,
        "artifact_pattern": artifact_pattern,
        "truth_model": truth_model,
        "canonicality": canonicality,
        "description": description,
        "inputs": inputs,
        "capabilities": capabilities,
    }


def _canonical_recommendation(component_id: str, canonicality: str) -> str:
    if canonicality.startswith("CANONICAL"):
        return "KEEP_CANONICAL"
    if canonicality.startswith("SHOULD_BECOME"):
        return "PROMOTE_AFTER_CONTRACT_AND_RECONCILIATION"
    if "DUPLICATE" in canonicality:
        return "CONSOLIDATE_OR_DEPRECATE_DUPLICATE_READ_MODEL"
    if "LEGACY" in canonicality:
        return "KEEP_READ_ONLY_LEGACY_UNTIL_CONSUMERS_MIGRATE"
    if component_id == "sleeve_realized_pnl":
        return "HARDEN_BEFORE_CANONICAL"
    return "KEEP_WITH_EXPLICIT_AUTHORITY_LABEL"


def _conflict_risk(capability: str) -> str:
    text = capability.lower()
    if "pnl" in text or "return" in text:
        return "HIGH"
    if "exit" in text or "win_rate" in text or "ranking" in text:
        return "MEDIUM"
    return "LOW"


def _duplicate_recommendation(capability: str) -> str:
    text = capability.lower()
    if "unrealized" in text:
        return "Partition simulated paper marks from broker-backed open-position marks."
    if "realized" in text or "return" in text:
        return "Use broker/accounting economic truth for canonical PnL and leave paper/manual reports as attribution context."
    if "exit" in text:
        return "Use exit_decision_v1 as authority and keep projections as read models."
    return "Declare one authority and mark other sources derived/read-only."


def _runtime_truth_context(truth_root: Path, day_utc: str) -> dict[str, Any]:
    path = truth_root / "reports" / "aegis_runtime_truth_kernel_v1" / day_utc / "runtime_truth_kernel.v1.json"
    payload = _read_json(path)
    return {
        "runtime_truth_kernel_path": str(path),
        "runtime_truth_kernel_exists": path.exists(),
        "runtime_truth_classification": str(payload.get("runtime_truth_classification") or "UNKNOWN"),
        "highest_readiness_layer": str(payload.get("highest_readiness_layer") or "UNKNOWN"),
        "trade_advice_allowed": bool(payload.get("trade_advice_allowed")),
        "manual_trade_capture_allowed": bool(payload.get("manual_trade_capture_allowed")),
    }


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _now_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_performance_architecture_review_v1")
    parser.add_argument("--truth_root", default=os.environ.get("AEGIS_TRUTH_ROOT", "/home/node/constellation_runtime_data/truth"))
    parser.add_argument("--day", default=os.environ.get("TARGET_DAY", datetime.now(UTC).date().isoformat()))
    parser.add_argument("--repo_root", default=str(Path(__file__).resolve().parents[2]))
    args = parser.parse_args(argv)

    repo_root = Path(args.repo_root).expanduser().resolve()
    truth_root = Path(args.truth_root).expanduser().resolve()
    day_utc = str(args.day).strip()
    if len(day_utc) != 10:
        raise SystemExit(f"FAIL: invalid day: {day_utc}")
    path = write_performance_architecture_review_v1(repo_root=repo_root, truth_root=truth_root, day_utc=day_utc)
    print(json.dumps({"day_utc": day_utc, "path": str(path), "review_only": True}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
