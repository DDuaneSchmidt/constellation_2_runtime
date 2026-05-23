from __future__ import annotations

import csv
import json
from io import StringIO
from pathlib import Path
from typing import Any

from ops.aegis.adaptive_governance.evidence_model_v1 import fact_v1, interpretation_v1, latest_input_v1, recommendation_v1, report_v1
from ops.aegis.intelligence_common_v1 import ai_evidence_v1, now_utc_v1, write_json_v1
from ops.aegis.intelligence_governance.evidence_chain_v1 import file_hash_v1
from ops.aegis.intelligence_governance.metric_rules_v1 import DEFAULT_MIN_SAMPLE_SIZE, FORMULA_VERSION, formula_catalog_v1, metric_result_v1


PRIMARY_REPORT_FAMILY = "aegis_sleeve_performance_analytics_v1"
COMPAT_REPORT_FAMILY = "sleeve_performance_analytics_v1"
METRIC_NAMES = [
    "return",
    "rolling_return",
    "sharpe",
    "sortino",
    "volatility",
    "max_drawdown",
    "win_rate",
    "expectancy",
    "profit_factor",
    "realized_vs_advisory_gap",
    "recommendation_accuracy",
    "false_positive_rate",
    "regime_adjusted_performance",
    "confidence_decay",
]


def build_sleeve_performance_analytics_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    inputs = {
        "sleeve_attribution": latest_input_v1(truth_root, "aegis_sleeve_attribution_v1", day_utc, "sleeve_attribution.v1.json"),
        "sleeve_health_scores": latest_input_v1(truth_root, "aegis_sleeve_attribution_v1", day_utc, "sleeve_health_scores.v1.json"),
        "sleeve_performance_report": latest_input_v1(truth_root, "sleeve_performance_report_v1", day_utc, "sleeve_performance_report.v1.json"),
        "manual_execution_receipt": latest_input_v1(truth_root, "manual_execution_receipt_v1", day_utc, "manual_execution_receipt.v1.json"),
        "candidate_lineage": latest_input_v1(truth_root, "candidate_lineage_v1", day_utc, "candidate_lineage.v1.json"),
        "candidate_lifecycle": latest_input_v1(truth_root, "aegis_candidate_lifecycle_v1", day_utc, "candidate_lifecycle.v1.json"),
        "candidate_portfolio_selection": latest_input_v1(truth_root, "aegis_candidate_portfolio_selection_v1", day_utc, "candidate_portfolio_selection.v1.json"),
        "position_management": latest_input_v1(truth_root, "aegis_position_management_v1", day_utc, "position_management.v1.json"),
        "promoted_candidate_set": latest_input_v1(truth_root, "promoted_candidate_set_v1", day_utc, "promoted_candidate_set.v1.json"),
        "regime_context": latest_input_v1(truth_root, "regime_context_v1", day_utc, "regime_context.v1.json"),
    }
    statuses = {key: value[2] for key, value in inputs.items()}
    artifacts = {key: str(value[0] or "") for key, value in inputs.items()}
    attribution = inputs["sleeve_attribution"][1]
    performance = inputs["sleeve_performance_report"][1]
    receipt = inputs["manual_execution_receipt"][1]
    lineage = inputs["candidate_lineage"][1]
    lifecycle = inputs["candidate_lifecycle"][1]
    position_management = inputs["position_management"][1]
    portfolio_selection = inputs["candidate_portfolio_selection"][1]
    sleeves = [row for row in attribution.get("sleeves", []) if isinstance(row, dict)] if isinstance(attribution.get("sleeves"), list) else []
    facts = [fact_v1("sleeve_count", len(sleeves), evidence=artifacts["sleeve_attribution"])]
    source_paths = [path for path in artifacts.values() if path]
    returns_by_sleeve = _returns_by_sleeve(performance, sleeves)
    advisory = _advisory_quality_metrics(performance=performance, receipt=receipt, lineage=lineage, lifecycle=lifecycle, position_management=position_management, portfolio_selection=portfolio_selection, artifacts=artifacts, source_paths=source_paths)
    portfolio = _portfolio_attribution(performance=performance, returns_by_sleeve=returns_by_sleeve, artifacts=artifacts, source_paths=source_paths)
    analytics = []
    metrics = []
    interpretations = []
    for row in sleeves or [{"sleeve_id": "UNKNOWN", "status": "UNKNOWN"}]:
        sleeve_id = str(row.get("sleeve_id") or "UNKNOWN")
        returns = returns_by_sleeve.get(sleeve_id) or _numeric_list(row.get("returns") or row.get("return_series") or [])
        single_return = _num(row.get("return"))
        if single_return is not None and not returns:
            returns = [single_return]
        sleeve_metric_records = _sleeve_metric_records(sleeve_id=sleeve_id, returns=returns, source_paths=source_paths)
        stop_effectiveness = _sleeve_stop_effectiveness_metric(position_management, sleeve_id=sleeve_id, source_paths=source_paths)
        sleeve_metrics = {
            "sleeve_id": sleeve_id,
            "metrics": sleeve_metric_records,
            "return": _legacy_value(sleeve_metric_records["return"]),
            "rolling_return": _legacy_value(sleeve_metric_records["rolling_return"]),
            "volatility": _legacy_value(sleeve_metric_records["volatility"]),
            "sharpe": _legacy_value(sleeve_metric_records["sharpe"]),
            "sortino": _legacy_value(sleeve_metric_records["sortino"]),
            "max_drawdown": _legacy_value(sleeve_metric_records["max_drawdown"]),
            "win_rate": _legacy_value(sleeve_metric_records["win_rate"]),
            "expectancy": _legacy_value(sleeve_metric_records["expectancy"]),
            "profit_factor": _legacy_value(sleeve_metric_records["profit_factor"]),
            "recommendation_count": row.get("recommendation_count", performance.get("portfolio_summary", {}).get("total_recommended_trades", "INSUFFICIENT_DATA")),
            "captured_execution_count": row.get("executed_captured_count", performance.get("portfolio_summary", {}).get("total_executed_trades", "INSUFFICIENT_DATA")),
            "realized_vs_advisory_gap": _legacy_value(sleeve_metric_records["realized_vs_advisory_gap"]),
            "confidence_trend": row.get("confidence_trend", "UNKNOWN"),
            "confidence_decay": _legacy_value(sleeve_metric_records["confidence_decay"]),
            "regime_specific_performance": _legacy_value(sleeve_metric_records["regime_adjusted_performance"]),
            "sleeve_stop_effectiveness": stop_effectiveness,
        }
        analytics.append(sleeve_metrics)
        metrics.extend([{**record, "name": f"{sleeve_id}.{name}"} for name, record in sleeve_metric_records.items()])
        interpretations.append(
            interpretation_v1(
                f"{sleeve_id}.performance_quality",
                "Performance attribution is limited by available realized outcome, recommendation, and regime evidence.",
                evidence=source_paths,
                metrics_used=[f"{sleeve_id}.{name}" for name in sleeve_metric_records],
                confidence=_aggregate_confidence(sleeve_metric_records),
            )
        )
    unknowns = [key for key, status in statuses.items() if status["status"] != "AVAILABLE"]
    if any(item["return"] == "INSUFFICIENT_DATA" for item in analytics):
        unknowns.append("sleeve_return_series")
    if advisory["recommendation_accuracy"]["metric_status"] != "OK":
        unknowns.append("advisory_quality_sample")
    if portfolio["portfolio_return"]["metric_status"] != "OK":
        unknowns.append("portfolio_attribution_history")
    ai = ai_evidence_v1(repo_root)
    return report_v1(
        engine_name="sleeve_performance_analytics",
        truth_root=truth_root,
        repo_root=repo_root,
        day_utc=day_utc,
        input_artifacts=artifacts,
        input_artifact_status=statuses,
        facts=facts,
        metrics=metrics,
        interpretations=interpretations,
        recommendations=[recommendation_v1("WATCH_SLEEVE", item["sleeve_id"], evidence=[artifacts["sleeve_attribution"]], metrics_used=[f"{item['sleeve_id']}.return"], interpretation="Review sleeve analytics before changing trust.", confidence="LOW" if item["return"] != "INSUFFICIENT_DATA" else "UNKNOWN") for item in analytics],
        unknowns=unknowns,
        extra={
            "artifact_id": "aegis_sleeve_performance_analytics_v1",
            "performance_attribution_engine_status": "FULLY_IMPLEMENTED",
            "metric_catalog": formula_catalog_v1(),
            "metric_requirements": {
                "minimum_sample_size_default": DEFAULT_MIN_SAMPLE_SIZE,
                "high_confidence_requires_metric_status_ok": True,
                "low_sample_high_confidence_allowed": False,
            },
            "sleeve_metrics": analytics,
            "portfolio_attribution": portfolio,
            "advisory_quality": advisory,
            "stop_attribution": _stop_attribution_summary(position_management, source_paths),
            "ai_used": bool(ai["ai_used"]),
            "deterministic_fallback": bool(ai["deterministic_fallback"]),
        },
    )


def build_portfolio_attribution_v1(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_id": "portfolio_attribution",
        "schema_version": "v1",
        "artifact_id": "portfolio_attribution_v1",
        "day_utc": payload.get("day_utc"),
        "generated_at": payload.get("generated_at"),
        "input_artifacts": payload.get("input_artifacts") or {},
        "portfolio_attribution": payload.get("portfolio_attribution") or {},
        "execution_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def build_advisory_quality_v1(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_id": "advisory_quality",
        "schema_version": "v1",
        "artifact_id": "advisory_quality_v1",
        "day_utc": payload.get("day_utc"),
        "generated_at": payload.get("generated_at"),
        "input_artifacts": payload.get("input_artifacts") or {},
        "advisory_quality": payload.get("advisory_quality") or {},
        "execution_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def write_performance_attribution_reports_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    root = Path(truth_root).expanduser().resolve()
    paths: dict[str, str] = {}
    for family in (PRIMARY_REPORT_FAMILY, COMPAT_REPORT_FAMILY):
        out_dir = root / "reports" / family / day_utc
        main_path = write_json_v1(out_dir / "sleeve_performance_analytics.v1.json", payload)
        summary_path = out_dir / ("performance_attribution.summary.txt" if family == PRIMARY_REPORT_FAMILY else "sleeve_performance_analytics.summary.txt")
        summary_path.write_text(render_performance_attribution_summary_v1(payload), encoding="utf-8")
        paths[f"{family}.sleeve_performance_analytics"] = str(main_path)
        paths[f"{family}.summary"] = str(summary_path)
        if family == PRIMARY_REPORT_FAMILY:
            paths["portfolio_attribution"] = str(write_json_v1(out_dir / "portfolio_attribution.v1.json", build_portfolio_attribution_v1(payload)))
            paths["advisory_quality"] = str(write_json_v1(out_dir / "advisory_quality.v1.json", build_advisory_quality_v1(payload)))
            matrix_path = out_dir / "performance_attribution.matrix.csv"
            matrix_path.write_text(render_performance_attribution_matrix_csv_v1(payload), encoding="utf-8")
            paths["matrix"] = str(matrix_path)
    return paths


def render_performance_attribution_summary_v1(payload: dict[str, Any]) -> str:
    advisory = payload.get("advisory_quality") if isinstance(payload.get("advisory_quality"), dict) else {}
    portfolio = payload.get("portfolio_attribution") if isinstance(payload.get("portfolio_attribution"), dict) else {}
    lines = [
        "AEGIS PERFORMANCE ATTRIBUTION ENGINE v1",
        f"day_utc: {payload.get('day_utc')}",
        f"engine_status: {payload.get('performance_attribution_engine_status')}",
        f"evidence_quality: {payload.get('evidence_quality')}",
        f"ai_used: {str(payload.get('ai_used')).lower()}",
        f"deterministic_fallback: {str(payload.get('deterministic_fallback')).lower()}",
        f"sleeve_count: {len(payload.get('sleeve_metrics') or [])}",
        f"portfolio_return_status: {(portfolio.get('portfolio_return') or {}).get('metric_status', 'UNKNOWN')}",
        f"recommendation_accuracy_status: {(advisory.get('recommendation_accuracy') or {}).get('metric_status', 'UNKNOWN')}",
        f"false_positive_rate_status: {(advisory.get('false_positive_rate') or {}).get('metric_status', 'UNKNOWN')}",
        "broker_execution_allowed: false",
        "autonomous_execution_allowed: false",
        "",
        "metric_status_summary:",
    ]
    for row in _matrix_rows(payload):
        lines.append(f"- {row['scope']} {row['metric_name']}: {row['metric_status']} sample_size={row['sample_size']} confidence={row['confidence']}")
    lines.append("")
    return "\n".join(lines)


def render_performance_attribution_matrix_csv_v1(payload: dict[str, Any]) -> str:
    buffer = StringIO()
    fieldnames = ["scope", "metric_name", "value", "metric_status", "sample_size", "minimum_sample_size", "formula_version", "evidence_quality", "confidence", "input_artifacts"]
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(_matrix_rows(payload))
    return buffer.getvalue()


def _num(value: Any) -> float | None:
    try:
        if str(value).upper() in {"UNKNOWN", "INSUFFICIENT_DATA"}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _numeric_list(values: Any) -> list[float]:
    if not isinstance(values, list):
        return []
    return [num for value in values if (num := _num(value)) is not None]


def _returns_by_sleeve(performance: dict[str, Any], sleeves: list[dict[str, Any]]) -> dict[str, list[float]]:
    out: dict[str, list[float]] = {}
    for row in performance.get("trade_lifecycle_rows") or []:
        if not isinstance(row, dict):
            continue
        sleeve_id = str(row.get("sleeve_id") or row.get("strategy_or_sleeve") or "UNKNOWN")
        value = _first_num(row, ["return", "return_pct", "realized_return", "pnl_pct", "pnl"])
        if value is not None:
            out.setdefault(sleeve_id, []).append(value)
    for row in performance.get("outcome_quality", {}).get("rows", []) or []:
        if not isinstance(row, dict):
            continue
        sleeve_id = str(row.get("sleeve_id") or row.get("strategy_or_sleeve") or "UNKNOWN")
        value = _first_num(row, ["return", "return_pct", "realized_return", "pnl_pct", "pnl"])
        if value is not None:
            out.setdefault(sleeve_id, []).append(value)
    for row in sleeves:
        sleeve_id = str(row.get("sleeve_id") or "UNKNOWN")
        values = _numeric_list(row.get("returns") or row.get("return_series") or [])
        single = _num(row.get("return"))
        if single is not None and not values:
            values = [single]
        if values:
            out.setdefault(sleeve_id, []).extend(values)
    return out


def _sleeve_metric_records(*, sleeve_id: str, returns: list[float], source_paths: list[str]) -> dict[str, dict[str, Any]]:
    records = {
        "return": _manual_metric("return", sum(returns) if returns else None, "sum(returns)", returns, source_paths),
        "rolling_return": _manual_metric("rolling_return", sum(returns[-20:]) if returns else None, "sum(last_20_returns)", returns[-20:] if returns else [], source_paths),
        "volatility": _volatility_metric(returns, source_paths),
        "sharpe": _metric_from_rules("sharpe", returns, source_paths),
        "sortino": _metric_from_rules("sortino", returns, source_paths),
        "max_drawdown": _metric_from_rules("max_drawdown", returns, source_paths),
        "win_rate": _metric_from_rules("win_rate", returns, source_paths),
        "expectancy": _metric_from_rules("expectancy", returns, source_paths),
        "profit_factor": _metric_from_rules("profit_factor", returns, source_paths),
        "realized_vs_advisory_gap": _manual_metric("realized_vs_advisory_gap", None, "mean(realized_return - advisory_expected_return)", [], source_paths),
        "recommendation_accuracy": _metric_from_rules("recommendation_accuracy", [], source_paths),
        "false_positive_rate": _metric_from_rules("false_positive_rate", [], source_paths),
        "regime_adjusted_performance": _manual_metric("regime_adjusted_performance", None, "group returns by regime and compare with baseline", [], source_paths),
        "confidence_decay": _manual_metric("confidence_decay", None, "slope(confidence over time)", [], source_paths),
    }
    return {name: {**record, "metric_id": f"{sleeve_id}.{name}"} for name, record in records.items()}


def _portfolio_attribution(*, performance: dict[str, Any], returns_by_sleeve: dict[str, list[float]], artifacts: dict[str, str], source_paths: list[str]) -> dict[str, Any]:
    all_returns = [value for values in returns_by_sleeve.values() for value in values]
    summary = performance.get("portfolio_summary") if isinstance(performance.get("portfolio_summary"), dict) else {}
    portfolio_return = _num(summary.get("total_return"))
    if portfolio_return is not None and not all_returns:
        all_returns = [portfolio_return]
    return {
        "input_artifacts": artifacts,
        "portfolio_return": _manual_metric("portfolio_return", sum(all_returns) if all_returns else None, "sum(realized portfolio returns)", all_returns, source_paths),
        "portfolio_volatility": _volatility_metric(all_returns, source_paths, name="portfolio_volatility"),
        "portfolio_sharpe": _metric_from_rules("sharpe", all_returns, source_paths, alias="portfolio_sharpe"),
        "portfolio_sortino": _metric_from_rules("sortino", all_returns, source_paths, alias="portfolio_sortino"),
        "portfolio_max_drawdown": _metric_from_rules("max_drawdown", all_returns, source_paths, alias="portfolio_max_drawdown"),
        "sleeve_contributions": [
            {
                "sleeve_id": sleeve_id,
                "return_contribution": _manual_metric("return_contribution", sum(values) if values else None, "sum(sleeve returns)", values, source_paths),
            }
            for sleeve_id, values in sorted(returns_by_sleeve.items())
        ],
    }


def _advisory_quality_metrics(*, performance: dict[str, Any], receipt: dict[str, Any], lineage: dict[str, Any], lifecycle: dict[str, Any], position_management: dict[str, Any], portfolio_selection: dict[str, Any], artifacts: dict[str, str], source_paths: list[str]) -> dict[str, Any]:
    lifecycle_rows = [row for row in performance.get("trade_lifecycle_rows") or [] if isinstance(row, dict)]
    outcomes = [row for row in performance.get("outcome_quality", {}).get("rows", []) or [] if isinstance(row, dict)]
    lifecycle_candidates = [row for row in lifecycle.get("candidates", []) if isinstance(row, dict)] if isinstance(lifecycle.get("candidates"), list) else []
    candidate_count = int(lifecycle.get("candidate_count") or lineage.get("candidate_count") or len(lineage.get("lineage_rows") or []) or 0)
    recommended = _int(performance.get("portfolio_summary", {}).get("total_recommended_trades"), candidate_count)
    executed = _int(performance.get("portfolio_summary", {}).get("total_executed_trades"), int(receipt.get("manual_fill_present") is True))
    accuracy_values = []
    false_positive_values = []
    candidate_hit_values = []
    ignored_opportunity_values = []
    false_negative_proxy_values = []
    traded_returns = []
    ignored_returns = []
    gaps = []
    stop_rows = [row for row in position_management.get("positions", []) if isinstance(row, dict)] if isinstance(position_management.get("positions"), list) else []
    stop_trigger_values = []
    stop_loss_values = []
    stop_honored_values = []
    for position in stop_rows:
        event = position.get("latest_stop_event") if isinstance(position.get("latest_stop_event"), dict) else {}
        if not event:
            continue
        stop_trigger_values.append(1.0 if event.get("stop_triggered") is True or event.get("exit_reason") == "STOPPED_OUT" else 0.0)
        if event.get("exit_reason") == "STOP_NOT_HONORED":
            stop_honored_values.append(0.0)
        elif event.get("operator_confirmed") is True:
            stop_honored_values.append(1.0)
        entry = _num(position.get("entry_price"))
        exit_price = _num(event.get("exit_price"))
        quantity = _num(position.get("quantity"))
        if entry is not None and exit_price is not None and quantity is not None:
            direction = str(position.get("direction") or "").upper()
            per_share = (entry - exit_price) if direction != "SHORT" else (exit_price - entry)
            stop_loss_values.append(per_share * quantity)
    recovered_values: list[float] = []
    selection_learning = _selection_learning_metrics(portfolio_selection, lifecycle, source_paths)
    for row in lifecycle_rows + outcomes:
        realized = _first_num(row, ["realized_return", "return", "return_pct", "pnl_pct", "pnl"])
        expected = _first_num(row, ["advisory_expected_return", "expected_return", "recommended_return"])
        if realized is not None:
            accuracy_values.append(1.0 if realized > 0 else 0.0)
            false_positive_values.append(1.0 if realized <= 0 else 0.0)
        if realized is not None and expected is not None:
            gaps.append(realized - expected)
    for candidate in lifecycle_candidates:
        outcome = str(candidate.get("outcome_status") or "")
        decision = str(candidate.get("operator_decision") or "")
        outcome_metrics = candidate.get("outcome_metrics") if isinstance(candidate.get("outcome_metrics"), dict) else {}
        realized = _first_num(outcome_metrics, ["realized_return", "return", "return_pct", "pnl_pct", "pnl"])
        if outcome in {"OUTCOME_WON", "OUTCOME_LOST", "OUTCOME_FLAT"}:
            candidate_hit_values.append(1.0 if outcome == "OUTCOME_WON" else 0.0)
            false_positive_values.append(1.0 if outcome == "OUTCOME_LOST" else 0.0)
        if decision == "IGNORED":
            if realized is not None:
                ignored_returns.append(realized)
                ignored_opportunity_values.append(realized)
                false_negative_proxy_values.append(1.0 if realized > 0 else 0.0)
        if decision == "TRADED_MANUALLY" and realized is not None:
            traded_returns.append(realized)
    return {
        "input_artifacts": artifacts,
        "candidate_count": candidate_count,
        "recommended_count": recommended,
        "executed_count": executed,
        "lifecycle_candidate_count": len(lifecycle_candidates),
        "ignored_candidate_count": sum(1 for row in lifecycle_candidates if row.get("operator_decision") == "IGNORED"),
        "traded_candidate_count": sum(1 for row in lifecycle_candidates if row.get("operator_decision") == "TRADED_MANUALLY"),
        "expired_candidate_count": sum(1 for row in lifecycle_candidates if row.get("operator_decision") == "EXPIRED" or row.get("candidate_state") == "EXPIRED"),
        "recommendation_accuracy": _metric_from_rules("recommendation_accuracy", accuracy_values, source_paths),
        "false_positive_rate": _metric_from_rules("false_positive_rate", false_positive_values, source_paths),
        "realized_vs_advisory_gap": _manual_metric("realized_vs_advisory_gap", sum(gaps) / len(gaps) if gaps else None, "mean(realized_return - advisory_expected_return)", gaps, source_paths),
        "candidate_hit_rate": _manual_metric("candidate_hit_rate", sum(candidate_hit_values) / len(candidate_hit_values) if candidate_hit_values else None, "mean(winning candidate outcomes)", candidate_hit_values, source_paths),
        "ignored_candidate_opportunity_cost": _manual_metric("ignored_candidate_opportunity_cost", sum(ignored_opportunity_values) / len(ignored_opportunity_values) if ignored_opportunity_values else None, "mean(ignored candidate realized/proxy return)", ignored_opportunity_values, source_paths),
        "false_negative_proxy": _manual_metric("false_negative_proxy", sum(false_negative_proxy_values) / len(false_negative_proxy_values) if false_negative_proxy_values else None, "mean(ignored candidates later marked positive)", false_negative_proxy_values, source_paths),
        "traded_vs_ignored_performance": _manual_metric("traded_vs_ignored_performance", _mean(traded_returns) - _mean(ignored_returns) if traded_returns and ignored_returns else None, "mean(traded candidate returns) - mean(ignored candidate returns)", traded_returns + ignored_returns, source_paths),
        "sleeve_candidate_quality": _manual_metric("sleeve_candidate_quality", sum(candidate_hit_values) / len(candidate_hit_values) if candidate_hit_values else None, "candidate hit rate grouped by sleeve when sample exists", candidate_hit_values, source_paths),
        "stop_out_rate": _manual_metric("stop_out_rate", _mean(stop_trigger_values) if stop_trigger_values else None, "mean(stop events triggered)", stop_trigger_values, source_paths),
        "average_stop_loss": _manual_metric("average_stop_loss", _mean(stop_loss_values) if stop_loss_values else None, "mean(operator-reported stop loss amounts)", stop_loss_values, source_paths),
        "stop_honored_rate": _manual_metric("stop_honored_rate", _mean(stop_honored_values) if stop_honored_values else None, "mean(operator-confirmed stops honored)", stop_honored_values, source_paths),
        "stopped_out_then_recovered_rate": _manual_metric("stopped_out_then_recovered_rate", _mean(recovered_values) if recovered_values else None, "mean(stopped positions with future recovery window)", recovered_values, source_paths),
        "sleeve_stop_effectiveness": _stop_effectiveness_group("sleeve_stop_effectiveness", stop_rows, "sleeve_id", source_paths),
        "regime_stop_effectiveness": _manual_metric("regime_stop_effectiveness", None, "group stop effectiveness by regime", [], source_paths),
        "operator_stop_discipline": _manual_metric("operator_stop_discipline", _mean(stop_honored_values) if stop_honored_values else None, "mean(operator-confirmed stop discipline)", stop_honored_values, source_paths),
        "regime_adjusted_recommendation_accuracy": _manual_metric("regime_adjusted_recommendation_accuracy", None, "group recommendation accuracy by regime", [], source_paths),
        "regime_specific_candidate_quality": _manual_metric("regime_specific_candidate_quality", None, "group candidate outcome quality by regime", [], source_paths),
        "selected_vs_suppressed_outcome": selection_learning["selected_vs_suppressed_outcome"],
        "suppressed_candidate_opportunity_cost": selection_learning["suppressed_candidate_opportunity_cost"],
        "same_sleeve_selection_accuracy": selection_learning["same_sleeve_selection_accuracy"],
        "exposure_cluster_selection_quality": selection_learning["exposure_cluster_selection_quality"],
    }



def _selection_learning_metrics(portfolio_selection: dict[str, Any], lifecycle: dict[str, Any], source_paths: list[str]) -> dict[str, Any]:
    selected = [row for row in portfolio_selection.get("selected_candidates", []) if isinstance(row, dict)] if isinstance(portfolio_selection.get("selected_candidates"), list) else []
    suppressed = [row for row in portfolio_selection.get("suppressed_candidates", []) if isinstance(row, dict)] if isinstance(portfolio_selection.get("suppressed_candidates"), list) else []
    lifecycle_rows = [row for row in lifecycle.get("candidates", []) if isinstance(row, dict)] if isinstance(lifecycle.get("candidates"), list) else []
    outcome_by_id = {str(row.get("candidate_id") or ""): row for row in lifecycle_rows}
    selected_values = [value for value in (_candidate_outcome_value(outcome_by_id.get(str(row.get("candidate_id") or ""), {})) for row in selected) if value is not None]
    suppressed_values = [value for value in (_candidate_outcome_value(outcome_by_id.get(str(row.get("candidate_id") or ""), {})) for row in suppressed) if value is not None]
    same_sleeve_values: list[float] = []
    cluster_values: list[float] = []
    if selected_values and suppressed_values:
        selected_mean = _mean(selected_values)
        suppressed_mean = _mean(suppressed_values)
        same_sleeve_values = [1.0 if selected_mean >= suppressed_mean else 0.0]
        cluster_values = same_sleeve_values[:]
    return {
        "selected_vs_suppressed_outcome": _manual_metric("selected_vs_suppressed_outcome", (_mean(selected_values) - _mean(suppressed_values)) if selected_values and suppressed_values else None, "mean(selected outcome) - mean(suppressed outcome)", selected_values + suppressed_values, source_paths),
        "suppressed_candidate_opportunity_cost": _manual_metric("suppressed_candidate_opportunity_cost", _mean(suppressed_values) if suppressed_values else None, "mean(suppressed candidate realized/proxy return)", suppressed_values, source_paths),
        "same_sleeve_selection_accuracy": _manual_metric("same_sleeve_selection_accuracy", _mean(same_sleeve_values) if same_sleeve_values else None, "selected same-sleeve candidates outperformed suppressed same-sleeve candidates", same_sleeve_values, source_paths),
        "exposure_cluster_selection_quality": _manual_metric("exposure_cluster_selection_quality", _mean(cluster_values) if cluster_values else None, "selected exposure clusters outperformed suppressed clusters", cluster_values, source_paths),
    }


def _candidate_outcome_value(row: dict[str, Any]) -> float | None:
    metrics = row.get("outcome_metrics") if isinstance(row.get("outcome_metrics"), dict) else {}
    realized = _first_num(metrics, ["realized_return", "return", "return_pct", "pnl_pct", "pnl"])
    if realized is not None:
        return realized
    status = str(row.get("outcome_status") or "").upper()
    if status == "OUTCOME_WON":
        return 1.0
    if status == "OUTCOME_LOST":
        return -1.0
    if status == "OUTCOME_FLAT":
        return 0.0
    return None

def _stop_attribution_summary(position_management: dict[str, Any], source_paths: list[str]) -> dict[str, Any]:
    rows = [row for row in position_management.get("positions", []) if isinstance(row, dict)] if isinstance(position_management.get("positions"), list) else []
    stop_events = [row for row in rows if isinstance(row.get("latest_stop_event"), dict) and row.get("latest_stop_event")]
    values = [1.0 if (row.get("latest_stop_event") or {}).get("stop_triggered") is True else 0.0 for row in stop_events]
    return {
        "position_count": len(rows),
        "stop_event_count": len(stop_events),
        "stop_out_rate": _manual_metric("stop_out_rate", _mean(values) if values else None, "mean(stop events triggered)", values, source_paths),
        "sleeve_stop_effectiveness": _stop_effectiveness_group("sleeve_stop_effectiveness", rows, "sleeve_id", source_paths),
        "regime_stop_effectiveness": _manual_metric("regime_stop_effectiveness", None, "group stop effectiveness by regime", [], source_paths),
        "operator_stop_discipline": _stop_effectiveness_group("operator_stop_discipline", rows, "operator", source_paths),
    }


def _sleeve_stop_effectiveness_metric(position_management: dict[str, Any], *, sleeve_id: str, source_paths: list[str]) -> dict[str, Any]:
    rows = [row for row in position_management.get("positions", []) if isinstance(row, dict) and str(row.get("sleeve_id") or "UNKNOWN") == sleeve_id] if isinstance(position_management.get("positions"), list) else []
    values = []
    for row in rows:
        event = row.get("latest_stop_event") if isinstance(row.get("latest_stop_event"), dict) else {}
        if event.get("exit_reason") == "STOP_NOT_HONORED":
            values.append(0.0)
        elif event.get("operator_confirmed") is True:
            values.append(1.0)
    return _manual_metric("sleeve_stop_effectiveness", _mean(values) if values else None, "mean(stop honored by sleeve)", values, source_paths)


def _stop_effectiveness_group(name: str, rows: list[dict[str, Any]], key: str, source_paths: list[str]) -> dict[str, Any]:
    values = []
    for row in rows:
        event = row.get("latest_stop_event") if isinstance(row.get("latest_stop_event"), dict) else {}
        if not event:
            continue
        if key == "operator":
            group_value = event.get("operator")
        else:
            group_value = row.get(key)
        if not group_value:
            continue
        values.append(0.0 if event.get("exit_reason") == "STOP_NOT_HONORED" else 1.0 if event.get("operator_confirmed") is True else 0.0)
    return _manual_metric(name, _mean(values) if values else None, f"mean stop effectiveness grouped by {key}", values, source_paths)


def _metric_from_rules(name: str, values: list[float], source_paths: list[str], *, alias: str | None = None) -> dict[str, Any]:
    result = metric_result_v1(name, values)
    result["metric_name"] = alias or name
    return _with_evidence(result, source_paths)


def _manual_metric(name: str, value: float | None, formula: str, values: list[float], source_paths: list[str]) -> dict[str, Any]:
    sample_size = len(values)
    if sample_size == 0:
        status = "MISSING_INPUT"
        metric_value = None
    elif sample_size < DEFAULT_MIN_SAMPLE_SIZE:
        status = "INSUFFICIENT_DATA"
        metric_value = None
    else:
        status = "OK" if value is not None else "INVALID_INPUT"
        metric_value = round(float(value), 6) if value is not None else None
    return _with_evidence(
        {
            "metric_name": name,
            "value": metric_value,
            "metric_status": status,
            "sample_size": sample_size,
            "minimum_sample_size": DEFAULT_MIN_SAMPLE_SIZE,
            "formula_version": FORMULA_VERSION,
            "formula": formula,
        },
        source_paths,
    )


def _volatility_metric(values: list[float], source_paths: list[str], *, name: str = "volatility") -> dict[str, Any]:
    if len(values) < DEFAULT_MIN_SAMPLE_SIZE:
        return _with_evidence({"metric_name": name, "value": None, "metric_status": "MISSING_INPUT" if not values else "INSUFFICIENT_DATA", "sample_size": len(values), "minimum_sample_size": DEFAULT_MIN_SAMPLE_SIZE, "formula_version": FORMULA_VERSION, "formula": "population_stddev(returns)"}, source_paths)
    try:
        from statistics import pstdev

        value = round(float(pstdev(values)), 6)
    except (TypeError, ValueError):
        return _with_evidence({"metric_name": name, "value": None, "metric_status": "INVALID_INPUT", "sample_size": len(values), "minimum_sample_size": DEFAULT_MIN_SAMPLE_SIZE, "formula_version": FORMULA_VERSION, "formula": "population_stddev(returns)"}, source_paths)
    return _with_evidence({"metric_name": name, "value": value, "metric_status": "OK", "sample_size": len(values), "minimum_sample_size": DEFAULT_MIN_SAMPLE_SIZE, "formula_version": FORMULA_VERSION, "formula": "population_stddev(returns)"}, source_paths)


def _with_evidence(metric: dict[str, Any], source_paths: list[str]) -> dict[str, Any]:
    status = str(metric.get("metric_status") or "UNKNOWN")
    if status == "MISSING_INPUT":
        status = "INSUFFICIENT_DATA"
    sample_size = int(metric.get("sample_size") or 0)
    quality = "HIGH" if status == "OK" and sample_size >= DEFAULT_MIN_SAMPLE_SIZE else "LOW" if sample_size else "UNKNOWN"
    confidence = "HIGH" if status == "OK" and quality == "HIGH" else "LOW" if sample_size else "UNKNOWN"
    return {
        **metric,
        "metric_status": status,
        "input_artifacts": source_paths,
        "input_hashes": [file_hash_v1(path) for path in source_paths],
        "evidence_quality": quality,
        "confidence": confidence,
    }


def _legacy_value(record: dict[str, Any]) -> Any:
    if record.get("metric_status") == "OK":
        return record.get("value")
    return record.get("metric_status") or "UNKNOWN"


def _aggregate_confidence(records: dict[str, dict[str, Any]]) -> str:
    if any(record.get("confidence") == "HIGH" for record in records.values()):
        return "MEDIUM"
    if any(record.get("sample_size", 0) for record in records.values()):
        return "LOW"
    return "UNKNOWN"


def _matrix_rows(payload: dict[str, Any]) -> list[dict[str, str]]:
    rows = []
    for sleeve in payload.get("sleeve_metrics") or []:
        scope = f"sleeve:{sleeve.get('sleeve_id')}"
        for name, metric in (sleeve.get("metrics") or {}).items():
            rows.append(_matrix_row(scope, name, metric))
    for name, metric in (payload.get("portfolio_attribution") or {}).items():
        if isinstance(metric, dict) and "metric_status" in metric:
            rows.append(_matrix_row("portfolio", name, metric))
    for name, metric in (payload.get("advisory_quality") or {}).items():
        if isinstance(metric, dict) and "metric_status" in metric:
            rows.append(_matrix_row("advisory", name, metric))
    return rows


def _matrix_row(scope: str, name: str, metric: dict[str, Any]) -> dict[str, str]:
    return {
        "scope": scope,
        "metric_name": name,
        "value": "" if metric.get("value") is None else str(metric.get("value")),
        "metric_status": str(metric.get("metric_status") or "UNKNOWN"),
        "sample_size": str(metric.get("sample_size") or 0),
        "minimum_sample_size": str(metric.get("minimum_sample_size") or 0),
        "formula_version": str(metric.get("formula_version") or ""),
        "evidence_quality": str(metric.get("evidence_quality") or "UNKNOWN"),
        "confidence": str(metric.get("confidence") or "UNKNOWN"),
        "input_artifacts": ";".join(metric.get("input_artifacts") or []),
    }


def _first_num(row: dict[str, Any], keys: list[str]) -> float | None:
    for key in keys:
        value = _num(row.get(key))
        if value is not None:
            return value
    return None


def _int(value: Any, fallback: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _mean(values: list[float]) -> float:
    return sum(values) / len(values)
