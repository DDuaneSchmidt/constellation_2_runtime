from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean, median, pstdev
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.challengers.challenger_track import load_challenger_research_track
from research_lab.challengers.challenger_variant import build_challenger_variant, write_challenger_variant
from research_lab.contracts.schemas import validate_contract
from research_lab.longitudinal.candidate_run import longitudinal_run_dir
from research_lab.storage.hashing import content_hash, short_hash
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.parquet_io import file_sha256, read_parquet_records, write_parquet_records
from research_lab.storage.paths import ensure_store_layout
from research_lab.stability.stability_registry import (
    CREATED_BY as STABILITY_CREATED_BY,
    RESEARCH_LABEL as STABILITY_RESEARCH_LABEL,
    finalize_report,
    metric_summary as stability_metric_summary,
    write_stability_artifacts,
)


GENERATED_AT = "1970-01-01T00:00:00Z"
RESEARCH_LABEL = "RESEARCH_ONLY"
SCHEMA_VERSION = "challenger_evidence_batch.v1"
MINIMUM_MEASURED_CANDIDATES = 40


def _registry_path(store: Path) -> Path:
    return store / "registries" / "challenger_evidence_batches.jsonl"


def _batch_dir(store: Path) -> Path:
    return store / "challenger_evidence_batches"


def _item_dir(store: Path) -> Path:
    return store / "challenger_evidence_items"


def _batch_path(store: Path, batch_id: str) -> Path:
    return _batch_dir(store) / f"{batch_id}.json"


def _item_path(store: Path, item_id: str) -> Path:
    return _item_dir(store) / f"{item_id}.json"


def _event_study_path(store: Path, event_study_id: str) -> Path:
    return store / "event_studies" / f"{event_study_id}.json"


def _backtest_path(store: Path, backtest_id: str) -> Path:
    return store / "backtests" / f"{backtest_id}.json"


def _available_ref(refs: list[dict[str, Any]], artifact_type: str) -> bool:
    return any(ref.get("artifact_type") == artifact_type and ref.get("status") == "available" for ref in refs)


def _refresh_refs(refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    refreshed = []
    for ref in refs:
        path = Path(str(ref.get("path") or ""))
        row = dict(ref)
        if not path.exists():
            row["status"] = "missing"
            row["content_hash"] = ""
        refreshed.append(row)
    return refreshed


def _blocked_item(hypothesis: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "challenger_hypothesis_id": hypothesis["challenger_hypothesis_id"],
        "challenger_variant_id": "",
        "parent_sleeve_id": hypothesis["parent_sleeve_id"],
        "variant_name": hypothesis["variant_name"],
        "deterministic_rule_delta": hypothesis["deterministic_rule_delta"],
        "status": "blocked",
        "generated_evidence_ids": [],
        "event_study_evidence_id": "",
        "backtest_evidence_id": "",
        "longitudinal_run_id": "",
        "regime_fragility_report_id": "",
        "expectancy_drift_report_id": "",
        "evidence_quality": "blocked",
        "exclusion_flags": [reason],
        "failure_reason": reason,
        "research_label": RESEARCH_LABEL,
        "blocker": {"reason": reason, "recoverable": reason == "cost_sensitivity_requires_supported_cost_model_snapshot"},
    }


def _source_artifact_ids(track: dict[str, Any]) -> dict[str, Any]:
    req = track.get("evidence_requirements") or {}
    return {
        "dataset_snapshot_id": str(req.get("dataset_snapshot_id") or ""),
        "regime_snapshot_id": str(req.get("regime_snapshot_id") or ""),
        "cost_model_snapshot_id": str(req.get("cost_model_snapshot_id") or ""),
        "incumbent_event_study_evidence_id": str(req.get("event_study_evidence_package_id") or ""),
        "incumbent_backtest_evidence_id": str(req.get("backtest_evidence_package_id") or ""),
        "incumbent_longitudinal_run_id": str(req.get("longitudinal_run_id") or ""),
        "trigger_expectancy_drift_report_id": str(req.get("expectancy_drift_report_id") or ""),
        "trigger_regime_fragility_report_id": str(req.get("regime_fragility_report_id") or ""),
        "trigger_sleeve_stability_report_id": str(req.get("sleeve_stability_report_id") or ""),
    }


def _artifact_ids(track: dict[str, Any], hypothesis: dict[str, Any], rows: list[dict[str, Any]], quality: str) -> dict[str, str]:
    variant = build_challenger_variant(
        track=track,
        hypothesis=hypothesis,
        source_artifact_ids=_source_artifact_ids(track),
        generated_at=GENERATED_AT,
    )
    seed_payload = {
        "challenger_track_id": track["challenger_track_id"],
        "challenger_variant_id": variant["challenger_variant_id"],
        "challenger_hypothesis_id": hypothesis["challenger_hypothesis_id"],
        "variant_name": hypothesis["variant_name"],
        "row_count": len(rows),
        "row_hash": content_hash({"rows": rows}, sort_lists=False),
        "quality": quality,
    }
    seed = content_hash(seed_payload, sort_lists=True)
    return {
        "challenger_variant_id": variant["challenger_variant_id"],
        "event_study_evidence_id": f"ches_{short_hash(content_hash({'seed': seed, 'kind': 'event'}), 16)}",
        "backtest_evidence_id": f"chbt_{short_hash(content_hash({'seed': seed, 'kind': 'backtest'}), 16)}",
        "longitudinal_run_id": f"lcr_ch_{short_hash(content_hash({'seed': seed, 'kind': 'longitudinal'}), 16)}",
    }


def _metric_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    values = [float(row["post_cost_return"]) for row in rows if row.get("post_cost_return") is not None]
    excess = [float(row["excess_return"]) for row in rows if row.get("excess_return") is not None]
    return {
        "row_count": len(rows),
        "measured_candidate_count": len({str(row.get("candidate_id")) for row in rows if row.get("candidate_id")}),
        "mean_post_cost_return": mean(values) if values else None,
        "median_post_cost_return": median(values) if values else None,
        "win_rate": sum(1 for value in values if value > 0) / len(values) if values else None,
        "mean_excess_return": mean(excess) if excess else None,
        "downside_tail_metric": sorted(values)[max(int(len(values) * 0.10) - 1, 0)] if values else None,
    }


def _metrics_by_window(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_window: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_window[str(row.get("outcome_window") or "")].append(row)
    return {window: _metric_summary(by_window[window]) for window in sorted(by_window, key=lambda value: int(value.rstrip("d") or 0))}


def _regime_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_regime: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_regime[str(row.get("risk_regime") or "UNKNOWN")].append(row)
    return {regime: _metric_summary(by_regime[regime]) for regime in sorted(by_regime)}


def _quality(metrics: dict[str, Any], regime: dict[str, Any], *, has_backtest: bool) -> str:
    measured = int(metrics.get("measured_candidate_count") or 0)
    mean_return = metrics.get("mean_post_cost_return")
    win_rate = metrics.get("win_rate")
    if measured < MINIMUM_MEASURED_CANDIDATES:
        return "insufficient"
    if mean_return is None or win_rate is None:
        return "insufficient"
    covered_regimes = [row for row in regime.values() if int(row.get("measured_candidate_count") or 0) >= 10]
    if mean_return > 0.005 and win_rate >= 0.55 and len(covered_regimes) >= 1 and has_backtest:
        return "strong"
    if mean_return > 0 and win_rate >= 0.50 and has_backtest:
        return "moderate"
    return "weak"


def _exclusion_flags(metrics: dict[str, Any], quality: str) -> list[str]:
    flags: list[str] = []
    if quality == "insufficient":
        flags.append("insufficient observations")
    mean_return = metrics.get("mean_post_cost_return")
    if mean_return is not None and float(mean_return) < 0:
        flags.append("negative post-cost expectancy")
    return flags


def _window_num(value: Any) -> int:
    return int(str(value).rstrip("d") or 0)


def _windows(rows: list[dict[str, Any]]) -> list[str]:
    return sorted({str(row.get("outcome_window")) for row in rows if row.get("outcome_window")}, key=_window_num)


def _split_metrics(rows: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any]]:
    ordered = sorted(rows, key=lambda row: (str(row.get("as_of_date") or ""), str(row.get("candidate_id") or ""), str(row.get("outcome_window") or "")))
    midpoint = len(ordered) // 2
    return stability_metric_summary(ordered[:midpoint]), stability_metric_summary(ordered[midpoint:])


def _drift_status(baseline: dict[str, Any], latest: dict[str, Any]) -> tuple[str, list[str]]:
    baseline_mean = baseline.get("mean_post_cost_return")
    latest_mean = latest.get("mean_post_cost_return")
    baseline_win = baseline.get("win_rate")
    latest_win = latest.get("win_rate")
    if baseline_mean is None or latest_mean is None or baseline_win is None or latest_win is None:
        return "watch", ["missing_baseline_or_latest_metric"]
    mean_delta = float(latest_mean) - float(baseline_mean)
    win_delta = float(latest_win) - float(baseline_win)
    reasons = [f"mean_post_cost_delta={mean_delta:.6f}", f"win_rate_delta={win_delta:.6f}"]
    mean_down = mean_delta < -0.001
    win_down = win_delta < -0.03
    mean_up = mean_delta > 0.001
    win_up = win_delta > 0.03
    if mean_down and win_down:
        return "degrading", reasons + ["latest_mean_post_cost_and_win_rate_below_baseline"]
    if mean_up and win_up:
        return "improving", reasons + ["latest_mean_post_cost_and_win_rate_above_baseline"]
    if abs(mean_delta) <= 0.001 and abs(win_delta) <= 0.03:
        return "stable", reasons + ["latest_metrics_within_small_difference_thresholds"]
    if mean_down or win_down:
        return "watch", reasons + ["mixed_deterioration_signal"]
    return "stable", reasons + ["mixed_without_clear_deterioration"]


def _expectancy_drift_report(*, sleeve_id: str, sleeve_version_id: str, longitudinal_run_id: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_window = {window: [row for row in rows if str(row.get("outcome_window")) == window] for window in _windows(rows)}
    candidate_count = len({str(row.get("candidate_id")) for row in rows if row.get("candidate_id")})
    measured_candidate_count = candidate_count
    baseline_metrics: dict[str, Any] = {}
    latest_metrics: dict[str, Any] = {}
    rolling_metrics: dict[str, Any] = {}
    statuses: dict[str, str] = {}
    reasons: list[str] = []
    for window, window_rows in by_window.items():
        baseline, latest = _split_metrics(window_rows)
        baseline_metrics[window] = baseline
        latest_metrics[window] = latest
        statuses[window], status_reasons = _drift_status(baseline, latest)
        reasons.extend([f"{window}:{reason}" for reason in status_reasons])
        rolling_metrics[window] = []
    if measured_candidate_count < 40:
        status = "insufficient_data"
        reasons = [f"measured_candidate_count {measured_candidate_count} below 40"]
    elif any(value == "degrading" for value in statuses.values()):
        status = "degrading"
    elif any(value == "watch" for value in statuses.values()):
        status = "watch"
    elif statuses and all(value == "improving" for value in statuses.values()):
        status = "improving"
    else:
        status = "stable"
    recommended = {
        "insufficient_data": "collect_more_candidates",
        "stable": "continue_research",
        "improving": "continue_research",
        "watch": "review_sleeve",
        "degrading": "challenge_sleeve",
    }[status]
    report = {
        "expectancy_drift_report_id": "",
        "sleeve_id": sleeve_id,
        "sleeve_version_id": sleeve_version_id,
        "longitudinal_run_id": longitudinal_run_id,
        "created_at": GENERATED_AT,
        "created_by": STABILITY_CREATED_BY,
        "windows_analyzed": list(by_window),
        "rolling_window_size": 20,
        "candidate_count": candidate_count,
        "measured_candidate_count": measured_candidate_count,
        "drift_status": status,
        "drift_reasons": sorted(set(reasons)),
        "rolling_metrics": rolling_metrics,
        "latest_metrics": latest_metrics,
        "baseline_metrics": baseline_metrics,
        "recommended_action": recommended,
        "research_label": STABILITY_RESEARCH_LABEL,
        "schema_version": "expectancy_drift_report.v1",
        "content_hash": "",
    }
    finalize_report(report, id_field="expectancy_drift_report_id", prefix="edr")
    validate_contract("expectancy_drift_report", report)
    return report


def _regime_fragility_report(*, sleeve_id: str, sleeve_version_id: str, longitudinal_run_id: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    windows = _windows(rows)
    regime_metrics: dict[str, Any] = {}
    covered: list[dict[str, Any]] = []
    for dimension in ["drawdown_regime", "risk_regime", "trend_regime", "vol_regime"]:
        values = sorted({str(row.get(dimension) or row.get("risk_regime") or "UNKNOWN") for row in rows})
        regime_metrics[dimension] = {}
        for value in values:
            regime_metrics[dimension][value] = {}
            for window in windows:
                bucket = [
                    row
                    for row in rows
                    if str(row.get(dimension) or row.get("risk_regime") or "UNKNOWN") == value
                    and str(row.get("outcome_window")) == window
                ]
                summary = stability_metric_summary(bucket)
                regime_metrics[dimension][value][window] = summary
                if int(summary.get("measured_count") or 0) >= 10 and summary.get("mean_post_cost_return") is not None:
                    covered.append(
                        {
                            "dimension": dimension,
                            "regime": value,
                            "outcome_window": window,
                            "measured_count": summary["measured_count"],
                            "mean_post_cost_return": summary["mean_post_cost_return"],
                        }
                    )
    measured_count = len({str(row.get("candidate_id")) for row in rows if row.get("candidate_id")})
    if measured_count < 30:
        status = "insufficient_data"
        reasons = [f"measured_candidate_count {measured_count} below 30"]
    elif not covered:
        status = "watch"
        reasons = ["no_regime_bucket_has_10_measured_candidates"]
    else:
        means = [float(row["mean_post_cost_return"]) for row in covered]
        spread = max(means) - min(means)
        has_positive = any(value > 0 for value in means)
        has_negative = any(value < 0 for value in means)
        thin = any(int(row["measured_count"]) < 20 for row in covered)
        reasons = [f"best_worst_mean_spread={spread:.6f}"]
        if has_positive and has_negative and spread > 0.01:
            status = "fragile"
            reasons.append("positive_and_negative_regime_means_with_large_spread")
        elif thin or spread > 0.005:
            status = "watch"
            reasons.append("regime_sample_sizes_are_thin" if thin else "best_worst_spread_above_watch_threshold")
        else:
            status = "robust"
            reasons.append("covered_regimes_have_consistent_low_spread")
    recommended = {
        "insufficient_data": "collect_more_candidates",
        "robust": "continue_research",
        "watch": "review_sleeve",
        "fragile": "challenge_sleeve",
    }[status]
    sorted_best = sorted(covered, key=lambda row: (-float(row["mean_post_cost_return"]), row["dimension"], row["regime"], row["outcome_window"]))
    sorted_worst = sorted(covered, key=lambda row: (float(row["mean_post_cost_return"]), row["dimension"], row["regime"], row["outcome_window"]))
    report = {
        "regime_fragility_report_id": "",
        "sleeve_id": sleeve_id,
        "sleeve_version_id": sleeve_version_id,
        "longitudinal_run_id": longitudinal_run_id,
        "created_at": GENERATED_AT,
        "created_by": STABILITY_CREATED_BY,
        "regime_dimensions": ["risk_regime", "trend_regime", "vol_regime", "drawdown_regime"],
        "windows_analyzed": windows,
        "fragility_status": status,
        "fragility_reasons": reasons,
        "regime_metrics": regime_metrics,
        "best_regimes": sorted_best[:5],
        "worst_regimes": sorted_worst[:5],
        "regime_coverage": {
            "total_measured_candidate_count": measured_count,
            "covered_regime_window_count": len(covered),
            "thin_regime_window_count": sum(1 for row in covered if int(row["measured_count"]) < 20),
        },
        "recommended_action": recommended,
        "research_label": STABILITY_RESEARCH_LABEL,
        "schema_version": "regime_fragility_report.v1",
        "content_hash": "",
    }
    finalize_report(report, id_field="regime_fragility_report_id", prefix="rfr")
    validate_contract("regime_fragility_report", report)
    return report


def _sleeve_stability_report(
    *,
    sleeve_id: str,
    sleeve_version_id: str,
    expectancy_drift_report_id: str,
    regime_fragility_report_id: str,
    drift_status: str,
    fragility_status: str,
    measured_candidate_count: int,
    drift_reasons: list[str],
    fragility_reasons: list[str],
) -> dict[str, Any]:
    if drift_status == "insufficient_data" or fragility_status == "insufficient_data":
        status = "insufficient_data"
    elif drift_status == "degrading":
        status = "degrading"
    elif fragility_status == "fragile":
        status = "fragile"
    elif drift_status == "watch" or fragility_status == "watch":
        status = "watch"
    else:
        status = "stable"
    recommended = {
        "insufficient_data": "collect_more_candidates",
        "stable": "continue_research",
        "watch": "review_sleeve",
        "fragile": "challenge_sleeve",
        "degrading": "challenge_sleeve",
    }[status]
    report = {
        "sleeve_stability_report_id": "",
        "sleeve_id": sleeve_id,
        "sleeve_version_id": sleeve_version_id,
        "created_at": GENERATED_AT,
        "created_by": STABILITY_CREATED_BY,
        "expectancy_drift_report_id": expectancy_drift_report_id,
        "regime_fragility_report_id": regime_fragility_report_id,
        "overall_stability_status": status,
        "key_findings": [
            f"expectancy_drift_status={drift_status}",
            f"regime_fragility_status={fragility_status}",
            f"measured_candidate_count={measured_candidate_count}",
            f"drift_reason={drift_reasons[0]}" if drift_reasons else "drift_reason=none",
            f"fragility_reason={fragility_reasons[0]}" if fragility_reasons else "fragility_reason=none",
        ],
        "recommended_action": recommended,
        "next_allowed_actions": ["collect_more_candidates", "continue_research"] if status == "insufficient_data" else ["review_sleeve", "challenge_sleeve"],
        "research_label": STABILITY_RESEARCH_LABEL,
        "schema_version": "sleeve_stability_report.v1",
        "content_hash": "",
    }
    finalize_report(report, id_field="sleeve_stability_report_id", prefix="ssr")
    validate_contract("sleeve_stability_report", report)
    return report


def _evidence_completeness(ids: dict[str, str]) -> dict[str, Any]:
    required = [
        "challenger_variant_id",
        "event_study_evidence_id",
        "backtest_evidence_id",
        "longitudinal_run_id",
        "expectancy_drift_report_id",
        "regime_fragility_report_id",
        "sleeve_stability_report_id",
    ]
    missing = [field for field in required if not ids.get(field)]
    return {
        "complete": not missing,
        "completeness_score": (len(required) - len(missing)) / len(required),
        "missing_fields": missing,
        "required_fields": required,
    }


def _measured_rows_for_run(store: Path, longitudinal_run_id: str) -> list[dict[str, Any]]:
    root = longitudinal_run_dir(longitudinal_run_id, store_root=store)
    path = root / "outcome_index.parquet"
    if not path.exists():
        raise RuntimeError(f"outcome_index.parquet missing: {path}")
    rows = read_parquet_records(path)
    return sorted(
        [
            row
            for row in rows
            if str(row.get("outcome_status") or "").lower() == "measured"
            and row.get("post_cost_return") is not None
            and row.get("candidate_id")
        ],
        key=lambda row: (str(row.get("as_of_date") or ""), str(row.get("candidate_id") or ""), str(row.get("outcome_window") or "")),
    )


def _candidate_groups(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["candidate_id"])].append(row)
    return grouped


def _nearest_window(rows: list[dict[str, Any]], *, pick: str) -> list[dict[str, Any]]:
    windows = sorted({int(str(row.get("outcome_window")).rstrip("d")) for row in rows if row.get("outcome_window")})
    if not windows:
        return []
    target = windows[0] if pick == "shorter" else windows[-1]
    return [row for row in rows if str(row.get("outcome_window")) == f"{target}d"]


def _apply_delta(hypothesis: dict[str, Any], rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], str | None]:
    delta = hypothesis.get("deterministic_rule_delta") or {}
    variant = str(hypothesis.get("variant_name") or "")
    if "cost_model_stress" in delta:
        return [], "cost_sensitivity_requires_supported_cost_model_snapshot"
    if any(key in delta for key in ["ml_model", "optimizer", "capital_allocation"]):
        return [], "variant_requires_forbidden_ml_optimizer_or_capital_allocation"
    if variant == "shorter_holding_period":
        return _nearest_window(rows, pick="shorter"), None
    if variant == "longer_holding_period":
        return _nearest_window(rows, pick="longer"), None
    if variant == "rank_threshold_variant":
        return [row for row in rows if str(row.get("ranking_bucket") or "") == "top"], None
    if variant == "regime_filtered_variant":
        regime_mean = _regime_metrics(rows)
        allowed = {regime for regime, metrics in regime_mean.items() if (metrics.get("mean_post_cost_return") or 0) >= 0}
        return [row for row in rows if str(row.get("risk_regime") or "UNKNOWN") in allowed], None
    groups = _candidate_groups(rows)
    if variant == "stricter_signal_threshold":
        ranked_candidates = sorted(
            groups,
            key=lambda candidate_id: (
                -max(float(row.get("ranking_score") or 0.0) for row in groups[candidate_id]),
                candidate_id,
            ),
        )
        keep = set(ranked_candidates[: max(len(ranked_candidates) // 2, 1)])
        return [row for row in rows if str(row.get("candidate_id")) in keep], None
    if variant == "lower_turnover_variant":
        kept: set[str] = set()
        by_symbol: dict[str, list[str]] = defaultdict(list)
        for candidate_id, candidate_rows in sorted(groups.items()):
            symbol = str(candidate_rows[0].get("symbol") or "UNKNOWN")
            by_symbol[symbol].append(candidate_id)
        for symbol, candidate_ids in by_symbol.items():
            for idx, candidate_id in enumerate(sorted(candidate_ids)):
                if idx % 2 == 0:
                    kept.add(candidate_id)
        return [row for row in rows if str(row.get("candidate_id")) in kept], None
    return [], "deterministic_rule_delta_unresolved"


def _generated_item(track: dict[str, Any], hypothesis: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    metrics = _metric_summary(rows)
    by_window = _metrics_by_window(rows)
    by_regime = _regime_metrics(rows)
    has_backtest = bool(track.get("evidence_requirements", {}).get("backtest_evidence_package_id"))
    quality = _quality(metrics, by_regime, has_backtest=has_backtest)
    status = "insufficient_evidence" if quality == "insufficient" else "generated"
    artifact_ids = _artifact_ids(track, hypothesis, rows, quality)
    drift_report = _expectancy_drift_report(
        sleeve_id=track["incumbent_sleeve_id"],
        sleeve_version_id=track["incumbent_sleeve_version_id"],
        longitudinal_run_id=artifact_ids["longitudinal_run_id"],
        rows=rows,
    )
    fragility_report = _regime_fragility_report(
        sleeve_id=track["incumbent_sleeve_id"],
        sleeve_version_id=track["incumbent_sleeve_version_id"],
        longitudinal_run_id=artifact_ids["longitudinal_run_id"],
        rows=rows,
    )
    stability_report = _sleeve_stability_report(
        sleeve_id=track["incumbent_sleeve_id"],
        sleeve_version_id=track["incumbent_sleeve_version_id"],
        expectancy_drift_report_id=drift_report["expectancy_drift_report_id"],
        regime_fragility_report_id=fragility_report["regime_fragility_report_id"],
        drift_status=str(drift_report["drift_status"]),
        fragility_status=str(fragility_report["fragility_status"]),
        measured_candidate_count=int(drift_report["measured_candidate_count"]),
        drift_reasons=drift_report.get("drift_reasons") or [],
        fragility_reasons=fragility_report.get("fragility_reasons") or [],
    )
    seed = content_hash(
        {
            "challenger_track_id": track["challenger_track_id"],
            "challenger_hypothesis_id": hypothesis["challenger_hypothesis_id"],
            "challenger_variant_id": artifact_ids["challenger_variant_id"],
            "metrics": metrics,
            "by_window": by_window,
            "by_regime": by_regime,
            "quality": quality,
            "artifact_ids": artifact_ids,
        },
        sort_lists=True,
    )
    item_id = f"chev_{short_hash(seed, 16)}"
    return {
        "challenger_evidence_item_id": item_id,
        "challenger_hypothesis_id": hypothesis["challenger_hypothesis_id"],
        "challenger_variant_id": artifact_ids["challenger_variant_id"],
        "parent_sleeve_id": hypothesis["parent_sleeve_id"],
        "variant_name": hypothesis["variant_name"],
        "deterministic_rule_delta": hypothesis["deterministic_rule_delta"],
        "status": status,
        "generated_evidence_ids": [
            item_id,
            artifact_ids["challenger_variant_id"],
            artifact_ids["event_study_evidence_id"],
            artifact_ids["backtest_evidence_id"],
            artifact_ids["longitudinal_run_id"],
            drift_report["expectancy_drift_report_id"],
            fragility_report["regime_fragility_report_id"],
            stability_report["sleeve_stability_report_id"],
        ],
        "event_study_evidence_id": artifact_ids["event_study_evidence_id"],
        "backtest_evidence_id": artifact_ids["backtest_evidence_id"],
        "longitudinal_run_id": artifact_ids["longitudinal_run_id"],
        "regime_fragility_report_id": fragility_report["regime_fragility_report_id"],
        "expectancy_drift_report_id": drift_report["expectancy_drift_report_id"],
        "sleeve_stability_report_id": stability_report["sleeve_stability_report_id"],
        "evidence_quality": quality,
        "exclusion_flags": _exclusion_flags(metrics, quality),
        "failure_reason": "" if status == "generated" else "generated_observations_below_minimum_evidence_threshold",
        "research_label": RESEARCH_LABEL,
        "derived_metrics": metrics,
        "metrics_by_window": by_window,
        "metrics_by_risk_regime": by_regime,
        "source_outcome_index": str(longitudinal_run_dir(str(track.get("evidence_requirements", {}).get("longitudinal_run_id") or ""), store_root=None) / "outcome_index.parquet"),
        "materialization_method": "fully_materialized_rule_delta_evidence_chain",
        "evidence_completeness": _evidence_completeness(
            {
                "challenger_variant_id": artifact_ids["challenger_variant_id"],
                "event_study_evidence_id": artifact_ids["event_study_evidence_id"],
                "backtest_evidence_id": artifact_ids["backtest_evidence_id"],
                "longitudinal_run_id": artifact_ids["longitudinal_run_id"],
                "expectancy_drift_report_id": drift_report["expectancy_drift_report_id"],
                "regime_fragility_report_id": fragility_report["regime_fragility_report_id"],
                "sleeve_stability_report_id": stability_report["sleeve_stability_report_id"],
            }
        ),
        "artifact_lineage": {
            "challenger_variant_id": artifact_ids["challenger_variant_id"],
            "event_study_evidence_id": artifact_ids["event_study_evidence_id"],
            "backtest_evidence_id": artifact_ids["backtest_evidence_id"],
            "longitudinal_run_id": artifact_ids["longitudinal_run_id"],
            "expectancy_drift_report_id": drift_report["expectancy_drift_report_id"],
            "regime_fragility_report_id": fragility_report["regime_fragility_report_id"],
            "sleeve_stability_report_id": stability_report["sleeve_stability_report_id"],
            "source_incumbent_longitudinal_run_id": str(track.get("evidence_requirements", {}).get("longitudinal_run_id") or ""),
        },
    }


def _governance_constraints() -> dict[str, bool]:
    return {
        "evidence_generation_promotes_challenger": False,
        "evidence_generation_retires_incumbent": False,
        "evidence_generation_mutates_sleeve_state": False,
        "evidence_generation_mutates_paper_trial_state": False,
        "evidence_generation_mutates_candidate_ledger_state": False,
        "evidence_generation_authorizes_trading": False,
        "human_review_required_before_future_lifecycle_decision": True,
        "broker_execution_allowed": False,
        "live_trading_allowed": False,
        "autonomous_trading_allowed": False,
        "order_management_allowed": False,
        "portfolio_optimizer_allowed": False,
        "capital_allocation_allowed": False,
        "automatic_promotion_allowed": False,
        "sleeve_mutation_allowed": False,
        "paper_trial_mutation_allowed": False,
        "candidate_mutation_allowed": False,
        "ml_black_box_ranking_allowed": False,
    }


def _recommended_action(items: list[dict[str, Any]], blocked: list[dict[str, Any]]) -> str:
    generated = [item for item in items if item["status"] == "generated"]
    usable = [item for item in generated if item["evidence_quality"] in {"strong", "moderate"}]
    if not generated:
        return "investigate_blockers"
    if len(blocked) > len(generated):
        return "investigate_blockers"
    if len(usable) >= 2:
        return "compare_challengers"
    return "collect_more_observations"


def _quality_summary(items: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(str(item.get("evidence_quality") or "unknown") for item in items)
    return {key: counts.get(key, 0) for key in ["strong", "moderate", "weak", "insufficient", "blocked"]}


def _volatility(values: list[float]) -> float | None:
    return pstdev(values) if len(values) > 1 else None


def _write_event_study_artifact(*, store: Path, track: dict[str, Any], item: dict[str, Any], rows: list[dict[str, Any]], actor: str) -> dict[str, Any]:
    values = [float(row["post_cost_return"]) for row in rows if row.get("post_cost_return") is not None]
    event_id = str(item["event_study_evidence_id"])
    artifact = {
        "event_study_id": event_id,
        "challenger_variant_id": item["challenger_variant_id"],
        "challenger_track_id": track["challenger_track_id"],
        "challenger_hypothesis_id": item["challenger_hypothesis_id"],
        "incumbent_sleeve_id": track["incumbent_sleeve_id"],
        "generated_at": GENERATED_AT,
        "schema_version": "challenger_event_study.v1",
        "research_label": RESEARCH_LABEL,
        "method": "materialized_challenger_event_study_from_rule_delta_outcomes",
        "forward_return_distributions": item.get("metrics_by_window") or {},
        "hit_rate": item.get("derived_metrics", {}).get("win_rate"),
        "expectancy": item.get("derived_metrics", {}).get("mean_post_cost_return"),
        "volatility": _volatility(values),
        "drawdown_profile": {"downside_tail_metric": item.get("derived_metrics", {}).get("downside_tail_metric")},
        "regime_segmentation": item.get("metrics_by_risk_regime") or {},
        "benchmark_comparison": {"mean_excess_return": item.get("derived_metrics", {}).get("mean_excess_return")},
        "source_artifact_ids": _source_artifact_ids(track),
        "content_hash": "",
    }
    artifact["content_hash"] = content_hash(artifact, exclude={"generated_at"}, sort_lists=True)
    path = _event_study_path(store, event_id)
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite immutable challenger event study: {path}")
    write_json(path, artifact, overwrite=False)
    row = {
        "event_study_id": event_id,
        "challenger_variant_id": item["challenger_variant_id"],
        "challenger_track_id": track["challenger_track_id"],
        "challenger_hypothesis_id": item["challenger_hypothesis_id"],
        "content_hash": artifact["content_hash"],
        "generated_at": artifact["generated_at"],
        "schema_version": artifact["schema_version"],
    }
    append_jsonl(store / "registries" / "challenger_event_studies.jsonl", row)
    write_audit_event(actor=actor, entity_type="challenger_event_study", entity_id=event_id, action="challenger_event_study_written", new_state_hash=artifact["content_hash"], reason="Wrote immutable research-only challenger event-study artifact.", metadata={"registry_row": row}, store_root=store)
    return artifact


def _write_backtest_artifact(*, store: Path, track: dict[str, Any], item: dict[str, Any], rows: list[dict[str, Any]], actor: str) -> dict[str, Any]:
    metrics = item.get("derived_metrics") or {}
    values = [float(row["post_cost_return"]) for row in rows if row.get("post_cost_return") is not None]
    backtest_id = str(item["backtest_evidence_id"])
    artifact = {
        "backtest_id": backtest_id,
        "challenger_variant_id": item["challenger_variant_id"],
        "challenger_track_id": track["challenger_track_id"],
        "challenger_hypothesis_id": item["challenger_hypothesis_id"],
        "incumbent_sleeve_id": track["incumbent_sleeve_id"],
        "generated_at": GENERATED_AT,
        "schema_version": "challenger_backtest.v1",
        "research_label": RESEARCH_LABEL,
        "method": "materialized_challenger_backtest_from_rule_delta_outcomes",
        "cagr": metrics.get("mean_post_cost_return"),
        "sharpe": (float(metrics["mean_post_cost_return"]) / _volatility(values)) if metrics.get("mean_post_cost_return") is not None and _volatility(values) not in {None, 0} else None,
        "max_drawdown": metrics.get("downside_tail_metric"),
        "expectancy": metrics.get("mean_post_cost_return"),
        "turnover": {"observation_count": metrics.get("measured_candidate_count")},
        "exposure": {"measured_candidate_count": metrics.get("measured_candidate_count")},
        "benchmark_excess_return": metrics.get("mean_excess_return"),
        "rolling_performance": item.get("metrics_by_window") or {},
        "source_artifact_ids": _source_artifact_ids(track),
        "content_hash": "",
    }
    artifact["content_hash"] = content_hash(artifact, exclude={"generated_at"}, sort_lists=True)
    path = _backtest_path(store, backtest_id)
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite immutable challenger backtest: {path}")
    write_json(path, artifact, overwrite=False)
    row = {
        "backtest_id": backtest_id,
        "challenger_variant_id": item["challenger_variant_id"],
        "challenger_track_id": track["challenger_track_id"],
        "challenger_hypothesis_id": item["challenger_hypothesis_id"],
        "content_hash": artifact["content_hash"],
        "generated_at": artifact["generated_at"],
        "schema_version": artifact["schema_version"],
    }
    append_jsonl(store / "registries" / "challenger_backtests.jsonl", row)
    write_audit_event(actor=actor, entity_type="challenger_backtest", entity_id=backtest_id, action="challenger_backtest_written", new_state_hash=artifact["content_hash"], reason="Wrote immutable research-only challenger backtest artifact.", metadata={"registry_row": row}, store_root=store)
    return artifact


def _write_longitudinal_artifact(*, store: Path, track: dict[str, Any], item: dict[str, Any], rows: list[dict[str, Any]], actor: str) -> dict[str, Any]:
    run_id = str(item["longitudinal_run_id"])
    root = longitudinal_run_dir(run_id, store_root=store)
    if root.exists():
        raise FileExistsError(f"Refusing to overwrite challenger longitudinal run: {root}")
    root.mkdir(parents=True, exist_ok=False)
    run = {
        "longitudinal_run_id": run_id,
        "sleeve_id": track["incumbent_sleeve_id"],
        "sleeve_version_id": track["incumbent_sleeve_version_id"],
        "hypothesis_id": item["challenger_hypothesis_id"],
        "challenger_variant_id": item["challenger_variant_id"],
        "source_evidence_package_id": item["event_study_evidence_id"],
        "dataset_snapshot_id": str(track.get("evidence_requirements", {}).get("dataset_snapshot_id") or ""),
        "regime_snapshot_id": str(track.get("evidence_requirements", {}).get("regime_snapshot_id") or ""),
        "cost_model_snapshot_id": str(track.get("evidence_requirements", {}).get("cost_model_snapshot_id") or ""),
        "start_date": min((str(row.get("as_of_date")) for row in rows), default=""),
        "end_date": max((str(row.get("as_of_date")) for row in rows), default=""),
        "frequency": "materialized_rule_delta",
        "created_at": GENERATED_AT,
        "created_by": actor,
        "schema_version": "longitudinal_candidate_run.v1",
        "research_label": RESEARCH_LABEL,
        "candidate_lifecycle": "materialized_challenger_candidate_history",
        "ranking_persistence": {"ranking_buckets": sorted({str(row.get("ranking_bucket") or "") for row in rows})},
        "regime_behavior": item.get("metrics_by_risk_regime") or {},
        "decay_analysis": item.get("metrics_by_window") or {},
        "longitudinal_expectancy": item.get("derived_metrics") or {},
        "content_hash": "",
    }
    run["content_hash"] = content_hash(run, exclude={"created_at"}, sort_lists=True)
    candidate_index = []
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("as_of_date") or "")].append(row)
    for as_of_date in sorted(grouped):
        candidate_ids = sorted({str(row.get("candidate_id")) for row in grouped[as_of_date] if row.get("candidate_id")})
        candidate_index.append(
            {
                "longitudinal_run_id": run_id,
                "as_of_date": as_of_date,
                "candidate_count": len(candidate_ids),
                "measured_candidate_count": len(candidate_ids),
                "generation_status": "materialized_from_challenger_rule_delta",
                "outcome_status": "measured",
            }
        )
    write_json(root / "longitudinal_run.json", run, overwrite=False)
    write_parquet_records(root / "candidate_batch_index.parquet", candidate_index, allow_json_fallback=True)
    write_parquet_records(root / "outcome_index.parquet", rows, allow_json_fallback=True)
    row = {
        "longitudinal_run_id": run_id,
        "sleeve_id": track["incumbent_sleeve_id"],
        "sleeve_version_id": track["incumbent_sleeve_version_id"],
        "challenger_variant_id": item["challenger_variant_id"],
        "candidate_batch_count": len(candidate_index),
        "candidate_count": len({str(row.get("candidate_id")) for row in rows if row.get("candidate_id")}),
        "content_hash": run["content_hash"],
        "candidate_batch_index_hash": file_sha256(root / "candidate_batch_index.parquet"),
        "outcome_index_hash": file_sha256(root / "outcome_index.parquet"),
        "created_at": run["created_at"],
        "schema_version": run["schema_version"],
    }
    append_jsonl(store / "registries" / "longitudinal_candidate_runs.jsonl", row)
    write_audit_event(actor=actor, entity_type="longitudinal_candidate_run", entity_id=run_id, action="challenger_longitudinal_run_materialized", new_state_hash=run["content_hash"], reason="Materialized immutable challenger longitudinal candidate run.", metadata={"registry_row": row}, store_root=store)
    return run


def _write_stability_chain(*, store: Path, track: dict[str, Any], item: dict[str, Any], rows: list[dict[str, Any]], actor: str) -> dict[str, Any]:
    drift = _expectancy_drift_report(sleeve_id=track["incumbent_sleeve_id"], sleeve_version_id=track["incumbent_sleeve_version_id"], longitudinal_run_id=str(item["longitudinal_run_id"]), rows=rows)
    fragility = _regime_fragility_report(sleeve_id=track["incumbent_sleeve_id"], sleeve_version_id=track["incumbent_sleeve_version_id"], longitudinal_run_id=str(item["longitudinal_run_id"]), rows=rows)
    stability = _sleeve_stability_report(
        sleeve_id=track["incumbent_sleeve_id"],
        sleeve_version_id=track["incumbent_sleeve_version_id"],
        expectancy_drift_report_id=drift["expectancy_drift_report_id"],
        regime_fragility_report_id=fragility["regime_fragility_report_id"],
        drift_status=str(drift["drift_status"]),
        fragility_status=str(fragility["fragility_status"]),
        measured_candidate_count=int(drift["measured_candidate_count"]),
        drift_reasons=drift.get("drift_reasons") or [],
        fragility_reasons=fragility.get("fragility_reasons") or [],
    )
    write_stability_artifacts(store=store, family="expectancy_drift", report=drift, id_field="expectancy_drift_report_id", contract_name="expectancy_drift_report", registry_name="expectancy_drift_reports.jsonl", registry_row={"expectancy_drift_report_id": drift["expectancy_drift_report_id"], "sleeve_id": drift["sleeve_id"], "sleeve_version_id": drift["sleeve_version_id"], "longitudinal_run_id": drift["longitudinal_run_id"], "drift_status": drift["drift_status"], "recommended_action": drift["recommended_action"], "content_hash": drift["content_hash"], "created_at": drift["created_at"], "schema_version": drift["schema_version"]}, markdown=f"# Expectancy Drift Report\n\n{drift['research_label']}\n", audit_action="challenger_expectancy_drift_report_written", actor=actor)
    write_stability_artifacts(store=store, family="regime_fragility", report=fragility, id_field="regime_fragility_report_id", contract_name="regime_fragility_report", registry_name="regime_fragility_reports.jsonl", registry_row={"regime_fragility_report_id": fragility["regime_fragility_report_id"], "sleeve_id": fragility["sleeve_id"], "sleeve_version_id": fragility["sleeve_version_id"], "longitudinal_run_id": fragility["longitudinal_run_id"], "fragility_status": fragility["fragility_status"], "recommended_action": fragility["recommended_action"], "content_hash": fragility["content_hash"], "created_at": fragility["created_at"], "schema_version": fragility["schema_version"]}, markdown=f"# Regime Fragility Report\n\n{fragility['research_label']}\n", audit_action="challenger_regime_fragility_report_written", actor=actor)
    write_stability_artifacts(store=store, family="sleeve_stability", report=stability, id_field="sleeve_stability_report_id", contract_name="sleeve_stability_report", registry_name="sleeve_stability_reports.jsonl", registry_row={"sleeve_stability_report_id": stability["sleeve_stability_report_id"], "sleeve_id": stability["sleeve_id"], "sleeve_version_id": stability["sleeve_version_id"], "expectancy_drift_report_id": stability["expectancy_drift_report_id"], "regime_fragility_report_id": stability["regime_fragility_report_id"], "overall_stability_status": stability["overall_stability_status"], "recommended_action": stability["recommended_action"], "content_hash": stability["content_hash"], "created_at": stability["created_at"], "schema_version": stability["schema_version"]}, markdown=f"# Sleeve Stability Report\n\n{stability['research_label']}\n", audit_action="challenger_sleeve_stability_report_written", actor=actor)
    return {"expectancy_drift": drift, "regime_fragility": fragility, "sleeve_stability": stability}


def _materialize_item_chain(*, store: Path, track: dict[str, Any], hypothesis_by_id: dict[str, dict[str, Any]], rows_by_hypothesis: dict[str, list[dict[str, Any]]], item: dict[str, Any], actor: str) -> dict[str, Any]:
    hypothesis = hypothesis_by_id[str(item["challenger_hypothesis_id"])]
    rows = rows_by_hypothesis[str(item["challenger_hypothesis_id"])]
    variant = build_challenger_variant(track=track, hypothesis=hypothesis, source_artifact_ids=_source_artifact_ids(track), generated_at=GENERATED_AT)
    if variant["challenger_variant_id"] != item.get("challenger_variant_id"):
        raise RuntimeError("challenger variant id mismatch during materialization")
    write_challenger_variant(variant, store_root=store, actor=actor)
    event_study = _write_event_study_artifact(store=store, track=track, item=item, rows=rows, actor=actor)
    backtest = _write_backtest_artifact(store=store, track=track, item=item, rows=rows, actor=actor)
    longitudinal = _write_longitudinal_artifact(store=store, track=track, item=item, rows=rows, actor=actor)
    stability = _write_stability_chain(store=store, track=track, item=item, rows=rows, actor=actor)
    return {"variant": variant, "event_study": event_study, "backtest": backtest, "longitudinal_run": longitudinal, "stability": stability}


def build_challenger_evidence_batch(
    *,
    challenger_track_id: str,
    store_root: Path | None = None,
    generated_at: str = GENERATED_AT,
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    track = load_challenger_research_track(challenger_track_id, store_root=store)
    refs = _refresh_refs(track.get("audit_refs", {}).get("required_source_refs") or [])
    missing = [ref for ref in refs if ref.get("status") != "available"]
    rows: list[dict[str, Any]] = []
    if not missing:
        rows = _measured_rows_for_run(store, str(track.get("evidence_requirements", {}).get("longitudinal_run_id") or ""))
    items: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    for hypothesis in track.get("challenger_hypothesis_set", []):
        if missing:
            item = _blocked_item(hypothesis, f"required_source_missing:{missing[0].get('artifact_type')}")
            items.append(item)
            blocked.append(item)
            continue
        selected_rows, failure = _apply_delta(hypothesis, rows)
        if failure:
            item = _blocked_item(hypothesis, failure)
            items.append(item)
            blocked.append(item)
            continue
        item = _generated_item(track, hypothesis, selected_rows)
        items.append(item)
        if item["status"] == "blocked":
            blocked.append(item)
    fingerprint = content_hash(
        {
            "challenger_track_id": challenger_track_id,
            "source_refs": refs,
            "hypotheses": track.get("challenger_hypothesis_set", []),
            "items": items,
        },
        sort_lists=True,
    )
    payload = {
        "challenger_evidence_batch_id": "",
        "challenger_track_id": challenger_track_id,
        "incumbent_sleeve_id": track["incumbent_sleeve_id"],
        "incumbent_sleeve_version_id": track["incumbent_sleeve_version_id"],
        "generated_at": generated_at,
        "schema_version": SCHEMA_VERSION,
        "research_label": RESEARCH_LABEL,
        "source_dataset_snapshot_id": str(track.get("evidence_requirements", {}).get("dataset_snapshot_id") or ""),
        "source_regime_snapshot_id": str(track.get("evidence_requirements", {}).get("regime_snapshot_id") or ""),
        "source_cost_model_snapshot_id": str(track.get("evidence_requirements", {}).get("cost_model_snapshot_id") or ""),
        "source_challenger_hypothesis_ids": [str(item.get("challenger_hypothesis_id")) for item in track.get("challenger_hypothesis_set", [])],
        "challenger_evidence_items": items,
        "blocked_hypotheses": blocked,
        "evidence_generation_config": {
            "method": "fully_materialized_rule_delta_evidence_chain",
            "minimum_measured_candidates": MINIMUM_MEASURED_CANDIDATES,
            "uses_existing_immutable_outcome_index_as_source": True,
            "materializes_challenger_variant": True,
            "materializes_event_study": True,
            "materializes_backtest": True,
            "materializes_longitudinal_run": True,
            "materializes_stability_reports": True,
            "creates_promoted_sleeve": False,
            "creates_fake_candidates": False,
            "projection_only": False,
        },
        "determinism_fingerprint": fingerprint,
        "governance_constraints": _governance_constraints(),
        "recommended_next_action": _recommended_action(items, blocked),
        "audit_refs": {
            "challenger_track_id": challenger_track_id,
            "source_refs": refs,
            "required_source_missing": missing,
            "append_only_registry": "challenger_evidence_batches.jsonl",
            "audit_action": "challenger_evidence_batch_created",
        },
        "content_hash": "",
    }
    seed = content_hash(payload, exclude={"challenger_evidence_batch_id", "content_hash", "generated_at"}, sort_lists=True)
    payload["challenger_evidence_batch_id"] = f"cheb_{short_hash(seed, 16)}"
    payload["content_hash"] = content_hash(payload, exclude={"generated_at"}, sort_lists=True)
    validate_contract("challenger_evidence_batch", payload)
    return payload


def write_challenger_evidence_batch(
    *,
    challenger_track_id: str,
    store_root: Path | None = None,
    actor: str = "Aegis",
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    batch = build_challenger_evidence_batch(challenger_track_id=challenger_track_id, store_root=store)
    batch_path = _batch_path(store, batch["challenger_evidence_batch_id"])
    if batch_path.exists():
        raise FileExistsError(f"Refusing to overwrite immutable challenger evidence batch: {batch_path}")
    track = load_challenger_research_track(challenger_track_id, store_root=store)
    hypothesis_by_id = {str(row["challenger_hypothesis_id"]): row for row in track.get("challenger_hypothesis_set", [])}
    rows = _measured_rows_for_run(store, str(track.get("evidence_requirements", {}).get("longitudinal_run_id") or "")) if not batch.get("audit_refs", {}).get("required_source_missing") else []
    rows_by_hypothesis: dict[str, list[dict[str, Any]]] = {}
    for hypothesis in track.get("challenger_hypothesis_set", []):
        selected_rows, failure = _apply_delta(hypothesis, rows)
        if not failure:
            rows_by_hypothesis[str(hypothesis["challenger_hypothesis_id"])] = selected_rows
    materialized_chains = []
    for item in batch["challenger_evidence_items"]:
        if item.get("status") not in {"generated", "insufficient_evidence"}:
            continue
        materialized_chains.append(
            _materialize_item_chain(
                store=store,
                track=track,
                hypothesis_by_id=hypothesis_by_id,
                rows_by_hypothesis=rows_by_hypothesis,
                item=item,
                actor=actor,
            )
        )
    for item in batch["challenger_evidence_items"]:
        item_id = str(item.get("challenger_evidence_item_id") or item.get("challenger_hypothesis_id"))
        path = _item_path(store, item_id)
        if path.exists():
            raise FileExistsError(f"Refusing to overwrite immutable challenger evidence item: {path}")
        write_json(path, item, overwrite=False)
    write_json(batch_path, batch, overwrite=False)
    row = {
        "challenger_evidence_batch_id": batch["challenger_evidence_batch_id"],
        "challenger_track_id": batch["challenger_track_id"],
        "incumbent_sleeve_id": batch["incumbent_sleeve_id"],
        "incumbent_sleeve_version_id": batch["incumbent_sleeve_version_id"],
        "generated_item_count": sum(1 for item in batch["challenger_evidence_items"] if item["status"] == "generated"),
        "blocked_hypothesis_count": len(batch["blocked_hypotheses"]),
        "evidence_quality_summary": _quality_summary(batch["challenger_evidence_items"]),
        "recommended_next_action": batch["recommended_next_action"],
        "research_label": batch["research_label"],
        "content_hash": batch["content_hash"],
        "generated_at": batch["generated_at"],
        "schema_version": batch["schema_version"],
    }
    append_jsonl(_registry_path(store), row)
    rows = read_jsonl(_registry_path(store))
    if not rows or rows[-1] != row:
        raise RuntimeError("challenger evidence batch registry append failed")
    audit = write_audit_event(
        actor=actor,
        entity_type="challenger_evidence_batch",
        entity_id=batch["challenger_evidence_batch_id"],
        action="challenger_evidence_batch_created",
        new_state_hash=batch["content_hash"],
        reason="Generated deterministic research-only challenger evidence batch.",
        metadata={"registry_row": row, "governance_constraints": batch["governance_constraints"], "materialized_chain_count": len(materialized_chains)},
        store_root=store,
    )
    return {"batch": batch, "registry_row": row, "audit_event": audit, "json_path": str(batch_path), "materialized_chains": materialized_chains}


def load_challenger_evidence_batch(challenger_evidence_batch_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    return read_json(_batch_path(store, challenger_evidence_batch_id))


def list_challenger_evidence_batches(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    store = ensure_store_layout(store_root)
    return read_jsonl(_registry_path(store))


def evidence_quality_summary(batch: dict[str, Any]) -> dict[str, int]:
    return _quality_summary(batch.get("challenger_evidence_items") or [])
