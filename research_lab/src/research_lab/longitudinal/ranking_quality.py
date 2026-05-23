from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from statistics import mean, median
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.contracts.schemas import validate_contract
from research_lab.longitudinal.candidate_run import longitudinal_run_dir
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import append_jsonl, read_json, write_json
from research_lab.storage.parquet_io import read_parquet_records
from research_lab.storage.paths import ensure_store_layout


def _window_num(value: Any) -> int:
    return int(str(value).rstrip("d"))


def _spearman(rows: list[dict[str, Any]]) -> float | None:
    pairs = [(float(row["ranking_score"]), float(row["post_cost_return"])) for row in rows if row.get("ranking_score") is not None and row.get("post_cost_return") is not None]
    n = len(pairs)
    if n < 2:
        return None
    rank_x = {i: rank for rank, (i, _) in enumerate(sorted(enumerate(pairs), key=lambda item: (-item[1][0], item[0])), start=1)}
    rank_y = {i: rank for rank, (i, _) in enumerate(sorted(enumerate(pairs), key=lambda item: (-item[1][1], item[0])), start=1)}
    xs = [rank_x[i] for i in range(n)]
    ys = [rank_y[i] for i in range(n)]
    mx, my = mean(xs), mean(ys)
    denom_x = sum((x - mx) ** 2 for x in xs)
    denom_y = sum((y - my) ** 2 for y in ys)
    if denom_x == 0 or denom_y == 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (denom_x * denom_y) ** 0.5


def _bucket_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for bucket in ["top", "middle", "low"]:
        values = [float(row["post_cost_return"]) for row in rows if row.get("ranking_bucket") == bucket and row.get("post_cost_return") is not None]
        result[bucket] = {
            "measured_count": len(values),
            "mean_post_cost_return": mean(values) if values else None,
            "median_post_cost_return": median(values) if values else None,
            "win_rate": sum(1 for value in values if value > 0) / len(values) if values else None,
        }
    return result


def _deciles(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if len(rows) < 50:
        return {"decile_status": "insufficient_sample"}
    ranked = sorted(rows, key=lambda row: (-float(row["ranking_score"]), row["candidate_id"]))
    result = {"decile_status": "computed"}
    for idx in range(10):
        chunk = ranked[idx * len(ranked) // 10 : (idx + 1) * len(ranked) // 10]
        values = [float(row["post_cost_return"]) for row in chunk if row.get("post_cost_return") is not None]
        result[f"decile_{idx + 1}_{'best' if idx == 0 else 'worst' if idx == 9 else 'mid'}"] = {
            "measured_count": len(values),
            "mean_post_cost_return": mean(values) if values else None,
        }
    return result


def _quality_status(measured_count: int, spread: float | None, corr: float | None) -> str:
    if measured_count < 30:
        return "insufficient_sample"
    spread_positive = spread is not None and spread > 0
    corr_positive = corr is not None and corr > 0
    if spread_positive and corr_positive:
        return "useful"
    if spread_positive or corr_positive:
        return "mixed"
    return "weak"


def build_ranking_quality_report(longitudinal_run_id: str, *, store_root: Path | None = None, created_at: str | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    root = longitudinal_run_dir(longitudinal_run_id, store_root=store)
    run = read_json(root / "longitudinal_run.json")
    outcome_rows = [row for row in read_parquet_records(root / "outcome_index.parquet") if row.get("outcome_status") == "measured"]
    by_window: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in outcome_rows:
        by_window[str(row["outcome_window"])].append(row)
    metrics: dict[str, Any] = {}
    statuses: dict[str, str] = {}
    for window in sorted(by_window, key=_window_num):
        rows = by_window[window]
        values = [float(row["post_cost_return"]) for row in rows if row.get("post_cost_return") is not None]
        excess = [float(row["excess_return"]) for row in rows if row.get("excess_return") is not None]
        bucket = _bucket_metrics(rows)
        top_mean = bucket["top"]["mean_post_cost_return"]
        low_mean = bucket["low"]["mean_post_cost_return"]
        spread = top_mean - low_mean if top_mean is not None and low_mean is not None else None
        corr = _spearman(rows)
        status = _quality_status(len(values), spread, corr)
        statuses[window] = status
        metrics[window] = {
            "candidate_count": len(rows),
            "measured_count": len(values),
            "mean_post_cost_return": mean(values) if values else None,
            "median_post_cost_return": median(values) if values else None,
            "win_rate": sum(1 for value in values if value > 0) / len(values) if values else None,
            "mean_excess_return": mean(excess) if excess else None,
            "by_ranking_bucket": bucket,
            "deciles": _deciles(rows),
            "top_bucket_mean_return": top_mean,
            "low_bucket_mean_return": low_mean,
            "top_minus_low_spread": spread,
            "spearman_rank_correlation": corr,
            "ranking_quality_status": status,
        }
    ordered_status = ["useful", "mixed", "weak", "insufficient_sample"]
    overall = next((status for status in ordered_status if status in statuses.values()), "insufficient_sample")
    report = {
        "longitudinal_run_id": longitudinal_run_id,
        "sleeve_id": run["sleeve_id"],
        "sleeve_version_id": run["sleeve_version_id"],
        "candidate_count": sum(int(row.get("candidate_count", 0)) for row in read_parquet_records(root / "candidate_batch_index.parquet")),
        "measured_count": len({row["candidate_id"] for row in outcome_rows}),
        "metrics_by_window": metrics,
        "ranking_quality_status_by_window": statuses,
        "overall_ranking_quality_status": overall,
        "created_at": created_at or utc_now_iso(),
        "schema_version": "ranking_quality_report.v1",
        "compliance_label": "Ranking analytics are historical research/observation, not live achieved portfolio results or investment advice.",
    }
    report["content_hash"] = content_hash(report, exclude={"created_at"}, sort_lists=True)
    report["ranking_quality_report_id"] = f"rqr_{longitudinal_run_id}_{short_hash(report['content_hash'], 10)}"
    report["content_hash"] = content_hash(report, exclude={"created_at"}, sort_lists=True)
    validate_contract("ranking_quality_report", report)
    return report


def ranking_quality_markdown(report: dict[str, Any]) -> str:
    lines = ["# Ranking Quality Report", "", report["compliance_label"], ""]
    for window, metrics in report["metrics_by_window"].items():
        lines.append(f"## {window}")
        lines.append(f"- status: {metrics['ranking_quality_status']}")
        lines.append(f"- measured_count: {metrics['measured_count']}")
        lines.append(f"- top_minus_low_spread: {metrics['top_minus_low_spread']}")
        lines.append(f"- spearman_rank_correlation: {metrics['spearman_rank_correlation']}")
    return "\n".join(lines) + "\n"


def write_ranking_quality_report(longitudinal_run_id: str, *, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    root = longitudinal_run_dir(longitudinal_run_id, store_root=store)
    if (root / "ranking_quality_report.json").exists():
        return read_json(root / "ranking_quality_report.json")
    report = build_ranking_quality_report(longitudinal_run_id, store_root=store)
    write_json(root / "ranking_quality_report.json", report, overwrite=False)
    (root / "ranking_quality_report.md").write_text(ranking_quality_markdown(report), encoding="utf-8")
    row = {
        "ranking_quality_report_id": report["ranking_quality_report_id"],
        "longitudinal_run_id": longitudinal_run_id,
        "sleeve_id": report["sleeve_id"],
        "sleeve_version_id": report["sleeve_version_id"],
        "overall_ranking_quality_status": report["overall_ranking_quality_status"],
        "content_hash": report["content_hash"],
        "created_at": report["created_at"],
        "schema_version": report["schema_version"],
    }
    append_jsonl(ensure_store_layout(store) / "registries" / "ranking_quality_reports.jsonl", row)
    write_audit_event(actor=actor, entity_type="longitudinal_candidate_run", entity_id=longitudinal_run_id, action="ranking_quality_report_written", new_state_hash=report["content_hash"], reason="Wrote deterministic ranking quality report.", metadata={"registry_row": row}, store_root=store)
    return report
