from __future__ import annotations

from collections import defaultdict
from pathlib import Path
import shutil
from statistics import mean, median
from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.research.research_plan import slugify
from research_lab.storage.hashing import content_hash, sha256_hex, short_hash, utc_now_iso
from research_lab.storage.manifest_io import write_json
from research_lab.storage.parquet_io import file_sha256, write_parquet_records
from research_lab.storage.paths import ensure_store_layout, evidence_uri


EVIDENCE_SCHEMA_VERSION = "evidence_package.v1"


def evidence_quality_for_event_count(event_count: int) -> str:
    if event_count < 30:
        return "insufficient_sample"
    if event_count < 100:
        return "preliminary"
    return "moderate"


def build_event_study_summary(
    *,
    research_plan: dict[str, Any],
    events: list[dict[str, Any]],
    forward_returns: list[dict[str, Any]],
    unavailable_counts_by_window: dict[str, int],
    category_by_symbol: dict[str, str] | None = None,
    regime_snapshot_id: str | None = None,
    cost_model_snapshot_id: str | None = None,
) -> dict[str, Any]:
    grouped_returns: dict[int, list[float]] = defaultdict(list)
    grouped_gross_returns: dict[int, list[float]] = defaultdict(list)
    grouped_post_cost_returns: dict[int, list[float]] = defaultdict(list)
    for row in forward_returns:
        grouped_returns[int(row["forward_window"])].append(float(row["forward_return"]))
        grouped_gross_returns[int(row["forward_window"])].append(float(row.get("gross_forward_return", row["forward_return"])))
        if "post_cost_forward_return" in row:
            grouped_post_cost_returns[int(row["forward_window"])].append(float(row["post_cost_forward_return"]))

    windows = sorted(int(window) for window in research_plan["forward_return_windows"])
    events_by_symbol: dict[str, int] = defaultdict(int)
    for event in events:
        events_by_symbol[event["symbol"]] += 1
    categories = category_by_symbol or {}

    def _window_map(fn) -> dict[str, float | int | None]:
        result: dict[str, float | int | None] = {}
        for window in windows:
            values = grouped_returns.get(window, [])
            result[str(window)] = fn(values) if values else None
        return result

    def _summary_for(grouped: dict[int, list[float]]) -> dict[str, dict[str, float | int | None]]:
        return {
            str(window): {
                "sample_size": len(grouped.get(window, [])),
                "mean": mean(grouped[window]) if grouped.get(window) else None,
                "median": median(grouped[window]) if grouped.get(window) else None,
                "win_rate": (
                    sum(1 for value in grouped[window] if value > 0) / len(grouped[window])
                    if grouped.get(window)
                    else None
                ),
                "min": min(grouped[window]) if grouped.get(window) else None,
                "max": max(grouped[window]) if grouped.get(window) else None,
            }
            for window in windows
        }

    def _grouped_metric_rows(rows: list[dict[str, Any]], group_field: str, return_field: str) -> dict[str, Any]:
        grouped_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            grouped_rows[str(row.get(group_field) or "unknown")].append(row)
        result: dict[str, Any] = {}
        for group, items in sorted(grouped_rows.items()):
            by_window: dict[int, list[float]] = defaultdict(list)
            for item in items:
                by_window[int(item["forward_window"])].append(float(item[return_field]))
            result[group] = _summary_for(by_window)
        return result

    event_count = len(events)
    by_symbol: dict[str, Any] = {}
    for symbol in sorted(events_by_symbol):
        symbol_returns = [row for row in forward_returns if row["symbol"] == symbol]
        by_symbol[symbol] = {
            "event_count": events_by_symbol[symbol],
            "category": categories.get(symbol, "UNKNOWN"),
            "sample_size_by_window": {
                str(window): sum(1 for row in symbol_returns if int(row["forward_window"]) == window)
                for window in windows
            },
            "mean_forward_return_by_window": {
                str(window): (
                    mean([float(row["forward_return"]) for row in symbol_returns if int(row["forward_window"]) == window])
                    if any(int(row["forward_window"]) == window for row in symbol_returns)
                    else None
                )
                for window in windows
            },
            "win_rate_by_window": {
                str(window): (
                    sum(1 for row in symbol_returns if int(row["forward_window"]) == window and float(row["forward_return"]) > 0)
                    / sum(1 for row in symbol_returns if int(row["forward_window"]) == window)
                    if any(int(row["forward_window"]) == window for row in symbol_returns)
                    else None
                )
                for window in windows
            },
        }

    by_category_events: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_category_returns: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        by_category_events[categories.get(event["symbol"], "UNKNOWN")].append(event)
    for row in forward_returns:
        by_category_returns[categories.get(row["symbol"], "UNKNOWN")].append(row)
    by_category: dict[str, Any] = {}
    for category in sorted(by_category_events):
        category_returns = by_category_returns.get(category, [])
        by_category[category] = {
            "event_count": len(by_category_events[category]),
            "symbols": sorted({event["symbol"] for event in by_category_events[category]}),
            "sample_size_by_window": {
                str(window): sum(1 for row in category_returns if int(row["forward_window"]) == window)
                for window in windows
            },
            "mean_forward_return_by_window": {
                str(window): (
                    mean([float(row["forward_return"]) for row in category_returns if int(row["forward_window"]) == window])
                    if any(int(row["forward_window"]) == window for row in category_returns)
                    else None
                )
                for window in windows
            },
            "win_rate_by_window": {
                str(window): (
                    sum(1 for row in category_returns if int(row["forward_window"]) == window and float(row["forward_return"]) > 0)
                    / sum(1 for row in category_returns if int(row["forward_window"]) == window)
                    if any(int(row["forward_window"]) == window for row in category_returns)
                    else None
                )
                for window in windows
            },
        }

    by_forward_window = {
        str(window): {
            "sample_size": len(grouped_returns.get(window, [])),
            "mean_forward_return": mean(grouped_returns[window]) if grouped_returns.get(window) else None,
            "median_forward_return": median(grouped_returns[window]) if grouped_returns.get(window) else None,
            "win_rate": (
                sum(1 for value in grouped_returns[window] if value > 0) / len(grouped_returns[window])
                if grouped_returns.get(window)
                else None
            ),
        }
        for window in windows
    }

    evidence_quality = evidence_quality_for_event_count(event_count)
    post_cost_summary = _summary_for(grouped_post_cost_returns) if grouped_post_cost_returns else {}
    gross_summary = _summary_for(grouped_gross_returns)
    post_cost_positive_windows = [
        str(window)
        for window, values in grouped_post_cost_returns.items()
        if values and mean(values) > 0
    ]
    regime_fields = ["risk_regime", "trend_regime", "vol_regime", "drawdown_regime"]
    by_regime = {
        field: {
            "gross": _grouped_metric_rows(forward_returns, field, "gross_forward_return"),
            "post_cost": _grouped_metric_rows(forward_returns, field, "post_cost_forward_return")
            if grouped_post_cost_returns
            else {},
        }
        for field in regime_fields
        if any(field in row for row in forward_returns)
    }
    by_symbol_and_regime: dict[str, Any] = {}
    for symbol in sorted({row["symbol"] for row in forward_returns}):
        symbol_rows = [row for row in forward_returns if row["symbol"] == symbol]
        by_symbol_and_regime[symbol] = {
            "risk_regime": {
                "gross": _grouped_metric_rows(symbol_rows, "risk_regime", "gross_forward_return"),
                "post_cost": _grouped_metric_rows(symbol_rows, "risk_regime", "post_cost_forward_return")
                if grouped_post_cost_returns
                else {},
            }
        }
    by_category_and_regime: dict[str, Any] = {}
    for category in sorted({categories.get(row["symbol"], "UNKNOWN") for row in forward_returns}):
        category_rows = [row for row in forward_returns if categories.get(row["symbol"], "UNKNOWN") == category]
        by_category_and_regime[category] = {
            "risk_regime": {
                "gross": _grouped_metric_rows(category_rows, "risk_regime", "gross_forward_return"),
                "post_cost": _grouped_metric_rows(category_rows, "risk_regime", "post_cost_forward_return")
                if grouped_post_cost_returns
                else {},
            }
        }
    summary = {
        "research_plan_id": research_plan["research_plan_id"],
        "hypothesis_id": research_plan["hypothesis_id"],
        "dataset_snapshot_id": research_plan["dataset_snapshot_id"],
        "universe_snapshot_id": research_plan["universe_snapshot_id"],
        "event_count": event_count,
        "symbol_count": len({event["symbol"] for event in events}),
        "date_range": research_plan["date_range"],
        "forward_windows": windows,
        "mean_forward_return_by_window": _window_map(lambda values: mean(values)),
        "median_forward_return_by_window": _window_map(lambda values: median(values)),
        "win_rate_by_window": _window_map(lambda values: sum(1 for value in values if value > 0) / len(values)),
        "min_forward_return_by_window": _window_map(lambda values: min(values)),
        "max_forward_return_by_window": _window_map(lambda values: max(values)),
        "sample_size_by_window": _window_map(lambda values: len(values)),
        "unavailable_forward_returns_by_window": unavailable_counts_by_window,
        "symbols_with_events": sorted(events_by_symbol),
        "events_by_symbol": dict(sorted(events_by_symbol.items())),
        "evidence_quality": evidence_quality,
        "gross_summary": gross_summary,
        "post_cost_summary": post_cost_summary,
        "by_regime": by_regime,
        "by_symbol_and_regime": by_symbol_and_regime,
        "by_category_and_regime": by_category_and_regime,
        "cost_model_snapshot_id": cost_model_snapshot_id,
        "regime_snapshot_id": regime_snapshot_id,
        "evidence_diagnostics": {
            "post_cost_positive_windows": sorted(post_cost_positive_windows),
            "regime_coverage": (
                sum(1 for row in forward_returns if row.get("risk_regime") not in {None, "unknown"}) / len(forward_returns)
                if forward_returns
                else 0
            ),
            "cost_model_applied": cost_model_snapshot_id is not None,
            "regime_snapshot_applied": regime_snapshot_id is not None,
        },
        "overall": {},
        "by_symbol": by_symbol,
        "by_category": by_category,
        "by_forward_window": by_forward_window,
        "sample_size_quality": {
            "event_count": event_count,
            "evidence_quality": evidence_quality,
            "thresholds": {"insufficient_sample_lt": 30, "preliminary_lt": 100, "moderate_gte": 100},
        },
        "summary_metrics": {},
        "schema_version": "event_study_result.v1",
    }
    summary["overall"] = {
        "event_count": summary["event_count"],
        "symbol_count": summary["symbol_count"],
        "mean_forward_return_by_window": summary["mean_forward_return_by_window"],
        "median_forward_return_by_window": summary["median_forward_return_by_window"],
        "win_rate_by_window": summary["win_rate_by_window"],
        "sample_size_by_window": summary["sample_size_by_window"],
    }
    summary["summary_metrics"] = {
        "mean_forward_return_by_window": summary["mean_forward_return_by_window"],
        "median_forward_return_by_window": summary["median_forward_return_by_window"],
        "win_rate_by_window": summary["win_rate_by_window"],
        "sample_size_by_window": summary["sample_size_by_window"],
    }
    validate_contract("event_study_result", summary)
    return summary


def summary_markdown(summary: dict[str, Any]) -> str:
    lines = [
        f"# Event Study Summary: {summary['hypothesis_id']}",
        "",
        f"- Research plan: `{summary['research_plan_id']}`",
        f"- Dataset snapshot: `{summary['dataset_snapshot_id']}`",
        f"- Evidence quality: `{summary['evidence_quality']}`",
        f"- Event count: {summary['event_count']}",
        f"- Symbol count: {summary['symbol_count']}",
        f"- Date range: {summary['date_range']['start']} to {summary['date_range']['end']}",
        "",
        "## Forward Return Windows",
    ]
    for window in summary["forward_windows"]:
        key = str(window)
        lines.append(
            f"- {window}d: sample_size={summary['sample_size_by_window'][key]}, "
            f"mean={summary['mean_forward_return_by_window'][key]}, "
            f"median={summary['median_forward_return_by_window'][key]}, "
            f"win_rate={summary['win_rate_by_window'][key]}"
        )
    if summary.get("post_cost_summary"):
        lines.extend(["", "## Post-Cost Forward Return Windows"])
        for window in summary["forward_windows"]:
            key = str(window)
            row = summary["post_cost_summary"].get(key, {})
            lines.append(
                f"- {window}d: sample_size={row.get('sample_size')}, "
                f"mean={row.get('mean')}, median={row.get('median')}, win_rate={row.get('win_rate')}"
            )
    if summary.get("by_regime"):
        lines.extend(["", "## Regime-Conditioned Results"])
        for regime_name, regime_payload in sorted(summary["by_regime"].items()):
            lines.append(f"- {regime_name}: groups={sorted(regime_payload.get('post_cost', {}).keys())}")
    lines.extend(["", "No trading execution, broker submission, or sleeve mutation is performed by this evidence package."])
    return "\n".join(lines) + "\n"


def _package_hash_payload(
    *,
    research_plan: dict[str, Any],
    dataset_snapshot: dict[str, Any],
    event_table_hash: str,
    forward_returns_hash: str,
    summary_hash: str,
    regime_snapshot_hash: str | None = None,
    cost_model_snapshot_hash: str | None = None,
) -> dict[str, Any]:
    return {
        "research_plan_id": research_plan["research_plan_id"],
        "hypothesis_id": research_plan["hypothesis_id"],
        "dataset_snapshot_id": research_plan["dataset_snapshot_id"],
        "universe_snapshot_id": research_plan["universe_snapshot_id"],
        "research_plan_hash": research_plan["content_hash"],
        "dataset_snapshot_hash": dataset_snapshot["content_hash"],
        "event_table_hash": event_table_hash,
        "forward_returns_hash": forward_returns_hash,
        "summary_hash": summary_hash,
        "runner_name": research_plan["runner_name"],
        "runner_version": research_plan["runner_version"],
        "regime_snapshot_hash": regime_snapshot_hash,
        "cost_model_snapshot_hash": cost_model_snapshot_hash,
    }


def write_event_study_evidence_package(
    *,
    research_plan: dict[str, Any],
    dataset_snapshot: dict[str, Any],
    events: list[dict[str, Any]],
    forward_returns: list[dict[str, Any]],
    summary: dict[str, Any],
    created_by: str = "Aegis",
    store_root: Path | None = None,
    allow_json_fallback: bool = False,
    regime_snapshot: dict[str, Any] | None = None,
    cost_model_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    package_seed = content_hash(
        {
            "research_plan_id": research_plan["research_plan_id"],
            "dataset_snapshot_id": dataset_snapshot["dataset_snapshot_id"],
            "event_definition": research_plan["event_definition"],
            "forward_return_windows": research_plan["forward_return_windows"],
            "event_count": len(events),
            "regime_snapshot_id": regime_snapshot["regime_snapshot_id"] if regime_snapshot else None,
            "cost_model_snapshot_id": cost_model_snapshot["cost_model_snapshot_id"] if cost_model_snapshot else None,
        },
        sort_lists=False,
    )
    today = utc_now_iso()[:10].replace("-", "")
    evidence_package_id = f"ev_{slugify(research_plan['hypothesis_id'])}_{today}_{short_hash(package_seed, 10)}"
    package_dir = store / "evidence_packages" / evidence_package_id
    if package_dir.exists():
        raise FileExistsError(f"Refusing to overwrite immutable evidence package: {package_dir}")
    stage_dir = store / "tmp" / f"{evidence_package_id}.staging"
    if stage_dir.exists():
        shutil.rmtree(stage_dir)
    stage_dir.mkdir(parents=True, exist_ok=False)

    research_plan_path = stage_dir / "research_plan.json"
    write_json(research_plan_path, research_plan, overwrite=False)
    event_table_path = stage_dir / "event_table.parquet"
    forward_returns_path = stage_dir / "forward_returns.parquet"
    write_parquet_records(event_table_path, events, allow_json_fallback=allow_json_fallback)
    write_parquet_records(forward_returns_path, forward_returns, allow_json_fallback=allow_json_fallback)
    summary_path = stage_dir / "summary.json"
    write_json(summary_path, summary, overwrite=False)
    summary_md_path = stage_dir / "summary.md"
    summary_md_path.write_text(summary_markdown(summary), encoding="utf-8")
    logs_path = stage_dir / "logs.txt"
    logs_path.write_text("Deterministic event_study_v1 completed without broker execution or sleeve mutation.\n", encoding="utf-8")

    event_table_hash = file_sha256(event_table_path)
    forward_returns_hash = file_sha256(forward_returns_path)
    summary_hash = file_sha256(summary_path)
    package_hash_payload = _package_hash_payload(
        research_plan=research_plan,
        dataset_snapshot=dataset_snapshot,
        event_table_hash=event_table_hash,
        forward_returns_hash=forward_returns_hash,
        summary_hash=summary_hash,
        regime_snapshot_hash=regime_snapshot["content_hash"] if regime_snapshot else None,
        cost_model_snapshot_hash=cost_model_snapshot["content_hash"] if cost_model_snapshot else None,
    )
    runner_input_hash = content_hash(
        {
            "research_plan_hash": research_plan["content_hash"],
            "dataset_snapshot_hash": dataset_snapshot["content_hash"],
            "regime_snapshot_hash": regime_snapshot["content_hash"] if regime_snapshot else None,
            "cost_model_snapshot_hash": cost_model_snapshot["content_hash"] if cost_model_snapshot else None,
        },
        sort_lists=True,
    )
    runner_output_hash = content_hash(package_hash_payload, sort_lists=True)
    manifest = {
        "evidence_package_id": evidence_package_id,
        "research_plan_id": research_plan["research_plan_id"],
        "hypothesis_id": research_plan["hypothesis_id"],
        "dataset_snapshot_id": dataset_snapshot["dataset_snapshot_id"],
        "universe_snapshot_id": research_plan["universe_snapshot_id"],
        "runner_name": research_plan["runner_name"],
        "runner_version": research_plan["runner_version"],
        "event_definition": research_plan["event_definition"],
        "forward_return_windows": research_plan["forward_return_windows"],
        "symbols": research_plan["symbols"],
        "date_range": research_plan["date_range"],
        "event_count": len(events),
        "evidence_quality": summary["evidence_quality"],
        "summary_hash": summary_hash,
        "event_table_hash": event_table_hash,
        "forward_returns_hash": forward_returns_hash,
        "research_plan_hash": research_plan["content_hash"],
        "dataset_snapshot_hash": dataset_snapshot["content_hash"],
        "regime_snapshot_id": regime_snapshot["regime_snapshot_id"] if regime_snapshot else None,
        "regime_snapshot_hash": regime_snapshot["content_hash"] if regime_snapshot else None,
        "cost_model_snapshot_id": cost_model_snapshot["cost_model_snapshot_id"] if cost_model_snapshot else None,
        "cost_model_snapshot_hash": cost_model_snapshot["content_hash"] if cost_model_snapshot else None,
        "post_cost_outputs_hash": content_hash({"forward_returns": forward_returns, "summary": summary}, sort_lists=False)
        if cost_model_snapshot
        else None,
        "runner_input_hash": runner_input_hash,
        "runner_output_hash": runner_output_hash,
        "artifact_uris": [
            f"{evidence_uri(evidence_package_id)}/event_table.parquet",
            f"{evidence_uri(evidence_package_id)}/forward_returns.parquet",
            f"{evidence_uri(evidence_package_id)}/summary.json",
            f"{evidence_uri(evidence_package_id)}/summary.md",
        ],
        "summary_uri": f"{evidence_uri(evidence_package_id)}/summary.json",
        "created_at": utc_now_iso(),
        "created_by": created_by,
        "schema_version": EVIDENCE_SCHEMA_VERSION,
    }
    manifest["manifest_hash"] = content_hash(
        manifest,
        exclude={"created_at", "manifest_hash", "evidence_package_id", "artifact_uris", "summary_uri"},
        sort_lists=True,
    )
    validate_contract("evidence_package", manifest)
    write_json(stage_dir / "evidence_manifest.json", manifest, overwrite=False)
    stage_dir.rename(package_dir)
    return manifest
