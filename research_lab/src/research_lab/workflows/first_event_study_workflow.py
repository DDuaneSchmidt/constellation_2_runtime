from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from research_lab.datasets.coverage_report import load_coverage_report
from research_lab.datasets.dataset_registry import load_dataset_snapshot
from research_lab.research.research_plan import build_research_plan
from research_lab.research.research_plan_registry import load_research_plan, store_research_plan
from research_lab.runners.event_study_runner import run_event_study
from research_lab.storage.duckdb_query import canonical_parquet_path


def _years_between(first: str, last: str) -> float:
    return (date.fromisoformat(last[:10]) - date.fromisoformat(first[:10])).days / 365.25


def evidence_readiness_for_dataset(dataset_snapshot_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    missing: list[str] = []
    dataset = load_dataset_snapshot(dataset_snapshot_id, store_root=store_root)
    canonical_path = canonical_parquet_path(dataset_snapshot_id, store_root=store_root)
    if not canonical_path.exists():
        missing.append("canonical_parquet_missing")
    if int(dataset.get("row_count") or 0) <= 0:
        missing.append("row_count_must_be_positive")
    if int(dataset.get("symbol_count") or 0) < 3:
        missing.append("symbol_count_must_be_at_least_3")
    if dataset.get("quality_status") not in {"pass", "pass_with_warnings"}:
        missing.append("quality_status_must_be_pass_or_pass_with_warnings")
    try:
        coverage = load_coverage_report(dataset_snapshot_id, store_root=store_root)
    except Exception:
        coverage = None
        missing.append("coverage_report_missing")
    qualifying_symbols: list[str] = []
    if coverage:
        for row in coverage.get("coverage_by_symbol", []):
            if row.get("row_count", 0) > 0 and row.get("first_date") and row.get("last_date"):
                if _years_between(row["first_date"], row["last_date"]) >= 3.0:
                    qualifying_symbols.append(row["symbol"])
        if len(qualifying_symbols) < 3:
            missing.append("at_least_3_symbols_need_3_years_of_data")
    return {
        "dataset_snapshot_id": dataset_snapshot_id,
        "ready": not missing,
        "missing_requirements": sorted(set(missing)),
        "dataset": dataset,
        "coverage_report": coverage,
        "qualifying_symbols_with_3y_history": sorted(qualifying_symbols),
        "schema_version": "evidence_readiness.v1",
    }


def create_standard_event_study(
    *,
    dataset_snapshot_id: str,
    hypothesis_id: str,
    threshold: float,
    forward_windows: list[int],
    store_root: Path | None = None,
    actor: str = "Aegis",
) -> dict[str, Any]:
    readiness = evidence_readiness_for_dataset(dataset_snapshot_id, store_root=store_root)
    if not readiness["ready"]:
        raise RuntimeError("Dataset is not evidence-ready: " + ", ".join(readiness["missing_requirements"]))
    dataset = readiness["dataset"]
    plan = build_research_plan(
        hypothesis_id=hypothesis_id,
        title="Liquid ETF drop mean-reversion event study",
        hypothesis="After large negative daily returns in liquid ETFs, subsequent forward returns show measurable mean-reversion behavior.",
        dataset_snapshot_id=dataset_snapshot_id,
        universe_snapshot_id=dataset["universe_snapshot_id"],
        symbols=list(dataset["symbols"]),
        start=dataset["start_date"],
        end=dataset["end_date"],
        event_definition={"type": "daily_return_below_threshold", "params": {"return_column": "adj_close", "threshold": threshold}},
        forward_return_windows=forward_windows,
        created_by=actor,
    )
    registry_row = store_research_plan(plan, store_root=store_root)
    return {"research_plan": plan, "registry_row": registry_row, "evidence_readiness": readiness}


def run_standard_event_study(
    *,
    research_plan_id: str,
    store_root: Path | None = None,
    actor: str = "Aegis",
    allow_json_fallback: bool = False,
) -> dict[str, Any]:
    plan = load_research_plan(research_plan_id, store_root=store_root)
    readiness = evidence_readiness_for_dataset(plan["dataset_snapshot_id"], store_root=store_root)
    if not readiness["ready"]:
        raise RuntimeError("Dataset is not evidence-ready: " + ", ".join(readiness["missing_requirements"]))
    result = run_event_study(
        research_plan_id=research_plan_id,
        store_root=store_root,
        actor=actor,
        allow_json_fallback=allow_json_fallback,
    )
    return {"evidence_readiness": readiness, "event_study": result}
