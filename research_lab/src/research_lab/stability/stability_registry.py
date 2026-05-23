from __future__ import annotations

import math
from pathlib import Path
from statistics import mean, median
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.contracts.schemas import validate_contract
from research_lab.longitudinal.candidate_run import longitudinal_run_dir
from research_lab.storage.hashing import content_hash, short_hash
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.parquet_io import read_parquet_records
from research_lab.storage.paths import ensure_store_layout


RESEARCH_LABEL = (
    "Research governance view only. No broker execution. No live trading. "
    "No autonomous trading. No sleeve mutation. Backtests/model outputs are "
    "hypothetical research evidence, not achieved portfolio performance."
)
CREATED_AT = "1970-01-01T00:00:00Z"
CREATED_BY = "Aegis Research Lab Stability Packet 15"


def report_root(store: Path, family: str) -> Path:
    return store / "stability_reports" / family


def registry_path(store: Path, name: str) -> Path:
    return store / "registries" / name


def stable_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result) or math.isinf(result):
        return None
    return result


def metric_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    post_cost = [value for value in (stable_float(row.get("post_cost_return")) for row in rows) if value is not None]
    excess = [value for value in (stable_float(row.get("excess_return")) for row in rows) if value is not None]
    return {
        "candidate_count": len(rows),
        "measured_count": len(post_cost),
        "mean_post_cost_return": mean(post_cost) if post_cost else None,
        "median_post_cost_return": median(post_cost) if post_cost else None,
        "win_rate": sum(1 for value in post_cost if value > 0) / len(post_cost) if post_cost else None,
        "mean_excess_return": mean(excess) if excess else None,
    }


def window_num(value: Any) -> int:
    return int(str(value).rstrip("d"))


def sorted_windows(rows: list[dict[str, Any]]) -> list[str]:
    return sorted({str(row.get("outcome_window")) for row in rows if row.get("outcome_window")}, key=window_num)


def measured_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if str(row.get("outcome_status") or "").lower() == "measured"
        and stable_float(row.get("post_cost_return")) is not None
    ]


def sort_outcomes(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            str(row.get("as_of_date") or ""),
            str(row.get("candidate_id") or ""),
            str(row.get("outcome_window") or ""),
        ),
    )


def load_longitudinal_context(longitudinal_run_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    root = longitudinal_run_dir(longitudinal_run_id, store_root=store)
    run_path = root / "longitudinal_run.json"
    outcome_path = root / "outcome_index.parquet"
    candidate_path = root / "candidate_batch_index.parquet"
    if not root.exists():
        raise RuntimeError(f"Longitudinal run missing: {longitudinal_run_id}")
    if not run_path.exists():
        raise RuntimeError(f"longitudinal_run.json missing: {run_path}")
    if not outcome_path.exists():
        raise RuntimeError(f"outcome_index.parquet missing: {outcome_path}")
    run = read_json(run_path)
    rows = read_parquet_records(outcome_path)
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise RuntimeError(f"outcome index malformed: {outcome_path}")
    measured = measured_rows(rows)
    candidate_rows = read_parquet_records(candidate_path) if candidate_path.exists() else []
    return {
        "store": store,
        "root": root,
        "run": run,
        "rows": rows,
        "measured_rows": sort_outcomes(measured),
        "candidate_rows": candidate_rows,
        "candidate_count": len({str(row.get("candidate_id")) for row in rows if row.get("candidate_id")}),
        "measured_candidate_count": len({str(row.get("candidate_id")) for row in measured if row.get("candidate_id")}),
        "windows": sorted_windows(measured),
    }


def finalize_report(report: dict[str, Any], *, id_field: str, prefix: str) -> dict[str, Any]:
    seed = content_hash(report, exclude={"content_hash", id_field, "created_at"}, sort_lists=True)
    report[id_field] = f"{prefix}_{short_hash(seed, 16)}"
    report["content_hash"] = content_hash(report, exclude={"created_at"}, sort_lists=True)
    return report


def verify_report_hash(report: dict[str, Any]) -> None:
    actual = content_hash(report, exclude={"created_at"}, sort_lists=True)
    if actual != report.get("content_hash"):
        raise RuntimeError(f"hash mismatch detected for report: expected {report.get('content_hash')} actual {actual}")


def write_stability_artifacts(
    *,
    store: Path,
    family: str,
    report: dict[str, Any],
    id_field: str,
    contract_name: str,
    registry_name: str,
    registry_row: dict[str, Any],
    markdown: str,
    audit_action: str,
    actor: str,
) -> dict[str, Any]:
    verify_report_hash(report)
    validate_contract(contract_name, report)
    root = report_root(store, family)
    report_id = str(report[id_field])
    json_path = root / f"{report_id}.json"
    md_path = root / f"{report_id}.md"
    if json_path.exists() or md_path.exists():
        raise FileExistsError(f"Refusing to overwrite immutable stability report: {json_path}")
    write_json(json_path, report, overwrite=False)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(markdown, encoding="utf-8")
    registry_file = registry_path(store, registry_name)
    append_jsonl(registry_file, registry_row)
    if not read_jsonl(registry_file) or read_jsonl(registry_file)[-1] != registry_row:
        raise RuntimeError(f"registry append failed: {registry_file}")
    write_audit_event(
        actor=actor,
        entity_type="stability_report",
        entity_id=report_id,
        action=audit_action,
        new_state_hash=report["content_hash"],
        reason="Generated immutable read-only research governance stability report.",
        metadata={
            "registry_row": registry_row,
            "json_path": str(json_path),
            "markdown_path": str(md_path),
            "no_execution": True,
            "no_sleeve_mutation": True,
        },
        store_root=store,
    )
    return report


def latest_report(store: Path, *, registry_name: str, family: str, id_field: str, sleeve_id: str | None = None) -> dict[str, Any] | None:
    rows = read_jsonl(registry_path(store, registry_name))
    if sleeve_id:
        rows = [row for row in rows if row.get("sleeve_id") == sleeve_id]
    rows = [row for row in rows if row.get(id_field)]
    if not rows:
        return None
    latest = sorted(rows, key=lambda row: (str(row.get("created_at") or ""), str(row.get(id_field))))[-1]
    path = report_root(store, family) / f"{latest[id_field]}.json"
    return read_json(path) if path.exists() else None
