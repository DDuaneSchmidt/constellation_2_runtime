from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from constellation_2.common.atlas_v2_research_os.artifact_store import DEFAULT_STORE_ROOT
from constellation_2.common.atlas_v2_research_os.direct_candidate_data_validation import build_direct_candidate_data_validation_report
from constellation_2.common.atlas_v2_research_os.local_market_data_import import discover_local_market_data_files
from constellation_2.common.atlas_v2_research_os.market_data_schema_validation import validate_market_data_schema

AUTHORITY_BOUNDARY = {
    "offline_refresh_only": True,
    "production_pipeline_integration_authorized": False,
    "trade_recommendation_authorized": False,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "portfolio_construction_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
    "candidate_production_promotion_authorized": False,
    "replay_qualification_changes_authorized": False,
}

TRACKED_CLASSES = ("CONFIRMED", "WEAKENED", "INSUFFICIENT_DATA")


def build_direct_validation_refresh(
    *,
    root: str | Path = DEFAULT_STORE_ROOT,
    previous_path: str | Path | None = None,
    out_dir: str | Path | None = None,
    created_at: str | None = None,
    force: bool = False,
) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    previous_report_path = Path(previous_path) if previous_path else root_path / "direct_candidate_data_validation" / "latest.json"
    previous_report = _read_json(previous_report_path, {})
    previous_missing_symbols = _previous_missing_symbols(previous_report)
    current_available_symbols = _schema_valid_symbols(root_path)
    newly_available_symbols = sorted(previous_missing_symbols & current_available_symbols)
    should_rerun = bool(newly_available_symbols or force)
    refreshed_report = build_direct_candidate_data_validation_report(root=root_path, created_at=created) if should_rerun else previous_report
    delta = _build_delta(
        previous_report=previous_report,
        refreshed_report=refreshed_report,
        previous_report_path=previous_report_path,
        newly_available_symbols=newly_available_symbols,
        current_available_symbols=sorted(current_available_symbols),
        created_at=created,
        validation_rerun_performed=should_rerun,
        force=force,
    )
    output_dir = Path(out_dir) if out_dir else root_path / "direct_validation_refresh" / created[:10]
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "validation_delta_report.md"
    report_path.write_text(render_validation_delta_report(delta), encoding="utf-8")
    delta["output_path"] = str(report_path)
    return delta


def render_validation_delta_report(delta: dict[str, Any]) -> str:
    before = delta["classification_counts_before"]
    after = delta["classification_counts_after"]
    lines = [
        "# Direct Validation Refresh Delta Report",
        "",
        f"Created: {delta['created_at']}",
        "",
        "Status: `GENERATED_ONLY`",
        "",
        "## Authority Boundary",
        "",
        "- Offline validation refresh only.",
        "- No production pipeline integration.",
        "- No replay, qualification, candidate, paper-forward, governance, or memory changes.",
        "- No trade recommendations.",
        "- No live trading, broker execution, capital allocation, position sizing, portfolio construction, or automatic paper placement.",
        "",
        "## Refresh Decision",
        "",
        f"- Previous report: `{delta['previous_report_path']}`",
        f"- Validation rerun performed: `{delta['validation_rerun_performed']}`",
        f"- Force rerun: `{delta['force']}`",
        f"- Newly available symbols: {', '.join(delta['newly_available_symbols']) or 'NONE'}",
        f"- Current schema-valid symbols: {', '.join(delta['current_available_symbols']) or 'NONE'}",
        "",
        "## Classification Delta",
        "",
        "| Classification | Before | After | Delta |",
        "| --- | ---: | ---: | ---: |",
    ]
    for classification in TRACKED_CLASSES:
        before_count = int(before.get(classification, 0))
        after_count = int(after.get(classification, 0))
        lines.append(f"| `{classification}` | {before_count} | {after_count} | {after_count - before_count:+d} |")
    lines.extend(
        [
            "",
            "Tracked summary:",
            "",
            f"- confirmed: `{after.get('CONFIRMED', 0)}`",
            f"- weakened: `{after.get('WEAKENED', 0)}`",
            f"- insufficient_data: `{after.get('INSUFFICIENT_DATA', 0)}`",
            "",
            "## Candidate Deltas",
            "",
            "| Candidate ID | Before | After | Change | Newly Available Symbols Used |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for row in delta["candidate_deltas"]:
        lines.append(
            "| `{candidate_id}` | `{before}` | `{after}` | {change} | {symbols} |".format(
                candidate_id=row["candidate_id"],
                before=row["before_classification"],
                after=row["after_classification"],
                change=row["classification_change"],
                symbols=", ".join(f"`{symbol}`" for symbol in row["newly_available_symbols_used"]) or "NONE",
            )
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- This utility compares a prior direct validation report with an offline refreshed report.",
            "- The refreshed report is not written to `direct_candidate_data_validation/latest.json`.",
            "- If no newly available symbols are detected and `--force` is not used, no validation rerun is performed and the after-state mirrors the previous report.",
            "",
        ]
    )
    return "\n".join(lines)


def _build_delta(
    *,
    previous_report: dict[str, Any],
    refreshed_report: dict[str, Any],
    previous_report_path: Path,
    newly_available_symbols: list[str],
    current_available_symbols: list[str],
    created_at: str,
    validation_rerun_performed: bool,
    force: bool,
) -> dict[str, Any]:
    before_rows = _rows_by_candidate_id(previous_report)
    after_rows = _rows_by_candidate_id(refreshed_report)
    candidate_ids = sorted(set(before_rows) | set(after_rows))
    newly_available_set = set(newly_available_symbols)
    candidate_deltas = []
    for candidate_id in candidate_ids:
        before = before_rows.get(candidate_id, {})
        after = after_rows.get(candidate_id, {})
        before_class = str(before.get("classification") or "MISSING")
        after_class = str(after.get("classification") or "MISSING")
        used_symbols = sorted(set(after.get("resolved_symbols") or after.get("candidate_symbols") or []) & newly_available_set)
        candidate_deltas.append(
            {
                "candidate_id": candidate_id,
                "before_classification": before_class,
                "after_classification": after_class,
                "classification_change": "UNCHANGED" if before_class == after_class else f"{before_class} -> {after_class}",
                "newly_available_symbols_used": used_symbols,
            }
        )
    return {
        "schema_id": "atlas_v2_research_os_direct_validation_refresh_delta",
        "schema_version": "1.0",
        "report_type": "DIRECT_VALIDATION_REFRESH_DELTA",
        "created_at": created_at,
        "previous_report_path": str(previous_report_path),
        "validation_rerun_performed": validation_rerun_performed,
        "force": force,
        "newly_available_symbols": newly_available_symbols,
        "current_available_symbols": current_available_symbols,
        "classification_counts_before": _classification_counts(previous_report),
        "classification_counts_after": _classification_counts(refreshed_report),
        "candidate_deltas": candidate_deltas,
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
    }


def _classification_counts(report: dict[str, Any]) -> dict[str, int]:
    rows = report.get("candidate_validations") or []
    counts = Counter(str(row.get("classification") or "MISSING") for row in rows)
    return {classification: int(counts.get(classification, 0)) for classification in TRACKED_CLASSES}


def _rows_by_candidate_id(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("candidate_id")): row
        for row in report.get("candidate_validations") or []
        if row.get("candidate_id")
    }


def _previous_missing_symbols(report: dict[str, Any]) -> set[str]:
    missing: set[str] = set()
    for row in report.get("candidate_validations") or []:
        symbols = {str(symbol).upper() for symbol in (row.get("resolved_symbols") or row.get("candidate_symbols") or []) if str(symbol).strip()}
        if not symbols:
            continue
        data_gap = str(row.get("data_gap") or "")
        parsed_partial_symbols = _parse_partial_missing_symbols(data_gap)
        if parsed_partial_symbols:
            missing.update(parsed_partial_symbols)
        elif not row.get("data_exists_locally"):
            missing.update(symbols)
    return missing


def _parse_partial_missing_symbols(data_gap: str) -> set[str]:
    marker = "Partial direct data only; missing local CSVs for attributed symbols:"
    if marker not in data_gap:
        return set()
    raw_symbols = data_gap.split(marker, 1)[1]
    return {part.strip().upper() for part in raw_symbols.split(",") if part.strip()}


def _schema_valid_symbols(root: Path) -> set[str]:
    discovered = {}
    for base_dir in [Path.cwd(), root]:
        for file in discover_local_market_data_files(base_dir=base_dir):
            discovered[file.path] = file
    symbols: set[str] = set()
    for file in discovered.values():
        validation = validate_market_data_schema(file)
        if validation.status == "PASS":
            symbols.add(validation.symbol.upper())
    return symbols


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _default_out_dir(root: Path) -> Path:
    return root / "direct_validation_refresh" / _now()[:10]


def main() -> int:
    parser = argparse.ArgumentParser(description="Offline direct validation refresh delta report.")
    parser.add_argument("--root", default=str(DEFAULT_STORE_ROOT), help="Atlas Research OS reports root.")
    parser.add_argument("--previous", default=None, help="Previous direct validation report JSON path.")
    parser.add_argument("--out", default=None, help="Output directory for validation_delta_report.md.")
    parser.add_argument("--created-at", default=None, help="Created-at timestamp for deterministic runs.")
    parser.add_argument("--force", action="store_true", help="Rerun validation even if no newly available symbols are detected.")
    args = parser.parse_args()

    root = Path(args.root)
    out_dir = Path(args.out) if args.out else _default_out_dir(root)
    delta = build_direct_validation_refresh(root=root, previous_path=args.previous, out_dir=out_dir, created_at=args.created_at, force=args.force)
    print(delta["output_path"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
