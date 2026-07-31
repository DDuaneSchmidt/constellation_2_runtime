from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .holdout_event_row_materializer import CANONICAL_COLUMNS

REPORT_DIRNAME = "holdout_reconstruction_from_databento"
BACKFILL_DIRNAME = "holdout_event_backfill_materializer"
GOVERNED_MANIFEST_DIRNAME = "governed_holdout_outcome_data_acquisition_manifest"
EXACT_REPLAY_DIRNAME = "exact_replay_without_fallback"
ATTRITION_DIRNAME = "direct_replay_attrition_audit"
MANUAL_INTRADAY_DIR = Path("data/manual_intraday_import")

DATABENTO_SYMBOLS = {"AAPL", "AMZN", "BAC", "IWM", "JPM", "META", "MSFT", "SPY", "TLT", "TSLA", "USO"}
RETURN_WINDOWS = ("30m", "1h", "next_session_close")
FORBIDDEN_ACTIONS = [
    "fabricate timestamps",
    "fabricate returns",
    "fabricate split dates",
    "use rows before split_date as holdout when split_date is missing",
    "allow lookahead leakage",
    "run holdout replay",
    "run holdout validation",
    "trade",
    "promote candidates",
]

RECONSTRUCTED_COLUMNS = CANONICAL_COLUMNS + [
    "reconstruction_classification",
    "resolved_symbol",
    "resolved_timestamp",
    "entry_close",
    "future_timestamp",
    "future_close",
    "databento_source_file",
]

AUDIT_COLUMNS = [
    "event_id",
    "candidate_id",
    "family_id",
    "classification",
    "resolved_symbol",
    "resolved_symbol_source",
    "resolved_timestamp",
    "resolved_timestamp_source",
    "split_date",
    "return_window",
    "return_observed",
    "entry_timestamp",
    "entry_close",
    "future_timestamp",
    "future_close",
    "databento_source_file",
    "reason",
]

UNRECONSTRUCTED_COLUMNS = [
    "event_id",
    "candidate_id",
    "family_id",
    "classification",
    "resolved_symbol",
    "resolved_symbol_source",
    "resolved_timestamp",
    "split_date",
    "missing_fields",
    "reason",
]

LOOKAHEAD_COLUMNS = [
    "event_id",
    "candidate_id",
    "family_id",
    "resolved_symbol",
    "resolved_timestamp",
    "split_date",
    "risk_classification",
    "risk_reason",
]


def run_holdout_reconstruction_from_databento(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_holdout_reconstruction_from_databento(root=root, created_at=created_at)
    write_holdout_reconstruction_from_databento(report, root=root)
    return report


def build_holdout_reconstruction_from_databento(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    rows = _read_csv(root_path / BACKFILL_DIRNAME / "backfilled_holdout_event_rows.csv")
    manifest_rows = _read_csv(root_path / GOVERNED_MANIFEST_DIRNAME / "holdout_outcome_acquisition_manifest.csv")
    exact_replay_rows = _read_csv(root_path / EXACT_REPLAY_DIRNAME / "exact_replay_results.csv")
    blocked_exact_rows = _read_csv(root_path / EXACT_REPLAY_DIRNAME / "blocked_exact_replay.csv")
    attrition_detail_rows = _read_csv(root_path / ATTRITION_DIRNAME / "candidate_stage_details.csv")
    manifest_index = _manifest_index(manifest_rows)
    available_files = _available_databento_files(MANUAL_INTRADAY_DIR)
    bar_cache: dict[str, dict[str, dict[str, str]]] = {}

    reconstructed: list[dict[str, str]] = []
    unreconstructed: list[dict[str, str]] = []
    audit: list[dict[str, str]] = []
    lookahead: list[dict[str, str]] = []

    for row in rows:
        resolved_symbol, symbol_source = _resolve_symbol(row, manifest_index)
        resolved_timestamp, timestamp_source = _resolve_timestamp(row)
        split_date = _clean(row.get("split_date"))
        classification = ""
        reason = ""
        computed: dict[str, str] = {}

        if not resolved_symbol:
            classification = "RECONSTRUCTED_MISSING_SYMBOL"
            reason = "No row or governed-manifest symbol resolves to the Databento governed symbol set."
        elif resolved_timestamp is None:
            classification = "RECONSTRUCTED_MISSING_TIMESTAMP"
            reason = "No event timestamp/date is present; reconstruction would require fabricating timestamp evidence."
        elif not split_date:
            classification = "RECONSTRUCTED_BLOCKED_SPLIT_DATE"
            reason = "split_date is missing; return computation is blocked by holdout split discipline."
        elif _parse_timestamp(split_date) and resolved_timestamp <= _parse_timestamp(split_date):  # type: ignore[operator]
            classification = "RECONSTRUCTED_LOOKAHEAD_RISK"
            reason = "Event timestamp is not strictly after split_date."
        elif resolved_symbol not in available_files:
            classification = "RECONSTRUCTION_NOT_POSSIBLE"
            reason = "Resolved symbol is governed but no 1-minute Databento CSV is available locally."
        else:
            bars = bar_cache.setdefault(resolved_symbol, _load_bars(available_files[resolved_symbol]))
            computed = _compute_first_available_return(bars, resolved_timestamp)
            if not computed:
                classification = "RECONSTRUCTED_MISSING_FUTURE_BAR"
                reason = "No deterministic future return window has both entry and future bars."
            else:
                computed["source_file"] = str(available_files[resolved_symbol])
                classification = "RECONSTRUCTED_COMPLETE"
                reason = "Source-backed symbol, timestamp, split_date, entry bar, and future bar are present."

        output_row = {field: _clean(row.get(field)) for field in CANONICAL_COLUMNS}
        if classification == "RECONSTRUCTED_COMPLETE":
            output_row["symbol"] = resolved_symbol
            output_row["timestamp"] = _format_ts(resolved_timestamp) if resolved_timestamp else output_row["timestamp"]
            output_row["date"] = output_row["date"] or output_row["timestamp"][:10]
            output_row["return_window"] = computed["return_window"]
            output_row["return_observed"] = computed["return_observed"]
            output_row["data_source"] = computed["source_file"]
            output_row["created_by"] = "build_113_holdout_reconstruction_from_databento"
            output_row["evidence_status"] = "RECONSTRUCTED_COMPLETE"
            output_row["notes"] = _append_note(output_row.get("notes", ""), "Databento exact OHLCV return reconstructed without holdout replay.")

        reconstructed_row = {
            **output_row,
            "reconstruction_classification": classification,
            "resolved_symbol": resolved_symbol,
            "resolved_timestamp": _format_ts(resolved_timestamp) if resolved_timestamp else "",
            "entry_close": computed.get("entry_close", ""),
            "future_timestamp": computed.get("future_timestamp", ""),
            "future_close": computed.get("future_close", ""),
            "databento_source_file": computed.get("source_file", str(available_files.get(resolved_symbol, ""))),
        }
        reconstructed.append(reconstructed_row)

        audit_row = {
            "event_id": _clean(row.get("event_id")),
            "candidate_id": _clean(row.get("candidate_id")),
            "family_id": _clean(row.get("family_id")),
            "classification": classification,
            "resolved_symbol": resolved_symbol,
            "resolved_symbol_source": symbol_source,
            "resolved_timestamp": _format_ts(resolved_timestamp) if resolved_timestamp else "",
            "resolved_timestamp_source": timestamp_source,
            "split_date": split_date,
            "return_window": computed.get("return_window", ""),
            "return_observed": computed.get("return_observed", ""),
            "entry_timestamp": computed.get("entry_timestamp", ""),
            "entry_close": computed.get("entry_close", ""),
            "future_timestamp": computed.get("future_timestamp", ""),
            "future_close": computed.get("future_close", ""),
            "databento_source_file": computed.get("source_file", str(available_files.get(resolved_symbol, ""))),
            "reason": reason,
        }
        audit.append(audit_row)
        if classification != "RECONSTRUCTED_COMPLETE":
            unreconstructed.append(
                {
                    "event_id": audit_row["event_id"],
                    "candidate_id": audit_row["candidate_id"],
                    "family_id": audit_row["family_id"],
                    "classification": classification,
                    "resolved_symbol": resolved_symbol,
                    "resolved_symbol_source": symbol_source,
                    "resolved_timestamp": audit_row["resolved_timestamp"],
                    "split_date": split_date,
                    "missing_fields": _missing_fields(row, resolved_symbol, resolved_timestamp, split_date),
                    "reason": reason,
                }
            )
        if classification in {"RECONSTRUCTED_LOOKAHEAD_RISK", "RECONSTRUCTED_BLOCKED_SPLIT_DATE"} or (resolved_timestamp and split_date and _parse_timestamp(split_date) and resolved_timestamp <= _parse_timestamp(split_date)):  # type: ignore[operator]
            lookahead.append(
                {
                    "event_id": audit_row["event_id"],
                    "candidate_id": audit_row["candidate_id"],
                    "family_id": audit_row["family_id"],
                    "resolved_symbol": resolved_symbol,
                    "resolved_timestamp": audit_row["resolved_timestamp"],
                    "split_date": split_date,
                    "risk_classification": classification if classification == "RECONSTRUCTED_LOOKAHEAD_RISK" else "RECONSTRUCTED_BLOCKED_SPLIT_DATE",
                    "risk_reason": reason,
                }
            )

    counts = Counter(row["classification"] for row in audit)
    summary = {
        "rows_reviewed": len(rows),
        "rows_reconstructed_complete": counts.get("RECONSTRUCTED_COMPLETE", 0),
        "rows_still_blocked": len(rows) - counts.get("RECONSTRUCTED_COMPLETE", 0),
        "returns_computed": counts.get("RECONSTRUCTED_COMPLETE", 0),
        "split_date_blockers": sum(1 for row in audit if not row["split_date"]),
        "lookahead_risk_blockers": counts.get("RECONSTRUCTED_LOOKAHEAD_RISK", 0),
        "holdout_replay_now_possible": False,
        "holdout_replay_now_possible_reason": "Holdout replay remains blocked until every required row has source-backed timestamp/date, symbol, return_observed, return_window, and split_date.",
        "recommended_next_build": "Import governed event timestamps and split_date metadata keyed to event_id/candidate_id, then rerun this reconstruction before any holdout replay.",
        "classification_counts": dict(sorted(counts.items())),
        "source_input_rows": {
            "holdout_event_backfill_materializer_rows": len(rows),
            "governed_manifest_rows": len(manifest_rows),
            "exact_replay_result_rows": len(exact_replay_rows),
            "blocked_exact_replay_rows": len(blocked_exact_rows),
            "direct_replay_attrition_detail_rows": len(attrition_detail_rows),
        },
    }
    return {
        "schema_id": "atlas_v2_research_os_holdout_reconstruction_from_databento",
        "schema_version": "1.0",
        "build": "113",
        "report_type": "HOLDOUT_RECONSTRUCTION_FROM_DATABENTO",
        "created_at": created,
        "day": created[:10],
        "source_inputs": {
            "manual_intraday_import": str(MANUAL_INTRADAY_DIR),
            "holdout_event_backfill_materializer": str(root_path / BACKFILL_DIRNAME),
            "governed_holdout_outcome_data_acquisition_manifest": str(root_path / GOVERNED_MANIFEST_DIRNAME),
            "exact_replay_without_fallback": str(root_path / EXACT_REPLAY_DIRNAME),
            "direct_replay_attrition_audit": str(root_path / ATTRITION_DIRNAME),
        },
        "databento_governed_symbols": sorted(DATABENTO_SYMBOLS),
        "deterministic_return_windows": list(RETURN_WINDOWS),
        "summary": summary,
        "reconstructed_holdout_event_rows": reconstructed,
        "reconstruction_audit_trail": audit,
        "unreconstructed_rows": unreconstructed,
        "lookahead_risk_report": lookahead,
        "authority_boundary": {
            "research_only": True,
            "holdout_replay_run": False,
            "holdout_validation_run": False,
            "trading_authority": False,
            "promotion_authority": False,
            "confidence_impact": "NONE",
            "forbidden_actions": FORBIDDEN_ACTIONS,
        },
    }


def write_holdout_reconstruction_from_databento(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "reconstructed_rows": out_dir / "reconstructed_holdout_event_rows.csv",
        "audit_trail": out_dir / "reconstruction_audit_trail.csv",
        "unreconstructed_rows": out_dir / "unreconstructed_rows.csv",
        "lookahead_risk_report": out_dir / "lookahead_risk_report.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_holdout_reconstruction_from_databento_summary(report), encoding="utf-8")
    _write_csv(paths["reconstructed_rows"], RECONSTRUCTED_COLUMNS, report.get("reconstructed_holdout_event_rows") or [])
    _write_csv(paths["audit_trail"], AUDIT_COLUMNS, report.get("reconstruction_audit_trail") or [])
    _write_csv(paths["unreconstructed_rows"], UNRECONSTRUCTED_COLUMNS, report.get("unreconstructed_rows") or [])
    _write_csv(paths["lookahead_risk_report"], LOOKAHEAD_COLUMNS, report.get("lookahead_risk_report") or [])
    return paths


def render_holdout_reconstruction_from_databento_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    counts = summary.get("classification_counts", {})
    lines = [
        "# Build 113 - Holdout Reconstruction From Databento",
        "",
        f"Rows reviewed: {summary.get('rows_reviewed')}",
        f"Rows reconstructed complete: {summary.get('rows_reconstructed_complete')}",
        f"Rows still blocked: {summary.get('rows_still_blocked')}",
        f"Returns computed: {summary.get('returns_computed')}",
        f"Split-date blockers: {summary.get('split_date_blockers')}",
        f"Lookahead-risk blockers: {summary.get('lookahead_risk_blockers')}",
        f"Holdout replay now possible: {summary.get('holdout_replay_now_possible')}",
        "",
        "## Classifications",
        "",
    ]
    lines.extend(f"- {key}: {value}" for key, value in counts.items())
    lines.extend(
        [
            "",
            "## Recommendation",
            "",
            str(summary.get("recommended_next_build")),
            "",
            "Research-only output. No holdout replay, trading, promotion, or confidence increase was run or authorized.",
            "",
        ]
    )
    return "\n".join(lines)


def _manifest_index(rows: list[dict[str, str]]) -> dict[tuple[str, str], dict[str, str]]:
    index: dict[tuple[str, str], dict[str, str]] = {}
    for row in rows:
        candidate_id = _clean(row.get("candidate_id"))
        family_id = _clean(row.get("family_id"))
        if candidate_id:
            index[("candidate", candidate_id)] = row
        if family_id:
            index[("family", family_id)] = row
    return index


def _resolve_symbol(row: dict[str, str], manifest_index: dict[tuple[str, str], dict[str, str]]) -> tuple[str, str]:
    row_symbol = _clean(row.get("symbol")).upper()
    if row_symbol in DATABENTO_SYMBOLS:
        return row_symbol, "holdout_event_backfill_materializer.symbol"
    for key in (("candidate", _clean(row.get("candidate_id"))), ("family", _clean(row.get("family_id")))):
        manifest = manifest_index.get(key)
        required_symbol = _clean(manifest.get("required_symbol") if manifest else "").upper()
        if required_symbol in DATABENTO_SYMBOLS:
            return required_symbol, f"governed_manifest.required_symbol.{key[0]}"
    return "", "none"


def _resolve_timestamp(row: dict[str, str]) -> tuple[datetime | None, str]:
    timestamp = _parse_timestamp(_clean(row.get("timestamp")))
    if timestamp:
        return timestamp, "holdout_event_backfill_materializer.timestamp"
    date_value = _clean(row.get("date"))
    if "T" in date_value:
        parsed = _parse_timestamp(date_value)
        if parsed:
            return parsed, "holdout_event_backfill_materializer.date_datetime"
    return None, "none"


def _available_databento_files(data_dir: Path) -> dict[str, Path]:
    found: dict[str, Path] = {}
    for symbol in DATABENTO_SYMBOLS:
        path = data_dir / f"{symbol}_1m.csv"
        if path.exists():
            found[symbol] = path
    return found


def _load_bars(path: Path) -> dict[str, dict[str, str]]:
    rows = _read_csv(path)
    return {_format_ts(_parse_timestamp(row.get("timestamp", ""))): row for row in rows if _parse_timestamp(row.get("timestamp", ""))}


def _compute_first_available_return(bars: dict[str, dict[str, str]], event_ts: datetime) -> dict[str, str]:
    entry_key = _format_ts(event_ts)
    entry = bars.get(entry_key)
    if not entry:
        return {}
    for window in RETURN_WINDOWS:
        future_ts = _future_timestamp(window, event_ts, bars)
        if not future_ts:
            continue
        future = bars.get(_format_ts(future_ts))
        if not future:
            continue
        entry_close = float(entry["close"])
        future_close = float(future["close"])
        if entry_close == 0:
            continue
        return {
            "return_window": window,
            "return_observed": f"{(future_close / entry_close) - 1:.10f}",
            "entry_timestamp": entry_key,
            "entry_close": f"{entry_close:.10f}",
            "future_timestamp": _format_ts(future_ts),
            "future_close": f"{future_close:.10f}",
            "source_file": "",
        }
    return {}


def _future_timestamp(window: str, event_ts: datetime, bars: dict[str, dict[str, str]]) -> datetime | None:
    if window == "30m":
        return event_ts + timedelta(minutes=30)
    if window == "1h":
        return event_ts + timedelta(hours=1)
    dates = sorted({_parse_timestamp(key).date() for key in bars if _parse_timestamp(key)})
    next_dates = [day for day in dates if day > event_ts.date()]
    if not next_dates:
        return None
    next_day = next_dates[0]
    candidates = [_parse_timestamp(key) for key in bars if _parse_timestamp(key) and _parse_timestamp(key).date() == next_day]
    candidates = [candidate for candidate in candidates if candidate]
    return max(candidates) if candidates else None


def _missing_fields(row: dict[str, str], symbol: str, timestamp: datetime | None, split_date: str) -> str:
    missing = []
    if not symbol:
        missing.append("symbol")
    if not timestamp:
        missing.extend(["timestamp", "date"])
    if not split_date:
        missing.append("split_date")
    if not _clean(row.get("return_observed")):
        missing.append("return_observed")
    if not _clean(row.get("return_window")):
        missing.append("return_window")
    return ",".join(dict.fromkeys(missing))


def _append_note(existing: str, note: str) -> str:
    return f"{existing}; {note}" if existing else note


def _parse_timestamp(value: str | None) -> datetime | None:
    text = _clean(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.fromisoformat(f"{text}T00:00:00+00:00")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _format_ts(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
