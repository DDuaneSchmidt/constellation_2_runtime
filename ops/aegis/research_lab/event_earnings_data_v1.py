from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from statistics import mean, median
from typing import Any

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1


EARNINGS_CALENDAR_FAMILY = "earnings_event_calendar_v1"
EVENT_WINDOW_FAMILY = "research_event_window_dataset_v1"
FORWARD_RETURN_FAMILY = "forward_return_event_study_v1"
EVENT_DATA_STAGING_ROOT = Path("/home/node/constellation_runtime_data/research_data/manual_drop")
DEFAULT_EVENT_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth")

EARNINGS_CALENDAR_ACCEPTED_FORMAT = (
    "CSV with columns: symbol,event_date,event_time,timing,event_type,source. "
    "Optional columns: eps_actual,eps_estimate,revenue_actual,revenue_estimate,guidance_flag,notes."
)
EVENT_WINDOW_ACCEPTED_FORMAT = "CSV with columns: symbol,date,open,high,low,close,volume."
TIMING_VALUES = {"PRE_MARKET", "AFTER_HOURS", "DURING_MARKET", "UNKNOWN"}
REQUIRED_CALENDAR_COLUMNS = ["symbol", "event_date", "event_time", "timing", "event_type", "source"]
OPTIONAL_CALENDAR_COLUMNS = [
    "eps_actual",
    "eps_estimate",
    "revenue_actual",
    "revenue_estimate",
    "guidance_flag",
    "notes",
]
REQUIRED_OHLCV_COLUMNS = ["symbol", "date", "open", "high", "low", "close", "volume"]
NVIDIA_HYPOTHESIS_ID = "rh-nvidia-earnings-event-dislocation-v1"


def event_upload_format_v1() -> dict[str, Any]:
    return {
        "earnings_event_calendar_v1": {
            "accepted_format": EARNINGS_CALENDAR_ACCEPTED_FORMAT,
            "required_columns": REQUIRED_CALENDAR_COLUMNS,
            "optional_columns": OPTIONAL_CALENDAR_COLUMNS,
        },
        "research_event_window_dataset_v1": {
            "accepted_format": EVENT_WINDOW_ACCEPTED_FORMAT,
            "required_columns": REQUIRED_OHLCV_COLUMNS,
        },
    }


def import_earnings_event_calendar_csv_v1(
    *,
    truth_root: Path,
    day_utc: str,
    input_path: Path,
    hypothesis_id: str = NVIDIA_HYPOTHESIS_ID,
    expected_symbols: list[str] | None = None,
    source_label: str = "OPERATOR_UPLOAD",
) -> dict[str, Any]:
    path = Path(input_path).expanduser().resolve()
    raw = path.read_text(encoding="utf-8")
    source_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    rows = list(csv.DictReader(StringIO(raw)))
    events: list[dict[str, Any]] = []
    errors: list[str] = []
    source_timestamp = _file_timestamp(path)
    expected = {str(item).upper() for item in (expected_symbols or ["NVDA"]) if str(item).strip()}
    headers = set(rows[0].keys()) if rows else set()
    missing_columns = [column for column in REQUIRED_CALENDAR_COLUMNS if column not in headers]
    if missing_columns:
        errors.append(f"missing_required_columns:{','.join(missing_columns)}")
    for index, row in enumerate(rows, start=1):
        symbol = str(row.get("symbol") or "").strip().upper()
        timing = str(row.get("timing") or "UNKNOWN").strip().upper()
        event_date = str(row.get("event_date") or "").strip()[:10]
        event_time = str(row.get("event_time") or "").strip()
        event_type = str(row.get("event_type") or "").strip()
        source = str(row.get("source") or source_label).strip()
        row_errors: list[str] = []
        if not symbol:
            row_errors.append("symbol_missing")
        if expected and symbol not in expected:
            row_errors.append("unexpected_symbol")
        if not _valid_yyyy_mm_dd(event_date):
            row_errors.append("event_date_invalid")
        if timing not in TIMING_VALUES:
            row_errors.append("timing_invalid")
        if not event_type:
            row_errors.append("event_type_missing")
        if not source:
            row_errors.append("source_missing")
        validation_status = "VALIDATED" if not row_errors else "INVALID"
        errors.extend(f"row_{index}:{error}" for error in row_errors)
        event = {
            "symbol": symbol,
            "event_date": event_date,
            "event_time": event_time,
            "timing": timing if timing in TIMING_VALUES else "UNKNOWN",
            "event_type": event_type,
            "source": source,
            "source_timestamp": source_timestamp,
            "source_hash": source_hash,
            "validation_status": validation_status,
        }
        for column in OPTIONAL_CALENDAR_COLUMNS:
            value = str(row.get(column) or "").strip()
            if value:
                event[column] = value
        events.append(event)
    payload = _base_payload(
        schema_id="earnings_event_calendar_v1",
        artifact_id="earnings_event_calendar_v1",
        day_utc=day_utc,
        hypothesis_id=hypothesis_id,
    )
    payload.update(
        {
            "status": "AVAILABLE" if events and not errors else "INVALID",
            "validation_status": "VALIDATED" if events and not errors else "INVALID",
            "event_count": len(events),
            "symbols": sorted({event["symbol"] for event in events if event.get("symbol")}),
            "events": sorted(events, key=lambda event: (str(event.get("event_date") or ""), str(event.get("symbol") or ""))),
            "source": source_label,
            "source_path": str(path),
            "source_timestamp": source_timestamp,
            "source_hash": source_hash,
            "accepted_upload_format": event_upload_format_v1()["earnings_event_calendar_v1"],
            "validation_errors": errors,
        }
    )
    out = _artifact_path(truth_root, EARNINGS_CALENDAR_FAMILY, day_utc, "earnings_event_calendar.v1.json", hypothesis_id)
    write_json_v1(out, payload)
    payload["artifact_path"] = str(out)
    payload["artifact_hash"] = _sha256_json(payload)
    return payload


def import_event_window_ohlcv_csv_v1(
    *,
    truth_root: Path,
    day_utc: str,
    input_path: Path,
    hypothesis_id: str = NVIDIA_HYPOTHESIS_ID,
    source_label: str = "OPERATOR_UPLOAD",
) -> dict[str, Any]:
    calendar_path, calendar = latest_earnings_event_calendar_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        hypothesis_id=hypothesis_id,
    )
    if not calendar_path or str(calendar.get("validation_status") or "").upper() != "VALIDATED":
        return write_missing_event_dataset_artifact_v1(
            truth_root=truth_root,
            day_utc=day_utc,
            hypothesis_id=hypothesis_id,
            missing_kind="earnings_event_calendar",
        )
    path = Path(input_path).expanduser().resolve()
    raw = path.read_text(encoding="utf-8")
    source_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    rows = list(csv.DictReader(StringIO(raw)))
    headers = set(rows[0].keys()) if rows else set()
    errors = [f"missing_required_columns:{','.join([column for column in REQUIRED_OHLCV_COLUMNS if column not in headers])}"] if rows and not set(REQUIRED_OHLCV_COLUMNS).issubset(headers) else []
    normalized_rows, row_errors = _normalize_ohlcv_rows(rows)
    errors.extend(row_errors)
    event_windows = _build_event_windows(calendar.get("events") or [], normalized_rows)
    payload = _base_payload(
        schema_id="research_event_window_dataset_v1",
        artifact_id="research_event_window_dataset_v1",
        day_utc=day_utc,
        hypothesis_id=hypothesis_id,
    )
    payload.update(
        {
            "status": "AVAILABLE" if event_windows and not errors else "INVALID",
            "validation_status": "VALIDATED" if event_windows and not errors else "INVALID",
            "source": source_label,
            "source_path": str(path),
            "source_timestamp": _file_timestamp(path),
            "source_hash": source_hash,
            "calendar_artifact_path": str(calendar_path),
            "calendar_source_hash": str(calendar.get("source_hash") or ""),
            "symbols": sorted({row["symbol"] for row in normalized_rows}),
            "event_count": len(calendar.get("events") or []),
            "sample_size": len([row for row in event_windows if row.get("symbol") == "NVDA"]),
            "event_windows": event_windows,
            "accepted_upload_format": event_upload_format_v1()["research_event_window_dataset_v1"],
            "validation_errors": errors,
            "lookahead_policy": "Forward returns are measured from the reaction close to later closes only; event timing determines reaction day.",
        }
    )
    out = _artifact_path(truth_root, EVENT_WINDOW_FAMILY, day_utc, "research_event_window_dataset.v1.json", hypothesis_id)
    write_json_v1(out, payload)
    payload["artifact_path"] = str(out)
    payload["artifact_hash"] = _sha256_json(payload)
    return payload


def maybe_import_staged_event_data_v1(*, truth_root: Path, day_utc: str, hypothesis_id: str = NVIDIA_HYPOTHESIS_ID) -> dict[str, Any]:
    resolved_truth_root = Path(truth_root).expanduser().resolve()
    if resolved_truth_root != DEFAULT_EVENT_TRUTH_ROOT.resolve():
        return {"staging_root": str(EVENT_DATA_STAGING_ROOT), "imports": [], "skipped": "global_file_drop_applies_only_to_runtime_truth_root"}
    staged_calendar = EVENT_DATA_STAGING_ROOT / "earnings_event_calendar.csv"
    staged_ohlcv = EVENT_DATA_STAGING_ROOT / "event_window_ohlcv.csv"
    imported: dict[str, Any] = {"staging_root": str(EVENT_DATA_STAGING_ROOT), "imports": []}
    if staged_calendar.exists():
        imported["imports"].append(
            import_earnings_event_calendar_csv_v1(
                truth_root=truth_root,
                day_utc=day_utc,
                input_path=staged_calendar,
                hypothesis_id=hypothesis_id,
                expected_symbols=["NVDA"],
                source_label="FILE_DROP_STAGING",
            )
        )
    if staged_ohlcv.exists():
        imported["imports"].append(
            import_event_window_ohlcv_csv_v1(
                truth_root=truth_root,
                day_utc=day_utc,
                input_path=staged_ohlcv,
                hypothesis_id=hypothesis_id,
                source_label="FILE_DROP_STAGING",
            )
        )
    return imported


def write_missing_event_dataset_artifact_v1(
    *,
    truth_root: Path,
    day_utc: str,
    hypothesis_id: str = NVIDIA_HYPOTHESIS_ID,
    missing_kind: str = "earnings_event_calendar",
) -> dict[str, Any]:
    missing_kind = str(missing_kind or "earnings_event_calendar")
    if missing_kind == "earnings_event_calendar":
        family = EARNINGS_CALENDAR_FAMILY
        filename = "missing_earnings_event_calendar.v1.json"
        schema_id = "earnings_event_calendar_v1"
        title = "External earnings calendar required"
        missing_items = ["NVDA historical earnings dates/times"]
        accepted = event_upload_format_v1()["earnings_event_calendar_v1"]
    else:
        family = EVENT_WINDOW_FAMILY
        filename = "missing_research_event_window_dataset.v1.json"
        schema_id = "research_event_window_dataset_v1"
        title = "Event-window OHLCV dataset required"
        missing_items = ["NVDA, SMH, SOXX, QQQ, XLK, SPY, and VIX event-window OHLCV"]
        accepted = event_upload_format_v1()["research_event_window_dataset_v1"]
    payload = _base_payload(schema_id=schema_id, artifact_id=f"missing_{schema_id}", day_utc=day_utc, hypothesis_id=hypothesis_id)
    payload.update(
        {
            "status": "MISSING",
            "validation_status": "MISSING",
            "title": title,
            "missing_items": missing_items,
            "accepted_upload_format": accepted,
            "next_action": "Place an approved CSV in /home/node/constellation_runtime_data/research_data/manual_drop or import it through the governed event-data import tool.",
            "source": "MISSING_DATASET_DECLARATION",
            "source_timestamp": _now(),
            "source_hash": "",
        }
    )
    out = _artifact_path(truth_root, family, day_utc, filename, hypothesis_id)
    write_json_v1(out, payload)
    payload["artifact_path"] = str(out)
    return payload


def event_earnings_data_status_v1(
    *,
    truth_root: Path,
    day_utc: str,
    hypothesis_id: str = NVIDIA_HYPOTHESIS_ID,
    write_missing: bool = False,
) -> dict[str, Any]:
    calendar_path, calendar = latest_earnings_event_calendar_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        hypothesis_id=hypothesis_id,
    )
    if not calendar_path or str(calendar.get("validation_status") or "").upper() != "VALIDATED":
        missing_path, missing_payload = _latest_artifact(truth_root, EARNINGS_CALENDAR_FAMILY, day_utc, hypothesis_id, "missing_earnings_event_calendar.v1.json")
        missing = write_missing_event_dataset_artifact_v1(
            truth_root=truth_root,
            day_utc=day_utc,
            hypothesis_id=hypothesis_id,
            missing_kind="earnings_event_calendar",
        ) if write_missing else ({"artifact_path": str(missing_path)} if missing_path else missing_payload or {})
        return {
            "status": "DATA_NEEDED",
            "event_data_status": "MISSING_EARNINGS_EVENT_CALENDAR",
            "blocker": "EARNINGS_EVENT_CALENDAR_REQUIRED",
            "summary": "External earnings calendar required.",
            "operator_status": "External earnings calendar required",
            "calendar_available": False,
            "event_window_available": False,
            "forward_return_study_available": False,
            "sample_size": 0,
            "event_count": 0,
            "missing_items": ["NVDA historical earnings dates/times"],
            "accepted_upload_format": event_upload_format_v1()["earnings_event_calendar_v1"],
            "calendar_artifact_path": str(calendar_path or missing.get("artifact_path") or ""),
            "event_window_artifact_path": "",
            "forward_return_artifact_path": "",
        }
    window_path, window = latest_event_window_dataset_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        hypothesis_id=hypothesis_id,
    )
    if not window_path or str(window.get("validation_status") or "").upper() != "VALIDATED":
        missing_path, missing_payload = _latest_artifact(truth_root, EVENT_WINDOW_FAMILY, day_utc, hypothesis_id, "missing_research_event_window_dataset.v1.json")
        missing = write_missing_event_dataset_artifact_v1(
            truth_root=truth_root,
            day_utc=day_utc,
            hypothesis_id=hypothesis_id,
            missing_kind="research_event_window_dataset",
        ) if write_missing else ({"artifact_path": str(missing_path)} if missing_path else missing_payload or {})
        return {
            "status": "DATA_NEEDED",
            "event_data_status": "MISSING_EVENT_WINDOW_OHLCV",
            "blocker": "EVENT_WINDOW_OHLCV_DATA_REQUIRED",
            "summary": "Event-window OHLCV dataset required.",
            "operator_status": "Event-window OHLCV dataset required",
            "calendar_available": True,
            "event_window_available": False,
            "forward_return_study_available": False,
            "sample_size": 0,
            "event_count": int(calendar.get("event_count") or 0),
            "missing_items": ["NVDA, SMH, SOXX, QQQ, XLK, SPY, and VIX event-window OHLCV"],
            "accepted_upload_format": event_upload_format_v1()["research_event_window_dataset_v1"],
            "calendar_artifact_path": str(calendar_path),
            "event_window_artifact_path": str(window_path or missing.get("artifact_path") or ""),
            "forward_return_artifact_path": "",
        }
    study_path, study = latest_forward_return_event_study_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        hypothesis_id=hypothesis_id,
    )
    return {
        "status": "AVAILABLE",
        "event_data_status": "GOVERNED_EVENT_DATA_AVAILABLE",
        "blocker": "",
        "summary": "Governed earnings calendar and event-window OHLCV are available.",
        "operator_status": "Governed event data available",
        "calendar_available": True,
        "event_window_available": True,
        "forward_return_study_available": bool(study_path),
        "sample_size": int(window.get("sample_size") or study.get("sample_size") or 0),
        "event_count": int(calendar.get("event_count") or window.get("event_count") or 0),
        "missing_items": [],
        "accepted_upload_format": event_upload_format_v1(),
        "calendar_artifact_path": str(calendar_path),
        "event_window_artifact_path": str(window_path),
        "forward_return_artifact_path": str(study_path or ""),
        "calendar_source_hash": str(calendar.get("source_hash") or ""),
        "event_window_source_hash": str(window.get("source_hash") or ""),
    }


def run_forward_return_event_study_v1(
    *,
    truth_root: Path,
    day_utc: str,
    hypothesis_id: str = NVIDIA_HYPOTHESIS_ID,
) -> dict[str, Any]:
    status = event_earnings_data_status_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        hypothesis_id=hypothesis_id,
        write_missing=True,
    )
    if status["status"] != "AVAILABLE":
        return status
    window_path, window = latest_event_window_dataset_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        hypothesis_id=hypothesis_id,
    )
    rows = window.get("event_windows") if isinstance(window.get("event_windows"), list) else []
    nvda_rows = [row for row in rows if str(row.get("symbol") or "").upper() == "NVDA"]
    metrics = _event_study_metrics(rows)
    payload = _base_payload(
        schema_id="forward_return_event_study_v1",
        artifact_id="forward_return_event_study_v1",
        day_utc=day_utc,
        hypothesis_id=hypothesis_id,
    )
    payload.update(
        {
            "status": "RESULT_READY" if len(nvda_rows) >= 20 else "INCONCLUSIVE_SAMPLE_SIZE",
            "validation_status": "VALIDATED",
            "sample_size": len(nvda_rows),
            "minimum_sample_size": 20,
            "event_count": len(nvda_rows),
            "source": "GOVERNED_EVENT_WINDOW_DATASET",
            "source_timestamp": _now(),
            "source_hash": str(window.get("source_hash") or _sha256_json(window)),
            "event_window_artifact_path": str(window_path or ""),
            "metrics": metrics,
            "costs_slippage": {
                "status": "PLACEHOLDER_GOVERNED_COST_MODEL_REQUIRED",
                "applied": False,
                "note": "Costs and slippage are disclosed as a placeholder only; no pass/fail or trade action is authorized from this result.",
            },
            "lookahead_policy": "No lookahead leakage: event timing determines reaction day; forward returns use closes after the reaction close only.",
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "automatic_sleeve_mutation_allowed": False,
            "trade_advice_allowed": False,
        }
    )
    out = _artifact_path(truth_root, FORWARD_RETURN_FAMILY, day_utc, "forward_return_event_study.v1.json", hypothesis_id)
    write_json_v1(out, payload)
    payload["artifact_path"] = str(out)
    payload["artifact_hash"] = _sha256_json(payload)
    return payload


def latest_earnings_event_calendar_v1(*, truth_root: Path, day_utc: str, hypothesis_id: str) -> tuple[Path | None, dict[str, Any]]:
    return _latest_artifact(truth_root, EARNINGS_CALENDAR_FAMILY, day_utc, hypothesis_id, "earnings_event_calendar.v1.json")


def latest_event_window_dataset_v1(*, truth_root: Path, day_utc: str, hypothesis_id: str) -> tuple[Path | None, dict[str, Any]]:
    return _latest_artifact(truth_root, EVENT_WINDOW_FAMILY, day_utc, hypothesis_id, "research_event_window_dataset.v1.json")


def latest_forward_return_event_study_v1(*, truth_root: Path, day_utc: str, hypothesis_id: str) -> tuple[Path | None, dict[str, Any]]:
    return _latest_artifact(truth_root, FORWARD_RETURN_FAMILY, day_utc, hypothesis_id, "forward_return_event_study.v1.json")


def _latest_artifact(truth_root: Path, family: str, day_utc: str, hypothesis_id: str, filename: str) -> tuple[Path | None, dict[str, Any]]:
    base = Path(truth_root).expanduser().resolve() / "reports" / family
    roots = [base / day_utc / hypothesis_id, base / day_utc, base]
    candidates: list[Path] = []
    for root in roots:
        if root.exists():
            candidates.extend(sorted(root.rglob(filename)))
    if not candidates:
        return None, {}
    candidates = sorted(set(candidates), key=lambda path: path.stat().st_mtime if path.exists() else 0, reverse=True)
    return candidates[0], read_json_v1(candidates[0])


def _artifact_path(truth_root: Path, family: str, day_utc: str, filename: str, hypothesis_id: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / family / day_utc / hypothesis_id / filename


def _base_payload(*, schema_id: str, artifact_id: str, day_utc: str, hypothesis_id: str) -> dict[str, Any]:
    return {
        "schema_id": schema_id,
        "schema_version": "v1",
        "artifact_id": artifact_id,
        "generated_at_utc": _now(),
        "day_utc": day_utc,
        "hypothesis_id": hypothesis_id,
        "human_approval_required": True,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "automatic_sleeve_mutation_allowed": False,
        "trade_advice_allowed": False,
        "fabricated_data": False,
    }


def _normalize_ohlcv_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    out: list[dict[str, Any]] = []
    errors: list[str] = []
    for index, row in enumerate(rows, start=1):
        symbol = str(row.get("symbol") or "").strip().upper()
        date = str(row.get("date") or "").strip()[:10]
        if not symbol or not _valid_yyyy_mm_dd(date):
            errors.append(f"row_{index}:symbol_or_date_invalid")
            continue
        normalized: dict[str, Any] = {"symbol": symbol, "date": date}
        for column in ["open", "high", "low", "close", "volume"]:
            value = _float_or_none(row.get(column))
            if value is None:
                errors.append(f"row_{index}:{column}_invalid")
            normalized[column] = value
        if all(normalized.get(column) is not None for column in ["open", "high", "low", "close", "volume"]):
            out.append(normalized)
    out.sort(key=lambda row: (row["symbol"], row["date"]))
    return out, errors


def _build_event_windows(events: list[dict[str, Any]], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_symbol: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_symbol.setdefault(str(row["symbol"]), []).append(row)
    for symbol_rows in by_symbol.values():
        symbol_rows.sort(key=lambda row: row["date"])
    windows: list[dict[str, Any]] = []
    symbols = sorted(by_symbol)
    for event in events:
        event_date = str(event.get("event_date") or "")[:10]
        timing = str(event.get("timing") or "UNKNOWN").upper()
        for symbol in symbols:
            symbol_rows = by_symbol.get(symbol) or []
            reaction_idx = _reaction_index(symbol_rows, event_date, timing)
            if reaction_idx is None or reaction_idx == 0:
                continue
            reaction = symbol_rows[reaction_idx]
            previous = symbol_rows[reaction_idx - 1]
            window = {
                "symbol": symbol,
                "event_symbol": str(event.get("symbol") or "").upper(),
                "event_date": event_date,
                "event_time": str(event.get("event_time") or ""),
                "timing": timing,
                "event_type": str(event.get("event_type") or ""),
                "reaction_date": reaction["date"],
                "previous_date": previous["date"],
                "event_day_gap": _return(reaction.get("open"), previous.get("close")),
                "same_day_return": _return(reaction.get("close"), reaction.get("open")),
                "forward_return_1d": _forward_return(symbol_rows, reaction_idx, 1),
                "forward_return_3d": _forward_return(symbol_rows, reaction_idx, 3),
                "forward_return_5d": _forward_return(symbol_rows, reaction_idx, 5),
                "volume_surprise": _volume_surprise(symbol_rows, reaction_idx),
                "source_hash": str(event.get("source_hash") or ""),
                "validation_status": "VALIDATED",
            }
            windows.append(window)
    windows.sort(key=lambda row: (row["event_date"], row["symbol"]))
    return windows


def _reaction_index(rows: list[dict[str, Any]], event_date: str, timing: str) -> int | None:
    if timing == "AFTER_HOURS":
        for idx, row in enumerate(rows):
            if str(row.get("date") or "") > event_date:
                return idx
        return None
    for idx, row in enumerate(rows):
        if str(row.get("date") or "") >= event_date:
            return idx
    return None


def _forward_return(rows: list[dict[str, Any]], reaction_idx: int, window: int) -> float | None:
    target_idx = reaction_idx + window
    if target_idx >= len(rows):
        return None
    return _return(rows[target_idx].get("close"), rows[reaction_idx].get("close"))


def _volume_surprise(rows: list[dict[str, Any]], reaction_idx: int) -> float | None:
    if reaction_idx < 3:
        return None
    lookback = rows[max(0, reaction_idx - 20) : reaction_idx]
    volumes = [float(row["volume"]) for row in lookback if row.get("volume") is not None]
    if not volumes:
        return None
    avg = mean(volumes)
    if avg == 0:
        return None
    return (float(rows[reaction_idx]["volume"]) - avg) / avg


def _event_study_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    nvda_rows = [row for row in rows if str(row.get("symbol") or "").upper() == "NVDA"]
    sector_rows = [row for row in rows if str(row.get("symbol") or "").upper() in {"SMH", "SOXX"}]
    index_rows = [row for row in rows if str(row.get("symbol") or "").upper() in {"QQQ", "XLK", "SPY"}]
    return {
        "event_count": _metric(len(nvda_rows), len(nvda_rows)),
        "gap_size": _distribution([row.get("event_day_gap") for row in nvda_rows]),
        "volume_surprise": _distribution([row.get("volume_surprise") for row in nvda_rows]),
        "forward_return_1d": _distribution([row.get("forward_return_1d") for row in nvda_rows]),
        "forward_return_3d": _distribution([row.get("forward_return_3d") for row in nvda_rows]),
        "forward_return_5d": _distribution([row.get("forward_return_5d") for row in nvda_rows]),
        "continuation_rate": _metric(_rate(nvda_rows, continuation=True), len(nvda_rows)),
        "reversal_rate": _metric(_rate(nvda_rows, continuation=False), len(nvda_rows)),
        "sector_spillover_expectancy": _distribution([row.get("forward_return_1d") for row in sector_rows]),
        "index_spillover_expectancy": _distribution([row.get("forward_return_1d") for row in index_rows]),
        "volatility_regime_breakdown": _volatility_regime_breakdown(rows, nvda_rows),
        "cost_adjusted_expectancy": {
            "value": None,
            "metric_status": "PLACEHOLDER_GOVERNED_COST_MODEL_REQUIRED",
            "sample_size": len(nvda_rows),
        },
    }


def _distribution(values: list[Any]) -> dict[str, Any]:
    clean = [float(value) for value in values if value is not None]
    if not clean:
        return {"value": None, "metric_status": "MISSING_INPUT", "sample_size": 0}
    return {
        "value": mean(clean),
        "mean": mean(clean),
        "median": median(clean),
        "min": min(clean),
        "max": max(clean),
        "sample_size": len(clean),
        "metric_status": "COMPUTED",
    }


def _metric(value: Any, sample_size: int) -> dict[str, Any]:
    return {"value": value, "sample_size": sample_size, "metric_status": "COMPUTED" if sample_size else "MISSING_INPUT"}


def _rate(rows: list[dict[str, Any]], *, continuation: bool) -> float | None:
    comparisons = []
    for row in rows:
        gap = row.get("event_day_gap")
        fwd = row.get("forward_return_1d")
        if gap is None or fwd is None or float(gap) == 0:
            continue
        same_direction = (float(gap) > 0 and float(fwd) > 0) or (float(gap) < 0 and float(fwd) < 0)
        comparisons.append(same_direction if continuation else not same_direction)
    if not comparisons:
        return None
    return sum(1 for item in comparisons if item) / len(comparisons)


def _volatility_regime_breakdown(rows: list[dict[str, Any]], nvda_rows: list[dict[str, Any]]) -> dict[str, Any]:
    vix_by_date = {str(row.get("event_date")): row.get("forward_return_1d") for row in rows if str(row.get("symbol") or "").upper() == "VIX"}
    paired = [(row, vix_by_date.get(str(row.get("event_date")))) for row in nvda_rows]
    clean_vix = [float(value) for _, value in paired if value is not None]
    if not clean_vix:
        return {"metric_status": "MISSING_VOLATILITY_PROXY", "sample_size": 0}
    cutoff = median(clean_vix)
    high = [row.get("forward_return_1d") for row, value in paired if value is not None and float(value) >= cutoff]
    low = [row.get("forward_return_1d") for row, value in paired if value is not None and float(value) < cutoff]
    return {"cutoff": cutoff, "high_volatility": _distribution(high), "low_volatility": _distribution(low), "metric_status": "COMPUTED"}


def _return(current: Any, basis: Any) -> float | None:
    try:
        current_f = float(current)
        basis_f = float(basis)
    except (TypeError, ValueError):
        return None
    if basis_f == 0:
        return None
    return (current_f / basis_f) - 1.0


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _valid_yyyy_mm_dd(value: str) -> bool:
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def _file_timestamp(path: Path) -> str:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except OSError:
        return _now()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_json(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
