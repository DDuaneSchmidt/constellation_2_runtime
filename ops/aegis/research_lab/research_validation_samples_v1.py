from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.research_lab.research_pipeline_v1 import ETF_DROP_MEAN_REVERSION_HYPOTHESIS_ID

REPORT_FAMILY = "aegis_research_validation_samples_v1"
REPORT_FILENAME = "research_validation_samples.v1.json"

SAFETY = {
    "research_only": True,
    "trade_advice_allowed": False,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "live_trading_allowed": False,
    "autonomous_live_trading_allowed": False,
    "paper_testing_sleeve_creation_allowed": False,
}


def research_validation_samples_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def build_research_validation_samples_v1(*, truth_root: Path | str, day_utc: str, hypothesis_id: str = ETF_DROP_MEAN_REVERSION_HYPOTHESIS_ID) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    results = _test_results(root, day, hypothesis_id)
    market_dates = _market_dates(root, day)
    computed_samples: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    required = 20
    for result_day, payload in results:
        required = int(payload.get("minimum_sample_size") or required or 20)
        event_availability = payload.get("event_data_availability") if isinstance(payload.get("event_data_availability"), Mapping) else {}
        event_rows = [row for row in event_availability.get("event_rows") or [] if isinstance(row, Mapping)]
        observed_rows = [row for row in event_availability.get("observed_rows") or [] if isinstance(row, Mapping)]
        if not event_rows:
            for row in observed_rows:
                excluded.append(_excluded(row, result_day, "NO_TRIGGER_EVENT", "The governed row did not satisfy daily_return <= -1.5% with VIX filter."))
            continue
        for row in event_rows:
            symbol = str(row.get("symbol") or "").upper()
            event_close = _number(row.get("close"))
            if not symbol:
                excluded.append(_excluded(row, result_day, "EVENT_SYMBOL_MISSING", "Trigger event row has no symbol."))
                continue
            next_day = _next_market_date(market_dates, result_day)
            if not next_day:
                excluded.append(_excluded(row, result_day, "FORWARD_WINDOW_PENDING", "No later governed market-data day exists yet."))
                continue
            next_close = _close_for_symbol(root, next_day, symbol)
            if event_close is None or next_close is None:
                excluded.append(_excluded(row, result_day, "FORWARD_CLOSE_MISSING", "Event close or next-window close is unavailable."))
                continue
            if event_close == 0:
                excluded.append(_excluded(row, result_day, "EVENT_CLOSE_INVALID", "Event close is zero, so forward return cannot be computed."))
                continue
            fwd_return = (next_close - event_close) / event_close
            sample = {
                "sample_id": _sample_id(hypothesis_id, symbol, result_day, next_day),
                "hypothesis_id": hypothesis_id,
                "symbol": symbol,
                "event_day": result_day,
                "sample_timestamp": f"{next_day}T20:00:00Z",
                "event_close": event_close,
                "forward_close_day": next_day,
                "forward_close": next_close,
                "forward_return_1d": fwd_return,
                "source_test_result_day": result_day,
                "source_test_result_path": str(_result_path(root, result_day, hypothesis_id)),
                "sample_status": "VALID",
            }
            computed_samples.append(sample)
    prior_samples = _prior_samples(root, day, hypothesis_id)
    samples = _merge_samples(prior_samples, computed_samples, target_day=day)
    current = len(samples)
    missing = max(required - current, 0)
    last_sample_at = max((str(row.get("sample_timestamp") or "") for row in samples), default="")
    next_expected = _next_sample_expected_at(excluded, market_dates, day, current=current, required=required)
    report = {
        "schema_id": "aegis_research_validation_samples",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day,
        "generated_at": _now(),
        "hypothesis_id": hypothesis_id,
        "required_samples": required,
        "current_samples": current,
        "missing_samples": missing,
        "last_sample_at": last_sample_at,
        "next_sample_expected_at": next_expected,
        "excluded_samples": excluded,
        "exclusion_reasons": _reason_counts(excluded),
        "samples": samples,
        "sample_count_monotonic": True,
        "sample_count_monotonic_basis": "prior accepted samples plus recomputed valid governed test-result observations up to target day; samples are keyed by hypothesis_id, symbol, event_day, and forward_close_day",
        "sample_count_reset_reason": "",
        "sample_producer": "ops.aegis.research_lab.research_validation_samples_v1",
        "forward_return_sample_source": "aegis_research_test_results_v1.event_data_availability.event_rows + aegis_market_data_v1 next governed market close",
        "should_rerun_qualification": current >= required,
        "status": "THRESHOLD_MET" if current >= required else "COLLECTING_EVIDENCE",
        "summary": f"{current}/{required} forward-return samples recorded; {missing} remaining.",
        "safety": dict(SAFETY),
        **SAFETY,
    }
    return report


def write_research_validation_samples_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None, hypothesis_id: str = ETF_DROP_MEAN_REVERSION_HYPOTHESIS_ID) -> Path:
    root = Path(truth_root).expanduser().resolve()
    body = dict(payload or build_research_validation_samples_v1(truth_root=root, day_utc=day_utc, hypothesis_id=hypothesis_id))
    return write_json_v1(research_validation_samples_path_v1(truth_root=root, day_utc=str(day_utc)), body)


def _sample_id(hypothesis_id: str, symbol: str, event_day: str, forward_close_day: str) -> str:
    raw = f"{hypothesis_id}|{symbol.upper()}|{event_day}|{forward_close_day}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _prior_samples(root: Path, day: str, hypothesis_id: str) -> list[dict[str, Any]]:
    base = root / "reports" / REPORT_FAMILY
    rows: list[dict[str, Any]] = []
    if not base.exists():
        return rows
    for path in sorted(base.glob(f"*/{REPORT_FILENAME}")):
        path_day = path.parent.name
        if path_day >= day:
            continue
        payload = _read_json(path)
        if str(payload.get("hypothesis_id") or hypothesis_id) != hypothesis_id:
            continue
        for row in payload.get("samples") or []:
            if isinstance(row, Mapping):
                sample = dict(row)
                sample.setdefault("sample_status", "VALID")
                sample.setdefault("sample_id", _sample_id(hypothesis_id, str(sample.get("symbol") or ""), str(sample.get("event_day") or ""), str(sample.get("forward_close_day") or "")))
                rows.append(sample)
    return rows


def _merge_samples(prior_samples: list[dict[str, Any]], computed_samples: list[dict[str, Any]], *, target_day: str) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for row in prior_samples + computed_samples:
        forward_day = str(row.get("forward_close_day") or "")
        if forward_day and forward_day > target_day:
            continue
        sample_id = str(row.get("sample_id") or _sample_id(str(row.get("hypothesis_id") or ""), str(row.get("symbol") or ""), str(row.get("event_day") or ""), forward_day))
        if not sample_id.strip():
            continue
        merged[sample_id] = {**row, "sample_id": sample_id, "sample_status": str(row.get("sample_status") or "VALID")}
    return sorted(merged.values(), key=lambda row: (str(row.get("sample_timestamp") or ""), str(row.get("sample_id") or "")))


def _next_sample_expected_at(excluded: list[dict[str, Any]], market_dates: list[str], day: str, *, current: int, required: int) -> str:
    if current >= required:
        return "qualification threshold met; rerun qualification"
    pending = [row for row in excluded if row.get("exclusion_reason") == "FORWARD_WINDOW_PENDING"]
    next_day = _next_market_date_after(market_dates, day)
    if pending:
        return f"{next_day}T20:00:00Z" if next_day else "next governed market close after pending trigger event"
    return f"{next_day}T20:00:00Z after next valid trigger event" if next_day else "next governed market close after next valid trigger event"


def _result_path(root: Path, day: str, hypothesis_id: str) -> Path:
    return root / "reports" / "aegis_research_test_results_v1" / day / hypothesis_id / "research_test_result.v1.json"


def _test_results(root: Path, day: str, hypothesis_id: str) -> list[tuple[str, dict[str, Any]]]:
    base = root / "reports" / "aegis_research_test_results_v1"
    out: list[tuple[str, dict[str, Any]]] = []
    if not base.exists():
        return out
    for path in sorted(base.glob(f"*/{hypothesis_id}/research_test_result.v1.json")):
        result_day = path.parents[1].name
        if result_day <= day:
            payload = _read_json(path)
            if payload:
                out.append((result_day, payload))
    return out


def _market_dates(root: Path, day: str) -> list[str]:
    base = root / "reports" / "aegis_market_data_v1"
    if not base.exists():
        return []
    return sorted(path.parent.name for path in base.glob("*/market_data.v1.json") if path.parent.name <= day)


def _next_market_date(dates: list[str], day: str) -> str:
    for candidate in dates:
        if candidate > day:
            return candidate
    return ""


def _next_market_date_after(dates: list[str], day: str) -> str:
    for candidate in dates:
        if candidate > day:
            return candidate
    return ""


def _close_for_symbol(root: Path, day: str, symbol: str) -> float | None:
    payload = _read_json(root / "reports" / "aegis_market_data_v1" / day / "market_data.v1.json")
    for row in _all_dicts(payload):
        if str(row.get("symbol") or row.get("canonical_symbol") or "").upper() != symbol:
            continue
        value = _number(row.get("close"))
        if value is None:
            value = _number(row.get("last_price"))
        if value is not None:
            return value
    return None


def _all_dicts(value: Any):
    if isinstance(value, Mapping):
        yield value
        for child in value.values():
            yield from _all_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _all_dicts(child)


def _excluded(row: Mapping[str, Any], result_day: str, reason: str, detail: str) -> dict[str, Any]:
    return {
        "hypothesis_id": ETF_DROP_MEAN_REVERSION_HYPOTHESIS_ID,
        "symbol": str(row.get("symbol") or "").upper(),
        "event_day": result_day,
        "exclusion_reason": reason,
        "detail": detail,
        "daily_return": row.get("daily_return"),
        "vix_filter_pass": row.get("vix_filter_pass"),
    }


def _reason_counts(rows: list[Mapping[str, Any]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in rows:
        reason = str(row.get("exclusion_reason") or "UNKNOWN")
        out[reason] = out.get(reason, 0) + 1
    return out


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _number(value: Any) -> float | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
