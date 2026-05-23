from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

from research_lab.candidates.candidate_registry import load_candidate_batch, load_candidates
from research_lab.candidates.outcome_record import OUTCOME_SCHEMA_VERSION, validate_outcome_record
from research_lab.costs.cost_adjustments import round_trip_cost_bps_for_symbol
from research_lab.costs.cost_model_registry import load_cost_model_snapshot
from research_lab.datasets.dataset_registry import load_dataset_snapshot
from research_lab.storage.duckdb_query import canonical_parquet_path, load_dataset_snapshot_rows
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.paths import ensure_store_layout


def _date_text(value: Any) -> str:
    return str(value)[:10]


def _rows_by_symbol(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        item = dict(row)
        item["symbol"] = str(item["symbol"]).upper()
        item["date"] = _date_text(item["date"])
        grouped[item["symbol"]].append(item)
    for symbol in grouped:
        grouped[symbol].sort(key=lambda item: item["date"])
    return grouped


def _unavailable_record(
    *,
    candidate: dict[str, Any],
    window: int,
    benchmark_symbol: str,
    reason: str,
    created_at: str,
) -> dict[str, Any]:
    payload = {
        "candidate_id": candidate["candidate_id"],
        "candidate_batch_id": candidate["candidate_batch_id"],
        "source_evidence_package_id": candidate["source_evidence_package_id"],
        "dataset_snapshot_id": candidate["dataset_snapshot_id"],
        "regime_snapshot_id": candidate["regime_snapshot_id"],
        "cost_model_snapshot_id": candidate["cost_model_snapshot_id"],
        "symbol": candidate["symbol"],
        "benchmark_symbol": benchmark_symbol,
        "signal_date": candidate["signal_date"],
        "as_of_date": candidate["as_of_date"],
        "outcome_window": f"{window}d",
        "outcome_start_date": candidate["signal_date"],
        "outcome_end_date": "",
        "gross_return": None,
        "post_cost_return": None,
        "benchmark_return": None,
        "excess_return": None,
        "outcome_status": "unavailable",
        "unavailable_reason": reason,
        "created_at": created_at,
        "schema_version": OUTCOME_SCHEMA_VERSION,
    }
    payload["content_hash"] = content_hash(payload)
    payload["outcome_record_id"] = f"out_{short_hash(payload['content_hash'], 16)}"
    payload["content_hash"] = content_hash(payload)
    validate_outcome_record(payload)
    return payload


def _measured_record(
    *,
    candidate: dict[str, Any],
    window: int,
    benchmark_symbol: str,
    start_row: dict[str, Any],
    end_row: dict[str, Any],
    benchmark_start: dict[str, Any],
    benchmark_end: dict[str, Any],
    round_trip_cost_bps: float,
    created_at: str,
) -> dict[str, Any]:
    gross = float(end_row["adj_close"]) / float(start_row["adj_close"]) - 1.0
    post_cost = gross - round_trip_cost_bps / 10000.0
    benchmark_return = float(benchmark_end["adj_close"]) / float(benchmark_start["adj_close"]) - 1.0
    payload = {
        "candidate_id": candidate["candidate_id"],
        "candidate_batch_id": candidate["candidate_batch_id"],
        "source_evidence_package_id": candidate["source_evidence_package_id"],
        "dataset_snapshot_id": candidate["dataset_snapshot_id"],
        "regime_snapshot_id": candidate["regime_snapshot_id"],
        "cost_model_snapshot_id": candidate["cost_model_snapshot_id"],
        "symbol": candidate["symbol"],
        "benchmark_symbol": benchmark_symbol,
        "signal_date": candidate["signal_date"],
        "as_of_date": candidate["as_of_date"],
        "outcome_window": f"{window}d",
        "outcome_start_date": start_row["date"],
        "outcome_end_date": end_row["date"],
        "gross_return": gross,
        "post_cost_return": post_cost,
        "benchmark_return": benchmark_return,
        "excess_return": post_cost - benchmark_return,
        "outcome_status": "measured",
        "unavailable_reason": "",
        "round_trip_cost_bps": round_trip_cost_bps,
        "created_at": created_at,
        "schema_version": OUTCOME_SCHEMA_VERSION,
    }
    payload["content_hash"] = content_hash(payload)
    payload["outcome_record_id"] = f"out_{short_hash(payload['content_hash'], 16)}"
    payload["content_hash"] = content_hash(payload)
    validate_outcome_record(payload)
    return payload


def measure_candidate_outcomes(
    *,
    candidate_batch_id: str,
    dataset_snapshot_id: str,
    cost_model_snapshot_id: str,
    benchmark_symbol: str = "SPY",
    windows: list[int] | None = None,
    store_root: Path | None = None,
) -> dict[str, Any]:
    windows = sorted({int(window) for window in (windows or [1, 2, 5, 10, 20])})
    if any(window <= 0 for window in windows):
        raise RuntimeError(f"Unsupported outcome window: {windows}")
    store = ensure_store_layout(store_root)
    batch = load_candidate_batch(candidate_batch_id, store_root=store)
    dataset = load_dataset_snapshot(dataset_snapshot_id, store_root=store)
    if not canonical_parquet_path(dataset_snapshot_id, store_root=store).exists():
        raise RuntimeError("Canonical parquet missing")
    cost_model = load_cost_model_snapshot(cost_model_snapshot_id, store_root=store)
    candidates = load_candidates(candidate_batch_id, store_root=store)
    rows = load_dataset_snapshot_rows(dataset_snapshot_id, store_root=store)
    grouped = _rows_by_symbol(rows)
    benchmark = benchmark_symbol.upper()
    if benchmark not in grouped:
        raise RuntimeError(f"Benchmark symbol missing: {benchmark}")
    created_at = utc_now_iso()
    outcome_rows: list[dict[str, Any]] = []
    for candidate in sorted(candidates, key=lambda row: row["candidate_id"]):
        symbol = str(candidate["symbol"]).upper()
        if symbol not in grouped:
            raise RuntimeError(f"Candidate references unknown symbol: {symbol}")
        symbol_rows = grouped[symbol]
        benchmark_rows = grouped[benchmark]
        symbol_dates = {row["date"]: index for index, row in enumerate(symbol_rows)}
        benchmark_dates = {row["date"]: index for index, row in enumerate(benchmark_rows)}
        start_date = _date_text(candidate.get("signal_date") or candidate.get("as_of_date"))
        start_index = symbol_dates.get(start_date)
        benchmark_start_index = benchmark_dates.get(start_date)
        for window in windows:
            if start_index is None or benchmark_start_index is None:
                outcome_rows.append(_unavailable_record(candidate=candidate, window=window, benchmark_symbol=benchmark, reason="missing_signal_date", created_at=created_at))
                continue
            end_index = start_index + window
            benchmark_end_index = benchmark_start_index + window
            if end_index >= len(symbol_rows) or benchmark_end_index >= len(benchmark_rows):
                outcome_rows.append(_unavailable_record(candidate=candidate, window=window, benchmark_symbol=benchmark, reason="insufficient_forward_data", created_at=created_at))
                continue
            cost_bps = round_trip_cost_bps_for_symbol(cost_model, symbol)
            outcome_rows.append(
                _measured_record(
                    candidate=candidate,
                    window=window,
                    benchmark_symbol=benchmark,
                    start_row=symbol_rows[start_index],
                    end_row=symbol_rows[end_index],
                    benchmark_start=benchmark_rows[benchmark_start_index],
                    benchmark_end=benchmark_rows[benchmark_end_index],
                    round_trip_cost_bps=cost_bps,
                    created_at=created_at,
                )
            )
    return {
        "candidate_batch": batch,
        "dataset_snapshot": dataset,
        "cost_model_snapshot": cost_model,
        "outcomes": sorted(outcome_rows, key=lambda row: (row["candidate_id"], int(str(row["outcome_window"]).rstrip("d")))),
        "windows": windows,
        "created_at": created_at,
    }

