from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

from research_lab.breadth.breadth_snapshot import build_breadth_snapshot, classify_breadth_regime
from research_lab.storage.duckdb_query import load_dataset_snapshot_rows
from research_lab.storage.hashing import normalize_timestamp


def _date_text(value: Any) -> str:
    return normalize_timestamp(str(value))[:10]


def _close(row: dict[str, Any]) -> float:
    return float(row.get("adj_close") if row.get("adj_close") is not None else row.get("close"))


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _pct(flags: list[bool]) -> float:
    return sum(1 for item in flags if item) / len(flags) if flags else 0.0


def calculate_breadth_metrics(rows: list[dict[str, Any]], *, universe_snapshot_id: str) -> list[dict[str, Any]]:
    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_symbol[str(row.get("symbol") or "").upper()].append(row)
    enriched_by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for symbol, symbol_rows in sorted(by_symbol.items()):
        ordered = sorted(symbol_rows, key=lambda item: _date_text(item.get("date")))
        closes = [_close(row) for row in ordered]
        for idx, row in enumerate(ordered):
            close = closes[idx]
            prior_1 = closes[idx - 1] if idx >= 1 else None
            prior_5 = closes[idx - 5] if idx >= 5 else None
            high_window = closes[max(0, idx - 19): idx + 1]
            low_window = closes[max(0, idx - 19): idx + 1]
            enriched_by_date[_date_text(row.get("date"))].append({
                "symbol": symbol,
                "close": close,
                "above_20": close > _mean(closes[max(0, idx - 19): idx + 1]),
                "above_50": close > _mean(closes[max(0, idx - 49): idx + 1]),
                "above_200": close > _mean(closes[max(0, idx - 199): idx + 1]),
                "positive_1d": prior_1 is not None and close > prior_1,
                "positive_5d": prior_5 is not None and close > prior_5,
                "new_20d_high": close >= max(high_window) if high_window else False,
                "new_20d_low": close <= min(low_window) if low_window else False,
            })
    metrics: list[dict[str, Any]] = []
    previous: dict[str, Any] | None = None
    for day in sorted(enriched_by_date):
        rows_for_day = enriched_by_date[day]
        advances = sum(1 for row in rows_for_day if row["positive_1d"])
        declines = sum(1 for row in rows_for_day if not row["positive_1d"])
        metric = {
            "date": day,
            "universe_snapshot_id": universe_snapshot_id,
            "symbol_count": len(rows_for_day),
            "pct_above_20dma": round(_pct([row["above_20"] for row in rows_for_day]), 6),
            "pct_above_50dma": round(_pct([row["above_50"] for row in rows_for_day]), 6),
            "pct_above_200dma": round(_pct([row["above_200"] for row in rows_for_day]), 6),
            "pct_positive_1d": round(_pct([row["positive_1d"] for row in rows_for_day]), 6),
            "pct_positive_5d": round(_pct([row["positive_5d"] for row in rows_for_day]), 6),
            "pct_new_20d_high": round(_pct([row["new_20d_high"] for row in rows_for_day]), 6),
            "pct_new_20d_low": round(_pct([row["new_20d_low"] for row in rows_for_day]), 6),
            "advance_decline_ratio": round(advances / declines, 6) if declines else float(advances),
            "breadth_regime": "unknown",
        }
        metric["breadth_regime"] = classify_breadth_regime(metric, previous)
        metrics.append(metric)
        previous = metric
    return metrics


def build_breadth_snapshot_from_dataset(*, dataset_snapshot_id: str, universe_snapshot_id: str, store_root: Path | None = None, created_by: str = "Aegis") -> tuple[dict[str, Any], list[dict[str, Any]]]:
    rows = load_dataset_snapshot_rows(dataset_snapshot_id, store_root=store_root)
    metrics = calculate_breadth_metrics(rows, universe_snapshot_id=universe_snapshot_id)
    snapshot = build_breadth_snapshot(dataset_snapshot_id=dataset_snapshot_id, universe_snapshot_id=universe_snapshot_id, metrics=metrics, created_by=created_by)
    return snapshot, metrics
