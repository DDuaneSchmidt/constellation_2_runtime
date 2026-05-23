from __future__ import annotations

from pathlib import Path
from statistics import mean, stdev
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.datasets.dataset_registry import load_dataset_snapshot
from research_lab.regimes.regime_registry import store_regime_snapshot
from research_lab.regimes.regime_snapshot import REGIME_MODEL_VERSION, regime_snapshot_content_hash
from research_lab.storage.duckdb_query import load_dataset_snapshot_rows
from research_lab.storage.hashing import sha256_hex, short_hash, utc_now_iso
from research_lab.storage.parquet_io import file_sha256, write_parquet_records
from research_lab.storage.paths import ensure_store_layout, regime_uri


def _date_text(value: Any) -> str:
    return str(value)[:10]


def build_regime_labels(rows: list[dict[str, Any]], *, benchmark_symbol: str = "SPY") -> list[dict[str, Any]]:
    symbol = benchmark_symbol.upper()
    symbol_rows = [dict(row) for row in rows if str(row.get("symbol", "")).upper() == symbol]
    if not symbol_rows:
        raise RuntimeError(f"Benchmark symbol not present in dataset: {symbol}")
    symbol_rows.sort(key=lambda row: _date_text(row["date"]))
    closes = [float(row["close"]) for row in symbol_rows]
    daily_returns: list[float | None] = [None]
    for idx in range(1, len(closes)):
        prior = closes[idx - 1]
        daily_returns.append(closes[idx] / prior - 1.0 if prior else None)

    def rolling_mean(values: list[float], idx: int, window: int) -> float | None:
        if idx + 1 < window:
            return None
        return mean(values[idx - window + 1 : idx + 1])

    def rolling_std(values: list[float | None], idx: int, window: int) -> float | None:
        if idx + 1 < window:
            return None
        sample = [value for value in values[idx - window + 1 : idx + 1] if value is not None]
        if len(sample) < window:
            return None
        return stdev(sample) * (252 ** 0.5)

    def rolling_max(values: list[float], idx: int, window: int) -> float:
        return max(values[max(0, idx - window + 1) : idx + 1])

    def quantile(values: list[float], q: float) -> float:
        ordered = sorted(values)
        if not ordered:
            raise ValueError("empty quantile")
        position = (len(ordered) - 1) * q
        lower = int(position)
        upper = min(lower + 1, len(ordered) - 1)
        if lower == upper:
            return ordered[lower]
        weight = position - lower
        return ordered[lower] * (1 - weight) + ordered[upper] * weight

    realized_vols = [rolling_std(daily_returns, idx, 20) for idx in range(len(symbol_rows))]

    labels: list[dict[str, Any]] = []
    for idx, row in enumerate(symbol_rows):
        close = closes[idx]
        ma_200 = rolling_mean(closes, idx, 200)
        realized_vol = realized_vols[idx]
        vol_window = [value for value in realized_vols[max(0, idx - 251) : idx + 1] if value is not None]
        vol_q25 = quantile(vol_window, 0.25) if len(vol_window) >= 60 else None
        vol_q75 = quantile(vol_window, 0.75) if len(vol_window) >= 60 else None
        high_252 = rolling_max(closes, idx, 252)
        drawdown = close / high_252 - 1.0 if high_252 else 0.0
        if ma_200 is None:
            trend = "unknown"
        else:
            trend = "bull_trend" if close > ma_200 else "bear_trend"
        if realized_vol is None or vol_q25 is None or vol_q75 is None:
            vol = "unknown"
        elif realized_vol >= vol_q75:
            vol = "high_vol"
        elif realized_vol <= vol_q25:
            vol = "low_vol"
        else:
            vol = "normal_vol"
        if drawdown <= -0.20:
            drawdown_regime = "severe_drawdown"
        elif drawdown <= -0.10:
            drawdown_regime = "correction"
        else:
            drawdown_regime = "normal_drawdown"
        if trend == "bull_trend" and vol != "high_vol":
            risk = "risk_on"
        elif trend == "bear_trend" or drawdown_regime == "severe_drawdown":
            risk = "risk_off"
        else:
            risk = "mixed"
        labels.append(
            {
                "date": _date_text(row["date"]),
                "benchmark_symbol": symbol,
                "spy_return_20d": closes[idx] / closes[idx - 20] - 1.0 if idx >= 20 and closes[idx - 20] else None,
                "spy_realized_vol_20d": realized_vol,
                "spy_drawdown_from_252d_high": drawdown,
                "spy_above_200dma": None if ma_200 is None else bool(close > ma_200),
                "trend_regime": trend,
                "vol_regime": vol,
                "drawdown_regime": drawdown_regime,
                "risk_regime": risk,
            }
        )
    return labels


def build_regime_snapshot(
    *,
    dataset_snapshot_id: str,
    benchmark_symbol: str = "SPY",
    store_root: Path | None = None,
    created_by: str = "Aegis",
    allow_json_fallback: bool = False,
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    dataset = load_dataset_snapshot(dataset_snapshot_id, store_root=store)
    rows = load_dataset_snapshot_rows(dataset_snapshot_id, store_root=store)
    labels = build_regime_labels(rows, benchmark_symbol=benchmark_symbol)
    seed = sha256_hex(
        {
            "dataset_snapshot_id": dataset_snapshot_id,
            "benchmark_symbol": benchmark_symbol.upper(),
            "regime_model_version": REGIME_MODEL_VERSION,
            "dataset_hash": dataset["content_hash"],
        }
    )
    regime_snapshot_id = f"rs_{benchmark_symbol.lower()}_{short_hash(seed, 12)}"
    regime_dir = store / "regimes" / regime_snapshot_id
    if regime_dir.exists():
        raise FileExistsError(f"Refusing to overwrite immutable regime snapshot: {regime_dir}")
    regime_dir.mkdir(parents=True, exist_ok=False)
    labels_path = regime_dir / "regime_labels.parquet"
    write_parquet_records(labels_path, labels, allow_json_fallback=allow_json_fallback)
    labels_hash = file_sha256(labels_path)
    created_at = utc_now_iso()
    snapshot = {
        "regime_snapshot_id": regime_snapshot_id,
        "dataset_snapshot_id": dataset_snapshot_id,
        "benchmark_symbol": benchmark_symbol.upper(),
        "created_at": created_at,
        "created_by": created_by,
        "regime_model_version": REGIME_MODEL_VERSION,
        "inputs": {
            "dataset_snapshot_hash": dataset["content_hash"],
            "canonical_file_hash": dataset.get("canonical_file_hash"),
            "benchmark_symbol": benchmark_symbol.upper(),
        },
        "labels": {
            "row_count": len(labels),
            "labels_hash": labels_hash,
            "labels_uri": f"{regime_uri(regime_snapshot_id)}/regime_labels.parquet",
        },
        "storage_uri": regime_uri(regime_snapshot_id),
        "content_hash": "",
        "schema_version": "regime_snapshot.v1",
    }
    snapshot["content_hash"] = regime_snapshot_content_hash(snapshot)
    registry_row = store_regime_snapshot(snapshot, store_root=store)
    write_audit_event(
        actor=created_by,
        entity_type="regime_snapshot",
        entity_id=regime_snapshot_id,
        action="regime_snapshot_created",
        new_state_hash=snapshot["content_hash"],
        reason="Created deterministic regime snapshot.",
        metadata={"dataset_snapshot_id": dataset_snapshot_id, "benchmark_symbol": benchmark_symbol.upper(), "registry_row": registry_row},
        store_root=store,
    )
    return {"regime_snapshot": snapshot, "registry_row": registry_row}
