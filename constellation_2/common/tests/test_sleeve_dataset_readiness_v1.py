from __future__ import annotations

import hashlib
import json
import sys
from datetime import date, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ops.tools.run_sleeve_dataset_readiness_v1 as readiness  # noqa: E402


def _market_days(start: date, count: int) -> list[date]:
    out: list[date] = []
    current = start
    while len(out) < count:
        if current.weekday() < 5:
            out.append(current)
        current += timedelta(days=1)
    return out


def _write_symbol(root: Path, symbol: str, days: list[date]) -> dict[str, object]:
    path = root / "market_data_snapshot_v1" / symbol / "2026.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    for idx, day in enumerate(days):
        lines.append(
            json.dumps(
                {
                    "dataset_version": "v1",
                    "symbol": symbol,
                    "timestamp_utc": f"{day.isoformat()}T00:00:00Z",
                    "open": 100 + idx,
                    "high": 101 + idx,
                    "low": 99 + idx,
                    "close": 100 + idx,
                    "volume": 1000,
                    "source_name": "test",
                    "source_hash": "a" * 64,
                    "ingested_utc": f"{day.isoformat()}T00:00:00Z",
                },
                sort_keys=True,
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"symbol": symbol, "year": 2026, "file": f"{symbol}/2026.jsonl", "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def _write_manifest(root: Path, symbols: list[str], *, rows: int = 120) -> None:
    days = _market_days(date(2026, 1, 1), rows)
    entries = [_write_symbol(root, symbol, days) for symbol in symbols]
    manifest = {
        "dataset_version": "v1",
        "symbols": sorted(symbols),
        "date_range": {"start": days[0].isoformat(), "end": days[-1].isoformat()},
        "files": entries,
        "global_hash": "test",
        "created_utc": f"{days[-1].isoformat()}T00:00:00Z",
        "source_snapshot_utc": f"{days[-1].isoformat()}T00:00:00Z",
    }
    path = root / "market_data_snapshot_v1" / "dataset_manifest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")


def test_contract_requirements_include_new_etf_symbols() -> None:
    rows = {row["sleeve_id"]: row for row in readiness.resolve_required_sleeve_datasets(repo_root=REPO_ROOT)}

    assert {"IEF", "LQD", "UUP", "DBC"}.issubset(set(rows["C2_CROSS_ASSET_TREND_V1"]["required_symbols"]))
    assert "LQD" in rows["C2_MARKET_NEUTRAL_SPREAD_V1"]["required_symbols"]
    assert rows["C2_CROSS_ASSET_TREND_V1"]["minimum_history_days"] == 100
    assert rows["C2_MARKET_NEUTRAL_SPREAD_V1"]["minimum_history_days"] == 60


def test_missing_required_datasets_block_activation(tmp_path: Path) -> None:
    _write_manifest(tmp_path, ["SPY", "QQQ", "IWM", "HYG"], rows=120)

    payload = readiness.build_sleeve_dataset_readiness(day_utc="2026-06-17", truth_root=tmp_path, repo_root=REPO_ROOT)

    assert payload["status"] == "FAIL"
    rows = {row["sleeve_id"]: row for row in payload["rows"]}
    assert "LQD" in rows["C2_MARKET_NEUTRAL_SPREAD_V1"]["missing_symbols"]
    assert {"IEF", "LQD", "UUP", "DBC"}.issubset(set(rows["C2_CROSS_ASSET_TREND_V1"]["missing_symbols"]))


def test_dataset_ready_passes_for_contract_universe(tmp_path: Path) -> None:
    required = sorted(
        {
            symbol
            for row in readiness.resolve_required_sleeve_datasets(repo_root=REPO_ROOT)
            for symbol in row["required_symbols"]
        }
    )
    _write_manifest(tmp_path, required, rows=120)

    payload = readiness.build_sleeve_dataset_readiness(day_utc="2026-06-17", truth_root=tmp_path, repo_root=REPO_ROOT)

    assert payload["status"] == "PASS"
    assert all(row["dataset_ready"] for row in payload["rows"])


def test_no_deprecated_hardcoded_symbol_basket_is_used() -> None:
    text = Path(readiness.__file__).read_text(encoding="utf-8")
    deprecated_primary = ",".join(["QQQ", "IWM", "GLD", "TLT"])
    deprecated_extended = ",".join(["SPY", "QQQ", "IWM", "TLT", "GLD", "HYG"])

    assert deprecated_primary not in text
    assert deprecated_extended not in text
