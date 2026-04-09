#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Tuple


MIN_PRICE = Decimal("10")
MIN_MEDIAN_DAILY_DOLLAR_VOLUME = Decimal("25000000")
MIN_BAR_HISTORY_SESSIONS = 20
TARGET_SYMBOL_COUNT = 200


class RankedUniverseError(Exception):
    pass


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise RankedUniverseError(f"MISSING_JSON:{path}")
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise RankedUniverseError(f"TOP_LEVEL_NOT_OBJECT:{path}")
    return obj


def _parse_decimal(value: Any, *, field: str) -> Decimal:
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise RankedUniverseError(f"BAD_DECIMAL:{field}={value!r}") from exc


def _iter_jsonl_rows(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for idx, line in enumerate(handle, start=1):
            raw = line.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise RankedUniverseError(f"JSONL_PARSE_FAILED:{path}:line={idx}") from exc
            if not isinstance(obj, dict):
                raise RankedUniverseError(f"JSONL_ROW_NOT_OBJECT:{path}:line={idx}")
            yield obj


def _manifest_path(truth_root: Path) -> Path:
    return (truth_root / "market_data_snapshot_v1" / "dataset_manifest.json").resolve()


def _artifact_path(truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "ranked_symbol_universe_v1" / day_utc / "ranked_symbol_universe.v1.json").resolve()


def _load_manifest_entries(truth_root: Path) -> List[Dict[str, Any]]:
    manifest = _read_json(_manifest_path(truth_root))
    files = manifest.get("files")
    if not isinstance(files, list):
        raise RankedUniverseError("MARKET_DATA_MANIFEST_FILES_NOT_LIST")
    out: List[Dict[str, Any]] = []
    for row in files:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "").strip().upper()
        rel_file = str(row.get("file") or "").strip()
        if not symbol or not rel_file:
            continue
        out.append({"symbol": symbol, "file": rel_file})
    return out


def _rows_up_to_day(truth_root: Path, rel_file: str, day_utc: str, symbol: str) -> List[Dict[str, Any]]:
    p = (truth_root / "market_data_snapshot_v1" / rel_file).resolve()
    if not str(p).startswith(str((truth_root / "market_data_snapshot_v1").resolve())):
        raise RankedUniverseError(f"MANIFEST_PATH_ESCAPES_MD_ROOT:{rel_file}")
    if not p.exists() or not p.is_file():
        raise RankedUniverseError(f"MARKET_DATA_FILE_MISSING:{p}")
    rows: List[Dict[str, Any]] = []
    for row in _iter_jsonl_rows(p):
        row_symbol = str(row.get("symbol") or "").strip().upper()
        ts = str(row.get("timestamp_utc") or "").strip()
        if row_symbol != symbol or len(ts) < 10:
            continue
        if ts[:10] <= day_utc:
            rows.append(row)
    rows.sort(key=lambda r: str(r.get("timestamp_utc") or ""))
    return rows


def _median_dollar_volume(rows: List[Dict[str, Any]]) -> Decimal:
    dv: List[float] = []
    for row in rows:
        close = _parse_decimal(row.get("close"), field="close")
        volume = _parse_decimal(row.get("volume"), field="volume")
        dv.append(float(close * volume))
    return Decimal(str(median(dv)))


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    text = format(value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _build_payload(*, day_utc: str, produced_utc: str, truth_root: Path) -> Dict[str, Any]:
    manifest_entries = _load_manifest_entries(truth_root)
    by_symbol: Dict[str, List[str]] = {}
    for row in manifest_entries:
        by_symbol.setdefault(str(row["symbol"]), []).append(str(row["file"]))

    metrics: List[Dict[str, Any]] = []
    ranked: List[Tuple[Decimal, str]] = []

    for symbol in sorted(by_symbol.keys()):
        symbol_rows: List[Dict[str, Any]] = []
        for rel_file in sorted(set(by_symbol[symbol])):
            symbol_rows.extend(_rows_up_to_day(truth_root, rel_file, day_utc, symbol))
        symbol_rows.sort(key=lambda r: str(r.get("timestamp_utc") or ""))

        eligible = True
        reasons: List[str] = []
        latest_close: Decimal | None = None
        mdv: Decimal | None = None
        has_same_day_bar = bool(symbol_rows) and str(symbol_rows[-1].get("timestamp_utc") or "")[:10] == day_utc

        if len(symbol_rows) < MIN_BAR_HISTORY_SESSIONS:
            eligible = False
            reasons.append("INSUFFICIENT_BAR_HISTORY")
        if not symbol_rows:
            eligible = False
            reasons.append("NO_BARS_UP_TO_DAY")
        if symbol_rows:
            tail = symbol_rows[-MIN_BAR_HISTORY_SESSIONS:]
            latest_close = _parse_decimal(tail[-1].get("close"), field="close")
            if latest_close < MIN_PRICE:
                eligible = False
                reasons.append("PRICE_BELOW_MIN")
            mdv = _median_dollar_volume(tail)
            if mdv < MIN_MEDIAN_DAILY_DOLLAR_VOLUME:
                eligible = False
                reasons.append("MEDIAN_DOLLAR_VOLUME_BELOW_MIN")
            if not has_same_day_bar:
                reasons.append("LATEST_COVERED_DAY_USED")
        if eligible and mdv is not None:
            ranked.append((mdv, symbol))

        metrics.append(
            {
                "symbol": symbol,
                "asset_type": "UNKNOWN",
                "eligible": bool(eligible),
                "latest_close": _decimal_text(latest_close),
                "median_daily_dollar_volume": _decimal_text(mdv),
                "bar_history_sessions": len(symbol_rows),
                "has_same_day_bar": bool(has_same_day_bar),
                "reason_codes": sorted(set(reasons)),
            }
        )

    ranked.sort(key=lambda row: (-row[0], row[1]))
    selected = [s for _, s in ranked[:TARGET_SYMBOL_COUNT]]
    reason_codes: List[str] = []
    if not selected:
        reason_codes.append("SELECTED_SYMBOL_COUNT_BELOW_TARGET_RANGE")

    return {
        "schema_id": "ranked_symbol_universe",
        "schema_version": "v1",
        "day_utc": day_utc,
        "produced_utc": produced_utc,
        "status": "PASS",
        "universe_scope": "LIQUIDITY_RANKED_SYMBOLS/DYNAMIC_SAME_DAY",
        "selection_policy": {
            "asset_scope": "US_EQUITY_ETF",
            "min_price": _decimal_text(MIN_PRICE),
            "min_median_daily_dollar_volume": _decimal_text(MIN_MEDIAN_DAILY_DOLLAR_VOLUME),
            "min_bar_history_sessions": MIN_BAR_HISTORY_SESSIONS,
            "exclude_leveraged_inverse_etf": True,
            "target_symbol_count": TARGET_SYMBOL_COUNT,
            "allowed_symbol_count_range": {"min": 150, "max": 300},
            "deterministic_sort": "liquidity_desc_then_symbol_asc",
        },
        "target_symbol_count": TARGET_SYMBOL_COUNT,
        "considered_symbol_count": len(by_symbol),
        "eligible_symbol_count": len(ranked),
        "selected_symbol_count": len(selected),
        "symbols": selected,
        "selection_metrics_by_symbol": metrics,
        "source_evidence": {
            "market_data_manifest_path": str(_manifest_path(truth_root)),
            "dynamic_discovery_used": False,
            "metadata_symbol_count": 0,
        },
        "reason_codes": reason_codes,
    }


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_ranked_symbol_universe_day_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", required=True)
    ap.add_argument("--produced_utc", default="")
    args = ap.parse_args()

    day_utc = str(args.day_utc).strip()
    truth_root = Path(str(args.truth_root).strip()).resolve()
    produced_utc = str(args.produced_utc).strip() or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    payload = _build_payload(day_utc=day_utc, produced_utc=produced_utc, truth_root=truth_root)
    out_path = _artifact_path(truth_root, day_utc)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    print(json.dumps({"status": "OK", "artifact_path": str(out_path), "selected_symbol_count": payload["selected_symbol_count"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
