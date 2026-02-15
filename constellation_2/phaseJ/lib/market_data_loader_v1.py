"""
Constellation 2.0 — Phase J
Market Data Snapshot Truth Spine — Replay Loader v1 (calendar-governed)

Truth-only, fail-closed:
- No network calls
- Verifies market-data manifest + sha256 before reading
- Verifies calendar manifest + sha256 before deciding required session days
- Strict UTC timestamp ordering within market-data files
- Completeness rule (v1, institutional):
  - Required days are those where market_calendar_v1 says is_trading_session(exchange, day_utc) == True.
  - If calendar is missing any day in requested range -> hard fail.
  - If market data is missing any required trading day for any symbol -> hard fail.

This replaces weekday heuristics and correctly handles exchange holidays (e.g., MLK Day).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

from constellation_2.phaseJ.lib.market_calendar_loader_v1 import MarketCalendarLoaderV1

REPO_ROOT = Path(__file__).resolve().parents[3]
TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()
SPINE_ROOT = (TRUTH_ROOT / "market_data_snapshot_v1").resolve()
MANIFEST_PATH = (SPINE_ROOT / "dataset_manifest.json").resolve()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _parse_day(d: str) -> date:
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise ValueError(f"Bad day format (expected YYYY-MM-DD): {d!r}")
    return date(int(d[0:4]), int(d[5:7]), int(d[8:10]))


def _day_iter(start: date, end: date) -> Iterator[date]:
    if end < start:
        raise ValueError("end < start")
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


@dataclass(frozen=True)
class Bar:
    symbol: str
    day_utc: str  # YYYY-MM-DD
    record: dict


class MarketDataSnapshotLoaderV1:
    def __init__(self, exchange: str) -> None:
        self.exchange = exchange.strip().upper()
        self.calendar = MarketCalendarLoaderV1()

        if not MANIFEST_PATH.exists():
            raise FileNotFoundError(f"Missing market data manifest: {MANIFEST_PATH}")
        with MANIFEST_PATH.open("r", encoding="utf-8") as f:
            self._manifest = json.load(f)

        self.dataset_version = self._manifest.get("dataset_version")
        self.symbols = list(self._manifest.get("symbols", []))

        self._file_index: Dict[Tuple[str, int], dict] = {}
        for e in self._manifest.get("files", []):
            key = (e["symbol"], int(e["year"]))
            if key in self._file_index:
                raise ValueError(f"Duplicate manifest entry for {key}")
            self._file_index[key] = e

    def _verify_file(self, entry: dict) -> Path:
        p = (SPINE_ROOT / entry["file"]).resolve()
        if not p.exists():
            raise FileNotFoundError(f"Manifest references missing file: {p}")
        sha_now = _sha256_file(p)
        if sha_now != entry["sha256"]:
            raise ValueError(f"SHA256 mismatch for {p}: manifest={entry['sha256']} actual={sha_now}")
        return p

    def iter_bars(self, symbols: List[str], start_day: str, end_day: str) -> Iterator[Bar]:
        """
        Deterministic order:
        - outer loop: day ascending across calendar-required session days
        - inner loop: symbol ascending
        """
        req_syms = sorted([s.strip().upper() for s in symbols])
        for s in req_syms:
            if s not in self.symbols:
                raise ValueError(f"Requested symbol not present in dataset: {s}")

        sd = _parse_day(start_day)
        ed = _parse_day(end_day)

        # Precompute required session days from calendar (fail-closed if calendar missing a day)
        required_days: List[str] = []
        for d in _day_iter(sd, ed):
            day_str = d.isoformat()
            if self.calendar.is_trading_session(self.exchange, day_str):
                required_days.append(day_str)

        per_symbol_cache: Dict[str, Dict[str, dict]] = {}

        for sym in req_syms:
            needed_years = sorted({int(day[0:4]) for day in required_days})
            day_map: Dict[str, dict] = {}
            last_ts: Optional[str] = None

            for y in needed_years:
                entry = self._file_index.get((sym, y))
                if entry is None:
                    raise FileNotFoundError(f"Missing required truth file for symbol/year: {sym}/{y}")
                p = self._verify_file(entry)

                with p.open("r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        obj = json.loads(line)
                        ts = obj.get("timestamp_utc")
                        if not isinstance(ts, str) or not ts.endswith("Z") or len(ts) < 20:
                            raise ValueError(f"Invalid timestamp_utc in {p}: {ts!r}")

                        if last_ts is not None and ts < last_ts:
                            raise ValueError(f"Non-monotonic timestamp order in {p}: {last_ts} -> {ts}")
                        last_ts = ts

                        day = ts[0:10]
                        if day in day_map:
                            raise ValueError(f"Duplicate day in truth for {sym} day={day} file={p}")
                        day_map[day] = obj

            per_symbol_cache[sym] = day_map

        for day_str in required_days:
            for sym in req_syms:
                obj = per_symbol_cache[sym].get(day_str)
                if obj is None:
                    raise FileNotFoundError(f"Missing required trading-session bar: exchange={self.exchange} symbol={sym} day_utc={day_str}")
                yield Bar(symbol=sym, day_utc=day_str, record=obj)
