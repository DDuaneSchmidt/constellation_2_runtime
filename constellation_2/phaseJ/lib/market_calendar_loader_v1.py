"""
Phase J — Market Calendar Truth Spine — Loader v1 (truth-only)

- Reads only from constellation_2/runtime/truth/market_calendar_v1/
- Verifies file sha256 against manifest before reading
- Provides is_trading_session(exchange, day_utc) -> bool
- Fail-closed if day is missing for requested exchange
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[3]
TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()
SPINE_ROOT = (TRUTH_ROOT / "market_calendar_v1").resolve()
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


class MarketCalendarLoaderV1:
    def __init__(self) -> None:
        if not MANIFEST_PATH.exists():
            raise FileNotFoundError(f"Missing market calendar manifest: {MANIFEST_PATH}")
        self._manifest = json.load(MANIFEST_PATH.open("r", encoding="utf-8"))
        self.dataset_version = self._manifest.get("dataset_version")
        self.exchanges = list(self._manifest.get("exchanges", []))

        self._file_index: Dict[Tuple[str, int], dict] = {}
        for e in self._manifest.get("files", []):
            key = (e["exchange"], int(e["year"]))
            if key in self._file_index:
                raise ValueError(f"Duplicate manifest entry for {key}")
            self._file_index[key] = e

        self._cache: Dict[Tuple[str, int], Dict[str, bool]] = {}

    def _verify_file(self, entry: dict) -> Path:
        p = (SPINE_ROOT / entry["file"]).resolve()
        if not p.exists():
            raise FileNotFoundError(f"Manifest references missing file: {p}")
        sha_now = _sha256_file(p)
        if sha_now != entry["sha256"]:
            raise ValueError(f"SHA256 mismatch for {p}: manifest={entry['sha256']} actual={sha_now}")
        return p

    def _load_year(self, exchange: str, year: int) -> Dict[str, bool]:
        key = (exchange, year)
        if key in self._cache:
            return self._cache[key]

        entry = self._file_index.get(key)
        if entry is None:
            raise FileNotFoundError(f"Missing calendar truth file for exchange/year: {exchange}/{year}")

        p = self._verify_file(entry)
        m: Dict[str, bool] = {}
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                day = obj.get("day_utc")
                its = obj.get("is_trading_session")
                if not isinstance(day, str) or len(day) != 10:
                    raise ValueError(f"Bad day_utc in {p}: {day!r}")
                if not isinstance(its, bool):
                    raise ValueError(f"Bad is_trading_session in {p} day={day}: {its!r}")
                if day in m:
                    raise ValueError(f"Duplicate day_utc in {p}: {day}")
                m[day] = its

        self._cache[key] = m
        return m

    def is_trading_session(self, exchange: str, day_utc: str) -> bool:
        ex = exchange.strip().upper()
        if ex not in self.exchanges:
            raise ValueError(f"Exchange not present in calendar dataset: {ex}")
        y = int(day_utc[0:4])
        m = self._load_year(ex, y)
        if day_utc not in m:
            raise FileNotFoundError(f"Missing calendar day: exchange={ex} day_utc={day_utc}")
        return m[day_utc]
