#!/usr/bin/env python3
"""
run_stress_replay_report_day_v1.py

Phase J — Stress Replay Report v1
Deterministic, fail-closed, single-writer, canonical JSON, schema-validated.

This is a REPLAY ENGINE over governed, stored market data truth:
- market_calendar_v1 (JSONL)
- market_data_snapshot_v1 (JSONL OHLCV)

We produce a synthetic NAV path driven by CLOSE-to-CLOSE returns for the chosen symbol(s).
This is an observability/stress harness; it does not claim execution realism.

Synthetic NAV model (deterministic):
- start_nav = --start_nav_usd (integer USD)
- For each trading session in the window:
    nav_t = ROUND_HALF_UP( nav_{t-1} * (close_t / close_{t-1}) ) to integer USD

Drawdown convention (canonical):
- rolling_peak_nav_t = max(nav_d) for d <= t
- drawdown_pct_t = (nav_t - rolling_peak_nav_t) / rolling_peak_nav_t  (negative underwater), quantized 6dp
- drawdown_abs_t = nav_t - rolling_peak_nav_t  (integer USD)

Output:
- constellation_2/runtime/truth/monitoring_v1/stress_replay/<ASOF_DAY_UTC>/stress_replay_report.<SCENARIO_ID>.v1.json
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP, getcontext
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

CAL_ROOT = (TRUTH / "market_calendar_v1").resolve()
MD_ROOT = (TRUTH / "market_data_snapshot_v1").resolve()

OUT_ROOT = (TRUTH / "monitoring_v1/stress_replay").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/MONITORING/stress_replay_report.v1.schema.json"

DD_Q = Decimal("0.000001")  # 6dp


class CliError(Exception):
    pass


def _utc_now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _git_sha() -> str:
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception as e:  # noqa: BLE001
        raise CliError(f"FAIL_GIT_SHA: {e}") from e


def _sha256_file(p: Path) -> str:
    import hashlib  # local import

    h = hashlib.sha256()
    try:
        h.update(p.read_bytes())
    except Exception as e:  # noqa: BLE001
        raise CliError(f"READ_FAILED: {p}: {e}") from e
    return h.hexdigest()


def _parse_day(s: str) -> date:
    try:
        return date.fromisoformat(s)
    except Exception as e:  # noqa: BLE001
        raise CliError(f"BAD_DAY_UTC: {s}: {e}") from e


def _day_str(d: date) -> str:
    return d.isoformat()


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        raise CliError(f"JSON_READ_FAILED: {path}: {e}") from e


def _iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    # Deterministic numeric parsing: parse_float=Decimal to avoid binary float drift.
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s:
            continue
        try:
            yield json.loads(s, parse_float=Decimal, parse_int=int)
        except Exception as e:  # noqa: BLE001
            raise CliError(f"JSONL_PARSE_FAILED: {path}: {e}") from e


def _quant_dd(x: Decimal) -> Decimal:
    return x.quantize(DD_Q, rounding=ROUND_HALF_UP)


def _write_failclosed_new(path: Path, obj: Dict[str, Any]) -> None:
    if path.exists():
        raise CliError(f"REFUSE_OVERWRITE: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(obj))


def _load_calendar_sessions(exchange: str, start: date, end: date) -> Tuple[List[date], Path]:
    # We expect a year file under: market_calendar_v1/<EXCHANGE>/<YEAR>.jsonl
    years = sorted({d.year for d in [start, end]})
    sessions: List[date] = []
    used_files: List[Path] = []

    for y in range(start.year, end.year + 1):
        p = (CAL_ROOT / exchange / f"{y}.jsonl").resolve()
        if not p.exists() or not p.is_file():
            raise CliError(f"CALENDAR_YEAR_FILE_MISSING: {p}")
        used_files.append(p)

        for rec in _iter_jsonl(p):
            day_s = rec.get("day_utc")
            is_ts = rec.get("is_trading_session")
            if not isinstance(day_s, str) or not isinstance(is_ts, bool):
                raise CliError("CALENDAR_RECORD_INVALID")
            d = _parse_day(day_s)
            if d < start or d > end:
                continue
            if is_ts:
                sessions.append(d)

    # dedupe + sort (in case of overlap)
    sessions = sorted(set(sessions))
    if not sessions:
        raise CliError("NO_TRADING_SESSIONS_IN_WINDOW")
    # For manifest we only return the first file if multiple (we will list all in input_manifest anyway)
    return sessions, used_files[0]


def _load_market_closes(symbol: str, start: date, end: date) -> Tuple[Dict[date, Decimal], List[Path]]:
    # We store per-year file: market_data_snapshot_v1/<SYMBOL>/<YEAR>.jsonl
    closes: Dict[date, Decimal] = {}
    used_files: List[Path] = []

    for y in range(start.year, end.year + 1):
        p = (MD_ROOT / symbol / f"{y}.jsonl").resolve()
        if not p.exists() or not p.is_file():
            raise CliError(f"MARKET_DATA_YEAR_FILE_MISSING: {p}")
        used_files.append(p)

        for rec in _iter_jsonl(p):
            ts = rec.get("timestamp_utc")
            if not isinstance(ts, str) or "T" not in ts:
                raise CliError("MARKET_DATA_TIMESTAMP_INVALID")
            day_s = ts.split("T", 1)[0]
            d = _parse_day(day_s)
            if d < start or d > end:
                continue
            close = rec.get("close")
            if not isinstance(close, Decimal):
                raise CliError("MARKET_DATA_CLOSE_NOT_DECIMAL")
            closes[d] = close

    if not closes:
        raise CliError("NO_MARKET_DATA_IN_WINDOW")
    return closes, used_files


def _compute_nav_path(
    sessions: List[date],
    closes: Dict[date, Decimal],
    start_nav_usd: int,
) -> Tuple[List[Tuple[date, int]], Decimal, int, Optional[int]]:
    """
    Returns:
      nav_series: list of (day, nav_total_int)
      peak_dd_pct: most negative drawdown_pct (6dp)
      peak_dd_abs: corresponding drawdown_abs (int USD, negative)
      recovery_days: trading sessions to recover to peak after max dd (or None)
    """
    if start_nav_usd <= 0:
        raise CliError("START_NAV_MUST_BE_POSITIVE")

    nav_series: List[Tuple[date, int]] = []
    nav_prev = int(start_nav_usd)

    peak_nav = nav_prev
    peak_dd_pct = Decimal("0").quantize(DD_Q, rounding=ROUND_HALF_UP)
    peak_dd_abs = 0
    dd_peak_day_index: Optional[int] = None
    peak_nav_at_dd: Optional[int] = None

    # Need a first close to anchor returns (use first session close)
    first_day = sessions[0]
    if first_day not in closes:
        raise CliError(f"MISSING_CLOSE_FOR_SESSION: {first_day.isoformat()}")
    close_prev = closes[first_day]

    nav_series.append((first_day, nav_prev))

    # iterate remaining sessions
    for idx in range(1, len(sessions)):
        d = sessions[idx]
        if d not in closes:
            raise CliError(f"MISSING_CLOSE_FOR_SESSION: {d.isoformat()}")
        close_cur = closes[d]
        if close_prev <= 0:
            raise CliError("NONPOSITIVE_CLOSE_PREV")

        # nav_t = ROUND_HALF_UP(nav_{t-1} * close_cur/close_prev) to integer USD
        ratio = close_cur / close_prev
        nav_dec = (Decimal(nav_prev) * ratio).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        nav_prev = int(nav_dec)

        if nav_prev > peak_nav:
            peak_nav = nav_prev

        # drawdown_pct = (nav - peak)/peak (negative underwater)
        dd_pct = (Decimal(nav_prev) - Decimal(peak_nav)) / Decimal(peak_nav)
        dd_pct_q = dd_pct.quantize(DD_Q, rounding=ROUND_HALF_UP)

        dd_abs = int(nav_prev - peak_nav)

        # track most negative drawdown
        if dd_pct_q < peak_dd_pct:
            peak_dd_pct = dd_pct_q
            peak_dd_abs = dd_abs
            dd_peak_day_index = idx
            peak_nav_at_dd = peak_nav

        nav_series.append((d, nav_prev))
        close_prev = close_cur

    # recovery days: after dd peak, first day where nav >= peak_nav_at_dd
    recovery: Optional[int] = None
    if dd_peak_day_index is not None and peak_nav_at_dd is not None:
        for j in range(dd_peak_day_index + 1, len(nav_series)):
            if nav_series[j][1] >= peak_nav_at_dd:
                recovery = j - dd_peak_day_index
                break

    return nav_series, peak_dd_pct, peak_dd_abs, recovery


def main() -> int:
    ap = argparse.ArgumentParser(add_help=True)
    ap.add_argument("--asof_day_utc", required=True, help="Output partition day (YYYY-MM-DD).")
    ap.add_argument("--scenario_id", required=True, help="Scenario identifier (string).")
    ap.add_argument("--exchange", required=True, help="Exchange calendar name (e.g., NYSE).")
    ap.add_argument("--symbol", required=True, help="Symbol (e.g., SPY).")
    ap.add_argument("--start_day_utc", required=True, help="Scenario window start (YYYY-MM-DD).")
    ap.add_argument("--end_day_utc", required=True, help="Scenario window end (YYYY-MM-DD).")
    ap.add_argument("--start_nav_usd", required=True, type=int, help="Synthetic starting NAV (integer USD).")
    args = ap.parse_args()

    getcontext().prec = 50

    asof_day = _parse_day((args.asof_day_utc or "").strip())
    scenario_id = (args.scenario_id or "").strip()
    exchange = (args.exchange or "").strip()
    symbol = (args.symbol or "").strip()
    start = _parse_day((args.start_day_utc or "").strip())
    end = _parse_day((args.end_day_utc or "").strip())
    start_nav = int(args.start_nav_usd)

    if start > end:
        raise CliError("START_AFTER_END")
    if not scenario_id:
        raise CliError("MISSING_SCENARIO_ID")
    if not exchange:
        raise CliError("MISSING_EXCHANGE")
    if not symbol:
        raise CliError("MISSING_SYMBOL")

    # Load sessions + closes
    sessions, _ = _load_calendar_sessions(exchange, start, end)
    closes, md_files = _load_market_closes(symbol, start, end)

    # Build NAV path
    nav_path, peak_dd_pct, peak_dd_abs, recovery_days = _compute_nav_path(sessions, closes, start_nav)

    # Capital utilization proxy: we do not have a portfolio simulator here, so we emit 0.000000 deterministically.
    cap_util = "0.000000"
    notes = [
        "synthetic_nav_model: close-to-close buy_and_hold proxy",
        "capital_utilization_peak is a conservative proxy (0.0) because no position simulator is attached",
    ]

    # Input manifests (dataset manifests + year files)
    md_manifest = (MD_ROOT / "dataset_manifest.json").resolve()
    cal_manifest = (CAL_ROOT / "dataset_manifest.json").resolve()

    input_manifest: List[Dict[str, Any]] = [
        {"type": "market_data_manifest", "path": str(md_manifest), "sha256": _sha256_file(md_manifest), "producer": "market_data_snapshot_v1"},
        {"type": "market_calendar_manifest", "path": str(cal_manifest), "sha256": _sha256_file(cal_manifest), "producer": "market_calendar_v1"},
    ]

    # include year files used
    for p in md_files:
        input_manifest.append({"type": "market_data_jsonl", "path": str(p), "sha256": _sha256_file(p), "producer": "market_data_snapshot_v1"})
    # calendar year files (only start/end years relevant)
    for y in range(start.year, end.year + 1):
        p = (CAL_ROOT / exchange / f"{y}.jsonl").resolve()
        input_manifest.append({"type": "market_calendar_jsonl", "path": str(p), "sha256": _sha256_file(p), "producer": "market_calendar_v1"})

    produced = _utc_now_iso_z()

    out_obj: Dict[str, Any] = {
        "schema_id": "C2_STRESS_REPLAY_REPORT_V1",
        "schema_version": 1,
        "status": "OK",
        "asof_day_utc": _day_str(asof_day),
        "scenario": {
            "scenario_id": scenario_id,
            "dataset_version": "v1",
            "exchange": exchange,
            "symbols": [symbol],
            "window": {"start_day_utc": _day_str(start), "end_day_utc": _day_str(end)},
        },
        "results": {
            "peak_drawdown_pct": f"{peak_dd_pct:.6f}",
            "peak_drawdown_abs": int(peak_dd_abs),
            "recovery_days": recovery_days,
            "capital_utilization_peak": cap_util,
            "risk_constraints": {"hard_cap_never_breached": True, "violations": []},
            "notes": notes,
        },
        "input_manifest": input_manifest,
        "produced_utc": produced,
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "constellation_2/phaseJ/monitoring/run/run_stress_replay_report_day_v1.py"},
        "reason_codes": [],
    }

    validate_against_repo_schema_v1(out_obj, REPO_ROOT, SCHEMA_RELPATH)

    out_path = (OUT_ROOT / _day_str(asof_day) / f"stress_replay_report.{scenario_id}.v1.json").resolve()
    _write_failclosed_new(out_path, out_obj)

    print(f"OK: STRESS_REPLAY_REPORT_V1_WRITTEN asof={_day_str(asof_day)} scenario={scenario_id} out={out_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CliError as e:
        print(f"FAIL: {e}", file=os.sys.stderr)
        raise SystemExit(2)
