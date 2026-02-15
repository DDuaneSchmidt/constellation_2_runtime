#!/usr/bin/env python3
"""
run_engine_daily_returns_day_v1.py

Phase J substrate:
Engine Daily Returns v1 writer (single-writer, deterministic, fail-closed)

Inputs (immutable truth):
- accounting attribution: constellation_2/runtime/truth/accounting_v1/attribution/<DAY>/engine_attribution.json
- accounting nav:        constellation_2/runtime/truth/accounting_v1/nav/<DAY>/nav.json

Daily return definition (deterministic):
- engine_pnl_to_date(D) = realized_pnl_to_date(D) + unrealized_pnl(D)
- delta_engine_pnl(D) = engine_pnl_to_date(D) - engine_pnl_to_date(prev_day)
- engine_daily_return(D) = delta_engine_pnl(D) / nav_total(prev_day)

Fail-closed:
- If prev nav_total == 0 and delta_engine_pnl != 0 -> FAIL
- Refuse overwrite of output file
- Canonical JSON write
- Validate against governed schema:
  governance/04_DATA/SCHEMAS/C2/MONITORING/engine_daily_returns.v1.schema.json
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP, getcontext
from pathlib import Path
from typing import Any, Dict, List, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

ATTR_ROOT = (TRUTH / "accounting_v1/attribution").resolve()
NAV_ROOT = (TRUTH / "accounting_v1/nav").resolve()

OUT_ROOT = (TRUTH / "monitoring_v1/engine_daily_returns").resolve()

SCHEMA_RELPATH_OUT = "governance/04_DATA/SCHEMAS/C2/MONITORING/engine_daily_returns.v1.schema.json"

RET_Q = Decimal("0.00000001")  # 8dp


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
    h.update(p.read_bytes())
    return h.hexdigest()


def _parse_day(s: str) -> date:
    try:
        return date.fromisoformat(s)
    except Exception as e:  # noqa: BLE001
        raise CliError(f"BAD_DAY_UTC: {s}: {e}") from e


def _day_str(d: date) -> str:
    return d.isoformat()


def _read_json(p: Path) -> Any:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        raise CliError(f"JSON_READ_FAILED: {p}: {e}") from e


def _quant_ret(x: Decimal) -> Decimal:
    return x.quantize(RET_Q, rounding=ROUND_HALF_UP)


def _list_days(root: Path) -> List[date]:
    if not root.exists() or not root.is_dir():
        raise CliError(f"MISSING_ROOT_DIR: {root}")
    out: List[date] = []
    for child in sorted(root.iterdir()):
        if child.is_dir():
            try:
                out.append(_parse_day(child.name))
            except CliError:
                continue
    return sorted(out)


def _prev_day(all_days: List[date], day: date) -> date:
    if day not in all_days:
        raise CliError(f"DAY_NOT_FOUND: {day.isoformat()}")
    idx = all_days.index(day)
    if idx == 0:
        raise CliError("NO_PREVIOUS_DAY_AVAILABLE")
    return all_days[idx - 1]


def _load_nav_total(d: date) -> int:
    p = (NAV_ROOT / _day_str(d) / "nav.json").resolve()
    if not p.exists() or not p.is_file():
        raise CliError(f"NAV_MISSING: {p}")
    obj = _read_json(p)
    if not isinstance(obj, dict):
        raise CliError("NAV_OBJ_NOT_DICT")
    nav = obj.get("nav")
    if not isinstance(nav, dict):
        raise CliError("NAV_FIELD_MISSING")
    nt = nav.get("nav_total")
    if not isinstance(nt, int) or nt < 0:
        raise CliError("NAV_TOTAL_INVALID")
    return nt


def _load_attr(d: date) -> Dict[str, Any]:
    p = (ATTR_ROOT / _day_str(d) / "engine_attribution.json").resolve()
    if not p.exists() or not p.is_file():
        raise CliError(f"ATTR_MISSING: {p}")
    obj = _read_json(p)
    if not isinstance(obj, dict):
        raise CliError("ATTR_OBJ_NOT_DICT")
    return obj


def _currency_and_pnl_map(attr_obj: Dict[str, Any]) -> Tuple[str, Dict[str, int]]:
    a = attr_obj.get("attribution")
    if not isinstance(a, dict):
        raise CliError("ATTRIBUTION_FIELD_MISSING")
    currency = a.get("currency")
    if not isinstance(currency, str) or not currency:
        raise CliError("ATTR_CURRENCY_INVALID")
    be = a.get("by_engine")
    if not isinstance(be, list):
        raise CliError("ATTR_BY_ENGINE_INVALID")

    out: Dict[str, int] = {}
    for it in be:
        if not isinstance(it, dict):
            continue
        eid = it.get("engine_id")
        if not isinstance(eid, str) or not eid:
            continue
        rp = it.get("realized_pnl_to_date")
        up = it.get("unrealized_pnl")
        if not isinstance(rp, int) or not isinstance(up, int):
            raise CliError("ATTR_PNL_FIELDS_NOT_INT")
        out[eid] = int(rp + up)
    return currency, out


def _write_failclosed_new(path: Path, obj: Dict[str, Any]) -> None:
    if path.exists():
        raise CliError(f"REFUSE_OVERWRITE: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    b = canonical_json_bytes_v1(obj)
    path.write_bytes(b)


def build(day_utc: str) -> Dict[str, Any]:
    d = _parse_day(day_utc)
    days = _list_days(ATTR_ROOT)
    prev = _prev_day(days, d)

    nav_prev = _load_nav_total(prev)

    attr_prev = _load_attr(prev)
    attr_cur = _load_attr(d)

    c0, pnl_prev = _currency_and_pnl_map(attr_prev)
    c1, pnl_cur = _currency_and_pnl_map(attr_cur)
    if c0 != c1:
        raise CliError("CURRENCY_MISMATCH")
    currency = c0

    engines = sorted(set(pnl_prev.keys()) | set(pnl_cur.keys()))
    if not engines:
        raise CliError("NO_ENGINES_FOUND")

    out_rows: List[Dict[str, Any]] = []
    for eid in engines:
        p0 = pnl_prev.get(eid, 0)
        p1 = pnl_cur.get(eid, 0)
        delta = int(p1 - p0)

        if nav_prev == 0:
            if delta != 0:
                raise CliError("DIV0_PREV_NAV_WITH_NONZERO_DELTA_PNL")
            r = Decimal("0")
        else:
            r = Decimal(delta) / Decimal(nav_prev)

        rq = _quant_ret(r)
        out_rows.append({"engine_id": eid, "daily_return": f"{rq:.8f}"})

    # input manifest includes prev+cur attr and prev nav
    prev_attr_path = (ATTR_ROOT / _day_str(prev) / "engine_attribution.json").resolve()
    cur_attr_path = (ATTR_ROOT / _day_str(d) / "engine_attribution.json").resolve()
    prev_nav_path = (NAV_ROOT / _day_str(prev) / "nav.json").resolve()

    input_manifest = [
        {"type": "accounting_attr", "path": str(prev_attr_path), "sha256": _sha256_file(prev_attr_path), "producer": "bundle_f_accounting_v1", "day_utc": _day_str(prev)},
        {"type": "accounting_attr", "path": str(cur_attr_path), "sha256": _sha256_file(cur_attr_path), "producer": "bundle_f_accounting_v1", "day_utc": _day_str(d)},
        {"type": "accounting_nav", "path": str(prev_nav_path), "sha256": _sha256_file(prev_nav_path), "producer": "bundle_f_accounting_v1", "day_utc": _day_str(prev)},
    ]

    obj: Dict[str, Any] = {
        "schema_id": "C2_ENGINE_DAILY_RETURNS_V1",
        "schema_version": 1,
        "status": "OK",
        "day_utc": day_utc,
        "returns": {"currency": currency, "by_engine": out_rows},
        "input_manifest": input_manifest,
        "produced_utc": _utc_now_iso_z(),
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "constellation_2/phaseJ/monitoring/run/run_engine_daily_returns_day_v1.py"},
        "reason_codes": [],
    }

    validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH_OUT)
    return obj


def main() -> int:
    ap = argparse.ArgumentParser(add_help=True)
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()

    getcontext().prec = 50

    day_utc = (args.day_utc or "").strip()
    if not day_utc:
        raise CliError("MISSING_DAY_UTC")

    obj = build(day_utc)

    out_path = (OUT_ROOT / day_utc / "engine_daily_returns.v1.json").resolve()
    _write_failclosed_new(out_path, obj)

    print(f"OK: ENGINE_DAILY_RETURNS_V1_WRITTEN day={day_utc} out={out_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CliError as e:
        print(f"FAIL: {e}", file=os.sys.stderr)
        raise SystemExit(2)
