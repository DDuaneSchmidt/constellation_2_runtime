#!/usr/bin/env python3
"""
run_capital_efficiency_day_v1.py

Phase J — Capital Efficiency Diagnostics v1
Deterministic, fail-closed, single-writer, canonical JSON, schema-validated.

Inputs (immutable truth):
- accounting_v1/nav/<DAY>/nav.json
- accounting_v1/exposure/<DAY>/exposure.json
- allocation_v1/summary/<DAY>/summary.json (for drawdown multiplier evidence; not required for arithmetic)
- defined_risk_v1/snapshots/<DAY>/defined_risk_snapshot.v1.json (optional; may be missing in bootstrap)

Output (immutable truth):
- monitoring_v1/capital_efficiency/<DAY>/capital_efficiency.v1.json

Bootstrap rules (fail-closed, deterministic):
- If nav_total == 0 and open_risk_total == 0 => utilization=0.000000, idle=1.000000
- If nav_total == 0 and open_risk_total != 0 => FAIL (division by zero would conceal risk)
- risk_budget_total is null unless a governed risk budget truth spine exists (not proven here)
- return_per_risk_unit is null unless both a return window and nonzero risk denominator are provable
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

NAV_PATH_ROOT = (TRUTH / "accounting_v1/nav").resolve()
EXP_PATH_ROOT = (TRUTH / "accounting_v1/exposure").resolve()
ALLOC_SUM_ROOT = (TRUTH / "allocation_v1/summary").resolve()
DEF_RISK_ROOT = (TRUTH / "defined_risk_v1/snapshots").resolve()

OUT_ROOT = (TRUTH / "monitoring_v1/capital_efficiency").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/MONITORING/capital_efficiency.v1.schema.json"

Q6 = Decimal("0.000001")
Q8 = Decimal("0.00000001")


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


def _read_json(p: Path) -> Any:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        raise CliError(f"JSON_READ_FAILED: {p}: {e}") from e


def _must_file(p: Path) -> Path:
    if not p.exists() or not p.is_file():
        raise CliError(f"MISSING_FILE: {p}")
    return p


def _quant6(x: Decimal) -> str:
    return f"{x.quantize(Q6, rounding=ROUND_HALF_UP):.6f}"


def _quant8(x: Decimal) -> str:
    return f"{x.quantize(Q8, rounding=ROUND_HALF_UP):.8f}"


def _write_failclosed_new(path: Path, obj: Dict[str, Any]) -> None:
    if path.exists():
        raise CliError(f"REFUSE_OVERWRITE: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(obj))


def build(day_utc: str) -> Dict[str, Any]:
    nav_p = _must_file(NAV_PATH_ROOT / day_utc / "nav.json")
    exp_p = _must_file(EXP_PATH_ROOT / day_utc / "exposure.json")
    alloc_p = _must_file(ALLOC_SUM_ROOT / day_utc / "summary.json")

    nav_obj = _read_json(nav_p)
    exp_obj = _read_json(exp_p)
    _ = _read_json(alloc_p)  # only for manifest + proof chain

    # nav_total
    nav = nav_obj.get("nav")
    if not isinstance(nav, dict):
        raise CliError("NAV_FIELD_MISSING")
    nav_total = nav.get("nav_total")
    if not isinstance(nav_total, int) or nav_total < 0:
        raise CliError("NAV_TOTAL_INVALID")

    # exposure fields
    exp = exp_obj.get("exposure")
    if not isinstance(exp, dict):
        raise CliError("EXPOSURE_FIELD_MISSING")
    currency = exp.get("currency")
    if not isinstance(currency, str) or not currency:
        raise CliError("EXPOSURE_CURRENCY_INVALID")
    open_risk_total = exp.get("defined_risk_total")
    if not isinstance(open_risk_total, int) or open_risk_total < 0:
        raise CliError("DEFINED_RISK_TOTAL_INVALID")

    # Optional defined risk snapshot
    defrisk_p = (DEF_RISK_ROOT / day_utc / "defined_risk_snapshot.v1.json").resolve()
    has_defined_risk = defrisk_p.exists() and defrisk_p.is_file()

    reason_codes: List[str] = []
    status = "OK"
    notes: List[str] = []

    if not has_defined_risk:
        status = "DEGRADED_MISSING_INPUTS"
        reason_codes.append("J_CAPEFF_MISSING_DEFINED_RISK_SNAPSHOT")
        notes.append("defined_risk_snapshot missing for day; using accounting exposure defined_risk_total only")

    # utilization + idle logic
    util_s: Optional[str]
    idle_s: Optional[str]

    if nav_total == 0:
        if open_risk_total != 0:
            raise CliError("DIV0_NAV_WITH_NONZERO_OPEN_RISK")
        util_s = _quant6(Decimal("0"))
        idle_s = _quant6(Decimal("1"))
        status = "DEGRADED_MISSING_INPUTS" if status == "OK" else status
        reason_codes.append("J_CAPEFF_NAV_ZERO_BOOTSTRAP")
        notes.append("nav_total=0; utilization defined as 0 when open_risk_total=0 (bootstrap)")
    else:
        util = (Decimal(open_risk_total) / Decimal(nav_total))
        if util < 0:
            util = Decimal("0")
        if util > 1:
            # allow but note; clamp for utilization percent reporting
            reason_codes.append("J_CAPEFF_UTIL_OVER_1_CLAMPED")
            util = Decimal("1")
        util_s = _quant6(util)
        idle = Decimal("1") - util
        if idle < 0:
            idle = Decimal("0")
        idle_s = _quant6(idle)

    # risk_budget_total is not proven in truth yet => null
    risk_budget_total = None
    risk_budget_efficiency = None
    notes.append("risk_budget_total is null: no governed risk budget truth spine proven for Phase J")

    # by_engine utilization: from exposure.by_engine[]
    by_engine_out: List[Dict[str, Any]] = []
    be = exp.get("by_engine")
    if isinstance(be, list):
        for it in be:
            if not isinstance(it, dict):
                continue
            key = it.get("key")
            dr = it.get("defined_risk")
            if not isinstance(key, str) or not key:
                continue
            if not isinstance(dr, int) or dr < 0:
                continue

            if nav_total == 0:
                if dr != 0:
                    # would imply risk but zero nav; fail-closed to avoid nonsense ratios
                    raise CliError("DIV0_NAV_WITH_NONZERO_ENGINE_DEFINED_RISK")
                eng_util = _quant6(Decimal("0"))
            else:
                eng_util = _quant6(Decimal(dr) / Decimal(nav_total))

            by_engine_out.append(
                {
                    "engine_id": key,
                    "utilization_rate": eng_util,
                    "return_per_risk_unit": None,
                    "notes": ["return_per_risk_unit null: no provable return/risk denominator yet"],
                }
            )
    else:
        status = "DEGRADED_MISSING_INPUTS" if status == "OK" else status
        reason_codes.append("J_CAPEFF_MISSING_BY_ENGINE")

    input_manifest: List[Dict[str, Any]] = [
        {"type": "accounting_nav", "path": str(nav_p), "sha256": _sha256_file(nav_p), "producer": "bundle_f_accounting_v1"},
        {"type": "accounting_exposure", "path": str(exp_p), "sha256": _sha256_file(exp_p), "producer": "bundle_f_accounting_v1"},
        {"type": "allocation_summary", "path": str(alloc_p), "sha256": _sha256_file(alloc_p), "producer": "bundle_g_allocation_v1"},
    ]
    if has_defined_risk:
        input_manifest.append({"type": "defined_risk_snapshot", "path": str(defrisk_p), "sha256": _sha256_file(defrisk_p), "producer": "defined_risk_v1"})

    obj: Dict[str, Any] = {
        "schema_id": "C2_CAPITAL_EFFICIENCY_V1",
        "schema_version": 1,
        "status": status,
        "day_utc": day_utc,
        "diagnostics": {
            "nav_total": int(nav_total),
            "risk_budget_total": risk_budget_total,
            "open_risk_total": int(open_risk_total),
            "capital_utilization_rate": util_s,
            "idle_capital_pct": idle_s,
            "risk_budget_efficiency": risk_budget_efficiency,
            "notes": notes,
        },
        "by_engine": by_engine_out,
        "input_manifest": input_manifest,
        "produced_utc": _utc_now_iso_z(),
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "constellation_2/phaseJ/monitoring/run/run_capital_efficiency_day_v1.py"},
        "reason_codes": reason_codes,
    }

    validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
    return obj


def main() -> int:
    ap = argparse.ArgumentParser(add_help=True)
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()

    day = (args.day_utc or "").strip()
    if not day:
        raise CliError("MISSING_DAY_UTC")

    obj = build(day)
    out_file = (OUT_ROOT / day / "capital_efficiency.v1.json").resolve()
    _write_failclosed_new(out_file, obj)

    print(f"OK: CAPITAL_EFFICIENCY_V1_WRITTEN day={day} out={out_file}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CliError as e:
        print(f"FAIL: {e}", file=os.sys.stderr)
        raise SystemExit(2)
