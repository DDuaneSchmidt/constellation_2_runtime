from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.phaseF.accounting.lib.immut_write_v1 import write_file_immutable_v1

# Drawdown convention authority (canonical, negative underwater)
# Contract: C2_DRAWDOWN_CONVENTION_V1
C2_DRAWDOWN_CONTRACT_ID = "C2_DRAWDOWN_CONVENTION_V1"
DRAWDOWN_QUANT = Decimal("0.000001")  # 6dp


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()
ALLOC_ROOT = (TRUTH_ROOT / "allocation_v1").resolve()

SCHEMA_SUMMARY = "governance/04_DATA/SCHEMAS/C2/ALLOCATION/allocation_summary.v1.schema.json"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_json_obj(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(str(path))
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(path)}")
    return obj


def _sha256_file(path: Path) -> str:
    import hashlib
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _lock_git_sha_if_exists(existing_path: Path, provided_sha: str) -> Optional[str]:
    if existing_path.exists() and existing_path.is_file():
        ex = _read_json_obj(existing_path)
        prod = ex.get("producer")
        ex_sha = prod.get("git_sha") if isinstance(prod, dict) else None
        if isinstance(ex_sha, str) and ex_sha.strip():
            if ex_sha.strip() != provided_sha:
                return ex_sha.strip()
    return None


def _parse_dd_pct_str_or_fail(nav_obj: Dict[str, Any]) -> Tuple[int, int, int, str]:
    """
    Fail-closed: require accounting nav history has populated drawdown fields.
    Returns: (nav_total_int, peak_nav_int, drawdown_abs_int, drawdown_pct_str)
    """
    nav = nav_obj.get("nav")
    if not isinstance(nav, dict):
        raise ValueError("ACCOUNTING_NAV_OBJECT_MISSING")
    nav_total = nav.get("nav_total")
    if not isinstance(nav_total, int):
        raise ValueError("ACCOUNTING_NAV_TOTAL_NOT_INT")

    hist = nav_obj.get("history")
    if not isinstance(hist, dict):
        raise ValueError("ACCOUNTING_HISTORY_MISSING")
    peak_nav = hist.get("peak_nav")
    dd_abs = hist.get("drawdown_abs")
    dd_pct = hist.get("drawdown_pct")
    if not isinstance(peak_nav, int):
        raise ValueError("ACCOUNTING_PEAK_NAV_NOT_INT")
    if not isinstance(dd_abs, int):
        raise ValueError("ACCOUNTING_DRAWDOWN_ABS_NOT_INT")
    if not isinstance(dd_pct, str) or not dd_pct.strip():
        raise ValueError("ACCOUNTING_DRAWDOWN_PCT_MISSING_OR_NOT_STRING")

    # Quantize/normalize dd_pct to 6dp for strict comparison (string must already be 6dp)
    d = Decimal(dd_pct).quantize(DRAWDOWN_QUANT, rounding=ROUND_HALF_UP)
    dd_pct_s = f"{d:.6f}"
    return int(nav_total), int(peak_nav), int(dd_abs), dd_pct_s


def drawdown_multiplier_v1(drawdown_pct_s: str) -> str:
    """
    Canonical multiplier rule per C2_DRAWDOWN_CONVENTION_V1 and G_THROTTLE_RULES_V1.
    Returns multiplier as string with 2 dp.
    """
    dd = Decimal(drawdown_pct_s).quantize(DRAWDOWN_QUANT, rounding=ROUND_HALF_UP)

    if dd <= Decimal("-0.150000"):
        return "0.25"
    if dd <= Decimal("-0.100000"):
        return "0.50"
    if dd <= Decimal("-0.050000"):
        return "0.75"
    return "1.00"


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="run_allocation_day_v1",
        description="C2 Bundle G Allocation v1 (bootstrap: summary + audit-grade drawdown enforcement block).",
    )
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--producer_git_sha", required=True, help="Producing git sha (explicit)")
    ap.add_argument("--producer_repo", default="constellation_2_runtime", help="Producer repo id")
    args = ap.parse_args(argv)

    day_utc = args.day_utc.strip()
    producer_sha = str(args.producer_git_sha).strip()
    producer_repo = str(args.producer_repo).strip()
    module = "constellation_2/phaseG/allocation/run/run_allocation_day_v1.py"

    summary_dir = (ALLOC_ROOT / "summary" / day_utc).resolve()
    summary_path = summary_dir / "summary.json"

    ex_sha = _lock_git_sha_if_exists(summary_path, producer_sha)
    if ex_sha is not None:
        print(f"FAIL: PRODUCER_GIT_SHA_MISMATCH_FOR_EXISTING_DAY: existing={ex_sha} provided={producer_sha}", file=sys.stderr)
        return 4

    produced_utc = f"{day_utc}T00:00:00Z"

    nav_path = (TRUTH_ROOT / "accounting_v1" / "nav" / day_utc / "nav.json").resolve()
    try:
        nav_obj = _read_json_obj(nav_path)
        nav_status = str(nav_obj.get("status") or "").strip() or "UNKNOWN"
        nav_sha = _sha256_file(nav_path)
        nav_total, peak_nav, dd_abs, dd_pct_s = _parse_dd_pct_str_or_fail(nav_obj)
    except Exception as e:
        print(f"FAIL: ACCOUNTING_DAY_NAV_MISSING_OR_INVALID: {e}", file=sys.stderr)
        return 2

    pos_eff_ptr_path = (TRUTH_ROOT / "positions_v1" / "effective_v1" / "days" / day_utc / "positions_effective_pointer.v1.json").resolve()
    pos_eff_ptr_sha: Optional[str] = None
    if pos_eff_ptr_path.exists() and pos_eff_ptr_path.is_file():
        try:
            _ = _read_json_obj(pos_eff_ptr_path)
            pos_eff_ptr_sha = _sha256_file(pos_eff_ptr_path)
        except Exception as e:
            print(f"FAIL: POSITIONS_EFFECTIVE_POINTER_INVALID: {e}", file=sys.stderr)
            return 2

    # Deterministic drawdown enforcement block (always present; fail-closed handled above)
    mult = drawdown_multiplier_v1(dd_pct_s)
    thresholds = [
        {"drawdown_pct": "0.000000", "multiplier": "1.00"},
        {"drawdown_pct": "-0.050000", "multiplier": "0.75"},
        {"drawdown_pct": "-0.100000", "multiplier": "0.50"},
        {"drawdown_pct": "-0.150000", "multiplier": "0.25"},
    ]

    dd_block = {
        "contract_id": C2_DRAWDOWN_CONTRACT_ID,
        "nav_source_path": str(nav_path),
        "nav_source_sha256": nav_sha,
        "nav_asof_day_utc": day_utc,
        "rolling_peak_nav": int(peak_nav),
        "nav_total": int(nav_total),
        "drawdown_abs": int(dd_abs),
        "drawdown_pct": dd_pct_s,
        "multiplier": mult,
        "thresholds": thresholds,
    }

    reason_codes: List[str] = []
    notes: List[str] = []

    if nav_status != "OK":
        reason_codes.append("G_BLOCK_ACCOUNTING_NOT_OK")
        notes.append("bootstrap: no intents processed; accounting not OK would block all new entries")
    else:
        reason_codes.append("G_ACCOUNTING_OK")
        notes.append("bootstrap: no intents processed")

    input_manifest: List[Dict[str, Any]] = [
        {"type": "other", "path": str(nav_path), "sha256": nav_sha, "day_utc": day_utc, "producer": "bundle_f_accounting_v1"}
    ]
    if pos_eff_ptr_sha is not None:
        input_manifest.append({"type": "other", "path": str(pos_eff_ptr_path), "sha256": pos_eff_ptr_sha, "day_utc": day_utc, "producer": "positions_effective_v1"})

    summary_obj: Dict[str, Any] = {
        "schema_id": "C2_ALLOCATION_SUMMARY_V1",
        "schema_version": 1,
        "produced_utc": produced_utc,
        "day_utc": day_utc,
        "producer": {"repo": producer_repo, "git_sha": producer_sha, "module": module},
        "status": "OK",
        "reason_codes": reason_codes,
        "input_manifest": input_manifest,
        "summary": {"decisions": [], "counts": {"allow": 0, "block": 0}, "notes": notes, "drawdown_enforcement": dd_block},
    }

    validate_against_repo_schema_v1(summary_obj, REPO_ROOT, SCHEMA_SUMMARY)
    s_bytes = canonical_json_bytes_v1(summary_obj) + b"\n"
    _ = write_file_immutable_v1(path=summary_path, data=s_bytes, create_dirs=True)

    print("OK: ALLOCATION_BOOTSTRAP_SUMMARY_WRITTEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
