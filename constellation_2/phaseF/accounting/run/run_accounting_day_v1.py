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
from constellation_2.phaseF.accounting.lib.paths_v1 import REPO_ROOT, day_paths_v1 as accounting_day_paths_v1
from constellation_2.phaseF.cash_ledger.lib.paths_v1 import day_paths_v1 as cash_day_paths_v1
from constellation_2.phaseF.positions.lib.paths_effective_v1 import day_paths_effective_v1 as pos_effective_day_paths_v1
from constellation_2.phaseF.positions.lib.paths_v2 import day_paths_v2 as pos_day_paths_v2
from constellation_2.phaseF.positions.lib.paths_v3 import day_paths_v3 as pos_day_paths_v3
from constellation_2.phaseF.positions.lib.paths_v4 import day_paths_v4 as pos_day_paths_v4
from constellation_2.phaseF.position_lifecycle.lib.paths_v1 import day_paths_v1 as lifecycle_day_paths_v1
from constellation_2.phaseF.defined_risk.lib.paths_v1 import day_paths_v1 as defined_risk_day_paths_v1


SCHEMA_NAV = "governance/04_DATA/SCHEMAS/C2/ACCOUNTING/accounting_nav.v1.schema.json"
SCHEMA_EXPOSURE = "governance/04_DATA/SCHEMAS/C2/ACCOUNTING/accounting_exposure.v1.schema.json"
SCHEMA_ATTR = "governance/04_DATA/SCHEMAS/C2/ACCOUNTING/accounting_attribution.v1.schema.json"
SCHEMA_LATEST = "governance/04_DATA/SCHEMAS/C2/ACCOUNTING/accounting_latest_pointer.v1.schema.json"
SCHEMA_FAILURE = "governance/04_DATA/SCHEMAS/C2/ACCOUNTING/accounting_failure.v1.schema.json"

SCHEMA_DEFINED_RISK_SNAPSHOT_V1 = "governance/04_DATA/SCHEMAS/C2/RISK/defined_risk_snapshot.v1.schema.json"

DRAWDOWN_QUANT = Decimal("0.000001")  # 6dp per C2_DRAWDOWN_CONVENTION_V1


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


def _produced_utc_idempotent(existing_path: Path, fallback: str) -> str:
    if existing_path.exists() and existing_path.is_file():
        try:
            ex = _read_json_obj(existing_path)
            pu = ex.get("produced_utc")
            if isinstance(pu, str) and pu.strip():
                return pu.strip()
        except Exception:
            pass
    return fallback


def _lock_git_sha_if_exists(existing_path: Path, provided_sha: str) -> Optional[str]:
    if existing_path.exists() and existing_path.is_file():
        ex = _read_json_obj(existing_path)
        prod = ex.get("producer")
        ex_sha = prod.get("git_sha") if isinstance(prod, dict) else None
        if isinstance(ex_sha, str) and ex_sha.strip() and ex_sha.strip() != provided_sha:
            return ex_sha.strip()
    return None


def _cents_to_int_dollars_failclosed(cents: int) -> int:
    if cents % 100 != 0:
        raise ValueError("CENTS_NOT_DIVISIBLE_BY_100_FOR_INTEGER_DOLLARS")
    return int(cents // 100)


def _expiry_bucket_from_expiry_utc(expiry_utc: str) -> str:
    s = (expiry_utc or "").strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:7]
    return "unknown"


def _parse_day_key(s: str) -> Tuple[int, int, int]:
    v = (s or "").strip()
    if len(v) != 10 or v[4] != "-" or v[7] != "-":
        raise ValueError(f"BAD_DAY_KEY: {s!r}")
    return (int(v[0:4]), int(v[5:7]), int(v[8:10]))

def _compute_peak_and_drawdown_or_fail(day_utc: str, nav_total_int: int) -> Tuple[int, int, str]:
    """
    Canonical drawdown convention C2_DRAWDOWN_CONVENTION_V1

    peak_nav_t = max(NAV_d) for d <= t
    drawdown_abs = nav_total - peak_nav
    drawdown_pct = (nav_total - peak_nav) / peak_nav  (quantized to 6dp, ROUND_HALF_UP, stored as string)

    Contract notes:
    - NAV_t must be integer >= 0 (NAV may be 0)
    - peak_nav_t must be > 0 (otherwise drawdown undefined)
    """
    if not isinstance(nav_total_int, int):
        raise ValueError("NAV_TOTAL_NOT_INT_FOR_DRAWDOWN")
    if nav_total_int < 0:
        raise ValueError("NAV_TOTAL_NEGATIVE_FOR_DRAWDOWN")

    y, m, d = _parse_day_key(day_utc)

    nav_root = (REPO_ROOT / "constellation_2" / "runtime" / "truth" / "accounting_v1" / "nav").resolve()
    if not nav_root.exists() or not nav_root.is_dir():
        # No history exists. Drawdown can only be defined if NAV is positive (peak must be > 0).
        if nav_total_int == 0:
            raise ValueError("NO_POSITIVE_PEAK_AVAILABLE_FOR_DRAWDOWN")
        peak = nav_total_int
        dd_abs = 0
        dd_pct = Decimal("0").quantize(DRAWDOWN_QUANT, rounding=ROUND_HALF_UP)
        return peak, dd_abs, f"{dd_pct:.6f}"

    # Determine peak from all prior <= day nav.json files that have integer nav_total.
    peak = 0
    for day_dir in sorted([p for p in nav_root.iterdir() if p.is_dir()], key=lambda p: p.name):
        dn = day_dir.name
        try:
            y2, m2, d2 = _parse_day_key(dn)
        except Exception:
            continue
        if (y2, m2, d2) > (y, m, d):
            continue
        nav_path = (day_dir / "nav.json").resolve()
        if not nav_path.exists() or not nav_path.is_file():
            continue
        obj = _read_json_obj(nav_path)
        nav = obj.get("nav")
        if not isinstance(nav, dict):
            continue
        v = nav.get("nav_total")
        if not isinstance(v, int):
            continue
        if v > peak:
            peak = v

    # Incorporate current day NAV into peak.
    if nav_total_int > peak:
        peak = nav_total_int

    if peak <= 0:
        raise ValueError("NO_POSITIVE_PEAK_AVAILABLE_FOR_DRAWDOWN")

    dd_abs = int(nav_total_int - peak)
    dd_pct = (Decimal(dd_abs) / Decimal(peak)).quantize(DRAWDOWN_QUANT, rounding=ROUND_HALF_UP)
    return int(peak), int(dd_abs), f"{dd_pct:.6f}"

def _build_failure(
    *,
    day_utc: str,
    producer_repo: str,
    producer_git_sha: str,
    module: str,
    status: str,
    reason_codes: List[str],
    input_manifest: List[Dict[str, Any]],
    code: str,
    message: str,
    details: Dict[str, Any],
    attempted_outputs: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "schema_id": "C2_ACCOUNTING_FAILURE_V1",
        "schema_version": 1,
        "produced_utc": _utc_now_iso(),
        "day_utc": day_utc,
        "producer": {"repo": producer_repo, "git_sha": producer_git_sha, "module": module},
        "status": status,
        "reason_codes": reason_codes,
        "input_manifest": input_manifest,
        "failure": {"code": code, "message": message, "details": details, "attempted_outputs": attempted_outputs},
    }


def _select_positions_input_for_day(day_utc: str) -> Tuple[Path, str, str, Optional[Path]]:
    """
    Deterministic selection:
    1) If positions_effective pointer exists -> use it (points to the snapshot path).
    2) Else prefer newest snapshot version present: v4, then v3, then v2.
    Fail-closed if none exist.
    """
    pos_eff = pos_effective_day_paths_v1(day_utc)
    pos_v4 = pos_day_paths_v4(day_utc)
    pos_v3 = pos_day_paths_v3(day_utc)
    pos_v2 = pos_day_paths_v2(day_utc)

    pos_ptr_path_used: Optional[Path] = None

    if pos_eff.pointer_path.exists() and pos_eff.pointer_path.is_file():
        ptr = _read_json_obj(pos_eff.pointer_path)
        p = ptr.get("pointers") if isinstance(ptr, dict) else None
        snap_path_s = p.get("snapshot_path") if isinstance(p, dict) else None
        if not isinstance(snap_path_s, str) or not snap_path_s.strip():
            raise ValueError("POSITIONS_EFFECTIVE_POINTER_INVALID: missing pointers.snapshot_path")
        pos_snapshot_path = Path(snap_path_s).resolve()
        return (pos_snapshot_path, "positions_effective_v1", "positions_effective_pointer", pos_eff.pointer_path)

    if pos_v4.snapshot_path.exists() and pos_v4.snapshot_path.is_file():
        return (pos_v4.snapshot_path, "positions_v4_snapshot", "positions_truth", pos_ptr_path_used)

    if pos_v3.snapshot_path.exists() and pos_v3.snapshot_path.is_file():
        return (pos_v3.snapshot_path, "positions_v3_snapshot", "positions_truth", pos_ptr_path_used)

    if pos_v2.snapshot_path.exists() and pos_v2.snapshot_path.is_file():
        return (pos_v2.snapshot_path, "positions_v2_snapshot", "positions_truth", pos_ptr_path_used)

    raise FileNotFoundError(f"POSITIONS_SNAPSHOT_MISSING_ALL_VERSIONS: day={day_utc}")


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="run_accounting_day_v1",
        description="C2 Bundle F Accounting v1 (bootstrap: schema-valid, deterministic, immutable).",
    )
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--producer_git_sha", required=True, help="Producing git sha (explicit)")
    ap.add_argument("--producer_repo", default="constellation_2_runtime", help="Producer repo id")
    args = ap.parse_args(argv)

    day_utc = str(args.day_utc).strip()
    producer_sha = str(args.producer_git_sha).strip()
    producer_repo = str(args.producer_repo).strip()
    module = "constellation_2/phaseF/accounting/run/run_accounting_day_v1.py"

    out = accounting_day_paths_v1(day_utc)

    for p in (out.nav_path, out.exposure_path, out.attribution_path):
        ex_sha = _lock_git_sha_if_exists(p, producer_sha)
        if ex_sha is not None:
            print(f"FAIL: PRODUCER_GIT_SHA_MISMATCH_FOR_EXISTING_DAY: existing={ex_sha} provided={producer_sha}", file=sys.stderr)
            return 4

    cash_paths = cash_day_paths_v1(day_utc)

    try:
        pos_snapshot_path, pos_producer, pos_input_type, pos_ptr_path_used = _select_positions_input_for_day(day_utc)
    except Exception as e:
        failure = _build_failure(
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_git_sha=producer_sha,
            module=module,
            status="FAIL_CORRUPT_INPUTS",
            reason_codes=["POSITIONS_SNAPSHOT_MISSING_OR_POINTER_INVALID"],
            input_manifest=[
                {"type": "other", "path": str(pos_effective_day_paths_v1(day_utc).pointer_path), "sha256": "0" * 64, "day_utc": day_utc, "producer": "positions_effective_v1"}
            ],
            code="FAIL_CORRUPT_INPUTS",
            message=str(e),
            details={"error": str(e)},
            attempted_outputs=[
                {"path": str(out.nav_path), "sha256": None},
                {"path": str(out.exposure_path), "sha256": None},
                {"path": str(out.attribution_path), "sha256": None},
            ],
        )
        validate_against_repo_schema_v1(failure, REPO_ROOT, SCHEMA_FAILURE)
        b = canonical_json_bytes_v1(failure) + b"\n"
        _ = write_file_immutable_v1(path=out.failure_path, data=b, create_dirs=True)
        print("FAIL: POSITIONS_SNAPSHOT_MISSING_OR_POINTER_INVALID (failure artifact written)", file=sys.stderr)
        return 2

    try:
        cash = _read_json_obj(cash_paths.snapshot_path)
        positions = _read_json_obj(pos_snapshot_path)
    except Exception as e:
        input_manifest = [
            {"type": "cash_ledger", "path": str(cash_paths.snapshot_path), "sha256": "0" * 64, "day_utc": day_utc, "producer": "cash_ledger_v1"},
        ]
        if pos_ptr_path_used is not None:
            input_manifest.append({"type": "other", "path": str(pos_ptr_path_used), "sha256": "0" * 64, "day_utc": day_utc, "producer": "positions_effective_v1"})
        input_manifest.append({"type": "positions_truth", "path": str(pos_snapshot_path), "sha256": "0" * 64, "day_utc": day_utc, "producer": pos_producer})

        failure = _build_failure(
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_git_sha=producer_sha,
            module=module,
            status="FAIL_CORRUPT_INPUTS",
            reason_codes=["MISSING_REQUIRED_INPUTS"],
            input_manifest=input_manifest,
            code="FAIL_CORRUPT_INPUTS",
            message=str(e),
            details={"error": str(e)},
            attempted_outputs=[
                {"path": str(out.nav_path), "sha256": None},
                {"path": str(out.exposure_path), "sha256": None},
                {"path": str(out.attribution_path), "sha256": None},
            ],
        )
        validate_against_repo_schema_v1(failure, REPO_ROOT, SCHEMA_FAILURE)
        b = canonical_json_bytes_v1(failure) + b"\n"
        _ = write_file_immutable_v1(path=out.failure_path, data=b, create_dirs=True)
        print("FAIL: MISSING_REQUIRED_INPUTS (failure artifact written)", file=sys.stderr)
        return 2

    lifecycle = None
    lifecycle_paths = lifecycle_day_paths_v1(day_utc)
    if lifecycle_paths.snapshot_path.exists() and lifecycle_paths.snapshot_path.is_file():
        try:
            lifecycle = _read_json_obj(lifecycle_paths.snapshot_path)
        except Exception as e:
            failure = _build_failure(
                day_utc=day_utc,
                producer_repo=producer_repo,
                producer_git_sha=producer_sha,
                module=module,
                status="FAIL_CORRUPT_INPUTS",
                reason_codes=["POSITION_LIFECYCLE_INVALID"],
                input_manifest=[{"type": "lifecycle_truth", "path": str(lifecycle_paths.snapshot_path), "sha256": "0" * 64, "day_utc": day_utc, "producer": "position_lifecycle_v1"}],
                code="FAIL_CORRUPT_INPUTS",
                message=str(e),
                details={"error": str(e)},
                attempted_outputs=[
                    {"path": str(out.nav_path), "sha256": None},
                    {"path": str(out.exposure_path), "sha256": None},
                    {"path": str(out.attribution_path), "sha256": None},
                ],
            )
            validate_against_repo_schema_v1(failure, REPO_ROOT, SCHEMA_FAILURE)
            b = canonical_json_bytes_v1(failure) + b"\n"
            _ = write_file_immutable_v1(path=out.failure_path, data=b, create_dirs=True)
            print("FAIL: POSITION_LIFECYCLE_INVALID (failure artifact written)", file=sys.stderr)
            return 2

    defined_risk = None
    defined_risk_paths = defined_risk_day_paths_v1(day_utc)
    if defined_risk_paths.snapshot_path.exists() and defined_risk_paths.snapshot_path.is_file():
        try:
            defined_risk = _read_json_obj(defined_risk_paths.snapshot_path)
            validate_against_repo_schema_v1(defined_risk, REPO_ROOT, SCHEMA_DEFINED_RISK_SNAPSHOT_V1)
        except Exception as e:
            failure = _build_failure(
                day_utc=day_utc,
                producer_repo=producer_repo,
                producer_git_sha=producer_sha,
                module=module,
                status="FAIL_CORRUPT_INPUTS",
                reason_codes=["DEFINED_RISK_INVALID"],
                input_manifest=[{"type": "other", "path": str(defined_risk_paths.snapshot_path), "sha256": "0" * 64, "day_utc": day_utc, "producer": "defined_risk_v1"}],
                code="FAIL_CORRUPT_INPUTS",
                message=str(e),
                details={"error": str(e)},
                attempted_outputs=[
                    {"path": str(out.nav_path), "sha256": None},
                    {"path": str(out.exposure_path), "sha256": None},
                    {"path": str(out.attribution_path), "sha256": None},
                ],
            )
            validate_against_repo_schema_v1(failure, REPO_ROOT, SCHEMA_FAILURE)
            b = canonical_json_bytes_v1(failure) + b"\n"
            _ = write_file_immutable_v1(path=out.failure_path, data=b, create_dirs=True)
            print("FAIL: DEFINED_RISK_INVALID (failure artifact written)", file=sys.stderr)
            return 2

    try:
        cash_total_cents = int(cash["snapshot"]["cash_total_cents"])
    except Exception as e:
        print(f"FAIL: CASH_LEDGER_INVALID: {e}", file=sys.stderr)
        return 4

    cash_total = _cents_to_int_dollars_failclosed(cash_total_cents)

    status = "DEGRADED_MISSING_MARKS"
    reason_codes = ["BOOTSTRAP_NAV_CASH_ONLY", "MISSING_MARKS", "MISSING_INSTRUMENT_IDENTITY"]

    try:
        sid = str(positions.get("schema_id") or "")
        sver = int(positions.get("schema_version") or 0)
        if sid == "C2_POSITIONS_SNAPSHOT_V3" and sver == 3:
            reason_codes = ["BOOTSTRAP_NAV_CASH_ONLY", "MISSING_MARKS"]
    except Exception:
        pass

    produced_utc = _produced_utc_idempotent(out.nav_path, f"{day_utc}T00:00:00Z")

    input_manifest: List[Dict[str, Any]] = [
        {"type": "cash_ledger", "path": str(cash_paths.snapshot_path), "sha256": _sha256_file(cash_paths.snapshot_path), "day_utc": day_utc, "producer": "cash_ledger_v1"},
    ]
    if pos_ptr_path_used is not None:
        input_manifest.append({"type": "other", "path": str(pos_ptr_path_used), "sha256": _sha256_file(pos_ptr_path_used), "day_utc": day_utc, "producer": "positions_effective_v1"})
    input_manifest.append({"type": "positions_truth", "path": str(pos_snapshot_path), "sha256": _sha256_file(pos_snapshot_path), "day_utc": day_utc, "producer": pos_producer})
    if lifecycle is not None:
        input_manifest.append({"type": "lifecycle_truth", "path": str(lifecycle_paths.snapshot_path), "sha256": _sha256_file(lifecycle_paths.snapshot_path), "day_utc": day_utc, "producer": "position_lifecycle_v1"})
    if defined_risk is not None:
        input_manifest.append({"type": "other", "path": str(defined_risk_paths.snapshot_path), "sha256": _sha256_file(defined_risk_paths.snapshot_path), "day_utc": day_utc, "producer": "defined_risk_v1"})

    components = [
        {
            "kind": "CASH",
            "symbol": "USD",
            "qty": cash_total,
            "mv": cash_total,
            "mark": {"bid": None, "ask": None, "last": None, "source": "CASH_LEDGER", "asof_utc": produced_utc},
        }
    ]

    try:
        peak_nav, drawdown_abs, drawdown_pct_s = _compute_peak_and_drawdown_or_fail(day_utc, int(cash_total))
    except Exception as e:
        failure = _build_failure(
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_git_sha=producer_sha,
            module=module,
            status="FAIL_CORRUPT_INPUTS",
            reason_codes=["DRAWDOWN_COMPUTE_FAILED_FAIL_CLOSED"],
            input_manifest=input_manifest,
            code="FAIL_CORRUPT_INPUTS",
            message=str(e),
            details={"error": str(e)},
            attempted_outputs=[
                {"path": str(out.nav_path), "sha256": None},
                {"path": str(out.exposure_path), "sha256": None},
                {"path": str(out.attribution_path), "sha256": None},
            ],
        )
        validate_against_repo_schema_v1(failure, REPO_ROOT, SCHEMA_FAILURE)
        b = canonical_json_bytes_v1(failure) + b"\n"
        _ = write_file_immutable_v1(path=out.failure_path, data=b, create_dirs=True)
        print("FAIL: DRAWDOWN_COMPUTE_FAILED_FAIL_CLOSED (failure artifact written)", file=sys.stderr)
        return 2

    nav_obj: Dict[str, Any] = {
        "schema_id": "C2_ACCOUNTING_NAV_V1",
        "schema_version": 1,
        "produced_utc": produced_utc,
        "day_utc": day_utc,
        "producer": {"repo": producer_repo, "git_sha": producer_sha, "module": module},
        "status": status,
        "reason_codes": sorted(set(reason_codes)),
        "input_manifest": input_manifest,
        "nav": {
            "currency": "USD",
            "nav_total": cash_total,
            "cash_total": cash_total,
            "gross_positions_value": 0,
            "realized_pnl_to_date": 0,
            "unrealized_pnl": 0,
            "components": components,
            "notes": ["bootstrap: marks missing; NAV is cash-only; positions listed elsewhere"],
        },
        "history": {"peak_nav": int(peak_nav), "drawdown_abs": int(drawdown_abs), "drawdown_pct": str(drawdown_pct_s)},
    }
    validate_against_repo_schema_v1(nav_obj, REPO_ROOT, SCHEMA_NAV)
    nav_bytes = canonical_json_bytes_v1(nav_obj) + b"\n"
    _ = write_file_immutable_v1(path=out.nav_path, data=nav_bytes, create_dirs=True)

    underlyings = set()
    expiry_buckets = set()
    pos_underlying_by_id: Dict[str, str] = {}
    pos_expiry_bucket_by_id: Dict[str, str] = {}

    if isinstance(lifecycle, dict):
        items_lc = ((lifecycle.get("lifecycle") or {}).get("items") or [])
        if isinstance(items_lc, list):
            for li in items_lc:
                if not isinstance(li, dict):
                    continue
                pid = str(li.get("position_id") or "").strip()
                instr = li.get("instrument")
                if pid and isinstance(instr, dict):
                    u = instr.get("underlying")
                    if isinstance(u, str) and u.strip():
                        pos_underlying_by_id[pid] = u.strip()
                        underlyings.add(u.strip())

                    bucket = "unknown"
                    legs = instr.get("legs")
                    if isinstance(legs, list) and legs:
                        lg0 = legs[0]
                        if isinstance(lg0, dict):
                            eu = lg0.get("expiry_utc")
                            if isinstance(eu, str) and eu.strip():
                                bucket = _expiry_bucket_from_expiry_utc(eu)
                    pos_expiry_bucket_by_id[pid] = bucket
                    expiry_buckets.add(bucket)

    defined_risk_dollars_by_id: Dict[str, int] = {}
    any_defined_risk = False
    if isinstance(defined_risk, dict):
        dr_items = ((defined_risk.get("defined_risk") or {}).get("items") or [])
        if isinstance(dr_items, list):
            for dri in dr_items:
                if not isinstance(dri, dict):
                    continue
                pid = str(dri.get("position_id") or "").strip()
                if not pid:
                    continue
                if str(dri.get("market_exposure_type") or "") != "DEFINED_RISK":
                    continue
                ml = dri.get("max_loss_cents")
                if ml is None:
                    continue
                if not isinstance(ml, int):
                    raise ValueError("DEFINED_RISK_MAX_LOSS_CENTS_NOT_INT")
                dollars = _cents_to_int_dollars_failclosed(int(ml))
                defined_risk_dollars_by_id[pid] = dollars
                any_defined_risk = True

    defined_risk_total = 0
    by_underlying_sum: Dict[str, int] = {}
    by_expiry_bucket_sum: Dict[str, int] = {}

    if any_defined_risk:
        for pid, dollars in defined_risk_dollars_by_id.items():
            defined_risk_total += int(dollars)
            u = pos_underlying_by_id.get(pid) or "unknown"
            b = pos_expiry_bucket_by_id.get(pid) or "unknown"
            by_underlying_sum[u] = int(by_underlying_sum.get(u, 0) + int(dollars))
            by_expiry_bucket_sum[b] = int(by_expiry_bucket_sum.get(b, 0) + int(dollars))

    exposure_reason_codes = sorted(set(reason_codes))
    exposure_notes = ["bootstrap: defined-risk exposure not provable without max-loss; grouping derived from lifecycle when available"]

    if any_defined_risk:
        exposure_notes = ["defined risk derived from defined_risk_v1; buckets from lifecycle when available"]
    else:
        exposure_reason_codes = sorted(set(exposure_reason_codes + ["EXPOSURE_BOOTSTRAP_DEFINED_RISK_UNKNOWN"]))

    exposure_obj: Dict[str, Any] = {
        "schema_id": "C2_ACCOUNTING_EXPOSURE_V1",
        "schema_version": 1,
        "produced_utc": _produced_utc_idempotent(out.exposure_path, produced_utc),
        "day_utc": day_utc,
        "producer": {"repo": producer_repo, "git_sha": producer_sha, "module": module},
        "status": status,
        "reason_codes": exposure_reason_codes,
        "input_manifest": input_manifest,
        "exposure": {
            "currency": "USD",
            "defined_risk_total": int(defined_risk_total),
            "by_engine": [{"key": "unknown", "defined_risk": 0}],
            "by_underlying": (
                [{"key": k, "defined_risk": int(by_underlying_sum.get(k, 0))} for k in sorted(by_underlying_sum.keys())]
                if any_defined_risk
                else ([{"key": k, "defined_risk": 0} for k in sorted(list(underlyings))] if underlyings else [{"key": "unknown", "defined_risk": 0}])
            ),
            "by_expiry_bucket": (
                [{"key": k, "defined_risk": int(by_expiry_bucket_sum.get(k, 0))} for k in sorted(by_expiry_bucket_sum.keys())]
                if any_defined_risk
                else ([{"key": k, "defined_risk": 0} for k in sorted(list(expiry_buckets))] if expiry_buckets else [{"key": "unknown", "defined_risk": 0}])
            ),
            "notes": exposure_notes,
        },
    }
    validate_against_repo_schema_v1(exposure_obj, REPO_ROOT, SCHEMA_EXPOSURE)
    exp_bytes = canonical_json_bytes_v1(exposure_obj) + b"\n"
    _ = write_file_immutable_v1(path=out.exposure_path, data=exp_bytes, create_dirs=True)

    items = positions.get("positions", {}).get("items", [])
    pos_count = len(items) if isinstance(items, list) else 0

    attr_obj: Dict[str, Any] = {
        "schema_id": "C2_ACCOUNTING_ATTRIBUTION_V1",
        "schema_version": 1,
        "produced_utc": _produced_utc_idempotent(out.attribution_path, produced_utc),
        "day_utc": day_utc,
        "producer": {"repo": producer_repo, "git_sha": producer_sha, "module": module},
        "status": status,
        "reason_codes": sorted(set(reason_codes + ["ENGINE_LINKAGE_UNKNOWN"])),
        "input_manifest": input_manifest,
        "attribution": {
            "currency": "USD",
            "by_engine": [
                {
                    "engine_id": "unknown",
                    "realized_pnl_to_date": 0,
                    "unrealized_pnl": 0,
                    "defined_risk_exposure": int(defined_risk_total),
                    "positions_count": int(pos_count),
                    "symbols": ["unknown"],
                }
            ],
            "notes": ["bootstrap: engine linkage not available; positions counted under unknown engine"],
        },
    }
    validate_against_repo_schema_v1(attr_obj, REPO_ROOT, SCHEMA_ATTR)
    attr_bytes = canonical_json_bytes_v1(attr_obj) + b"\n"
    _ = write_file_immutable_v1(path=out.attribution_path, data=attr_bytes, create_dirs=True)

    print("OK: ACCOUNTING_BOOTSTRAP_WRITTEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
