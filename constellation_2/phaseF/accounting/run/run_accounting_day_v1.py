from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1
from constellation_2.phaseF.accounting.lib.paths_v1 import REPO_ROOT, day_paths_v1 as accounting_day_paths_v1
from constellation_2.phaseF.cash_ledger.lib.paths_v1 import day_paths_v1 as cash_day_paths_v1
from constellation_2.phaseF.positions.lib.paths_v2 import day_paths_v2 as pos_day_paths_v2

SCHEMA_NAV = "governance/04_DATA/SCHEMAS/C2/ACCOUNTING/accounting_nav.v1.schema.json"
SCHEMA_EXPOSURE = "governance/04_DATA/SCHEMAS/C2/ACCOUNTING/accounting_exposure.v1.schema.json"
SCHEMA_ATTR = "governance/04_DATA/SCHEMAS/C2/ACCOUNTING/accounting_attribution.v1.schema.json"
SCHEMA_LATEST = "governance/04_DATA/SCHEMAS/C2/ACCOUNTING/accounting_latest_pointer.v1.schema.json"
SCHEMA_FAILURE = "governance/04_DATA/SCHEMAS/C2/ACCOUNTING/accounting_failure.v1.schema.json"


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
        if isinstance(ex_sha, str) and ex_sha.strip():
            if ex_sha.strip() != provided_sha:
                return ex_sha.strip()
    return None


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


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="run_accounting_day_v1",
        description="C2 Bundle F Accounting v1 (bootstrap: cash-only NAV, deterministic, immutable).",
    )
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--producer_git_sha", required=True, help="Producing git sha (explicit)")
    ap.add_argument("--producer_repo", default="constellation_2_runtime", help="Producer repo id")
    args = ap.parse_args(argv)

    day_utc = args.day_utc.strip()
    producer_sha = str(args.producer_git_sha).strip()
    producer_repo = str(args.producer_repo).strip()
    module = "constellation_2/phaseF/accounting/run/run_accounting_day_v1.py"

    out = accounting_day_paths_v1(day_utc)

    # Idempotency guard: if any of the day artifacts already exist, sha must match.
    for p in (out.nav_path, out.exposure_path, out.attribution_path):
        ex_sha = _lock_git_sha_if_exists(p, producer_sha)
        if ex_sha is not None:
            print(f"FAIL: PRODUCER_GIT_SHA_MISMATCH_FOR_EXISTING_DAY: existing={ex_sha} provided={producer_sha}", file=sys.stderr)
            return 4

    cash_paths = cash_day_paths_v1(day_utc)
    pos_paths = pos_day_paths_v2(day_utc)

    try:
        cash = _read_json_obj(cash_paths.snapshot_path)
        positions = _read_json_obj(pos_paths.snapshot_path)
    except Exception as e:
        failure = _build_failure(
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_git_sha=producer_sha,
            module=module,
            status="FAIL_CORRUPT_INPUTS",
            reason_codes=["MISSING_REQUIRED_INPUTS"],
            input_manifest=[
                {"type": "cash_ledger", "path": str(cash_paths.snapshot_path), "sha256": "0"*64, "day_utc": day_utc, "producer": "cash_ledger_v1"},
                {"type": "positions", "path": str(pos_paths.snapshot_path), "sha256": "0"*64, "day_utc": day_utc, "producer": "positions_v2"},
            ],
            code="FAIL_CORRUPT_INPUTS",
            message=str(e),
            details={"error": str(e)},
            attempted_outputs=[
                {"path": str(out.nav_path), "sha256": None},
                {"path": str(out.exposure_path), "sha256": None},
                {"path": str(out.attribution_path), "sha256": None},
                {"path": str(out.latest_path), "sha256": None},
            ],
        )
        validate_against_repo_schema_v1(failure, REPO_ROOT, SCHEMA_FAILURE)
        b = canonical_json_bytes_v1(failure) + b"\n"
        _ = write_file_immutable_v1(path=out.failure_path, data=b, create_dirs=True)
        print("FAIL: MISSING_REQUIRED_INPUTS (failure artifact written)", file=sys.stderr)
        return 2

    # Validate upstream schemas loosely via presence of key fields (fail closed if missing).
    try:
        cash_cents = int(cash["snapshot"]["cash_total_cents"])
    except Exception:
        print("FAIL: CASH_LEDGER_INVALID_SHAPE", file=sys.stderr)
        return 4

    # Bootstrap policy: NAV is cash-only because marks/instrument identity are missing.
    status = "DEGRADED_MISSING_MARKS"
    reason_codes = ["NAV_CASH_ONLY_BOOTSTRAP", "MISSING_MARKS", "MISSING_INSTRUMENT_IDENTITY"]

    produced_utc = _produced_utc_idempotent(out.nav_path, f"{day_utc}T00:00:00Z")

    input_manifest = [
        {"type": "cash_ledger", "path": str(cash_paths.snapshot_path), "sha256": _sha256_file(cash_paths.snapshot_path), "day_utc": day_utc, "producer": "cash_ledger_v1"},
        {"type": "positions", "path": str(pos_paths.snapshot_path), "sha256": _sha256_file(pos_paths.snapshot_path), "day_utc": day_utc, "producer": "positions_v2"},
    ]

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
            "cash_cents": cash_cents,
            "unrealized_cents": 0,
            "realized_cents": 0,
            "nav_total_cents": cash_cents,
            "peak_nav_cents": cash_cents,
            "drawdown_cents": 0
        }
    }
    validate_against_repo_schema_v1(nav_obj, REPO_ROOT, SCHEMA_NAV)
    nav_bytes = canonical_json_bytes_v1(nav_obj) + b"\n"
    wr_nav = write_file_immutable_v1(path=out.nav_path, data=nav_bytes, create_dirs=True)

    # Exposure: only counts; defined-risk unknown.
    items = positions.get("positions", {}).get("items", [])
    pos_count = len(items) if isinstance(items, list) else 0
    exposure_obj: Dict[str, Any] = {
        "schema_id": "C2_ACCOUNTING_EXPOSURE_V1",
        "schema_version": 1,
        "produced_utc": _produced_utc_idempotent(out.exposure_path, produced_utc),
        "day_utc": day_utc,
        "producer": {"repo": producer_repo, "git_sha": producer_sha, "module": module},
        "status": status,
        "reason_codes": sorted(set(reason_codes + ["EXPOSURE_COUNTS_ONLY"])),
        "input_manifest": input_manifest,
        "exposure": {
            "positions_total": pos_count,
            "defined_risk_positions_total": 0,
            "undefined_risk_positions_total": pos_count,
            "defined_risk_max_loss_cents": 0
        }
    }
    validate_against_repo_schema_v1(exposure_obj, REPO_ROOT, SCHEMA_EXPOSURE)
    exp_bytes = canonical_json_bytes_v1(exposure_obj) + b"\n"
    wr_exp = write_file_immutable_v1(path=out.exposure_path, data=exp_bytes, create_dirs=True)

    # Attribution: unknown engines until engine linkage exists.
    attr_obj: Dict[str, Any] = {
        "schema_id": "C2_ACCOUNTING_ATTRIBUTION_V1",
        "schema_version": 1,
        "produced_utc": _produced_utc_idempotent(out.attribution_path, produced_utc),
        "day_utc": day_utc,
        "producer": {"repo": producer_repo, "git_sha": producer_sha, "module": module},
        "status": status,
        "reason_codes": sorted(set(reason_codes + ["ENGINE_ATTRIBUTION_UNKNOWN"])),
        "input_manifest": input_manifest,
        "attribution": {
            "engines": [
                {"engine_id": "unknown", "nav_contrib_cents": cash_cents, "notes": ["bootstrap: no engine linkage yet"]}
            ]
        }
    }
    validate_against_repo_schema_v1(attr_obj, REPO_ROOT, SCHEMA_ATTR)
    attr_bytes = canonical_json_bytes_v1(attr_obj) + b"\n"
    wr_attr = write_file_immutable_v1(path=out.attribution_path, data=attr_bytes, create_dirs=True)

    latest_obj: Dict[str, Any] = {
        "schema_id": "C2_ACCOUNTING_LATEST_POINTER_V1",
        "schema_version": 1,
        "produced_utc": _produced_utc_idempotent(out.latest_path, produced_utc),
        "day_utc": day_utc,
        "producer": {"repo": producer_repo, "git_sha": producer_sha, "module": module},
        "status": status,
        "reason_codes": sorted(set(reason_codes)),
        "pointers": {
            "nav_path": str(out.nav_path),
            "nav_sha256": wr_nav.sha256,
            "exposure_path": str(out.exposure_path),
            "exposure_sha256": wr_exp.sha256,
            "attribution_path": str(out.attribution_path),
            "attribution_sha256": wr_attr.sha256
        }
    }
    validate_against_repo_schema_v1(latest_obj, REPO_ROOT, SCHEMA_LATEST)
    latest_bytes = canonical_json_bytes_v1(latest_obj) + b"\n"
    _ = write_file_immutable_v1(path=out.latest_path, data=latest_bytes, create_dirs=True)

    print("OK: ACCOUNTING_BOOTSTRAP_WRITTEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
