#!/usr/bin/env python3
"""
gen_economic_truth_availability_certificate_v1.py

Writes the day-scoped Economic Truth Availability Certificate (bundle gate file).

Fail-closed:
- Writes certificate even if not ready
- Exits non-zero if ready=false
- Canonical JSON; no floats; schema validated; immutable
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

DAY_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseD.lib.canon_json_v1 import (  # type: ignore
    canonical_hash_excluding_fields_v1,
    canonical_json_bytes_v1,
)

TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()
BUNDLE_ROOT = TRUTH_ROOT / "monitoring_v1" / "economic_nav_drawdown_v1"

CERT_ROOT = BUNDLE_ROOT / "certificates"
CERT_NAME = "economic_truth_availability_certificate.v1.json"

# Inputs / outputs
ACCOUNTING_NAV_ROOT = TRUTH_ROOT / "accounting_v1" / "nav"
SNAP_PATH_ROOT = BUNDLE_ROOT / "nav_snapshot"
LEDGER_ROOT = BUNDLE_ROOT / "nav_history_ledger"
PACK_ROOT = BUNDLE_ROOT / "drawdown_window_pack"

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/MONITORING/economic_truth_availability_certificate.v1.schema.json"
CONTRACT_RELPATH = "governance/05_CONTRACTS/C2/economic_truth_availability_certificate_v1.contract.md"


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_json_obj(p: Path) -> Dict[str, Any]:
    obj = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: JSON_NOT_OBJECT: {str(p)}")
    return obj


def _validate_schema_or_fail(obj: Dict[str, Any], schema_path: Path) -> None:
    try:
        import jsonschema  # type: ignore
    except Exception as e:
        raise SystemExit(f"FAIL: jsonschema not available: {e}")

    if not schema_path.exists():
        raise SystemExit(f"FAIL: missing governed schema: {str(schema_path)}")
    schema_obj = _read_json_obj(schema_path)
    try:
        jsonschema.validate(instance=obj, schema=schema_obj)
    except Exception as e:
        raise SystemExit(f"FAIL: schema validation failed: {e}")


@dataclass(frozen=True)
class _WriteResult:
    path: str
    sha256: str
    action: str


def _write_immutable_canon(path: Path, obj: Dict[str, Any]) -> _WriteResult:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json_bytes_v1(obj) + b"\n"
    sha = _sha256_bytes(payload)

    if path.exists():
        existing = path.read_bytes()
        if _sha256_bytes(existing) == sha:
            return _WriteResult(path=str(path), sha256=sha, action="EXISTS_IDENTICAL")
        raise SystemExit(f"FAIL: refusing overwrite (different bytes): {str(path)}")

    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    tmp.write_bytes(payload)
    os.replace(tmp, path)
    return _WriteResult(path=str(path), sha256=sha, action="WRITTEN")


def _git_sha_failclosed() -> str:
    import subprocess
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception as e:
        raise SystemExit(f"FAIL: cannot read git sha: {e!r}")


def _exists_file(p: Path) -> bool:
    return p.exists() and p.is_file()

def _return_if_existing_cert(out_path: Path, expected_day_utc: str, schema_path: Path) -> int | None:
    """
    Immutable truth rule (audit-grade):
    - If the certificate already exists at the immutable day-keyed path, do NOT attempt rewrite.
    - Treat the existing file as authoritative for that day.
    - Validate schema and canonical_json_hash.
    - Return exit code based on existing ready field (ready=true -> 0, ready=false -> 2).
    """
    if not out_path.exists():
        return None

    existing_bytes = out_path.read_bytes()
    existing_sha = _sha256_bytes(existing_bytes)
    existing = _read_json_obj(out_path)

    day_utc = str(existing.get("day_utc") or "").strip()
    schema_id = str(existing.get("schema_id") or "").strip()
    ready = existing.get("ready")

    if schema_id != "C2_ECONOMIC_TRUTH_AVAILABILITY_CERTIFICATE_V1":
        raise SystemExit(f"FAIL: EXISTING_CERT_SCHEMA_ID_MISMATCH: schema_id={schema_id!r} path={out_path}")
    if day_utc != expected_day_utc:
        raise SystemExit(
            f"FAIL: EXISTING_CERT_DAY_MISMATCH: day_utc={day_utc!r} expected={expected_day_utc!r} path={out_path}"
        )

    # Validate schema (fail-closed if invalid)
    _validate_schema_or_fail(existing, schema_path)

    # Validate canonical_json_hash matches recomputed value
    ch = existing.get("canonical_json_hash")
    if not isinstance(ch, str) or ch.strip() == "":
        raise SystemExit(f"FAIL: EXISTING_CERT_MISSING_CANONICAL_JSON_HASH: path={out_path}")
    recomputed = canonical_hash_excluding_fields_v1(existing, fields=("canonical_json_hash",))
    if recomputed != ch:
        raise SystemExit(
            f"FAIL: EXISTING_CERT_CANONICAL_JSON_HASH_MISMATCH: path={out_path} expected={recomputed} got={ch}"
        )

    print(f"ECON_CERT_V1 day_utc={expected_day_utc} ready={str(bool(ready)).lower()} path={str(out_path)} sha256={existing_sha} action=EXISTS")
    if ready is True:
        return 0
    return 2

def main() -> int:
    ap = argparse.ArgumentParser(prog="gen_economic_truth_availability_certificate_v1")
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if not DAY_RE.match(day):
        raise SystemExit(f"FAIL: bad --day_utc: {day!r}")

    schema_path = (REPO_ROOT / SCHEMA_RELPATH).resolve()
    contract_path = (REPO_ROOT / CONTRACT_RELPATH).resolve()

    out_path = (CERT_ROOT / day / CERT_NAME).resolve()
    rc_existing = _return_if_existing_cert(out_path, day, schema_path)
    if rc_existing is not None:
        return rc_existing

    # Evidence paths
    p_accounting_nav = (ACCOUNTING_NAV_ROOT / day / "nav.json").resolve()
    p_snap = (SNAP_PATH_ROOT / day / "nav_snapshot.v1.json").resolve()
    p_ledger = (LEDGER_ROOT / day / "nav_history_ledger.v1.json").resolve()
    p_pack = (PACK_ROOT / day / "drawdown_window_pack.v1.json").resolve()

    checks: List[Dict[str, Any]] = []
    missing: List[str] = []
    reason_codes: List[str] = []

    def _check(cid: str, p: Path) -> None:
        ok = _exists_file(p)
        if not ok:
            missing.append(str(p))
        checks.append({"check_id": cid, "pass": ok, "details": "exists" if ok else "missing", "evidence_paths": [str(p)]})

    _check("REQ_INPUT_ACCOUNTING_NAV", p_accounting_nav)
    _check("REQ_OUTPUT_NAV_SNAPSHOT", p_snap)
    _check("REQ_OUTPUT_NAV_HISTORY_LEDGER", p_ledger)
    _check("REQ_OUTPUT_DRAWDOWN_WINDOW_PACK", p_pack)

    # Schema validation checks (fail-closed if any schema missing, but record pass/fail)
    # We validate only the outputs we own here; if missing, treat as fail.
    def _validate_if_present(cid: str, p: Path, schema_relpath: str) -> None:
        ok = False
        details = "missing"
        if _exists_file(p):
            try:
                obj = _read_json_obj(p)
                _validate_schema_or_fail(obj, (REPO_ROOT / schema_relpath).resolve())
                ok = True
                details = "schema_valid"
            except Exception as e:
                ok = False
                details = f"schema_invalid: {e}"
        checks.append({"check_id": cid, "pass": ok, "details": details, "evidence_paths": [str(p)]})
        if not ok:
            reason_codes.append(cid)

    _validate_if_present("SCHEMA_NAV_SNAPSHOT_VALID", p_snap, "governance/04_DATA/SCHEMAS/C2/MONITORING/nav_snapshot.v1.schema.json")
    _validate_if_present("SCHEMA_NAV_HISTORY_LEDGER_VALID", p_ledger, "governance/04_DATA/SCHEMAS/C2/MONITORING/nav_history_ledger.v1.schema.json")
    _validate_if_present("SCHEMA_DRAWDOWN_WINDOW_PACK_VALID", p_pack, "governance/04_DATA/SCHEMAS/C2/MONITORING/drawdown_window_pack.v1.schema.json")

    ready = (len(missing) == 0) and (len(reason_codes) == 0)

    produced_utc = _now_utc_iso()
    git_sha = _git_sha_failclosed()

    input_manifest: List[Dict[str, str]] = [
        {"type": "accounting_nav", "path": str(p_accounting_nav), "sha256": _sha256_file(p_accounting_nav)} if _exists_file(p_accounting_nav) else {"type": "accounting_nav", "path": str(p_accounting_nav), "sha256": "0" * 64},
        {"type": "certificate_contract", "path": str(contract_path), "sha256": _sha256_file(contract_path)},
        {"type": "output_schema", "path": str(schema_path), "sha256": _sha256_file(schema_path)},
    ]

    out_obj: Dict[str, Any] = {
        "schema_id": "C2_ECONOMIC_TRUTH_AVAILABILITY_CERTIFICATE_V1",
        "schema_version": 1,
        "day_utc": day,
        "ready": bool(ready),
        "checks": checks,
        "missing_artifacts": missing,
        "reason_codes": reason_codes,
        "input_manifest": input_manifest,
        "produced_utc": produced_utc,
        "producer": {"git_sha": git_sha, "module": "ops/tools/gen_economic_truth_availability_certificate_v1.py", "repo": "constellation_2_runtime"},
        "canonical_json_hash": None,
    }

    out_obj["canonical_json_hash"] = canonical_hash_excluding_fields_v1(out_obj, fields=("canonical_json_hash",))
    _validate_schema_or_fail(out_obj, schema_path)

    out_path = (CERT_ROOT / day / CERT_NAME).resolve()
    wr = _write_immutable_canon(out_path, out_obj)

    print(f"ECON_CERT_V1 day_utc={day} ready={str(ready).lower()} path={wr.path} sha256={wr.sha256} action={wr.action}")
    if not ready:
        print(f"FAIL: ECON_CERT_V1 ready=false missing={len(missing)} reason_codes={reason_codes}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
