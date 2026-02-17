#!/usr/bin/env python3
"""
validate_economic_nav_drawdown_bundle_v1.py

One-command validator for the Economic NAV & Drawdown Truth Spine bundle for a given day.

Fail-closed and deterministic:
- Validates existence of required artifacts
- Validates each artifact against its governed schema
- Validates canonical_json_hash correctness (recompute with field null)
- Validates float prohibition via canonical_json_bytes_v1
- Exit code 0 if PASS
- Exit code 2 if FAIL
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, NoReturn

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

P_ACCOUNTING_NAV = TRUTH_ROOT / "accounting_v1" / "nav"

P_SNAP = BUNDLE_ROOT / "nav_snapshot"
P_LEDGER = BUNDLE_ROOT / "nav_history_ledger"
P_PACK = BUNDLE_ROOT / "drawdown_window_pack"
P_CERT = BUNDLE_ROOT / "certificates"

SCHEMA_SNAP = REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/MONITORING/nav_snapshot.v1.schema.json"
SCHEMA_LEDGER = REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/MONITORING/nav_history_ledger.v1.schema.json"
SCHEMA_PACK = REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/MONITORING/drawdown_window_pack.v1.schema.json"
SCHEMA_CERT = REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/MONITORING/economic_truth_availability_certificate.v1.schema.json"


def _fail(msg: str) -> NoReturn:
    print(msg, file=sys.stderr)
    raise SystemExit(2)


def _read_json_obj(p: Path) -> Dict[str, Any]:
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        _fail(f"FAIL: cannot parse JSON: path={p} error={e!r}")
    if not isinstance(obj, dict):
        _fail(f"FAIL: JSON_NOT_OBJECT: {str(p)}")
    return obj


def _validate_schema(obj: Dict[str, Any], schema_path: Path) -> None:
    try:
        import jsonschema  # type: ignore
    except Exception as e:
        _fail(f"FAIL: jsonschema not available: {e}")

    if not schema_path.exists():
        _fail(f"FAIL: missing governed schema: {str(schema_path)}")

    schema = _read_json_obj(schema_path)
    try:
        jsonschema.validate(instance=obj, schema=schema)
    except Exception as e:
        _fail(f"FAIL: schema validation failed for {str(schema_path)}: {e}")


def _check_canonical_hash(obj: Dict[str, Any], path: Path) -> None:
    ch = obj.get("canonical_json_hash")
    if not isinstance(ch, str) or ch.strip() == "":
        _fail(f"FAIL: missing canonical_json_hash: {str(path)}")

    recomputed = canonical_hash_excluding_fields_v1(obj, fields=("canonical_json_hash",))
    if recomputed != ch:
        _fail(
            f"FAIL: canonical_json_hash mismatch: path={str(path)} "
            f"expected={recomputed} got={ch}"
        )


def _exists_file(p: Path) -> None:
    if not (p.exists() and p.is_file()):
        _fail(f"FAIL: missing required artifact: {str(p)}")


def main() -> int:
    ap = argparse.ArgumentParser(prog="validate_economic_nav_drawdown_bundle_v1")
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if not DAY_RE.match(day):
        _fail(f"FAIL: bad --day_utc: {day!r}")

    # Required paths
    p_in = (P_ACCOUNTING_NAV / day / "nav.json").resolve()
    p_snap = (P_SNAP / day / "nav_snapshot.v1.json").resolve()
    p_ledger = (P_LEDGER / day / "nav_history_ledger.v1.json").resolve()
    p_pack = (P_PACK / day / "drawdown_window_pack.v1.json").resolve()
    p_cert = (P_CERT / day / "economic_truth_availability_certificate.v1.json").resolve()

    for p in [p_in, p_snap, p_ledger, p_pack, p_cert]:
        _exists_file(p)

    # Validate schemas + canonical hashes
    snap = _read_json_obj(p_snap)
    _validate_schema(snap, SCHEMA_SNAP)
    _check_canonical_hash(snap, p_snap)

    ledger = _read_json_obj(p_ledger)
    _validate_schema(ledger, SCHEMA_LEDGER)
    _check_canonical_hash(ledger, p_ledger)

    pack = _read_json_obj(p_pack)
    _validate_schema(pack, SCHEMA_PACK)
    _check_canonical_hash(pack, p_pack)

    cert = _read_json_obj(p_cert)
    _validate_schema(cert, SCHEMA_CERT)
    _check_canonical_hash(cert, p_cert)

    # Certificate must say ready=true
    ready = cert.get("ready")
    if ready is not True:
        _fail(f"FAIL: certificate ready!=true day_utc={day} ready={ready!r}")

    # Enforce float prohibition via canonical re-serialization
    _ = canonical_json_bytes_v1(snap)
    _ = canonical_json_bytes_v1(ledger)
    _ = canonical_json_bytes_v1(pack)
    _ = canonical_json_bytes_v1(cert)

    print(f"OK: ECON_NAV_DRAWDOWN_BUNDLE_V1 day_utc={day}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
