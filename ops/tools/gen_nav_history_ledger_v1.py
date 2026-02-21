#!/usr/bin/env python3
"""
gen_nav_history_ledger_v1.py

Generates a deterministic NAV history ledger (day-scoped).
Derived ONLY from NAV Snapshot Truth v1 artifacts.

Deterministic / audit-grade / fail-closed:
- Canonical JSON via canon_json_v1 (float forbidden)
- Immutable writes
- Schema validated (governed)
- Full input_manifest with sha256
- Self-hash canonical_json_hash excluding itself

Outputs:
  constellation_2/runtime/truth/monitoring_v1/economic_nav_drawdown_v1/nav_history_ledger/<DAY>/nav_history_ledger.v1.json

NOTE:
- latest.json pointer fan-out is forbidden. This writer does not write latest.json.
"""

from __future__ import annotations

import argparse
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

SNAP_ROOT = BUNDLE_ROOT / "nav_snapshot"
LEDGER_ROOT = BUNDLE_ROOT / "nav_history_ledger"

LEDGER_NAME = "nav_history_ledger.v1.json"

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/MONITORING/nav_history_ledger.v1.schema.json"
CONTRACT_RELPATH = "governance/05_CONTRACTS/C2/nav_history_ledger_v1.contract.md"


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_bytes(b: bytes) -> str:
    import hashlib
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    import hashlib
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


def _list_days_under(root: Path) -> List[str]:
    if not root.exists():
        return []
    out: List[str] = []
    for p in sorted(root.iterdir()):
        if p.is_dir() and DAY_RE.match(p.name):
            out.append(p.name)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(prog="gen_nav_history_ledger_v1")
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD (asof)")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if not DAY_RE.match(day):
        raise SystemExit(f"FAIL: bad --day_utc: {day!r}")

    days_all = _list_days_under(SNAP_ROOT)
    if not days_all:
        raise SystemExit(f"FAIL: no NAV snapshots found under {str(SNAP_ROOT)}")

    days_upto = [d for d in days_all if d <= day]
    if not days_upto:
        raise SystemExit(f"FAIL: no NAV snapshots at or before asof day={day}")

    schema_path = (REPO_ROOT / SCHEMA_RELPATH).resolve()
    contract_path = (REPO_ROOT / CONTRACT_RELPATH).resolve()

    rows: List[Dict[str, Any]] = []
    for d in days_upto:
        snap_path = (SNAP_ROOT / d / "nav_snapshot.v1.json").resolve()
        if not snap_path.exists():
            raise SystemExit(f"FAIL: snapshot missing (expected): {str(snap_path)}")
        snap_sha = _sha256_file(snap_path)
        snap = _read_json_obj(snap_path)

        end_nav = snap.get("end_nav")
        peak = snap.get("peak_nav_to_date")
        dd = snap.get("drawdown_pct")
        if not isinstance(end_nav, str) or not isinstance(peak, str) or not isinstance(dd, str):
            raise SystemExit(f"FAIL: snapshot missing required fields: {str(snap_path)}")

        rows.append(
            {
                "day_utc": d,
                "snapshot_path": str(snap_path),
                "snapshot_sha256": snap_sha,
                "end_nav": end_nav,
                "peak_nav_to_date": peak,
                "drawdown_pct": dd,
            }
        )

    produced_utc = _now_utc_iso()
    git_sha = _git_sha_failclosed()

    input_manifest: List[Dict[str, str]] = [
        {"type": "nav_snapshot_root", "path": str(SNAP_ROOT.resolve()), "sha256": _sha256_bytes(b"")},  # dir sentinel
        {"type": "nav_history_ledger_contract", "path": str(contract_path), "sha256": _sha256_file(contract_path)},
        {"type": "output_schema", "path": str(schema_path), "sha256": _sha256_file(schema_path)},
    ]

    ledger_obj: Dict[str, Any] = {
        "schema_id": "C2_NAV_HISTORY_LEDGER_V1",
        "schema_version": 1,
        "asof_day_utc": day,
        "days": rows,
        "input_manifest": input_manifest,
        "produced_utc": produced_utc,
        "producer": {"git_sha": git_sha, "module": "ops/tools/gen_nav_history_ledger_v1.py", "repo": "constellation_2_runtime"},
        "canonical_json_hash": None,
    }
    ledger_obj["canonical_json_hash"] = canonical_hash_excluding_fields_v1(ledger_obj, fields=("canonical_json_hash",))
    _validate_schema_or_fail(ledger_obj, schema_path)

    out_path = (LEDGER_ROOT / day / LEDGER_NAME).resolve()
    wr = _write_immutable_canon(out_path, ledger_obj)

    print(f"NAV_HISTORY_LEDGER_V1 asof_day_utc={day} rows={len(rows)} ledger_path={wr.path} ledger_sha256={wr.sha256} ledger_action={wr.action}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
