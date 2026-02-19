#!/usr/bin/env python3
"""
run_execution_reconciliation_day_v1.py

Bundle 1: Execution Reconciliation v1.

Compares:
- submission evidence (broker_submission_record.v2.json + ids)
- execution stream presence (by submission_id)
- fill ledger invariants

Writes (immutable):
  constellation_2/runtime/truth/reports/execution_reconciliation_v1/<DAY>/execution_reconciliation.v1.json

Fail-closed:
- missing submissions day dir
- missing fill ledgers
- any internal inconsistency (overfill, missing submission evidence)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any, Dict, List

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SUB_ROOT = (TRUTH / "execution_evidence_v1/submissions").resolve()
STREAM_ROOT = (TRUTH / "execution_stream_v1").resolve()
LEDGER_ROOT = (TRUTH / "fill_ledger_v1").resolve()
OUT_ROOT = (TRUTH / "reports" / "execution_reconciliation_v1").resolve()

SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/execution_reconciliation.v1.schema.json"


def _git_sha() -> str:
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception:
        return "UNKNOWN"


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    return _sha256_bytes(p.read_bytes())


def _sha256_dir_deterministic(root: Path) -> str:
    if not root.exists() or not root.is_dir():
        return _sha256_bytes(b"")
    items: List[str] = []
    for p in root.rglob("*"):
        if p.is_file():
            items.append(str(p.relative_to(root)).replace("\\", "/") + ":" + _sha256_file(p))
    items.sort()
    h = hashlib.sha256()
    for s in items:
        h.update(s.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def _read_json_obj(p: Path) -> Dict[str, Any]:
    o = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(o, dict):
        raise RuntimeError(f"TOP_LEVEL_NOT_OBJECT: {p}")
    return o


def _write_immutable(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = path.read_bytes()
        if _sha256_bytes(existing) == _sha256_bytes(payload):
            return
        raise RuntimeError(f"REFUSE_OVERWRITE_DIFFERENT_BYTES: {path}")
    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    tmp.write_bytes(payload)
    import os
    os.replace(tmp, path)


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_execution_reconciliation_day_v1")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    produced_utc = f"{day}T00:00:00Z"

    sub_day = (SUB_ROOT / day).resolve()
    if not sub_day.exists() or not sub_day.is_dir():
        raise SystemExit(f"FAIL: MISSING_SUBMISSIONS_DAY_DIR: {sub_day}")

    stream_day = (STREAM_ROOT / day).resolve()
    ledger_day = (LEDGER_ROOT / day).resolve()

    checks: List[Dict[str, Any]] = []
    reason_codes: List[str] = []
    status = "PASS"

    # Basic presence checks
    checks.append({"check_id": "SUBMISSIONS_DAY_DIR_PRESENT", "pass": True, "details": str(sub_day)})

    stream_present = stream_day.exists() and stream_day.is_dir()
    checks.append({"check_id": "STREAM_DAY_DIR_PRESENT", "pass": bool(stream_present), "details": str(stream_day)})
    if not stream_present:
        status = "FAIL"
        reason_codes.append("MISSING_EXECUTION_STREAM_DAY_DIR")

    ledger_present = ledger_day.exists() and ledger_day.is_dir()
    checks.append({"check_id": "FILL_LEDGER_DAY_DIR_PRESENT", "pass": bool(ledger_present), "details": str(ledger_day)})
    if not ledger_present:
        status = "FAIL"
        reason_codes.append("MISSING_FILL_LEDGER_DAY_DIR")

    # Per submission checks
    subdirs = sorted([p for p in sub_day.iterdir() if p.is_dir()])
    for sd in subdirs:
        sid = sd.name
        bsr_p = sd / "broker_submission_record.v2.json"
        if not bsr_p.exists():
            status = "FAIL"
            reason_codes.append("MISSING_BROKER_SUBMISSION_RECORD")
            checks.append({"check_id": "SUBMISSION_HAS_BSR", "pass": False, "details": f"{sid} missing broker_submission_record.v2.json"})
            continue

        checks.append({"check_id": "SUBMISSION_HAS_BSR", "pass": True, "details": sid})

        # Fill ledger required
        led_p = ledger_day / f"{sid}.fill_ledger.v1.json"
        if not led_p.exists():
            status = "FAIL"
            reason_codes.append("MISSING_FILL_LEDGER_FOR_SUBMISSION")
            checks.append({"check_id": "SUBMISSION_HAS_FILL_LEDGER", "pass": False, "details": f"{sid} missing {led_p.name}"})
            continue
        checks.append({"check_id": "SUBMISSION_HAS_FILL_LEDGER", "pass": True, "details": sid})

        # Invariant: filled_qty <= order_qty
        led = _read_json_obj(led_p)
        oq = led.get("order_qty")
        fq = led.get("filled_qty")
        if not (isinstance(oq, int) and isinstance(fq, int) and fq <= oq):
            status = "FAIL"
            reason_codes.append("FILL_LEDGER_OVERFILL_OR_INVALID")
            checks.append({"check_id": "FILL_LEDGER_INVARIANT", "pass": False, "details": f"{sid} oq={oq} fq={fq}"})
        else:
            checks.append({"check_id": "FILL_LEDGER_INVARIANT", "pass": True, "details": f"{sid} oq={oq} fq={fq}"})

    input_manifest = [
        {"type": "other", "path": str(sub_day), "sha256": _sha256_dir_deterministic(sub_day)},
        {"type": "other", "path": str(stream_day), "sha256": _sha256_dir_deterministic(stream_day)},
        {"type": "other", "path": str(ledger_day), "sha256": _sha256_dir_deterministic(ledger_day)},
    ]

    obj: Dict[str, Any] = {
        "schema_id": "C2_EXECUTION_RECONCILIATION_V1",
        "schema_version": 1,
        "produced_utc": produced_utc,
        "day_utc": day,
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_execution_reconciliation_day_v1.py"},
        "status": status,
        "reason_codes": sorted(set(reason_codes)) if reason_codes else [],
        "input_manifest": input_manifest,
        "checks": [{"check_id": c["check_id"], "pass": bool(c["pass"]), "details": str(c["details"])} for c in checks],
        "canonical_json_hash": "",
    }
    obj["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(obj)

    validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA)
    payload = canonical_json_bytes_v1(obj) + b"\n"

    out_path = (OUT_ROOT / day / "execution_reconciliation.v1.json").resolve()
    _write_immutable(out_path, payload)

    print(f"OK: EXECUTION_RECONCILIATION_WRITTEN day={day} status={status} path={str(out_path)}")
    return 0 if status == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())

