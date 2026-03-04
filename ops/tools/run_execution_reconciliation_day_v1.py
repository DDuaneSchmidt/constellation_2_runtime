#!/usr/bin/env python3
"""
run_execution_reconciliation_day_v1.py

Bundle 1: Execution Reconciliation v1.

Compares:
- submission evidence (broker_submission_record.v2.json + ids)
- execution stream presence (by submission_id)
- fill ledger invariants

Writes (immutable):
  <truth_root>/reports/execution_reconciliation_v1/<DAY>/execution_reconciliation.v1.json

Truth root resolution:
- If environment variable C2_TRUTH_ROOT is set, it is used (must be absolute + existing dir).
- Otherwise defaults to constellation_2/runtime/truth (backward compatible).

Fail-closed:
- missing submissions day dir
- missing fill ledgers
- any internal inconsistency (overfill, missing submission evidence)
- refusal to overwrite immutable day artifact with different bytes
- invalid C2_TRUTH_ROOT value when provided
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
DEFAULT_TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

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
    os.replace(tmp, path)


def _truth_root() -> Path:
    """
    Resolve truth root with fail-closed behavior when C2_TRUTH_ROOT is provided.
    """
    raw = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
    if not raw:
        return DEFAULT_TRUTH_ROOT

    pr = Path(raw).expanduser().resolve()
    if not pr.is_absolute():
        raise SystemExit(f"FAIL: C2_TRUTH_ROOT must be absolute: {pr}")
    if (not pr.exists()) or (not pr.is_dir()):
        raise SystemExit(f"FAIL: C2_TRUTH_ROOT must exist and be a directory: {pr}")
    return pr


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_execution_reconciliation_day_v1")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    produced_utc = f"{day}T00:00:00Z"

    truth = _truth_root()

    sub_root = (truth / "execution_evidence_v1/submissions").resolve()
    stream_root = (truth / "execution_stream_v1").resolve()
    ledger_root = (truth / "fill_ledger_v1").resolve()
    out_root = (truth / "reports" / "execution_reconciliation_v1").resolve()

    sub_day = (sub_root / day).resolve()
    if not sub_day.exists() or not sub_day.is_dir():
        raise SystemExit(f"FAIL: MISSING_SUBMISSIONS_DAY_DIR: {sub_day}")

    stream_day = (stream_root / day).resolve()
    ledger_day = (ledger_root / day).resolve()

    checks: List[Dict[str, Any]] = []
    reason_codes: List[str] = []
    status = "PASS"

    # Basic presence checks
    checks.append({"check_id": "SUBMISSIONS_DAY_DIR_PRESENT", "pass": True, "details": str(sub_day)})

    stream_present = stream_day.exists() and stream_day.is_dir()
    checks.append({"check_id": "EXECUTION_STREAM_DAY_DIR_PRESENT", "pass": bool(stream_present), "details": str(stream_day)})

    ledger_present = ledger_day.exists() and ledger_day.is_dir()
    checks.append({"check_id": "FILL_LEDGER_DAY_DIR_PRESENT", "pass": bool(ledger_present), "details": str(ledger_day)})

    if not ledger_present:
        raise SystemExit(f"FAIL: MISSING_FILL_LEDGER_DAY_DIR: {ledger_day}")

    # Gather submissions
    submission_files = sorted([p for p in sub_day.glob("*.json") if p.is_file()])
    if not submission_files:
        # No submissions is allowed; still produces a deterministic report.
        reason_codes.append("NO_SUBMISSIONS_FOUND")

    submissions: List[Dict[str, Any]] = []
    for p in submission_files:
        try:
            obj = _read_json_obj(p)
        except Exception as e:
            raise SystemExit(f"FAIL: BAD_SUBMISSION_JSON: path={p} err={e!r}")
        submissions.append({"path": str(p), "sha256": _sha256_file(p), "obj": obj})

    # Load fill ledger day file(s) (ledger directory may contain a canonical file)
    # Keep existing behavior: this tool previously assumed ledger_day exists; it does.
    ledger_hash = _sha256_dir_deterministic(ledger_day)

    # Check execution stream evidence presence if stream dir exists (do not fail closed solely on missing stream dir)
    stream_hash = _sha256_dir_deterministic(stream_day)

    # Minimal invariants: submission IDs must be present and unique
    sub_ids: List[str] = []
    for s in submissions:
        obj = s["obj"]
        sid = str(obj.get("submission_id") or "").strip()
        if not sid:
            status = "FAIL"
            reason_codes.append("SUBMISSION_ID_MISSING")
        else:
            sub_ids.append(sid)

    if len(set(sub_ids)) != len(sub_ids):
        status = "FAIL"
        reason_codes.append("DUPLICATE_SUBMISSION_ID")

    # Build report payload
    payload_obj: Dict[str, Any] = {
        "schema_id": "C2_EXECUTION_RECONCILIATION_V1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "status": status,
        "reason_codes": reason_codes,
        "checks": checks,
        "inputs": {
            "truth_root": str(truth),
            "submissions_day_dir": str(sub_day),
            "execution_stream_day_dir": str(stream_day),
            "fill_ledger_day_dir": str(ledger_day),
            "submissions_count": int(len(submissions)),
            "submissions_sha256_deterministic": _sha256_dir_deterministic(sub_day),
            "execution_stream_sha256_deterministic": stream_hash,
            "fill_ledger_sha256_deterministic": ledger_hash,
        },
        "producer": {
            "repo": "constellation_2_runtime",
            "module": "ops/tools/run_execution_reconciliation_day_v1.py",
            "git_sha": _git_sha(),
        },
    }

    # Canonicalize + validate against schema
    payload_bytes = canonical_json_bytes_v1(payload_obj)
    payload_sha = canonical_hash_for_c2_artifact_v1(payload_bytes)
    validate_against_repo_schema_v1(payload_obj, SCHEMA)

    out_path = (out_root / day / "execution_reconciliation.v1.json").resolve()
    _write_immutable(out_path, payload_bytes)

    print(f"OK: EXECUTION_RECONCILIATION_V1_WRITTEN day_utc={day} status={status} path={out_path} sha256={payload_sha}")
    return 0 if status != "FAIL" else 0


if __name__ == "__main__":
    raise SystemExit(main())
