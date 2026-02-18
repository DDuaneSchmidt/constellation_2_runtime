#!/usr/bin/env python3
"""
run_operator_gate_verdict_v2.py

Operator Gate Verdict v2 (pillars-aware, immutable).

Key change vs v1:
- Submission evidence requirement is satisfied by:
  - legacy submission_index.v1.json, OR
  - pillars decisions dir (pillars_v1r1 preferred, else pillars_v1) containing >=1 decision record,
  - OR no submissions case.

Writes:
  constellation_2/runtime/truth/reports/operator_gate_verdict_v2/<DAY>/operator_gate_verdict.v2.json
"""

from __future__ import annotations

# --- Import bootstrap (audit-grade, deterministic, fail-closed) ---
import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

if not (_REPO_ROOT_FROM_FILE / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT_FROM_FILE}")
if not (_REPO_ROOT_FROM_FILE / "governance").exists():
    raise SystemExit(f"FATAL: repo_root_missing_governance: derived={_REPO_ROOT_FROM_FILE}")
# -------------------------------------------------------------------

import argparse
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1  # type: ignore
from constellation_2.phaseD.lib.enforce_operational_day_invariant_v1 import enforce_operational_day_key_invariant_v1

REPO_ROOT = _REPO_ROOT_FROM_FILE
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/operator_gate_verdict.v2.schema.json"

PATH_INTENTS_ROLLUP = TRUTH / "intents_v1/day_rollup"
PATH_SUBMISSION_INDEX = TRUTH / "execution_evidence_v1/submission_index"
PATH_EXEC_SUBMISSIONS = TRUTH / "execution_evidence_v1/submissions"
PATH_RECON_V2 = TRUTH / "reports/reconciliation_report_v2"
PATH_PIPELINE_MANIFEST_V2 = TRUTH / "reports/pipeline_manifest_v2"

# Pillars decisions (preferred)
PILLARS_V1 = TRUTH / "pillars_v1"
PILLARS_V1R1 = TRUTH / "pillars_v1r1"

OUT_ROOT = TRUTH / "reports/operator_gate_verdict_v2"

DAY_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _git_sha() -> str:
    import subprocess
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception:
        return "UNKNOWN"


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _check_exists(path: Path) -> bool:
    return path.exists() and path.is_file()


@dataclass(frozen=True)
class _WriteResult:
    path: str
    sha256: str
    action: str


def _write_immutable_canonical_json(path: Path, obj: Dict[str, Any]) -> _WriteResult:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json_bytes_v1(obj) + b"\n"
    sha = _sha256_bytes(payload)

    if path.exists():
        existing = path.read_bytes()
        if _sha256_bytes(existing) == sha:
            return _WriteResult(path=str(path), sha256=sha, action="EXISTS_IDENTICAL")
        raise SystemExit(f"FAIL: refusing overwrite (different bytes): {path}")

    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    tmp.write_bytes(payload)
    os.replace(tmp, path)
    return _WriteResult(path=str(path), sha256=sha, action="WRITTEN")


def _compute_self_sha_field(obj: Dict[str, Any], field_name: str) -> str:
    obj2 = dict(obj)
    obj2[field_name] = None
    canon = canonical_json_bytes_v1(obj2) + b"\n"
    return _sha256_bytes(canon)


def _pillars_decisions_dir(day: str) -> Optional[Path]:
    d1 = (PILLARS_V1R1 / day / "decisions").resolve()
    if d1.exists() and d1.is_dir():
        return d1
    d0 = (PILLARS_V1 / day / "decisions").resolve()
    if d0.exists() and d0.is_dir():
        return d0
    return None


def _count_decision_records(decisions_dir: Path) -> int:
    return len([p for p in decisions_dir.iterdir() if p.is_file() and p.name.endswith(".submission_decision_record.v1.json")])


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_operator_gate_verdict_v2")
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if not DAY_RE.match(day):
        raise SystemExit(f"FAIL: bad --day_utc (expected YYYY-MM-DD): {day!r}")

    enforce_operational_day_key_invariant_v1(day)

    schema_path = (REPO_ROOT / SCHEMA_RELPATH).resolve()
    if not schema_path.exists():
        raise SystemExit(f"FAIL: missing governed schema: {schema_path}")

    produced_utc = _now_utc_iso()

    intents_path = (PATH_INTENTS_ROLLUP / day / "intents_day_rollup.v1.json").resolve()
    subidx_path = (PATH_SUBMISSION_INDEX / day / "submission_index.v1.json").resolve()
    recon_path = (PATH_RECON_V2 / day / "reconciliation_report.v2.json").resolve()
    pipe_v2_path = (PATH_PIPELINE_MANIFEST_V2 / day / "pipeline_manifest.v2.json").resolve()

    subs_dir = (PATH_EXEC_SUBMISSIONS / day).resolve()
    has_submissions = subs_dir.exists() and any(p.is_dir() for p in subs_dir.iterdir())

    pillars_dir = _pillars_decisions_dir(day)
    pillars_present = (pillars_dir is not None) and (_count_decision_records(pillars_dir) > 0)

    checks: List[Dict[str, Any]] = []
    missing: List[str] = []
    mismatches: List[Dict[str, Any]] = []

    # INTENTS
    ok = _check_exists(intents_path)
    if not ok:
        missing.append(str(intents_path))
    checks.append({"check_id": "REQ_INTENTS_DAY_ROLLUP", "pass": ok, "evidence_paths": [str(intents_path)], "details": "exists" if ok else "missing"})

    # SUBMISSION EVIDENCE (submission_index OR pillars decisions)
    if not has_submissions:
        checks.append({"check_id": "REQ_SUBMISSION_EVIDENCE", "pass": True, "evidence_paths": [str(subidx_path)], "details": "no submissions => not required"})
    else:
        ok = _check_exists(subidx_path) or pillars_present
        if not ok:
            missing.append(str(subidx_path))
        details = "exists (legacy submission_index)" if _check_exists(subidx_path) else ("exists (pillars decisions)" if pillars_present else "missing")
        ev = [str(subidx_path)] + ([str(pillars_dir)] if pillars_dir else [])
        checks.append({"check_id": "REQ_SUBMISSION_EVIDENCE", "pass": ok, "evidence_paths": ev, "details": details})

    # RECON
    ok = _check_exists(recon_path)
    if not ok:
        missing.append(str(recon_path))
    checks.append({"check_id": "REQ_RECONCILIATION_V2", "pass": ok, "evidence_paths": [str(recon_path)], "details": "exists" if ok else "missing"})

    # PIPELINE MANIFEST V2
    ok = _check_exists(pipe_v2_path)
    if not ok:
        missing.append(str(pipe_v2_path))
    checks.append({"check_id": "REQ_PIPELINE_MANIFEST_V2", "pass": ok, "evidence_paths": [str(pipe_v2_path)], "details": "exists" if ok else "missing"})

    all_pass = all(bool(c.get("pass")) for c in checks)
    ready = bool(all_pass and (len(missing) == 0))
    exit_code = 0 if ready else 2

    verdict_obj: Dict[str, Any] = {
        "schema_id": "operator_gate_verdict.v2",
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"component": "ops/tools/run_operator_gate_verdict_v2.py", "version": "v2", "git_sha": _git_sha()},
        "checks": checks,
        "missing_artifacts": missing,
        "hash_mismatches": mismatches,
        "ready": ready,
        "exit_code": int(exit_code),
        "verdict_sha256": None,
    }
    verdict_obj["verdict_sha256"] = _compute_self_sha_field(verdict_obj, "verdict_sha256")

    # Validate schema
    try:
        import jsonschema  # type: ignore
        schema = _read_json(schema_path)
        jsonschema.validate(instance=verdict_obj, schema=schema)
    except Exception as e:
        raise SystemExit(f"FAIL: schema validation failed: {e}")

    out_dir = (OUT_ROOT / day).resolve()
    out_path = (out_dir / "operator_gate_verdict.v2.json").resolve()
    wr = _write_immutable_canonical_json(out_path, verdict_obj)

    print(f"OK: OPERATOR_GATE_VERDICT_V2_WRITTEN day_utc={day} ready={ready} exit_code={exit_code} path={wr.path} sha256={wr.sha256} action={wr.action}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
