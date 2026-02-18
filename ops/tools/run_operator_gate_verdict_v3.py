#!/usr/bin/env python3
"""
run_operator_gate_verdict_v3.py

Operator Gate Verdict v3 (SAFE_IDLE aware; forward-only readiness).

Run:
  python3 ops/tools/run_operator_gate_verdict_v3.py --day_utc YYYY-MM-DD
"""

from __future__ import annotations

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

import argparse
import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1  # type: ignore
from constellation_2.phaseD.lib.enforce_operational_day_invariant_v1 import enforce_operational_day_key_invariant_v1

REPO_ROOT = _REPO_ROOT_FROM_FILE
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/operator_gate_verdict.v3.schema.json"

QUARANTINE_REGISTRY = (REPO_ROOT / "governance/02_REGISTRIES/TEST_DAY_KEY_QUARANTINE_V1.json").resolve()

PATH_EXEC_SUBMISSIONS = TRUTH / "execution_evidence_v1/submissions"
PATH_PIPELINE_MANIFEST_V2 = TRUTH / "reports/pipeline_manifest_v2"
PATH_RECON_V3 = TRUTH / "reports/reconciliation_report_v3"
PATH_OPERATOR_DAILY_GATE_V2 = TRUTH / "reports/operator_daily_gate_v2"

OUT_ROOT = TRUTH / "reports/operator_gate_verdict_v3"

DAY_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")


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


def _read_json_obj(path: Path) -> Dict[str, Any]:
    o = _read_json(path)
    if not isinstance(o, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return o


def _check_exists(path: Path) -> bool:
    return path.exists() and path.is_file()


def _compute_self_sha_field(obj: Dict[str, Any], field_name: str) -> str:
    obj2 = dict(obj)
    obj2[field_name] = None
    canon = canonical_json_bytes_v1(obj2) + b"\n"
    return _sha256_bytes(canon)


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
    import os
    os.replace(tmp, path)
    return _WriteResult(path=str(path), sha256=sha, action="WRITTEN")


def _is_quarantined(day: str) -> bool:
    if not QUARANTINE_REGISTRY.exists():
        return False
    try:
        obj = _read_json_obj(QUARANTINE_REGISTRY)
    except Exception:
        return True  # fail-closed if registry unreadable

    def contains(o: Any) -> bool:
        if isinstance(o, list):
            return day in [str(x) for x in o]
        if isinstance(o, dict):
            return any(contains(v) for v in o.values())
        return False

    return contains(obj)


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_operator_gate_verdict_v3")
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if not DAY_RE.match(day):
        raise SystemExit(f"FAIL: bad --day_utc (expected YYYY-MM-DD): {day!r}")

    enforce_operational_day_key_invariant_v1(day)

    produced_utc = f"{day}T00:00:00Z"

    pipe_v2_path = (PATH_PIPELINE_MANIFEST_V2 / day / "pipeline_manifest.v2.json").resolve()
    recon_path = (PATH_RECON_V3 / day / "reconciliation_report.v3.json").resolve()
    op_gate_path = (PATH_OPERATOR_DAILY_GATE_V2 / day / "operator_daily_gate.v2.json").resolve()

    subs_dir = (PATH_EXEC_SUBMISSIONS / day).resolve()
    has_submissions = subs_dir.exists() and any(p.is_dir() for p in subs_dir.iterdir())

    checks: List[Dict[str, Any]] = []
    missing: List[str] = []
    mismatches: List[Dict[str, Any]] = []

    # QUARANTINE
    q = _is_quarantined(day)
    checks.append({"check_id": "REQ_DAY_NOT_QUARANTINED", "pass": (not q), "evidence_paths": [str(QUARANTINE_REGISTRY)], "details": "not_quarantined" if not q else "quarantined"})
    if q:
        missing.append(f"QUARANTINED_DAY:{day}")

    # RECON v3 must be OK
    if _check_exists(recon_path):
        try:
            ro = _read_json_obj(recon_path)
            st = str(ro.get("status") or "").strip().upper()
            ok = (st == "OK")
            details = f"status={st}" if st else "status=MISSING"
        except Exception:
            ok = False
            details = "parse_error"
        if not ok:
            missing.append(str(recon_path))
        checks.append({"check_id": "REQ_RECONCILIATION_V3_OK", "pass": ok, "evidence_paths": [str(recon_path)], "details": details})
    else:
        missing.append(str(recon_path))
        checks.append({"check_id": "REQ_RECONCILIATION_V3_OK", "pass": False, "evidence_paths": [str(recon_path)], "details": "missing"})

    # PIPELINE manifest v2 status must be OK
    if _check_exists(pipe_v2_path):
        try:
            po = _read_json_obj(pipe_v2_path)
            st = str(po.get("status") or "").strip().upper()
            ok = (st == "OK")
            details = f"status={st}" if st else "status=MISSING"
        except Exception:
            ok = False
            details = "parse_error"
        if not ok:
            missing.append(str(pipe_v2_path))
        checks.append({"check_id": "REQ_PIPELINE_MANIFEST_V2_OK", "pass": ok, "evidence_paths": [str(pipe_v2_path)], "details": details})
    else:
        missing.append(str(pipe_v2_path))
        checks.append({"check_id": "REQ_PIPELINE_MANIFEST_V2_OK", "pass": False, "evidence_paths": [str(pipe_v2_path)], "details": "missing"})

    # OPERATOR daily gate v2 must be PASS
    if _check_exists(op_gate_path):
        try:
            go = _read_json_obj(op_gate_path)
            st = str(go.get("status") or "").strip().upper()
            ok = (st == "PASS")
            details = f"status={st}" if st else "status=MISSING"
        except Exception:
            ok = False
            details = "parse_error"
        if not ok:
            missing.append(str(op_gate_path))
        checks.append({"check_id": "REQ_OPERATOR_DAILY_GATE_V2_PASS", "pass": ok, "evidence_paths": [str(op_gate_path)], "details": details})
    else:
        missing.append(str(op_gate_path))
        checks.append({"check_id": "REQ_OPERATOR_DAILY_GATE_V2_PASS", "pass": False, "evidence_paths": [str(op_gate_path)], "details": "missing"})

    # INTENTS not required for SAFE_IDLE; keep as informational check only
    checks.append({"check_id": "INFO_SUBMISSIONS_PRESENT", "pass": True, "evidence_paths": [str(subs_dir)], "details": "submissions_present" if has_submissions else "no_submissions"})

    all_pass = all(bool(c.get("pass")) for c in checks)
    ready = bool(all_pass and (len(missing) == 0))
    exit_code = 0 if ready else 2

    verdict_obj: Dict[str, Any] = {
        "schema_id": "operator_gate_verdict.v3",
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"component": "ops/tools/run_operator_gate_verdict_v3.py", "version": "v3", "git_sha": _git_sha()},
        "checks": checks,
        "missing_artifacts": missing,
        "hash_mismatches": mismatches,
        "ready": ready,
        "exit_code": int(exit_code),
        "verdict_sha256": None,
    }
    verdict_obj["verdict_sha256"] = _compute_self_sha_field(verdict_obj, "verdict_sha256")

    # Validate schema via jsonschema (matches v2 approach)
    schema_path = (REPO_ROOT / SCHEMA_RELPATH).resolve()
    if not schema_path.exists():
        raise SystemExit(f"FAIL: missing governed schema: {schema_path}")

    try:
        import jsonschema  # type: ignore
        schema = _read_json(schema_path)
        jsonschema.validate(instance=verdict_obj, schema=schema)
    except Exception as e:
        raise SystemExit(f"FAIL: schema validation failed: {e}")

    out_dir = (OUT_ROOT / day).resolve()
    out_path = (out_dir / "operator_gate_verdict.v3.json").resolve()
    wr = _write_immutable_canonical_json(out_path, verdict_obj)

    print(f"OK: OPERATOR_GATE_VERDICT_V3_WRITTEN day_utc={day} ready={ready} exit_code={exit_code} path={wr.path} sha256={wr.sha256} action={wr.action}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
