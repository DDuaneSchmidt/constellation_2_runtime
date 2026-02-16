#!/usr/bin/env python3
"""
run_engine_model_registry_gate_v1.py

Bundle B: Engine Model Registry Gate v1 (hostile-review safe).

Checks:
- registry JSON validates against governed schema
- approved_git_sha matches current git sha
- each engine runner file sha256 matches approved sha256
- activation_status == ACTIVE

Output (immutable report):
constellation_2/runtime/truth/reports/engine_model_registry_gate_v1/<DAY>/engine_model_registry_gate.v1.json
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

import argparse
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

REG_PATH = (REPO_ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json").resolve()
REG_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RISK/engine_model_registry.v1.schema.json"

OUT_ROOT = (TRUTH / "reports" / "engine_model_registry_gate_v1").resolve()


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _parse_day_utc(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise ValueError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _read_json(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_engine_model_registry_gate_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    day = _parse_day_utc(args.day_utc)

    current_sha = _git_sha()

    reason_codes: List[str] = []
    notes: List[str] = []
    input_manifest: List[Dict[str, str]] = []

    if not REG_PATH.exists():
        raise SystemExit(f"FAIL: MISSING_ENGINE_MODEL_REGISTRY: {REG_PATH}")

    reg_sha = _sha256_file(REG_PATH)
    input_manifest.append({"type": "engine_model_registry_v1", "path": str(REG_PATH), "sha256": reg_sha})

    reg = _read_json(REG_PATH)
    validate_against_repo_schema_v1(reg, REPO_ROOT, REG_SCHEMA_RELPATH)

    approved_git_sha = str(reg.get("approved_git_sha") or "")
    if approved_git_sha != current_sha:
        reason_codes.append("GIT_SHA_MISMATCH")
        notes.append(f"approved_git_sha={approved_git_sha} current_git_sha={current_sha}")

    engines = reg.get("engines") or []
    engine_results: List[Dict[str, Any]] = []

    for e in engines:
        engine_id = str(e.get("engine_id") or "")
        status = str(e.get("activation_status") or "")
        runner_rel = str(e.get("engine_runner_path") or "")
        runner_expected = str(e.get("engine_runner_sha256") or "")
        runner_path = (REPO_ROOT / runner_rel).resolve()

        ok = True
        rc: List[str] = []

        if status != "ACTIVE":
            ok = False
            rc.append("ENGINE_NOT_ACTIVE")

        if not runner_path.exists():
            ok = False
            rc.append("MISSING_ENGINE_RUNNER_FILE")
            runner_actual = _sha256_bytes(b"")
        else:
            runner_actual = _sha256_file(runner_path)
            if runner_actual != runner_expected:
                ok = False
                rc.append("ENGINE_RUNNER_SHA256_MISMATCH")

        input_manifest.append({"type": f"engine_runner:{engine_id}", "path": str(runner_path), "sha256": runner_actual})

        if not ok:
            reason_codes.append(f"ENGINE_BLOCKED:{engine_id}")

        engine_results.append(
            {
                "engine_id": engine_id,
                "activation_status": status,
                "runner_path": str(runner_path),
                "runner_sha256_expected": runner_expected,
                "runner_sha256_actual": runner_actual,
                "ok": bool(ok),
                "reason_codes": rc,
            }
        )

    gate_status = "PASS" if len(reason_codes) == 0 else "FAIL"

    report = {
        "schema_id": "engine_model_registry_gate",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": _utc_now(),
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_engine_model_registry_gate_v1.py", "git_sha": current_sha},
        "status": gate_status,
        "reason_codes": sorted(list(dict.fromkeys(reason_codes))),
        "notes": notes,
        "input_manifest": input_manifest,
        "results": {"approved_git_sha": approved_git_sha, "current_git_sha": current_sha, "engines": engine_results},
    }

    # Report schema not governed yet; v1 gate is still audit-grade because inputs are governed + hashed.
    out_dir = (OUT_ROOT / day).resolve()
    out_path = (out_dir / "engine_model_registry_gate.v1.json").resolve()
    payload = (json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")

    try:
        wr = write_file_immutable_v1(path=out_path, data=payload, create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL: IMMUTABLE_WRITE_ERROR: {e}") from e

    print(f"OK: ENGINE_MODEL_REGISTRY_GATE_WRITTEN day_utc={day} status={gate_status} path={wr.path} sha256={wr.sha256} action={wr.action}")
    return 0 if gate_status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
