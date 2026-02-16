#!/usr/bin/env python3
"""
run_engine_model_registry_gate_v1.py

Bundle B: Engine Model Registry Gate v1 (hostile-review safe, satisfiable).

Checks (FAIL-CLOSED):
- registry JSON validates against governed schema
- each engine runner file sha256 matches expected sha256 in registry
- activation_status == ACTIVE

Checks (AUDIT-ONLY, non-blocking):
- approved_git_sha vs current_git_sha (reported in notes/results)

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
from typing import Any, Dict, List

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

REG_PATH = (REPO_ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json").resolve()
REG_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RISK/engine_model_registry.v1.schema.json"

OUT_ROOT = (TRUTH / "reports" / "engine_model_registry_gate_v1").resolve()


def _parse_day_utc(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise ValueError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


def _produced_utc_deterministic(day_utc: str) -> str:
    """
    Deterministic produced_utc for immutable, rerunnable daily truth artifacts.

    Rationale (contract-aligned):
    - This gate writes immutable truth under a day key.
    - Wall-clock timestamps would make reruns non-idempotent and trigger immutable rewrite failures.
    - The contract requires determinism; it does not require wall-clock produced_utc.
    """
    return f"{day_utc}T00:00:00Z"


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


def _git_sha_head() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _return_if_existing_report(out_path: Path, expected_day_utc: str) -> int | None:
    """
    Immutable truth rule (audit-grade):
    - If the report already exists at the immutable day-keyed path, do NOT attempt rewrite.
    - Treat the existing file as authoritative for that day.
    - Return its PASS/FAIL as exit code (PASS->0, FAIL->1).

    This makes the gate rerunnable without mutation, even across historical semantics changes.
    """
    if not out_path.exists():
        return None

    existing_sha = _sha256_file(out_path)
    existing = _read_json(out_path)

    schema_id = str(existing.get("schema_id") or "").strip()
    day_utc = str(existing.get("day_utc") or "").strip()
    status = str(existing.get("status") or "").strip().upper()

    if schema_id != "engine_model_registry_gate":
        raise SystemExit(f"FAIL: EXISTING_REPORT_SCHEMA_MISMATCH: schema_id={schema_id!r} path={out_path}")
    if day_utc != expected_day_utc:
        raise SystemExit(f"FAIL: EXISTING_REPORT_DAY_MISMATCH: day_utc={day_utc!r} expected={expected_day_utc!r} path={out_path}")
    if status not in ("PASS", "FAIL"):
        raise SystemExit(f"FAIL: EXISTING_REPORT_STATUS_INVALID: status={status!r} path={out_path}")

    print(
        f"OK: ENGINE_MODEL_REGISTRY_GATE_WRITTEN day_utc={expected_day_utc} status={status} path={out_path} sha256={existing_sha} action=EXISTS"
    )
    return 0 if status == "PASS" else 1


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_engine_model_registry_gate_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument(
        "--current_git_sha",
        default="",
        help="Optional (audit-only): explicit git sha; if empty, uses HEAD.",
    )
    args = ap.parse_args()

    day = _parse_day_utc(args.day_utc)

    out_dir = (OUT_ROOT / day).resolve()
    out_path = (out_dir / "engine_model_registry_gate.v1.json").resolve()

    # Immutable rerun safety: if report exists, treat it as authoritative and do not rewrite.
    existing_rc = _return_if_existing_report(out_path=out_path, expected_day_utc=day)
    if existing_rc is not None:
        return int(existing_rc)

    current_sha = (str(args.current_git_sha) or "").strip() or _git_sha_head()
    produced_utc = _produced_utc_deterministic(day)

    reason_codes: List[str] = []
    notes: List[str] = []
    input_manifest: List[Dict[str, str]] = []

    if not REG_PATH.exists():
        raise SystemExit(f"FAIL: MISSING_ENGINE_MODEL_REGISTRY: {REG_PATH}")

    reg_sha = _sha256_file(REG_PATH)
    input_manifest.append({"type": "engine_model_registry_v1", "path": str(REG_PATH), "sha256": reg_sha})

    reg = _read_json(REG_PATH)
    validate_against_repo_schema_v1(reg, REPO_ROOT, REG_SCHEMA_RELPATH)

    approved_git_sha = str(reg.get("approved_git_sha") or "").strip()
    if approved_git_sha == "":
        reason_codes.append("APPROVED_GIT_SHA_MISSING")
        notes.append("approved_git_sha missing/empty in registry (structural)")
    else:
        # AUDIT-ONLY: do not fail the gate on mismatch (unsatisfiable fixed point in single-repo governance)
        if approved_git_sha != current_sha:
            notes.append(f"approved_git_sha={approved_git_sha} current_git_sha={current_sha} (audit-only mismatch)")

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
        "produced_utc": produced_utc,
        "producer": {
            "repo": "constellation_2_runtime",
            "module": "ops/tools/run_engine_model_registry_gate_v1.py",
            "git_sha": current_sha,
        },
        "status": gate_status,
        "reason_codes": sorted(list(dict.fromkeys(reason_codes))),
        "notes": notes,
        "input_manifest": input_manifest,
        "results": {"approved_git_sha": approved_git_sha, "current_git_sha": current_sha, "engines": engine_results},
    }

    payload = (json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")

    try:
        wr = write_file_immutable_v1(path=out_path, data=payload, create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL: IMMUTABLE_WRITE_ERROR: {e}") from e

    print(
        f"OK: ENGINE_MODEL_REGISTRY_GATE_WRITTEN day_utc={day} status={gate_status} path={wr.path} sha256={wr.sha256} action={wr.action}"
    )
    return 0 if gate_status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
