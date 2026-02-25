#!/usr/bin/env python3
"""
run_active_engine_set_snapshot_v1.py

Active Engine Set Snapshot v1 (registry-derived; audit-grade; deterministic; fail-closed)

Purpose:
- Produce a daily immutable report enumerating ACTIVE engines from the governed engine registry.
- Provide an engine visibility surface independent of submissions/intents (UI is submission-centric).
- Eliminate orphan truth: this tool is the authoritative producer for:
  constellation_2/runtime/truth/reports/active_engine_set_v1/<DAY>/active_engine_set.v1.json

FAIL-CLOSED checks:
- Engine registry validates against governed schema.
- Every ACTIVE engine runner path exists and sha256 matches engine_runner_sha256 in registry.

Determinism:
- produced_utc is deterministic: <DAY>T00:00:00Z
- canonical_json_hash = sha256(canonical_json_bytes_v1(report) + b"\\n")

Immutable rule:
- If the report already exists for the day, treat it as authoritative and do not rewrite (action=EXISTS).
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
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

REG_PATH = (REPO_ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json").resolve()
REG_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RISK/engine_model_registry.v1.schema.json"

OUT_ROOT = (TRUTH / "reports" / "active_engine_set_v1").resolve()
OUT_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/active_engine_set.v1.schema.json"


def _parse_day_utc(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise ValueError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    if (not d[0:4].isdigit()) or (not d[5:7].isdigit()) or (not d[8:10].isdigit()):
        raise ValueError(f"BAD_DAY_UTC_NOT_NUMERIC_YYYY_MM_DD: {d!r}")
    return d


def _produced_utc_deterministic(day_utc: str) -> str:
    return f"{day_utc}T00:00:00Z"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_json(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _git_sha_head() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _return_if_existing_report(out_path: Path, expected_day_utc: str) -> Optional[int]:
    if not out_path.exists():
        return None
    existing_sha = _sha256_file(out_path)
    try:
        existing = _read_json(out_path)
    except Exception as e:
        raise SystemExit(f"FAIL: EXISTING_REPORT_UNREADABLE: {out_path}: {e}") from e

    schema_id = existing.get("schema_id")
    day_utc = existing.get("day_utc")
    if schema_id != "C2_ACTIVE_ENGINE_SET_V1":
        raise SystemExit(f"FAIL: EXISTING_REPORT_SCHEMA_MISMATCH: schema_id={schema_id!r} path={out_path}")
    if day_utc != expected_day_utc:
        raise SystemExit(f"FAIL: EXISTING_REPORT_DAY_MISMATCH: day_utc={day_utc!r} expected={expected_day_utc!r} path={out_path}")

    print(f"OK: ACTIVE_ENGINE_SET_V1_WRITTEN day_utc={expected_day_utc} path={out_path} sha256={existing_sha} action=EXISTS")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_active_engine_set_snapshot_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument(
        "--current_git_sha",
        default="",
        help="Optional: explicit git sha; if empty, uses HEAD.",
    )
    args = ap.parse_args()

    day = _parse_day_utc(args.day_utc)
    current_sha = (str(args.current_git_sha) or "").strip() or _git_sha_head()
    produced_utc = _produced_utc_deterministic(day)

    out_dir = (OUT_ROOT / day).resolve()
    out_path = (out_dir / "active_engine_set.v1.json").resolve()

    existing_rc = _return_if_existing_report(out_path=out_path, expected_day_utc=day)
    if existing_rc is not None:
        return int(existing_rc)

    if not REG_PATH.exists():
        raise SystemExit(f"FAIL: MISSING_ENGINE_MODEL_REGISTRY: {REG_PATH}")

    reg_sha = _sha256_file(REG_PATH)
    reg = _read_json(REG_PATH)
    validate_against_repo_schema_v1(reg, REPO_ROOT, REG_SCHEMA_RELPATH)

    registry_version = reg.get("registry_version")
    approved_git_sha = str(reg.get("approved_git_sha") or "").strip()

    engines = reg.get("engines") or []
    if not isinstance(engines, list):
        raise SystemExit("FAIL: REGISTRY_ENGINES_NOT_LIST")

    active: List[Dict[str, str]] = []
    for e in engines:
        if not isinstance(e, dict):
            continue
        if str(e.get("activation_status") or "") != "ACTIVE":
            continue

        engine_id = str(e.get("engine_id") or "").strip()
        runner_rel = str(e.get("engine_runner_path") or "").strip()
        runner_expected = str(e.get("engine_runner_sha256") or "").strip().lower()

        if not engine_id:
            raise SystemExit("FAIL: ACTIVE_ENGINE_MISSING_ENGINE_ID")
        if not runner_rel:
            raise SystemExit(f"FAIL: ACTIVE_ENGINE_MISSING_RUNNER_PATH: engine_id={engine_id}")
        if len(runner_expected) != 64:
            raise SystemExit(f"FAIL: ACTIVE_ENGINE_BAD_RUNNER_SHA256: engine_id={engine_id} sha256={runner_expected!r}")

        runner_path = (REPO_ROOT / runner_rel).resolve()
        if not runner_path.exists():
            raise SystemExit(f"FAIL: ACTIVE_ENGINE_RUNNER_MISSING: engine_id={engine_id} path={runner_path}")

        runner_actual = _sha256_file(runner_path).lower()
        if runner_actual != runner_expected:
            raise SystemExit(
                f"FAIL: ACTIVE_ENGINE_RUNNER_SHA256_MISMATCH: engine_id={engine_id} expected={runner_expected} actual={runner_actual} path={runner_path}"
            )

        active.append({"engine_id": engine_id, "runner_path": runner_rel, "runner_sha256": runner_actual})

    # Deterministic sort by engine_id to make canonical bytes stable
    active_sorted = sorted(active, key=lambda r: r["engine_id"])

    report: Dict[str, Any] = {
        "schema_id": "C2_ACTIVE_ENGINE_SET_V1",
        "schema_version": 1,
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_active_engine_set_snapshot_v1.py", "git_sha": current_sha},
        "registry_version": int(registry_version) if isinstance(registry_version, int) else int(registry_version) if str(registry_version).isdigit() else 0,
        "approved_git_sha": approved_git_sha,
        "active_engines": active_sorted,
        "canonical_json_hash": None,
    }

    try:
        payload = canonical_json_bytes_v1(report) + b"\n"
    except CanonicalizationError as e:
        raise SystemExit(f"FAIL: CANONICALIZATION_FAILED: {e}") from e

    report["canonical_json_hash"] = hashlib.sha256(payload).hexdigest()

    try:
        payload2 = canonical_json_bytes_v1(report) + b"\n"
    except CanonicalizationError as e:
        raise SystemExit(f"FAIL: CANONICALIZATION_FAILED_2: {e}") from e

    validate_against_repo_schema_v1(report, REPO_ROOT, OUT_SCHEMA_RELPATH)

    try:
        wr = write_file_immutable_v1(path=out_path, data=payload2, create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL: IMMUTABLE_WRITE_ERROR: {e}") from e

    print(f"OK: ACTIVE_ENGINE_SET_V1_WRITTEN day_utc={day} path={wr.path} sha256={wr.sha256} action={wr.action}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
