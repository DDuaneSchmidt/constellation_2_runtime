#!/usr/bin/env python3
"""
run_pipeline_manifest_v1.py

Bundle A: pipeline_manifest.v1.json writer (immutable truth artifact).

Purpose (hostile-review safe):
- One artifact answers: "is the full paper-trading pipeline structurally complete for this day?"
- Deterministic, fail-closed on schema violations
- Uses only runtime truth + governed schemas
- Provides counts + deterministic directory hashes so auditors can reproduce state

This is NOT a trading decision artifact. It is a structural readiness manifest.

Runnability requirement:
- Must run as:  python3 ops/tools/run_pipeline_manifest_v1.py --day_utc YYYY-MM-DD
- Must NOT require PYTHONPATH or other environment setup.
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
from typing import Any, Dict, List, Tuple

from constellation_2.phaseD.lib.enforce_operational_day_invariant_v1 import (
    enforce_operational_day_key_invariant_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/pipeline_manifest.v1.schema.json"

OUT_ROOT = (TRUTH / "reports" / "pipeline_manifest_v1").resolve()

INTENTS_ROOT = (TRUTH / "intents_v1/snapshots").resolve()
PREFLIGHT_ROOT = (TRUTH / "phaseC_preflight_v1").resolve()
OMS_ROOT = (TRUTH / "oms_decisions_v1/decisions").resolve()
ALLOCATION_ROOT = (TRUTH / "allocation_v1/summary").resolve()

PHASED_ROOT = (REPO_ROOT / "constellation_2/phaseD/outputs/submissions").resolve()
EXEC_TRUTH_ROOT = (TRUTH / "execution_evidence_v1/submissions").resolve()
EXEC_MANIFEST_ROOT = (TRUTH / "execution_evidence_v1/manifests").resolve()
SUBMISSION_INDEX_ROOT = (TRUTH / "execution_evidence_v1" / "submission_index").resolve()
POSITIONS_ROOT = (TRUTH / "positions_v1/snapshots").resolve()
CASH_ROOT = (TRUTH / "cash_ledger_v1/snapshots").resolve()
ACCOUNTING_ROOT = (TRUTH / "accounting_v1").resolve()
RISK_LEDGER_ROOT = (TRUTH / "risk_v1" / "engine_budget").resolve()
CAP_RISK_ROOT = (TRUTH / "reports" / "capital_risk_envelope_v1").resolve()

# Regime classification (authoritative v2)
REGIME_ROOT = (TRUTH / "monitoring_v1" / "regime_snapshot_v2").resolve()

RECON_ROOT_V2 = (TRUTH / "reports" / "reconciliation_report_v2").resolve()
RECON_ROOT_V1 = (TRUTH / "reports" / "reconciliation_report_v1").resolve()
GATE_ROOT = (TRUTH / "reports" / "operator_daily_gate_v1").resolve()

# --- Bundled C (control-plane) ---
C_KILL_ROOT = (TRUTH / "risk_v1" / "kill_switch_v1").resolve()
C_LIFE_ROOT = (TRUTH / "position_lifecycle_v1" / "ledger").resolve()
C_RECON_ROOT = (TRUTH / "reports" / "exposure_reconciliation_report_v1").resolve()
C_PLAN_ROOT = (TRUTH / "reports" / "delta_order_plan_v1").resolve()


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _parse_day_utc(s: str) -> str:
    """
    Strict UTC day key validator (institutional hardening).

    Requirements:
    - exactly 10 chars: YYYY-MM-DD
    - positions 4 and 7 are '-'
    - all other positions MUST be digits

    This rejects templates like "YYYY-MM-DD".
    """
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise ValueError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    if (not d[0:4].isdigit()) or (not d[5:7].isdigit()) or (not d[8:10].isdigit()):
        raise ValueError(f"BAD_DAY_UTC_NOT_NUMERIC_YYYY_MM_DD: {d!r}")
    return d


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_dir_deterministic(root: Path) -> str:
    if not root.exists() or not root.is_dir():
        return hashlib.sha256(b"").hexdigest()
    items: List[Tuple[str, str]] = []
    for p in root.rglob("*"):
        if p.is_file():
            rel = str(p.relative_to(root)).replace("\\", "/")
            items.append((rel, _sha256_file(p)))
    items.sort(key=lambda x: x[0])
    h = hashlib.sha256()
    for rel, fsha in items:
        h.update(rel.encode("utf-8"))
        h.update(b"\n")
        h.update(fsha.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def _count_files_matching(root: Path, glob_pat: str) -> int:
    if not root.exists() or not root.is_dir():
        return 0
    return len([p for p in root.glob(glob_pat) if p.is_file()])


def _read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _stage(
    stage_id: str,
    root: Path,
    present: bool,
    sha256: str,
    items_total: int,
    status: str,
    blocking: bool,
    reason_codes: List[str],
) -> Dict[str, Any]:
    return {
        "stage_id": stage_id,
        "status": status,
        "blocking": bool(blocking),
        "reason_codes": reason_codes,
        "counts": {"items_total": int(items_total), "items_ok": None, "items_fail": None},
        "artifacts": {"root": str(root), "present": bool(present), "sha256": sha256},
    }


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_pipeline_manifest_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    day = _parse_day_utc(args.day_utc)
    enforce_operational_day_key_invariant_v1(day)

    input_manifest: List[Dict[str, str]] = []
    stages: List[Dict[str, Any]] = []
    top_reason_codes: List[str] = []
    notes: List[str] = []

    blocking_failures = 0
    nonblocking_degradations = 0

    def add_input(t: str, p: Path, sha: str) -> None:
        input_manifest.append({"type": t, "path": str(p), "sha256": sha})

    # INTENTS
    intents_day = (INTENTS_ROOT / day).resolve()
    intents_present = intents_day.exists()
    intents_count = _count_files_matching(intents_day, "*.json") if intents_present else 0
    intents_sha = _sha256_dir_deterministic(intents_day) if intents_present else _sha256_bytes(b"")
    add_input("intents_day_dir", intents_day, intents_sha)
    intents_status = "OK" if intents_present and intents_count > 0 else ("MISSING" if not intents_present else "FAIL")
    intents_rc: List[str] = []
    if not intents_present:
        intents_rc.append("MISSING_INTENTS_DAY_DIR")
    elif intents_count == 0:
        intents_rc.append("EMPTY_INTENTS_DAY_DIR")
    if intents_status != "OK":
        blocking_failures += 1
        top_reason_codes += intents_rc
    stages.append(_stage("INTENTS", intents_day, intents_present, intents_sha, intents_count, intents_status, True, intents_rc))

    # PREFLIGHT
    preflight_day = (PREFLIGHT_ROOT / day).resolve()
    preflight_present = preflight_day.exists()
    preflight_count = _count_files_matching(preflight_day, "*.json") if preflight_present else 0
    preflight_sha = _sha256_dir_deterministic(preflight_day) if preflight_present else _sha256_bytes(b"")
    add_input("preflight_day_dir", preflight_day, preflight_sha)
    preflight_status = "OK" if preflight_present and preflight_count > 0 else ("MISSING" if not preflight_present else "FAIL")
    preflight_rc: List[str] = []
    if not preflight_present:
        preflight_rc.append("MISSING_PREFLIGHT_DAY_DIR")
    elif preflight_count == 0:
        preflight_rc.append("EMPTY_PREFLIGHT_DAY_DIR")
    if preflight_status != "OK":
        blocking_failures += 1
        top_reason_codes += preflight_rc
    stages.append(_stage("PREFLIGHT", preflight_day, preflight_present, preflight_sha, preflight_count, preflight_status, True, preflight_rc))

    # OMS
    oms_day = (OMS_ROOT / day).resolve()
    oms_present = oms_day.exists()
    oms_count = _count_files_matching(oms_day, "*.json") if oms_present else 0
    oms_sha = _sha256_dir_deterministic(oms_day) if oms_present else _sha256_bytes(b"")
    add_input("oms_day_dir", oms_day, oms_sha)
    oms_status = "OK" if oms_present and oms_count > 0 else ("MISSING" if not oms_present else "FAIL")
    oms_rc: List[str] = []
    if not oms_present:
        oms_rc.append("MISSING_OMS_DAY_DIR")
    elif oms_count == 0:
        oms_rc.append("EMPTY_OMS_DAY_DIR")
    if oms_status != "OK":
        blocking_failures += 1
        top_reason_codes += oms_rc
    stages.append(_stage("OMS", oms_day, oms_present, oms_sha, oms_count, oms_status, True, oms_rc))

    # ALLOCATION
    alloc_day = (ALLOCATION_ROOT / day).resolve()
    alloc_present = alloc_day.exists()
    alloc_count = _count_files_matching(alloc_day, "*.json") if alloc_present else 0
    alloc_sha = _sha256_dir_deterministic(alloc_day) if alloc_present else _sha256_bytes(b"")
    add_input("allocation_day_dir", alloc_day, alloc_sha)
    alloc_status = "OK" if alloc_present and alloc_count > 0 else ("MISSING" if not alloc_present else "FAIL")
    alloc_rc: List[str] = []
    if not alloc_present:
        alloc_rc.append("MISSING_ALLOCATION_DAY_DIR")
    elif alloc_count == 0:
        alloc_rc.append("EMPTY_ALLOCATION_DAY_DIR")
    if alloc_status != "OK":
        blocking_failures += 1
        top_reason_codes += alloc_rc
    stages.append(_stage("ALLOCATION", alloc_day, alloc_present, alloc_sha, alloc_count, alloc_status, True, alloc_rc))

    # PHASED submissions (non-authoritative)
    phased_present = PHASED_ROOT.exists() and PHASED_ROOT.is_dir()
    phased_sha = _sha256_dir_deterministic(PHASED_ROOT) if phased_present else _sha256_bytes(b"")
    phased_count = len([p for p in PHASED_ROOT.iterdir() if p.is_dir()]) if phased_present else 0
    add_input("phaseD_submissions_root", PHASED_ROOT, phased_sha)
    phased_rc: List[str] = []
    phased_status = "OK" if phased_present else "DEGRADED"
    if not phased_present:
        phased_rc.append("PHASED_SUBMISSIONS_ROOT_MISSING")
        nonblocking_degradations += 1
        top_reason_codes += phased_rc
    stages.append(_stage("PHASED_SUBMISSIONS", PHASED_ROOT, phased_present, phased_sha, phased_count, phased_status, False, phased_rc))

    # ENGINE RISK BUDGET LEDGER (required)
    risk_path = (RISK_LEDGER_ROOT / day / "engine_risk_budget_ledger.v1.json").resolve()
    risk_present = risk_path.exists()
    risk_sha = _sha256_file(risk_path) if risk_present else _sha256_bytes(b"")
    add_input("engine_risk_budget_ledger_v1", risk_path, risk_sha)
    risk_rc: List[str] = []
    risk_status = "OK" if risk_present else "MISSING"
    if not risk_present:
        risk_rc.append("MISSING_ENGINE_RISK_BUDGET_LEDGER")
        blocking_failures += 1
        top_reason_codes += risk_rc
        risk_status = "MISSING"
    else:
        rl = _read_json(risk_path)
        st = str(rl.get("status") or "FAIL").strip().upper()
        if st != "OK":
            risk_status = "FAIL"
            risk_rc.append("ENGINE_RISK_BUDGET_LEDGER_NOT_OK")
            blocking_failures += 1
            top_reason_codes += risk_rc
    stages.append(_stage("ENGINE_RISK_BUDGET_LEDGER", risk_path, risk_present, risk_sha, 1 if risk_present else 0, risk_status, True, risk_rc))

    # CAPITAL RISK ENVELOPE (required): PASS required
    cap_path = (CAP_RISK_ROOT / day / "capital_risk_envelope.v1.json").resolve()
    cap_present = cap_path.exists()
    cap_sha = _sha256_file(cap_path) if cap_present else _sha256_bytes(b"")
    add_input("capital_risk_envelope_v1", cap_path, cap_sha)
    cap_rc: List[str] = []
    cap_status = "OK" if cap_present else "MISSING"
    if not cap_present:
        cap_rc.append("MISSING_CAPITAL_RISK_ENVELOPE")
        blocking_failures += 1
        top_reason_codes += cap_rc
        cap_status = "MISSING"
    else:
        ce = _read_json(cap_path)
        st = str(ce.get("status") or "FAIL").strip().upper()
        if st != "PASS":
            cap_status = "FAIL"
            cap_rc.append("CAPITAL_RISK_ENVELOPE_NOT_PASS")
            blocking_failures += 1
            top_reason_codes += cap_rc
    stages.append(_stage("CAPITAL_RISK_ENVELOPE", cap_path, cap_present, cap_sha, 1 if cap_present else 0, cap_status, True, cap_rc))

    # REGIME CLASSIFICATION (required v2): status OK and blocking must be false
    regime_path = (REGIME_ROOT / day / "regime_snapshot.v2.json").resolve()
    regime_present = regime_path.exists()
    regime_sha = _sha256_file(regime_path) if regime_present else _sha256_bytes(b"")
    add_input("regime_snapshot_v2", regime_path, regime_sha)
    regime_rc: List[str] = []
    regime_status = "OK" if regime_present else "MISSING"
    if not regime_present:
        regime_rc.append("MISSING_REGIME_SNAPSHOT_V2")
        blocking_failures += 1
        top_reason_codes += regime_rc
        stages.append(_stage("REGIME_CLASSIFICATION", regime_path, regime_present, regime_sha, 0, "MISSING", True, regime_rc))
    else:
        try:
            rr = _read_json(regime_path)
            st = str(rr.get("status") or "FAIL").strip().upper()
            blk = bool(rr.get("blocking"))
            if st != "OK":
                regime_status = "FAIL"
                regime_rc.append("REGIME_STATUS_NOT_OK")
                blocking_failures += 1
                top_reason_codes += regime_rc
            if blk:
                regime_status = "FAIL"
                regime_rc.append("REGIME_BLOCKING_TRUE")
                blocking_failures += 1
                top_reason_codes += regime_rc
        except Exception:
            regime_status = "FAIL"
            regime_rc.append("REGIME_PARSE_ERROR")
            blocking_failures += 1
            top_reason_codes += regime_rc
        stages.append(_stage("REGIME_CLASSIFICATION", regime_path, regime_present, regime_sha, 1, regime_status, True, regime_rc))

    # Determine top-level status
    status = "OK"
    if blocking_failures > 0:
        status = "FAIL"
    elif nonblocking_degradations > 0:
        status = "DEGRADED"

    top_reason_codes = sorted(list(dict.fromkeys(top_reason_codes)))

    manifest = {
        "schema_id": "pipeline_manifest",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": _utc_now(),
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_pipeline_manifest_v1.py", "git_sha": _git_sha()},
        "status": status,
        "reason_codes": top_reason_codes,
        "notes": notes,
        "input_manifest": input_manifest,
        "stages": stages,
        "summary": {"blocking_failures": int(blocking_failures), "nonblocking_degradations": int(nonblocking_degradations)},
    }

    validate_against_repo_schema_v1(manifest, REPO_ROOT, SCHEMA_RELPATH)

    out_dir = (OUT_ROOT / day).resolve()
    out_path = (out_dir / "pipeline_manifest.v1.json").resolve()
    payload = (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")

    try:
        wr = write_file_immutable_v1(path=out_path, data=payload, create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL: IMMUTABLE_WRITE_ERROR: {e}") from e

    print(f"OK: PIPELINE_MANIFEST_WRITTEN day_utc={day} status={status} path={wr.path} sha256={wr.sha256} action={wr.action}")
    return 0 if status == "OK" else 1


if __name__ == "__main__":
    raise SystemExit(main())
