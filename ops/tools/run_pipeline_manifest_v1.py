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
# .../ops/tools/run_pipeline_manifest_v1.py -> repo root is parents[2]
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

# Fail-closed: verify expected repo structure exists
if not (_REPO_ROOT_FROM_FILE / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT_FROM_FILE}")
if not (_REPO_ROOT_FROM_FILE / "governance").exists():
    raise SystemExit(f"FATAL: repo_root_missing_governance: derived={_REPO_ROOT_FROM_FILE}")

import argparse
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/pipeline_manifest.v1.schema.json"
SCHEMA_PATH = (REPO_ROOT / SCHEMA_RELPATH).resolve()

OUT_ROOT = (TRUTH / "reports" / "pipeline_manifest_v1").resolve()

INTENTS_ROOT = (TRUTH / "intents_v1/snapshots").resolve()
PREFLIGHT_ROOT = (TRUTH / "phaseC_preflight_v1").resolve()
OMS_ROOT = (TRUTH / "oms_decisions_v1/decisions").resolve()
ALLOCATION_ROOT = (TRUTH / "allocation_v1/summary").resolve()

PHASED_ROOT = (REPO_ROOT / "constellation_2/phaseD/outputs/submissions").resolve()
EXEC_TRUTH_ROOT = (TRUTH / "execution_evidence_v1/submissions").resolve()
EXEC_MANIFEST_ROOT = (TRUTH / "execution_evidence_v1/manifests").resolve()
SUBMISSION_INDEX_ROOT = (TRUTH / "execution_evidence_v1/submissions").resolve()  # index file lives under exec_evidence submissions day
POSITIONS_ROOT = (TRUTH / "positions_v1/snapshots").resolve()
CASH_ROOT = (TRUTH / "cash_ledger_v1/snapshots").resolve()
ACCOUNTING_ROOT = (TRUTH / "accounting_v1").resolve()

RECON_ROOT = (TRUTH / "reports" / "reconciliation_report_v1").resolve()
GATE_ROOT = (TRUTH / "reports" / "operator_daily_gate_v1").resolve()


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


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_dir_deterministic(root: Path) -> str:
    """
    Deterministic directory hash:
    - iterate files under root
    - for each file: relpath + newline + sha256(file) + newline
    """
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
    intents_blocking = True
    if intents_status != "OK":
        blocking_failures += 1
        top_reason_codes += intents_rc
    stages.append(_stage("INTENTS", intents_day, intents_present, intents_sha, intents_count, intents_status, intents_blocking, intents_rc))

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
    preflight_blocking = True
    if preflight_status != "OK":
        blocking_failures += 1
        top_reason_codes += preflight_rc
    stages.append(_stage("PREFLIGHT", preflight_day, preflight_present, preflight_sha, preflight_count, preflight_status, preflight_blocking, preflight_rc))

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
    oms_blocking = True
    if oms_status != "OK":
        blocking_failures += 1
        top_reason_codes += oms_rc
    stages.append(_stage("OMS", oms_day, oms_present, oms_sha, oms_count, oms_status, oms_blocking, oms_rc))

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
    alloc_blocking = True
    if alloc_status != "OK":
        blocking_failures += 1
        top_reason_codes += alloc_rc
    stages.append(_stage("ALLOCATION", alloc_day, alloc_present, alloc_sha, alloc_count, alloc_status, alloc_blocking, alloc_rc))

    # PHASED submissions (non-authoritative)
    phased_present = PHASED_ROOT.exists() and PHASED_ROOT.is_dir()
    phased_sha = _sha256_dir_deterministic(PHASED_ROOT) if phased_present else _sha256_bytes(b"")
    phased_count = len([p for p in PHASED_ROOT.iterdir() if p.is_dir()]) if phased_present else 0
    add_input("phaseD_submissions_root", PHASED_ROOT, phased_sha)
    phased_rc: List[str] = []
    phased_status = "OK" if phased_present else "MISSING"
    phased_blocking = False
    if not phased_present:
        phased_status = "DEGRADED"
        phased_rc.append("PHASED_SUBMISSIONS_ROOT_MISSING")
        nonblocking_degradations += 1
        top_reason_codes += phased_rc
    stages.append(_stage("PHASED_SUBMISSIONS", PHASED_ROOT, phased_present, phased_sha, phased_count, phased_status, phased_blocking, phased_rc))

    # EXECUTION evidence truth
    exec_day = (EXEC_TRUTH_ROOT / day).resolve()
    exec_present = exec_day.exists()
    exec_sha = _sha256_dir_deterministic(exec_day) if exec_present else _sha256_bytes(b"")
    exec_count = len([p for p in exec_day.iterdir() if p.is_dir()]) if exec_present else 0
    add_input("exec_evidence_truth_day_dir", exec_day, exec_sha)
    exec_rc: List[str] = []
    exec_status = "OK" if exec_present else "MISSING"
    exec_blocking = True
    if not exec_present:
        exec_rc.append("MISSING_EXEC_EVIDENCE_TRUTH_DAY_DIR")
        blocking_failures += 1
        top_reason_codes += exec_rc
    stages.append(_stage("EXEC_EVIDENCE_TRUTH", exec_day, exec_present, exec_sha, exec_count, exec_status if exec_present else "MISSING", exec_blocking, exec_rc))

    # EXECUTION evidence manifest
    man_day = (EXEC_MANIFEST_ROOT / day).resolve()
    man_present = man_day.exists()
    man_sha = _sha256_dir_deterministic(man_day) if man_present else _sha256_bytes(b"")
    man_count = _count_files_matching(man_day, "*.json") if man_present else 0
    add_input("exec_evidence_manifest_day_dir", man_day, man_sha)
    man_rc: List[str] = []
    man_status = "OK" if man_present and man_count > 0 else ("MISSING" if not man_present else "FAIL")
    man_blocking = True
    if not man_present:
        man_rc.append("MISSING_EXEC_EVIDENCE_MANIFEST_DAY_DIR")
        blocking_failures += 1
        top_reason_codes += man_rc
    elif man_count == 0:
        man_rc.append("EMPTY_EXEC_EVIDENCE_MANIFEST_DAY_DIR")
        blocking_failures += 1
        top_reason_codes += man_rc
    stages.append(_stage("EXEC_EVIDENCE_MANIFEST", man_day, man_present, man_sha, man_count, man_status, man_blocking, man_rc))

    # SUBMISSION INDEX
    idx_path = (EXEC_TRUTH_ROOT / day / "submission_index.v1.json").resolve()
    idx_present = idx_path.exists()
    idx_sha = _sha256_file(idx_path) if idx_present else _sha256_bytes(b"")
    add_input("submission_index_v1", idx_path, idx_sha)
    idx_rc: List[str] = []
    idx_status = "OK" if idx_present else "MISSING"
    idx_blocking = True
    if not idx_present:
        idx_rc.append("MISSING_SUBMISSION_INDEX_V1")
        blocking_failures += 1
        top_reason_codes += idx_rc
    stages.append(_stage("SUBMISSION_INDEX", idx_path, idx_present, idx_sha, 1 if idx_present else 0, idx_status, idx_blocking, idx_rc))

    # POSITIONS
    pos_day = (POSITIONS_ROOT / day).resolve()
    pos_present = pos_day.exists()
    pos_sha = _sha256_dir_deterministic(pos_day) if pos_present else _sha256_bytes(b"")
    pos_count = _count_files_matching(pos_day, "*.json") if pos_present else 0
    add_input("positions_day_dir", pos_day, pos_sha)
    pos_rc: List[str] = []
    pos_status = "OK" if pos_present and pos_count > 0 else ("MISSING" if not pos_present else "FAIL")
    pos_blocking = True
    if not pos_present:
        pos_rc.append("MISSING_POSITIONS_DAY_DIR")
        blocking_failures += 1
        top_reason_codes += pos_rc
    elif pos_count == 0:
        pos_rc.append("EMPTY_POSITIONS_DAY_DIR")
        blocking_failures += 1
        top_reason_codes += pos_rc
    stages.append(_stage("POSITIONS", pos_day, pos_present, pos_sha, pos_count, pos_status, pos_blocking, pos_rc))

    # CASH LEDGER
    cash_day = (CASH_ROOT / day).resolve()
    cash_present = cash_day.exists()
    cash_sha = _sha256_dir_deterministic(cash_day) if cash_present else _sha256_bytes(b"")
    cash_count = _count_files_matching(cash_day, "*.json") if cash_present else 0
    add_input("cash_ledger_day_dir", cash_day, cash_sha)
    cash_rc: List[str] = []
    cash_status = "OK" if cash_present and cash_count > 0 else ("MISSING" if not cash_present else "FAIL")
    cash_blocking = True
    if not cash_present:
        cash_rc.append("MISSING_CASH_LEDGER_DAY_DIR")
        blocking_failures += 1
        top_reason_codes += cash_rc
    elif cash_count == 0:
        cash_rc.append("EMPTY_CASH_LEDGER_DAY_DIR")
        blocking_failures += 1
        top_reason_codes += cash_rc
    stages.append(_stage("CASH_LEDGER", cash_day, cash_present, cash_sha, cash_count, cash_status, cash_blocking, cash_rc))

    # ACCOUNTING (presence only)
    acct_present = ACCOUNTING_ROOT.exists() and ACCOUNTING_ROOT.is_dir()
    acct_sha = _sha256_dir_deterministic(ACCOUNTING_ROOT) if acct_present else _sha256_bytes(b"")
    add_input("accounting_root", ACCOUNTING_ROOT, acct_sha)
    acct_rc: List[str] = []
    acct_status = "OK" if acct_present else "MISSING"
    acct_blocking = False
    if not acct_present:
        acct_status = "DEGRADED"
        acct_rc.append("ACCOUNTING_ROOT_MISSING")
        nonblocking_degradations += 1
        top_reason_codes += acct_rc
    stages.append(_stage("ACCOUNTING", ACCOUNTING_ROOT, acct_present, acct_sha, 0, acct_status, acct_blocking, acct_rc))

    # RECONCILIATION (required)
    recon_path = (RECON_ROOT / day / "reconciliation_report.v1.json").resolve()
    recon_present = recon_path.exists()
    recon_sha = _sha256_file(recon_path) if recon_present else _sha256_bytes(b"")
    add_input("reconciliation_report_v1", recon_path, recon_sha)
    recon_rc: List[str] = []
    recon_status = "OK" if recon_present else "MISSING"
    recon_blocking = True
    if not recon_present:
        recon_rc.append("MISSING_RECONCILIATION_REPORT")
        blocking_failures += 1
        top_reason_codes += recon_rc
    else:
        rr = _read_json(recon_path)
        st = str(rr.get("status") or "MISSING")
        if st != "OK":
            recon_status = "FAIL"
            recon_rc.append("RECONCILIATION_NOT_OK")
            blocking_failures += 1
            top_reason_codes += recon_rc
    stages.append(_stage("RECONCILIATION", recon_path, recon_present, recon_sha, 1 if recon_present else 0, recon_status, recon_blocking, recon_rc))

    # OPERATOR GATE (required)
    gate_path = (GATE_ROOT / day / "operator_daily_gate.v1.json").resolve()
    gate_present = gate_path.exists()
    gate_sha = _sha256_file(gate_path) if gate_present else _sha256_bytes(b"")
    add_input("operator_daily_gate_v1", gate_path, gate_sha)
    gate_rc: List[str] = []
    gate_status = "OK" if gate_present else "MISSING"
    gate_blocking = True
    if not gate_present:
        gate_rc.append("MISSING_OPERATOR_DAILY_GATE")
        blocking_failures += 1
        top_reason_codes += gate_rc
    else:
        gg = _read_json(gate_path)
        st = str(gg.get("status") or "FAIL")
        if st != "PASS":
            gate_status = "FAIL"
            gate_rc.append("OPERATOR_DAILY_GATE_NOT_PASS")
            blocking_failures += 1
            top_reason_codes += gate_rc
    stages.append(_stage("OPERATOR_GATE", gate_path, gate_present, gate_sha, 1 if gate_present else 0, gate_status, gate_blocking, gate_rc))

    # Determine top-level status
    status = "OK"
    if blocking_failures > 0:
        status = "FAIL"
    elif nonblocking_degradations > 0:
        status = "DEGRADED"

    # Deduplicate reason codes deterministically
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

    # Validate against governed schema (fail-closed)
    validate_against_repo_schema_v1(manifest, REPO_ROOT, SCHEMA_RELPATH)

    out_dir = (OUT_ROOT / day).resolve()
    out_path = (out_dir / "pipeline_manifest.v1.json").resolve()
    payload = (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")

    try:
        wr = write_file_immutable_v1(path=out_path, data=payload, create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL: IMMUTABLE_WRITE_ERROR: {e}") from e

    print(
        "OK: PIPELINE_MANIFEST_WRITTEN "
        f"day_utc={day} status={status} path={wr.path} sha256={wr.sha256} action={wr.action}"
    )
    return 0 if status == "OK" else 1


if __name__ == "__main__":
    raise SystemExit(main())
