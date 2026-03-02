#!/usr/bin/env python3
"""
run_pipeline_manifest_v1.py

Bundle A: pipeline_manifest.v1.json writer (immutable truth artifact).

This writer MUST emit the legacy path expected by downstream tools:
  constellation_2/runtime/truth/reports/pipeline_manifest_v1/<DAY>/pipeline_manifest.v1.json

Schema:
  governance/04_DATA/SCHEMAS/C2/REPORTS/pipeline_manifest.v1.schema.json

Determinism:
- produced_utc is day-scoped: <DAY>T00:00:00Z
- directory hashes are deterministic (sorted relative paths + file sha256)

Fail-closed:
- schema validation required
- immutable write (EXISTS_IDENTICAL allowed; rewrite forbidden)

Usage:
  python3 ops/tools/run_pipeline_manifest_v1.py --day_utc YYYY-MM-DD
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/pipeline_manifest.v1.schema.json"

# --- v1 stage roots (matches historical artifacts) ---
INTENTS_ROOT = (TRUTH_ROOT / "intents_v1" / "snapshots").resolve()
PREFLIGHT_ROOT = (TRUTH_ROOT / "phaseC_preflight_v1").resolve()
OMS_ROOT = (TRUTH_ROOT / "oms_decisions_v1" / "decisions").resolve()
ALLOCATION_ROOT = (TRUTH_ROOT / "allocation_v1" / "summary").resolve()

PHASED_ROOT = (REPO_ROOT / "constellation_2" / "phaseD" / "outputs" / "submissions").resolve()

EXEC_TRUTH_ROOT = (TRUTH_ROOT / "execution_evidence_v1" / "submissions").resolve()
EXEC_MANIFEST_ROOT = (TRUTH_ROOT / "execution_evidence_v1" / "manifests").resolve()
SUBMISSION_INDEX_ROOT = (TRUTH_ROOT / "execution_evidence_v1" / "submission_index").resolve()

POSITIONS_ROOT = (TRUTH_ROOT / "positions_v1" / "snapshots").resolve()
CASH_LEDGER_ROOT = (TRUTH_ROOT / "cash_ledger_v1" / "snapshots").resolve()
ACCOUNTING_ROOT = (TRUTH_ROOT / "accounting_v1").resolve()

RECONCILIATION_ROOT = (TRUTH_ROOT / "reports" / "reconciliation_report_v1").resolve()
OPERATOR_DAILY_GATE_ROOT = (TRUTH_ROOT / "reports" / "operator_daily_gate_v1").resolve()

OUT_ROOT = (TRUTH_ROOT / "reports" / "pipeline_manifest_v1").resolve()


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
        return _sha256_bytes(b"")
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


def _count_subdirs(root: Path) -> int:
    if not root.exists() or not root.is_dir():
        return 0
    return len([p for p in root.iterdir() if p.is_dir()])


def _read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _git_sha() -> str:
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        s = out.decode("utf-8").strip()
        if len(s) != 40:
            raise ValueError("bad git sha length")
        return s
    except Exception as e:  # noqa: BLE001
        raise SystemExit(f"FAIL: unable to resolve git sha: {e}") from e


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
        "reason_codes": list(reason_codes),
        "counts": {"items_total": int(items_total), "items_ok": None, "items_fail": None},
        "artifacts": {"root": str(root), "present": bool(present), "sha256": str(sha256)},
    }


def _parse_day_utc(d: str) -> str:
    s = (d or "").strip()
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise SystemExit(f"FAIL: BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {s!r}")
    return s


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_pipeline_manifest_v1")
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    args = ap.parse_args()

    day = _parse_day_utc(args.day_utc)
    produced_utc = f"{day}T00:00:00Z"
    producer = {
        "repo": "constellation_2_runtime",
        "module": "ops/tools/run_pipeline_manifest_v1.py",
        "git_sha": _git_sha(),
    }

    input_manifest: List[Dict[str, Any]] = []

    def add_input(type_name: str, p: Path, sha: str) -> None:
        input_manifest.append({"path": str(p), "sha256": str(sha), "type": str(type_name)})

    stages: List[Dict[str, Any]] = []
    top_reason_codes: List[str] = []
    blocking_failures = 0
    nonblocking_degradations = 0

    # INTENTS
    intents_day = (INTENTS_ROOT / day).resolve()
    intents_present = intents_day.exists() and intents_day.is_dir()
    intents_count = _count_files_matching(intents_day, "*.json") if intents_present else 0
    intents_sha = _sha256_dir_deterministic(intents_day) if intents_present else _sha256_bytes(b"")
    add_input("intents_day_dir", intents_day, intents_sha)
    intents_rc: List[str] = []
    if not intents_present:
        intents_status = "MISSING"
        intents_rc.append("MISSING_INTENTS_DAY_DIR")
    elif intents_count == 0:
        intents_status = "FAIL"
        intents_rc.append("EMPTY_INTENTS_DAY_DIR")
    else:
        intents_status = "OK"
    if intents_status != "OK":
        blocking_failures += 1
        top_reason_codes += intents_rc
    stages.append(_stage("INTENTS", intents_day, intents_present, intents_sha, intents_count, intents_status, True, intents_rc))

    # PREFLIGHT
    preflight_day = (PREFLIGHT_ROOT / day).resolve()
    preflight_present = preflight_day.exists() and preflight_day.is_dir()
    preflight_count = _count_files_matching(preflight_day, "*.json") if preflight_present else 0
    preflight_sha = _sha256_dir_deterministic(preflight_day) if preflight_present else _sha256_bytes(b"")
    add_input("preflight_day_dir", preflight_day, preflight_sha)
    preflight_rc: List[str] = []
    if not preflight_present:
        preflight_status = "MISSING"
        preflight_rc.append("MISSING_PREFLIGHT_DAY_DIR")
    elif preflight_count == 0:
        preflight_status = "FAIL"
        preflight_rc.append("EMPTY_PREFLIGHT_DAY_DIR")
    else:
        preflight_status = "OK"
    if preflight_status != "OK":
        blocking_failures += 1
        top_reason_codes += preflight_rc
    stages.append(_stage("PREFLIGHT", preflight_day, preflight_present, preflight_sha, preflight_count, preflight_status, True, preflight_rc))

    # OMS
    oms_day = (OMS_ROOT / day).resolve()
    oms_present = oms_day.exists() and oms_day.is_dir()
    oms_count = _count_files_matching(oms_day, "*.json") if oms_present else 0
    oms_sha = _sha256_dir_deterministic(oms_day) if oms_present else _sha256_bytes(b"")
    add_input("oms_day_dir", oms_day, oms_sha)
    oms_rc: List[str] = []
    if not oms_present:
        oms_status = "MISSING"
        oms_rc.append("MISSING_OMS_DAY_DIR")
    elif oms_count == 0:
        oms_status = "FAIL"
        oms_rc.append("EMPTY_OMS_DAY_DIR")
    else:
        oms_status = "OK"
    if oms_status != "OK":
        blocking_failures += 1
        top_reason_codes += oms_rc
    stages.append(_stage("OMS", oms_day, oms_present, oms_sha, oms_count, oms_status, True, oms_rc))

    # ALLOCATION
    alloc_day = (ALLOCATION_ROOT / day).resolve()
    alloc_present = alloc_day.exists() and alloc_day.is_dir()
    alloc_count = _count_files_matching(alloc_day, "*.json") if alloc_present else 0
    alloc_sha = _sha256_dir_deterministic(alloc_day) if alloc_present else _sha256_bytes(b"")
    add_input("allocation_day_dir", alloc_day, alloc_sha)
    alloc_rc: List[str] = []
    if not alloc_present:
        alloc_status = "MISSING"
        alloc_rc.append("MISSING_ALLOCATION_DAY_DIR")
    elif alloc_count == 0:
        alloc_status = "FAIL"
        alloc_rc.append("EMPTY_ALLOCATION_DAY_DIR")
    else:
        alloc_status = "OK"
    if alloc_status != "OK":
        blocking_failures += 1
        top_reason_codes += alloc_rc
    stages.append(_stage("ALLOCATION", alloc_day, alloc_present, alloc_sha, alloc_count, alloc_status, True, alloc_rc))

    # PHASED_SUBMISSIONS (nonblocking)
    phased_present = PHASED_ROOT.exists() and PHASED_ROOT.is_dir()
    phased_count = _count_subdirs(PHASED_ROOT) if phased_present else 0
    phased_sha = _sha256_dir_deterministic(PHASED_ROOT) if phased_present else _sha256_bytes(b"")
    add_input("phaseD_submissions_root", PHASED_ROOT, phased_sha)
    phased_rc: List[str] = []
    if not phased_present:
        phased_status = "DEGRADED"
        phased_rc.append("PHASED_SUBMISSIONS_ROOT_MISSING")
        nonblocking_degradations += 1
        top_reason_codes += phased_rc
    else:
        phased_status = "OK"
    stages.append(_stage("PHASED_SUBMISSIONS", PHASED_ROOT, phased_present, phased_sha, phased_count, phased_status, False, phased_rc))

    # EXEC_EVIDENCE_TRUTH
    exec_day = (EXEC_TRUTH_ROOT / day).resolve()
    exec_present = exec_day.exists() and exec_day.is_dir()
    exec_subdir_count = _count_subdirs(exec_day) if exec_present else 0
    exec_sha = _sha256_dir_deterministic(exec_day) if exec_present else _sha256_bytes(b"")
    add_input("exec_evidence_truth_day_dir", exec_day, exec_sha)
    exec_rc: List[str] = []
    if not exec_present:
        exec_status = "MISSING"
        exec_rc.append("MISSING_EXEC_EVIDENCE_TRUTH_DAY_DIR")
    elif exec_subdir_count == 0:
        exec_status = "FAIL"
        exec_rc.append("EMPTY_EXEC_EVIDENCE_TRUTH_DAY_DIR")
    else:
        exec_status = "OK"
    if exec_status != "OK":
        blocking_failures += 1
        top_reason_codes += exec_rc
    stages.append(_stage("EXEC_EVIDENCE_TRUTH", exec_day, exec_present, exec_sha, exec_subdir_count, exec_status, True, exec_rc))

    # EXEC_EVIDENCE_MANIFEST
    man_day = (EXEC_MANIFEST_ROOT / day).resolve()
    man_present = man_day.exists() and man_day.is_dir()
    man_count = _count_files_matching(man_day, "*.json") if man_present else 0
    man_sha = _sha256_dir_deterministic(man_day) if man_present else _sha256_bytes(b"")
    add_input("exec_evidence_manifest_day_dir", man_day, man_sha)
    man_rc: List[str] = []
    if not man_present:
        man_status = "MISSING"
        man_rc.append("MISSING_EXEC_EVIDENCE_MANIFEST_DAY_DIR")
    elif man_count == 0:
        man_status = "FAIL"
        man_rc.append("EMPTY_EXEC_EVIDENCE_MANIFEST_DAY_DIR")
    else:
        man_status = "OK"
    if man_status != "OK":
        blocking_failures += 1
        top_reason_codes += man_rc
    stages.append(_stage("EXEC_EVIDENCE_MANIFEST", man_day, man_present, man_sha, man_count, man_status, True, man_rc))

    # SUBMISSION_INDEX (legacy file required if there are submissions; otherwise OK)
    subidx_path = (SUBMISSION_INDEX_ROOT / day / "submission_index.v1.json").resolve()
    subidx_present = subidx_path.exists() and subidx_path.is_file()
    subs_present = exec_present and exec_subdir_count > 0

    if not subs_present:
        # no submissions -> OK even without legacy index file
        si_root = subidx_path
        si_sha = _sha256_file(subidx_path) if subidx_present else _sha256_bytes(b"")
        add_input("submission_evidence_ok_no_submissions", si_root, si_sha)
        stages.append(_stage("SUBMISSION_INDEX", si_root, True, si_sha, 1 if subidx_present else 0, "OK", True, []))
    else:
        si_rc: List[str] = []
        if subidx_present:
            si_sha = _sha256_file(subidx_path)
            add_input("submission_index_v1", subidx_path, si_sha)
            stages.append(_stage("SUBMISSION_INDEX", subidx_path, True, si_sha, 1, "OK", True, []))
        else:
            si_rc.append("MISSING_SUBMISSION_INDEX_V1")
            blocking_failures += 1
            top_reason_codes += si_rc
            add_input("submission_index_v1", subidx_path, _sha256_bytes(b""))
            stages.append(_stage("SUBMISSION_INDEX", subidx_path, False, _sha256_bytes(b""), 0, "MISSING", True, si_rc))

    # POSITIONS
    pos_day = (POSITIONS_ROOT / day).resolve()
    pos_present = pos_day.exists() and pos_day.is_dir()
    pos_count = _count_files_matching(pos_day, "*.json") if pos_present else 0
    pos_sha = _sha256_dir_deterministic(pos_day) if pos_present else _sha256_bytes(b"")
    add_input("positions_day_dir", pos_day, pos_sha)
    pos_rc: List[str] = []
    if not pos_present:
        pos_status = "MISSING"
        pos_rc.append("MISSING_POSITIONS_DAY_DIR")
    elif pos_count == 0:
        pos_status = "FAIL"
        pos_rc.append("EMPTY_POSITIONS_DAY_DIR")
    else:
        pos_status = "OK"
    if pos_status != "OK":
        blocking_failures += 1
        top_reason_codes += pos_rc
    stages.append(_stage("POSITIONS", pos_day, pos_present, pos_sha, pos_count, pos_status, True, pos_rc))

    # CASH_LEDGER
    cash_day = (CASH_LEDGER_ROOT / day).resolve()
    cash_present = cash_day.exists() and cash_day.is_dir()
    cash_count = _count_files_matching(cash_day, "*.json") if cash_present else 0
    cash_sha = _sha256_dir_deterministic(cash_day) if cash_present else _sha256_bytes(b"")
    add_input("cash_ledger_day_dir", cash_day, cash_sha)
    cash_rc: List[str] = []
    if not cash_present:
        cash_status = "MISSING"
        cash_rc.append("MISSING_CASH_LEDGER_DAY_DIR")
    elif cash_count == 0:
        cash_status = "FAIL"
        cash_rc.append("EMPTY_CASH_LEDGER_DAY_DIR")
    else:
        cash_status = "OK"
    if cash_status != "OK":
        blocking_failures += 1
        top_reason_codes += cash_rc
    stages.append(_stage("CASH_LEDGER", cash_day, cash_present, cash_sha, cash_count, cash_status, True, cash_rc))

    # ACCOUNTING (informational)
    acct_present = ACCOUNTING_ROOT.exists() and ACCOUNTING_ROOT.is_dir()
    acct_sha = _sha256_dir_deterministic(ACCOUNTING_ROOT) if acct_present else _sha256_bytes(b"")
    add_input("accounting_root", ACCOUNTING_ROOT, acct_sha)
    stages.append(_stage("ACCOUNTING", ACCOUNTING_ROOT, acct_present, acct_sha, 0, "OK" if acct_present else "DEGRADED", False, []))

    # RECONCILIATION (required)
    rec_path = (RECONCILIATION_ROOT / day / "reconciliation_report.v1.json").resolve()
    rec_present = rec_path.exists() and rec_path.is_file()
    rec_sha = _sha256_file(rec_path) if rec_present else _sha256_bytes(b"")
    add_input("reconciliation_report_v1", rec_path, rec_sha)
    rec_rc: List[str] = []
    if not rec_present:
        rec_status = "MISSING"
        rec_rc.append("MISSING_RECONCILIATION_REPORT")
        blocking_failures += 1
        top_reason_codes += rec_rc
    else:
        rec_status = "OK"
    stages.append(_stage("RECONCILIATION", rec_path, rec_present, rec_sha, 1 if rec_present else 0, rec_status, True, rec_rc))

    # OPERATOR_GATE (required)
    op_path = (OPERATOR_DAILY_GATE_ROOT / day / "operator_daily_gate.v1.json").resolve()
    op_present = op_path.exists() and op_path.is_file()
    op_sha = _sha256_file(op_path) if op_present else _sha256_bytes(b"")
    add_input("operator_daily_gate_v1", op_path, op_sha)
    op_rc: List[str] = []
    if not op_present:
        op_status = "MISSING"
        op_rc.append("MISSING_OPERATOR_DAILY_GATE")
        blocking_failures += 1
        top_reason_codes += op_rc
    else:
        op_status = "OK"
    stages.append(_stage("OPERATOR_GATE", op_path, op_present, op_sha, 1 if op_present else 0, op_status, True, op_rc))

    # Deduplicate top reason codes while preserving order
    dedup_rc: List[str] = []
    seen = set()
    for rc in top_reason_codes:
        if rc not in seen:
            seen.add(rc)
            dedup_rc.append(rc)

    status = "PASS" if blocking_failures == 0 else "FAIL"
    out_obj: Dict[str, Any] = {
        "schema_id": "pipeline_manifest",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": producer,
        "status": status,
        "reason_codes": dedup_rc,
        "summary": {"blocking_failures": int(blocking_failures), "nonblocking_degradations": int(nonblocking_degradations)},
        "stages": stages,
        "input_manifest": input_manifest,
        "notes": [],
    }

    # Validate
    validate_against_repo_schema_v1(out_obj, REPO_ROOT, SCHEMA_RELPATH)

    out_path = (OUT_ROOT / day / "pipeline_manifest.v1.json").resolve()
    out_bytes = (json.dumps(out_obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")

    try:
        _ = write_file_immutable_v1(path=out_path, data=out_bytes, create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL_IMMUTABLE_WRITE: {e}") from e

    sha = _sha256_bytes(out_bytes)
    print(
        f"OK: PIPELINE_MANIFEST_V1_WRITTEN day_utc={day} status={status} "
        f"path={out_path} sha256={sha} blocking_failures={blocking_failures} nonblocking_degradations={nonblocking_degradations}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
