#!/usr/bin/env python3
"""
run_pipeline_manifest_v2.py

Pipeline Manifest v2 (pillars-aware, immutable truth artifact).

This writer mirrors the full v1 stage graph, with one controlled change:

- Stage SUBMISSION_INDEX is satisfied by:
  - legacy submission_index.v1.json (execution_evidence_v1/submission_index/<DAY>/submission_index.v1.json), OR
  - pillars decisions directory (pillars_v1r1 preferred, else pillars_v1) containing >=1 decision record, OR
  - "no submissions" case (OK).

Writes:
  constellation_2/runtime/truth/reports/pipeline_manifest_v2/<DAY>/pipeline_manifest.v2.json

Run:
  python3 ops/tools/run_pipeline_manifest_v2.py --day_utc YYYY-MM-DD
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
from typing import Any, Dict, List, Tuple, Optional

from constellation_2.phaseD.lib.enforce_operational_day_invariant_v1 import enforce_operational_day_key_invariant_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/pipeline_manifest.v2.schema.json"
OUT_ROOT = (TRUTH / "reports" / "pipeline_manifest_v2").resolve()

INTENTS_ROOT = (TRUTH / "intents_v1/snapshots").resolve()
PREFLIGHT_ROOT = (TRUTH / "phaseC_preflight_v1").resolve()
OMS_ROOT = (TRUTH / "oms_decisions_v1/decisions").resolve()
ALLOCATION_ROOT = (TRUTH / "allocation_v1/summary").resolve()

PHASED_ROOT = (REPO_ROOT / "constellation_2/phaseD/outputs/submissions").resolve()
EXEC_TRUTH_ROOT = (TRUTH / "execution_evidence_v1/submissions").resolve()
EXEC_MANIFEST_ROOT = (TRUTH / "execution_evidence_v1/manifests").resolve()
SUBMISSION_INDEX_ROOT = (TRUTH / "execution_evidence_v1" / "submission_index").resolve()

# Pillars decisions (preferred canonical submission evidence)
PILLARS_V1 = (TRUTH / "pillars_v1").resolve()
PILLARS_V1R1 = (TRUTH / "pillars_v1r1").resolve()

POSITIONS_ROOT = (TRUTH / "positions_v1/snapshots").resolve()
CASH_ROOT = (TRUTH / "cash_ledger_v1/snapshots").resolve()
ACCOUNTING_ROOT = (TRUTH / "accounting_v1").resolve()
RISK_LEDGER_ROOT = (TRUTH / "risk_v1" / "engine_budget").resolve()
CAP_RISK_ROOT = (TRUTH / "reports" / "capital_risk_envelope_v1").resolve()

REGIME_ROOT = (TRUTH / "monitoring_v1" / "regime_snapshot_v2").resolve()

RECON_ROOT_V2 = (TRUTH / "reports" / "reconciliation_report_v2").resolve()
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


def _stage(stage_id: str, root: Path, present: bool, sha256: str, items_total: int, status: str, blocking: bool, reason_codes: List[str]) -> Dict[str, Any]:
    return {
        "stage_id": stage_id,
        "status": status,
        "blocking": bool(blocking),
        "reason_codes": reason_codes,
        "counts": {"items_total": int(items_total), "items_ok": None, "items_fail": None},
        "artifacts": {"root": str(root), "present": bool(present), "sha256": sha256},
    }


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
    ap = argparse.ArgumentParser(prog="run_pipeline_manifest_v2")
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

    # EXEC_EVIDENCE_TRUTH (day dir exists and has >=1 submission dir)
    exec_day = (EXEC_TRUTH_ROOT / day).resolve()
    exec_present = exec_day.exists() and exec_day.is_dir()
    exec_subdir_count = len([p for p in exec_day.iterdir() if p.is_dir()]) if exec_present else 0
    exec_sha = _sha256_dir_deterministic(exec_day) if exec_present else _sha256_bytes(b"")
    add_input("exec_evidence_truth_day_dir", exec_day, exec_sha)
    exec_rc: List[str] = []
    if not exec_present:
        exec_status = "MISSING"
        exec_rc.append("MISSING_EXEC_EVIDENCE_TRUTH_DAY_DIR")
        blocking_failures += 1
        top_reason_codes += exec_rc
    elif exec_subdir_count == 0:
        exec_status = "FAIL"
        exec_rc.append("EMPTY_EXEC_EVIDENCE_TRUTH_DAY_DIR")
        blocking_failures += 1
        top_reason_codes += exec_rc
    else:
        exec_status = "OK"
    stages.append(_stage("EXEC_EVIDENCE_TRUTH", exec_day, exec_present, exec_sha, exec_subdir_count, exec_status, True, exec_rc))

    # EXEC_EVIDENCE_MANIFEST (day dir exists)
    man_day = (EXEC_MANIFEST_ROOT / day).resolve()
    man_present = man_day.exists() and man_day.is_dir()
    man_count = _count_files_matching(man_day, "*.json") if man_present else 0
    man_sha = _sha256_dir_deterministic(man_day) if man_present else _sha256_bytes(b"")
    add_input("exec_evidence_manifest_day_dir", man_day, man_sha)
    man_rc: List[str] = []
    if not man_present:
        man_status = "MISSING"
        man_rc.append("MISSING_EXEC_EVIDENCE_MANIFEST_DAY_DIR")
        blocking_failures += 1
        top_reason_codes += man_rc
    else:
        man_status = "OK" if man_count > 0 else "FAIL"
        if man_count == 0:
            man_rc.append("EMPTY_EXEC_EVIDENCE_MANIFEST_DAY_DIR")
            blocking_failures += 1
            top_reason_codes += man_rc
    stages.append(_stage("EXEC_EVIDENCE_MANIFEST", man_day, man_present, man_sha, man_count, man_status, True, man_rc))

    # SUBMISSION_INDEX (pillars-aware)
    subidx_path = (SUBMISSION_INDEX_ROOT / day / "submission_index.v1.json").resolve()
    subidx_present = subidx_path.exists() and subidx_path.is_file()

    pillars_dir = _pillars_decisions_dir(day)
    pillars_present = (pillars_dir is not None) and (_count_decision_records(pillars_dir) > 0)

    subs_present = exec_present and exec_subdir_count > 0

    if not subs_present:
        si_rc: List[str] = []
        si_status = "OK"
        si_root = pillars_dir if pillars_dir is not None else subidx_path
        si_sha = _sha256_dir_deterministic(pillars_dir) if (pillars_dir is not None) else (_sha256_file(subidx_path) if subidx_present else _sha256_bytes(b""))
        si_count = _count_decision_records(pillars_dir) if (pillars_dir is not None) else (1 if subidx_present else 0)
        add_input("submission_evidence_ok_no_submissions", si_root, si_sha)
        stages.append(_stage("SUBMISSION_INDEX", si_root, True, si_sha, si_count, si_status, True, si_rc))
    else:
        if subidx_present:
            si_rc = []
            si_status = "OK"
            si_sha = _sha256_file(subidx_path)
            add_input("submission_index_v1", subidx_path, si_sha)
            stages.append(_stage("SUBMISSION_INDEX", subidx_path, True, si_sha, 1, si_status, True, si_rc))
        elif pillars_present and (pillars_dir is not None):
            si_rc = []
            si_status = "OK"
            si_sha = _sha256_dir_deterministic(pillars_dir)
            add_input("pillars_decisions_dir", pillars_dir, si_sha)
            stages.append(_stage("SUBMISSION_INDEX", pillars_dir, True, si_sha, _count_decision_records(pillars_dir), si_status, True, si_rc))
        else:
            si_rc = ["MISSING_SUBMISSION_INDEX_V1"]
            si_status = "MISSING"
            blocking_failures += 1
            top_reason_codes += si_rc
            add_input("submission_index_v1", subidx_path, _sha256_bytes(b""))
            stages.append(_stage("SUBMISSION_INDEX", subidx_path, False, _sha256_bytes(b""), 0, si_status, True, si_rc))

    # The remaining stages match v1 behavior exactly (risk, capital envelope, regime, positions, cash, accounting, reconciliation, operator gate, bundled C...)
    # For brevity and audit safety, we reuse the same logic paths by reading existing truth surfaces.

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

    # REGIME CLASSIFICATION
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

    # POSITIONS
    pos_day = (POSITIONS_ROOT / day).resolve()
    pos_present = pos_day.exists() and pos_day.is_dir()
    pos_count = _count_files_matching(pos_day, "*.json") if pos_present else 0
    pos_sha = _sha256_dir_deterministic(pos_day) if pos_present else _sha256_bytes(b"")
    add_input("positions_day_dir", pos_day, pos_sha)
    pos_rc: List[str] = []
    pos_status = "OK" if pos_present and pos_count > 0 else ("MISSING" if not pos_present else "FAIL")
    if not pos_present:
        pos_rc.append("MISSING_POSITIONS_DAY_DIR")
        blocking_failures += 1
        top_reason_codes += pos_rc
    elif pos_count == 0:
        pos_rc.append("EMPTY_POSITIONS_DAY_DIR")
        blocking_failures += 1
        top_reason_codes += pos_rc
    stages.append(_stage("POSITIONS", pos_day, pos_present, pos_sha, pos_count, pos_status, True, pos_rc))

    # CASH_LEDGER
    cash_day = (CASH_ROOT / day).resolve()
    cash_present = cash_day.exists() and cash_day.is_dir()
    cash_count = _count_files_matching(cash_day, "*.json") if cash_present else 0
    cash_sha = _sha256_dir_deterministic(cash_day) if cash_present else _sha256_bytes(b"")
    add_input("cash_ledger_day_dir", cash_day, cash_sha)
    cash_rc: List[str] = []
    cash_status = "OK" if cash_present and cash_count > 0 else ("MISSING" if not cash_present else "FAIL")
    if not cash_present:
        cash_rc.append("MISSING_CASH_LEDGER_DAY_DIR")
        blocking_failures += 1
        top_reason_codes += cash_rc
    elif cash_count == 0:
        cash_rc.append("EMPTY_CASH_LEDGER_DAY_DIR")
        blocking_failures += 1
        top_reason_codes += cash_rc
    stages.append(_stage("CASH_LEDGER", cash_day, cash_present, cash_sha, cash_count, cash_status, True, cash_rc))

    # ACCOUNTING
    acct_present = ACCOUNTING_ROOT.exists() and ACCOUNTING_ROOT.is_dir()
    acct_sha = _sha256_dir_deterministic(ACCOUNTING_ROOT) if acct_present else _sha256_bytes(b"")
    add_input("accounting_root", ACCOUNTING_ROOT, acct_sha)
    stages.append(_stage("ACCOUNTING", ACCOUNTING_ROOT, acct_present, acct_sha, 0, "OK" if acct_present else "DEGRADED", False, []))

    # RECONCILIATION
    recon_path = (RECON_ROOT_V2 / day / "reconciliation_report.v2.json").resolve()
    recon_present = recon_path.exists()
    recon_sha = _sha256_file(recon_path) if recon_present else _sha256_bytes(b"")
    add_input("reconciliation_report_v2", recon_path, recon_sha)
    recon_rc: List[str] = []
    recon_status = "OK" if recon_present else "MISSING"
    if not recon_present:
        recon_rc.append("MISSING_RECONCILIATION_REPORT")
        blocking_failures += 1
        top_reason_codes += recon_rc
    stages.append(_stage("RECONCILIATION", recon_path, recon_present, recon_sha, 1 if recon_present else 0, recon_status, True, recon_rc))

    # OPERATOR_GATE
    gate_path = (GATE_ROOT / day / "operator_daily_gate.v1.json").resolve()
    gate_present = gate_path.exists()
    gate_sha = _sha256_file(gate_path) if gate_present else _sha256_bytes(b"")
    add_input("operator_daily_gate_v1", gate_path, gate_sha)
    gate_rc: List[str] = []
    gate_status = "OK" if gate_present else "MISSING"
    if not gate_present:
        gate_rc.append("MISSING_OPERATOR_DAILY_GATE")
        blocking_failures += 1
        top_reason_codes += gate_rc
    stages.append(_stage("OPERATOR_GATE", gate_path, gate_present, gate_sha, 1 if gate_present else 0, gate_status, True, gate_rc))

    # Bundled C: kill switch
    c_kill_path = (C_KILL_ROOT / day / "global_kill_switch_state.v1.json").resolve()
    c_kill_present = c_kill_path.exists()
    c_kill_sha = _sha256_file(c_kill_path) if c_kill_present else _sha256_bytes(b"")
    add_input("bundled_c_kill_switch_state_v1", c_kill_path, c_kill_sha)
    c_kill_rc: List[str] = []
    c_kill_status = "OK" if c_kill_present else "MISSING"
    if not c_kill_present:
        c_kill_rc.append("MISSING_BUNDLED_C_KILL_SWITCH")
        blocking_failures += 1
        top_reason_codes += c_kill_rc
    stages.append(_stage("BUNDLED_C_KILL_SWITCH", c_kill_path, c_kill_present, c_kill_sha, 1 if c_kill_present else 0, c_kill_status, True, c_kill_rc))

    # Bundled C: lifecycle ledger
    c_life_path = (C_LIFE_ROOT / day / "position_lifecycle_ledger.v1.json").resolve()
    c_life_present = c_life_path.exists()
    c_life_sha = _sha256_file(c_life_path) if c_life_present else _sha256_bytes(b"")
    add_input("bundled_c_lifecycle_ledger_v1", c_life_path, c_life_sha)
    c_life_rc: List[str] = []
    c_life_status = "OK" if c_life_present else "MISSING"
    if not c_life_present:
        c_life_rc.append("MISSING_BUNDLED_C_LIFECYCLE_LEDGER")
        blocking_failures += 1
        top_reason_codes += c_life_rc
    stages.append(_stage("BUNDLED_C_LIFECYCLE_LEDGER", c_life_path, c_life_present, c_life_sha, 1 if c_life_present else 0, c_life_status, True, c_life_rc))

    # Bundled C: exposure reconciliation
    c_recon_path = (C_RECON_ROOT / day / "exposure_reconciliation_report.v1.json").resolve()
    c_recon_present = c_recon_path.exists()
    c_recon_sha = _sha256_file(c_recon_path) if c_recon_present else _sha256_bytes(b"")
    add_input("bundled_c_exposure_reconciliation_v1", c_recon_path, c_recon_sha)
    c_recon_rc: List[str] = []
    c_recon_status = "OK" if c_recon_present else "MISSING"
    if not c_recon_present:
        c_recon_rc.append("MISSING_BUNDLED_C_EXPOSURE_RECONCILIATION")
        blocking_failures += 1
        top_reason_codes += c_recon_rc
    stages.append(_stage("BUNDLED_C_EXPOSURE_RECONCILIATION", c_recon_path, c_recon_present, c_recon_sha, 1 if c_recon_present else 0, c_recon_status, True, c_recon_rc))

    # Bundled C: delta order plan
    c_plan_path = (C_PLAN_ROOT / day / "delta_order_plan.v1.json").resolve()
    c_plan_present = c_plan_path.exists()
    c_plan_sha = _sha256_file(c_plan_path) if c_plan_present else _sha256_bytes(b"")
    add_input("bundled_c_delta_order_plan_v1", c_plan_path, c_plan_sha)
    c_plan_rc: List[str] = []
    c_plan_status = "OK" if c_plan_present else "MISSING"
    if not c_plan_present:
        c_plan_rc.append("MISSING_BUNDLED_C_DELTA_ORDER_PLAN")
        blocking_failures += 1
        top_reason_codes += c_plan_rc
    stages.append(_stage("BUNDLED_C_DELTA_ORDER_PLAN", c_plan_path, c_plan_present, c_plan_sha, 1 if c_plan_present else 0, c_plan_status, True, c_plan_rc))

    status = "OK"
    if blocking_failures > 0:
        status = "FAIL"
    elif nonblocking_degradations > 0:
        status = "DEGRADED"

    top_reason_codes = sorted(list(dict.fromkeys(top_reason_codes)))

    manifest = {
        "schema_id": "pipeline_manifest",
        "schema_version": "v2",
        "day_utc": day,
        "produced_utc": _utc_now(),
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_pipeline_manifest_v2.py", "git_sha": _git_sha()},
        "status": status,
        "reason_codes": top_reason_codes,
        "notes": notes,
        "input_manifest": input_manifest,
        "stages": stages,
        "summary": {"blocking_failures": int(blocking_failures), "nonblocking_degradations": int(nonblocking_degradations)},
    }

    validate_against_repo_schema_v1(manifest, REPO_ROOT, SCHEMA_RELPATH)

    out_dir = (OUT_ROOT / day).resolve()
    out_path = (out_dir / "pipeline_manifest.v2.json").resolve()
    payload = (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")

    try:
        wr = write_file_immutable_v1(path=out_path, data=payload, create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL: IMMUTABLE_WRITE_ERROR: {e}") from e

    print(f"OK: PIPELINE_MANIFEST_V2_WRITTEN day_utc={day} status={status} path={wr.path} sha256={wr.sha256} action={wr.action}")
    return 0 if status == "OK" else 1


if __name__ == "__main__":
    raise SystemExit(main())
