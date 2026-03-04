#!/usr/bin/env python3
"""
run_c2_paper_day_orchestrator_v2.py

Constellation 2.0 — Orchestrator V2 (Robust, Attempt-Scoped, Mode-Partitioned Pointers)

Publishes:
- Orchestrator attempt manifest v2 (attempt-scoped)
- Orchestrator run verdict v2 (attempt-scoped + pointer index)
- Pipeline manifest v3 (attempt-scoped + pointer index)  [mode-aware, attempt-derived]
- Pipeline manifest v2 compat (legacy canonical path, immutable)
- Pipeline manifest v1 compat (legacy canonical path, immutable)

Exit policy:
- 0 for PASS/DEGRADED/FAIL
- non-zero only for ABORTED (safety breach / account mismatch)

Single-account enforcement:
- DUO847203 only (hard gate)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
DEFAULT_TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SINGLE_ACCOUNT_ID = "DUO847203"

POINTER_INDEX_NAME = "canonical_pointer_index.v1.jsonl"
POINTER_LOCK_NAME = ".canonical_pointer_index.v1.lock"


def _json_dumps(obj: Any) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _require_repo_root_cwd() -> None:
    cwd = Path.cwd().resolve()
    if cwd != REPO_ROOT:
        raise SystemExit(f"FATAL: must run with cwd={REPO_ROOT} got={cwd}")


def _require_truth_root(p: str) -> Path:
    s = (p or "").strip()
    if not s:
        raise SystemExit("FAIL: --truth_root resolved to empty string")
    pr = Path(s).expanduser().resolve()
    if not pr.is_absolute():
        raise SystemExit(f"FAIL: --truth_root must be absolute: {pr}")
    if not pr.exists() or (not pr.is_dir()):
        raise SystemExit(f"FAIL: --truth_root must exist and be a directory: {pr}")
    return pr


def _require_day(day: str) -> str:
    d = (day or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise SystemExit(f"FAIL: bad day key (expected YYYY-MM-DD): {d!r}")
    return d


def _require_mode(mode: str) -> str:
    m = (mode or "").strip().upper()
    if m not in ("PAPER", "LIVE"):
        raise SystemExit(f"FAIL: bad --mode (expected PAPER|LIVE): {m!r}")
    return m


def _require_symbol(sym: str) -> str:
    s = (sym or "").strip().upper()
    if not s:
        raise SystemExit("FAIL: --symbol resolved to empty string")
    return s


def _validate_produced_utc_isoz(s: str) -> str:
    v = (s or "").strip()
    if (len(v) < 11) or (not v.endswith("Z")) or ("T" not in v):
        raise SystemExit(f"FAIL: --produced_utc must look like ISO-8601 UTC Z timestamp: {v!r}")
    return v


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    s = out.decode("utf-8").strip()
    if len(s) != 40:
        raise SystemExit(f"FAIL: bad git sha: {s!r}")
    return s


@dataclass(frozen=True)
class StageDef:
    stage_id: str
    cmd: List[str]
    required_for_paper: bool
    required_for_live: bool
    required_if_activity: bool
    blocking: bool
    skip_if_exists_paths: List[str]


@dataclass
class StageResult:
    stage_id: str
    classification: Dict[str, Any]
    executed: bool
    rc: int
    status: str  # OK|SKIP|FAIL
    reason_codes: List[str]
    outputs_present: List[str]


def _path_exists(p: str) -> bool:
    try:
        return Path(p).resolve().exists()
    except Exception:
        return False


def _detect_activity(truth_root: Path, day: str) -> Dict[str, Any]:
    intents_day = (truth_root / "intents_v1" / "snapshots" / day).resolve()
    exec_sub_day = (truth_root / "execution_evidence_v1" / "submissions" / day).resolve()
    fills_day = (truth_root / "fill_ledger_v1" / day).resolve()

    intents_n = 0
    if intents_day.exists() and intents_day.is_dir():
        intents_n = len([p for p in intents_day.glob("*.json") if p.is_file()])

    exec_n = 0
    if exec_sub_day.exists() and exec_sub_day.is_dir():
        exec_n = len([p for p in exec_sub_day.glob("*.json") if p.is_file()])

    fills_present = fills_day.exists()
    activity = (intents_n > 0) or (exec_n > 0) or bool(fills_present)

    return {
        "activity": bool(activity),
        "intents_json_count": int(intents_n),
        "exec_submission_json_count": int(exec_n),
        "fills_path_present": bool(fills_present),
        "paths": {
            "intents_day": str(intents_day),
            "exec_submissions_day": str(exec_sub_day),
            "fills_day": str(fills_day),
        },
    }


def _stage_required_for_mode(sd: StageDef, mode: str, has_activity: bool) -> Tuple[bool, bool]:
    base_required = bool(sd.required_for_paper) if mode == "PAPER" else bool(sd.required_for_live)
    if sd.required_if_activity and (not has_activity):
        return (False, False)
    return (bool(base_required), bool(sd.blocking))


def _run_cmd(stage_id: str, cmd: List[str], env: Dict[str, str]) -> int:
    try:
        return subprocess.call(cmd, env=env)
    except Exception as e:
        print(f"STAGE_EXCEPTION stage_id={stage_id} err={e!r}", file=sys.stderr)
        return 99


def _pointer_lock_acquire(lock_path: Path) -> int:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        raise SystemExit(f"FAIL: pointer lock busy: {lock_path}")
    os.write(fd, f"pid={os.getpid()}\n".encode("utf-8"))
    os.fsync(fd)
    return fd


def _pointer_lock_release(fd: int, lock_path: Path) -> None:
    try:
        os.close(fd)
    finally:
        try:
            os.unlink(str(lock_path))
        except FileNotFoundError:
            pass


def _read_last_pointer_seq(idx_path: Path, mode: str) -> int:
    if not idx_path.exists():
        return 0
    last = 0
    for line in idx_path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception:
            raise SystemExit(f"FAIL: invalid JSONL in pointer index: {idx_path}")
        if not isinstance(obj, dict):
            continue
        if str(obj.get("mode") or "").strip().upper() != mode:
            continue
        try:
            ps = int(obj.get("pointer_seq"))
        except Exception:
            continue
        if ps > last:
            last = ps
    return last


def _atomic_append_jsonl(path: Path, obj: Dict[str, Any]) -> Tuple[str, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = (json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    line_sha = _sha256_bytes(line)

    fd = os.open(str(path), os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
    try:
        os.write(fd, line)
        os.fsync(fd)
    finally:
        os.close(fd)

    dfd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)

    return (line_sha, str(path))


def _write_attempt_file(path: Path, data: bytes) -> Dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    cand_sha = _sha256_bytes(data)

    if path.exists():
        existing = path.read_bytes()
        ex_sha = _sha256_bytes(existing)
        if ex_sha == cand_sha:
            return {"ok": True, "action": "SKIP_IDENTICAL", "path": str(path), "sha256": cand_sha}
        raise SystemExit(
            f"FATAL: ATTEMPTED_REWRITE_ATTEMPT_SCOPED path={path} existing_sha={ex_sha} candidate_sha={cand_sha}"
        )

    tmp = path.parent / f".{path.name}.tmp.{os.getpid()}"
    tmp.write_bytes(data)
    fd = os.open(str(tmp), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(str(tmp), str(path))
    return {"ok": True, "action": "WROTE", "path": str(path), "sha256": cand_sha}


def _build_stage_defs(*, truth: Path, day: str, input_day: str, ib_account: str) -> List[StageDef]:
    feed_gate_out = str(truth / "reports" / "feed_attestation_gate_v1" / day / "feed_attestation_gate.v1.json")
    liq_gate_out = str(truth / "reports" / "liquidity_slippage_gate_v1" / day / "liquidity_slippage_gate.v1.json")
    sys_gate_out = str(truth / "reports" / "systemic_risk_gate_v3" / day / "systemic_risk_gate.v3.json")
    cap_gate_out = str(truth / "reports" / "capital_risk_envelope_v2" / day / "capital_risk_envelope.v2.json")
    gate_stack_out = str(truth / "reports" / "gate_stack_verdict_v1" / day / "gate_stack_verdict.v1.json")
    kill_switch_out = str(truth / "risk_v1" / "kill_switch_v1" / day / "global_kill_switch_state.v1.json")
    broker_recon_out = str(truth / "reports" / "broker_reconciliation_v2" / day / "broker_reconciliation.v2.json")

    exec_recon_out = str(truth / "reports" / "execution_reconciliation_v1" / day / "execution_reconciliation.v1.json")
    fills_out = str(truth / "fill_ledger_v1" / day / "fill_ledger.v1.json")
    nav_out = str(truth / "accounting_v2" / "nav" / day / "nav.v2.json")
    exec_submissions_day_dir = str(truth / "execution_evidence_v1" / "submissions" / day)
    pos_out = str(truth / "positions_v1" / "snapshots" / day / "positions_snapshot.v2.json")
    cash_out = str(truth / "cash_ledger_v1" / "snapshots" / day / "cash_ledger_snapshot.v1.json")
    op_stmt = str(
        REPO_ROOT
        / "constellation_2"
        / "operator_inputs"
        / "cash_ledger_operator_statements"
        / day
        / "operator_statement.v1.json"
    )

    return [
        StageDef(
            stage_id="A0_ENFORCE_SINGLE_ACCOUNT_TOPOLOGY",
            cmd=["/bin/true"],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=False,
            blocking=True,
            skip_if_exists_paths=[],
        ),
        StageDef(
            stage_id="B0_POSITIONS_SNAPSHOT_V2",
            cmd=[
                "python3",
                "-m",
                "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v2",
                "--day_utc",
                day,
                "--producer_git_sha",
                _git_sha(),
                "--producer_repo",
                "constellation_2_runtime",
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=False,
            blocking=True,
            skip_if_exists_paths=[pos_out],
        ),
        StageDef(
            stage_id="B0_CASH_LEDGER_SNAPSHOT_V1",
            cmd=[
                "python3",
                "-m",
                "constellation_2.phaseF.cash_ledger.run.run_cash_ledger_snapshot_day_v1",
                "--day_utc",
                day,
                "--operator_statement_json",
                op_stmt,
                "--producer_repo",
                "constellation_2_runtime",
                "--producer_git_sha",
                _git_sha(),
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=False,
            blocking=True,
            skip_if_exists_paths=[cash_out],
        ),
        StageDef(
            stage_id="B0_ENSURE_EXECUTION_SUBMISSIONS_DIR_V1",
            cmd=["/usr/bin/bash", "-lc", f"set -euo pipefail; mkdir -p '{exec_submissions_day_dir}'"],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=False,
            blocking=True,
            skip_if_exists_paths=[exec_submissions_day_dir],
        ),
        StageDef(
            stage_id="A1_BROKER_RECONCILIATION_GATE_V2_CHECK",
            cmd=[
                "python3",
                "ops/tools/run_broker_reconciliation_day_v2.py",
                "--day_utc",
                day,
                "--ib_account",
                ib_account,
                "--mode",
                "CHECK",
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=False,
            blocking=True,
            skip_if_exists_paths=[broker_recon_out],
        ),
        StageDef(
            stage_id="A2_FEED_ATTESTATION_GATE_V1",
            cmd=["python3", "ops/tools/run_feed_attestation_gate_v1.py", "--day_utc", day],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[feed_gate_out],
        ),
        StageDef(
            stage_id="A3_LIQUIDITY_SLIPPAGE_GATE_V1",
            cmd=["python3", "ops/tools/run_liquidity_slippage_gate_v1.py", "--day_utc", day],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[liq_gate_out],
        ),
        StageDef(
            stage_id="A4_SYSTEMIC_RISK_GATE_V3",
            cmd=["python3", "ops/tools/run_systemic_risk_gate_v3.py", "--day_utc", day],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[sys_gate_out],
        ),
        StageDef(
            stage_id="B0_ACCOUNTING_NAV_V2",
            cmd=[
                "python3",
                "ops/tools/run_accounting_nav_v2_day_v1.py",
                "--day_utc",
                day,
                "--producer_git_sha",
                _git_sha(),
                "--producer_repo",
                "constellation_2_runtime",
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=False,
            blocking=True,
            skip_if_exists_paths=[nav_out],
        ),
        StageDef(
            stage_id="A5_CAPITAL_RISK_ENVELOPE_GATE_V2",
            cmd=[
                "python3",
                "ops/tools/run_c2_capital_risk_envelope_gate_v2.py",
                "--out_day_utc",
                day,
                "--input_day_utc",
                input_day,
                "--produced_utc",
                f"{day}T00:00:00Z",
            ],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[cap_gate_out],
        ),
        StageDef(
            stage_id="A6_GATE_STACK_VERDICT_V1",
            cmd=["python3", "ops/tools/run_gate_stack_verdict_v1.py", "--day_utc", day],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[gate_stack_out],
        ),
        StageDef(
            stage_id="A7_GLOBAL_KILL_SWITCH_V1",
            cmd=["python3", "ops/tools/run_global_kill_switch_v1.py", "--day_utc", day],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=True,
            skip_if_exists_paths=[kill_switch_out],
        ),
        StageDef(
            stage_id="B1_FILL_LEDGER_V1",
            cmd=["python3", "ops/tools/run_fill_ledger_day_v1.py", "--day_utc", day],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=False,
            skip_if_exists_paths=[fills_out],
        ),
        StageDef(
            stage_id="B2_EXECUTION_RECONCILIATION_V1",
            cmd=["python3", "ops/tools/run_execution_reconciliation_day_v1.py", "--day_utc", day],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=True,
            blocking=False,
            skip_if_exists_paths=[exec_recon_out],
        ),
        StageDef(
            stage_id="A8_EXECUTION_READINESS_GATE_V1",
            cmd=["python3", "ops/tools/run_execution_readiness_gate_v1.py", "--day_utc", day],
            required_for_paper=True,
            required_for_live=True,
            required_if_activity=False,
            blocking=False,
            skip_if_exists_paths=[
                str(truth / "reports" / "execution_readiness_gate_v1" / day / "execution_readiness_gate.v1.json")
            ],
        ),
    ]


def _publish_pipeline_manifest_v3(
    *, day: str, mode: str, attempt_id: str, attempt_seq: int, attempt_manifest_path: Path, env: Dict[str, str]
) -> Tuple[bool, int]:
    cmd = [
        "python3",
        "ops/tools/run_pipeline_manifest_v3_mode_aware.py",
        "--day_utc",
        day,
        "--mode",
        mode,
        "--attempt_id",
        attempt_id,
        "--attempt_seq",
        str(int(attempt_seq)),
        "--attempt_manifest_path",
        str(attempt_manifest_path),
    ]
    rc = _run_cmd("PUBLISH_PIPELINE_MANIFEST_V3", cmd, env=env)
    return (rc == 0, int(rc))


def _publish_pipeline_manifest_v2_compat(*, day: str, attempt_manifest_path: Path, env: Dict[str, str]) -> Tuple[bool, int]:
    cmd = [
        "python3",
        "ops/tools/run_pipeline_manifest_v2_compat_from_attempt_v1.py",
        "--day_utc",
        day,
        "--attempt_manifest_path",
        str(attempt_manifest_path),
    ]
    rc = _run_cmd("PUBLISH_PIPELINE_MANIFEST_V2_COMPAT", cmd, env=env)
    return (rc == 0, int(rc))


def _publish_pipeline_manifest_v1_compat(*, day: str, attempt_manifest_path: Path, env: Dict[str, str]) -> Tuple[bool, int]:
    cmd = [
        "python3",
        "ops/tools/run_pipeline_manifest_v1_compat_from_attempt_v1.py",
        "--day_utc",
        day,
        "--attempt_manifest_path",
        str(attempt_manifest_path),
    ]
    rc = _run_cmd("PUBLISH_PIPELINE_MANIFEST_V1_COMPAT", cmd, env=env)
    return (rc == 0, int(rc))

def _enforce_fail_closed_from_stage_results(stage_results: List[Dict[str, Any]]) -> Tuple[str | None, List[str]]:
    """
    Fail-closed enforcement layer.

    This enforces that orchestrator status cannot be PASS when required or blocking stages failed.

    Returns:
      (override_status, extra_reason_codes)
    """
    override_status: str | None = None
    extra: List[str]


def _enforce_gate_stack_verdict(*, truth_root: Path, day: str, current_status: str, reason_codes: List[str]) -> str:
    """
    Enforce fail-closed using gate_stack_verdict_v1.

    - If gate stack is missing: FAIL (fail-closed) because it is a canonical pretrade spine.
    - If blocking_class == CLASS1_SYSTEM_HARD_STOP: ABORTED.
    - If status != PASS: FAIL.
    - If PASS: no change.
    """
    p = (truth_root / "reports" / "gate_stack_verdict_v1" / day / "gate_stack_verdict.v1.json").resolve()

    if not p.exists():
        if "GATE_STACK_VERDICT_MISSING" not in reason_codes:
            reason_codes.append("GATE_STACK_VERDICT_MISSING")
        return "FAIL" if current_status != "ABORTED" else current_status

    try:
        o = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        if "GATE_STACK_VERDICT_PARSE_ERROR" not in reason_codes:
            reason_codes.append("GATE_STACK_VERDICT_PARSE_ERROR")
        return "ABORTED"

    status = str(o.get("status") or "").strip().upper()
    blocking = str(o.get("blocking_class") or "").strip().upper()

    if blocking == "CLASS1_SYSTEM_HARD_STOP":
        if "CLASS1_SYSTEM_HARD_STOP" not in reason_codes:
            reason_codes.append("CLASS1_SYSTEM_HARD_STOP")
        return "ABORTED"

    if status and status != "PASS":
        if "GATE_STACK_VERDICT_NOT_PASS" not in reason_codes:
            reason_codes.append("GATE_STACK_VERDICT_NOT_PASS")
        return "FAIL" if current_status != "ABORTED" else current_status

    return current_status

def main() -> int:
    _require_repo_root_cwd()

    ap = argparse.ArgumentParser(prog="run_c2_paper_day_orchestrator_v2")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--input_day_utc", default="", help="Optional input day key (defaults to day_utc)")
    ap.add_argument("--mode", required=True, choices=["PAPER", "LIVE"])
    ap.add_argument("--symbol", default="SPY")
    ap.add_argument("--ib_account", required=True, help="IB account id (single-account mode enforced)")
    ap.add_argument("--produced_utc", required=True, help="UTC ISO-8601 Z timestamp (operator supplied)")
    ap.add_argument(
        "--truth_root",
        default=None,
        help="Absolute truth root directory. If omitted, uses the default constellation_2/runtime/truth",
    )
    args = ap.parse_args()

    truth_root = DEFAULT_TRUTH_ROOT if (args.truth_root is None) else _require_truth_root(args.truth_root)
    verdict_root = (truth_root / "reports" / "orchestrator_run_verdict_v2").resolve()

    day = _require_day(args.day_utc)
    input_day = _require_day((args.input_day_utc or "").strip() or day)
    mode = _require_mode(args.mode)
    symbol = _require_symbol(args.symbol)
    produced_utc = _validate_produced_utc_isoz(args.produced_utc)

    ib_account = str(args.ib_account or "").strip()
    if not ib_account:
        raise SystemExit("FAIL: --ib_account empty")

    # Hard single-account enforcement
    if ib_account != SINGLE_ACCOUNT_ID:
        git_sha = _git_sha()
        verdict = {
            "schema_id": "C2_ORCHESTRATOR_RUN_VERDICT_V2",
            "day_utc": day,
            "input_day_utc": input_day,
            "mode": mode,
            "symbol": symbol,
            "ib_account": ib_account,
            "produced_utc": produced_utc,
            "status": "ABORTED",
            "safety_breaches": ["IB_ACCOUNT_MISMATCH_SINGLE_ACCOUNT_MODE"],
            "reason_codes": [f"EXPECTED_SINGLE_ACCOUNT={SINGLE_ACCOUNT_ID}"],
            "stages": [],
            "replay": {"derived_from_attempt_manifest": True, "hashes": {}},
            "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_c2_paper_day_orchestrator_v2.py", "git_sha": git_sha},
        }
        attempt_id = f"{day}__{produced_utc}__{git_sha}__{mode}__{symbol}__{ib_account}"
        out_dir = (verdict_root / day / attempt_id).resolve()
        _write_attempt_file(out_dir / "orchestrator_run_verdict.v2.json", _json_dumps(verdict))
        return 7

    git_sha = _git_sha()

    cfg_hash = (os.environ.get("C2_ORCHESTRATOR_CONFIG_HASH") or "").strip().lower()
    if (len(cfg_hash) != 64) or any(c not in "0123456789abcdef" for c in cfg_hash):
        cfg_hash = "0" * 64

    alloc_raw = subprocess.check_output(
        [
            "python3",
            "ops/tools/run_pointer_attempt_alloc_v1.py",
            "--day_utc",
            day,
            "--mode",
            mode,
            "--orchestrator_config_hash",
            cfg_hash,
            "--git_sha",
            git_sha,
        ],
        text=True,
    ).strip()
    alloc = json.loads(alloc_raw)

    attempt_id = str(alloc.get("attempt_id") or "").strip()
    attempt_seq = int(alloc.get("attempt_seq") or 0)
    if (not attempt_id) or (attempt_seq <= 0):
        raise SystemExit(f"FATAL: run_pointer_attempt_alloc_v1 invalid payload: {alloc_raw}")

    act = _detect_activity(truth_root, day)
    has_activity = bool(act["activity"])

    stage_env = dict(os.environ)
    stage_env["PYTHONPATH"] = str(REPO_ROOT)
    stage_env["C2_TRUTH_ROOT"] = str(truth_root)
    stage_env["C2_PRODUCED_UTC"] = produced_utc

    stages = _build_stage_defs(truth=truth_root, day=day, input_day=input_day, ib_account=ib_account)

    stage_results: List[Dict[str, Any]] = []
    safety_breaches: List[str] = []
    reason_codes: List[str] = []
    any_required_fail = False
    any_optional_fail = False

    attempt_manifest: Dict[str, Any] = {
        "schema_id": "C2_ORCHESTRATOR_ATTEMPT_MANIFEST_V2",
        "day_utc": day,
        "input_day_utc": input_day,
        "mode": mode,
        "symbol": symbol,
        "ib_account": ib_account,
        "attempt_id": attempt_id,
        "attempt_seq": int(attempt_seq),
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_c2_paper_day_orchestrator_v2.py", "git_sha": git_sha},
        "activity": act,
        "stages": [],
        "outputs": [],
    }

    for sd in stages:
        required, blocking = _stage_required_for_mode(sd, mode, has_activity)
        classification = {
            "required_for_mode": {"PAPER": bool(sd.required_for_paper), "LIVE": bool(sd.required_for_live)},
            "required_if_activity": bool(sd.required_if_activity),
            "blocking": bool(sd.blocking),
            "effective_required": bool(required),
            "effective_blocking": bool(blocking),
        }

        if sd.stage_id == "A0_ENFORCE_SINGLE_ACCOUNT_TOPOLOGY":
            sr = StageResult(
                stage_id=sd.stage_id,
                classification=classification,
                executed=False,
                rc=0,
                status="OK",
                reason_codes=["SINGLE_ACCOUNT_ENFORCED"],
                outputs_present=[],
            )
            stage_results.append(sr.__dict__)
            attempt_manifest["stages"].append(sr.__dict__)
            continue

        outputs_present = [p for p in sd.skip_if_exists_paths if _path_exists(p)]
        if outputs_present:
            sr = StageResult(
                stage_id=sd.stage_id,
                classification=classification,
                executed=False,
                rc=0,
                status="SKIP",
                reason_codes=["SKIP_EXISTING_OUTPUTS_AVOID_REWRITE"],
                outputs_present=outputs_present,
            )
            stage_results.append(sr.__dict__)
            attempt_manifest["stages"].append(sr.__dict__)
            continue

        rc = _run_cmd(sd.stage_id, sd.cmd, env=stage_env)
        ok = (rc == 0)

        if ok:
            sr = StageResult(
                stage_id=sd.stage_id,
                classification=classification,
                executed=True,
                rc=int(rc),
                status="OK",
                reason_codes=[],
                outputs_present=[],
            )
            stage_results.append(sr.__dict__)
            attempt_manifest["stages"].append(sr.__dict__)
            continue

        if blocking:
            safety_breaches.append(f"{sd.stage_id}_BLOCKING_FAIL")
            fail_code = "SAFETY_BREACH_BLOCKING_STAGE_FAIL"
        else:
            if required:
                any_required_fail = True
                fail_code = "REQUIRED_STAGE_FAIL"
            else:
                any_optional_fail = True
                fail_code = "OPTIONAL_STAGE_FAIL"

        sr = StageResult(
            stage_id=sd.stage_id,
            classification=classification,
            executed=True,
            rc=int(rc),
            status="FAIL",
            reason_codes=[fail_code],
            outputs_present=[],
        )
        stage_results.append(sr.__dict__)
        attempt_manifest["stages"].append(sr.__dict__)

    if safety_breaches:
        status = "ABORTED"
        reason_codes += ["SAFETY_BREACH"]
    else:
        if any_required_fail:
            status = "FAIL"
            reason_codes += ["REQUIRED_FAILURE"]
        elif any_optional_fail or (not has_activity):
            status = "DEGRADED"
            if not has_activity:
                reason_codes += ["NO_ACTIVITY_DAY"]
        else:
            status = "PASS"

    # Fail-closed: gate stack verdict must be PASS for status PASS/DEGRADED
    status = _enforce_gate_stack_verdict(truth_root=truth_root, day=day, current_status=status, reason_codes=reason_codes)

    out_dir = (verdict_root / day / attempt_id).resolve()
    man_wr = _write_attempt_file(out_dir / "orchestrator_attempt_manifest.v2.json", _json_dumps(attempt_manifest))
    attempt_manifest_path = (out_dir / "orchestrator_attempt_manifest.v2.json").resolve()

    # Publish manifests (all non-blocking publishers)
    pub3_ok, pub3_rc = _publish_pipeline_manifest_v3(
        day=day, mode=mode, attempt_id=attempt_id, attempt_seq=int(attempt_seq), attempt_manifest_path=attempt_manifest_path, env=stage_env
    )
    if not pub3_ok:
        reason_codes.append("PIPELINE_MANIFEST_V3_PUBLISH_FAIL")
        reason_codes.append(f"PIPELINE_MANIFEST_V3_PUBLISH_RC={pub3_rc}")
        if status == "PASS":
            status = "DEGRADED"

    pub2_ok, pub2_rc = _publish_pipeline_manifest_v2_compat(day=day, attempt_manifest_path=attempt_manifest_path, env=stage_env)
    if not pub2_ok:
        reason_codes.append("PIPELINE_MANIFEST_V2_COMPAT_PUBLISH_FAIL")
        reason_codes.append(f"PIPELINE_MANIFEST_V2_COMPAT_PUBLISH_RC={pub2_rc}")
        if status == "PASS":
            status = "DEGRADED"

    pub1_ok, pub1_rc = _publish_pipeline_manifest_v1_compat(day=day, attempt_manifest_path=attempt_manifest_path, env=stage_env)
    if not pub1_ok:
        reason_codes.append("PIPELINE_MANIFEST_V1_COMPAT_PUBLISH_FAIL")
        reason_codes.append(f"PIPELINE_MANIFEST_V1_COMPAT_PUBLISH_RC={pub1_rc}")
        if status == "PASS":
            status = "DEGRADED"

    # Re-enforce gate stack after publishers (prevents accidental PASS)
    status = _enforce_gate_stack_verdict(truth_root=truth_root, day=day, current_status=status, reason_codes=reason_codes)

    replay_hashes: Dict[str, str] = {"attempt_manifest_sha256": str(man_wr["sha256"])}

    verdict = {
        "schema_id": "C2_ORCHESTRATOR_RUN_VERDICT_V2",
        "day_utc": day,
        "input_day_utc": input_day,
        "mode": mode,
        "symbol": symbol,
        "ib_account": ib_account,
        "attempt_id": attempt_id,
        "attempt_seq": int(attempt_seq),
        "produced_utc": produced_utc,
        "status": status,
        "safety_breaches": list(safety_breaches),
        "reason_codes": list(reason_codes),
        "stages": stage_results,
        "replay": {"derived_from_attempt_manifest": True, "hashes": replay_hashes},
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_c2_paper_day_orchestrator_v2.py", "git_sha": git_sha},
    }

    _write_attempt_file(out_dir / "orchestrator_run_verdict.v2.json", _json_dumps(verdict))

    day_root = (verdict_root / day).resolve()
    idx_path = (day_root / POINTER_INDEX_NAME).resolve()
    lock_path = (day_root / POINTER_LOCK_NAME).resolve()

    lock_fd = _pointer_lock_acquire(lock_path)
    try:
        last_seq = _read_last_pointer_seq(idx_path, mode)
        pointer_seq = last_seq + 1

        entry = {
            "schema_id": "C2_ORCHESTRATOR_RUN_VERDICT_V2_POINTER_INDEX_V1",
            "pointer_seq": int(pointer_seq),
            "day_utc": day,
            "mode": mode,
            "attempt_id": attempt_id,
            "attempt_seq": int(attempt_seq),
            "status": status,
            "authoritative": bool(status == "PASS"),
            "producer_git_sha": git_sha,
            "produced_utc": f"{day}T00:00:00Z",
            "points_to": str(out_dir / "orchestrator_run_verdict.v2.json"),
            "attempt_manifest_path": str(attempt_manifest_path),
        }
        line_sha, _ = _atomic_append_jsonl(idx_path, entry)
    finally:
        _pointer_lock_release(lock_fd, lock_path)

    _write_attempt_file(
        out_dir / "orchestrator_run_verdict.v2.pointer_append.sha256.json",
        _json_dumps({"append_line_sha256": line_sha, "index_path": str(idx_path)}),
    )

    if status == "ABORTED":
        return 9
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
