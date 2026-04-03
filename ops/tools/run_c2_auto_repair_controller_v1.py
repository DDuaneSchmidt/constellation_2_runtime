#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import signal
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
RUNTIME_ROOT = (REPO_ROOT / "constellation_2" / "runtime").resolve()
SLEEVE_REGISTRY = (REPO_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json").resolve()

HEALTH_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/auto_repair_health_supervisor.v1.schema.json"
TRIGGER_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/auto_repair_trigger_decision.v1.schema.json"
STATE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/auto_repair_controller_state.v1.schema.json"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _utc_iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def _ts_compact(dt: datetime) -> str:
    return dt.strftime("%Y%m%dT%H%M%SZ")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    return _sha256_bytes(p.read_bytes())


def _git_sha() -> str:
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT), text=True)
        return out.strip()
    except Exception:
        return "UNKNOWN"


def _read_json_obj(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise RuntimeError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _write_immutable(path: Path, payload: Dict[str, Any]) -> str:
    data = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    sha = _sha256_bytes(data)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = path.read_bytes()
        if _sha256_bytes(existing) == sha:
            return sha
        raise RuntimeError(f"REFUSE_OVERWRITE_DIFFERENT_BYTES: {path}")
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)
    return sha


def _resolve_truth_root(arg_truth_root: str) -> Path:
    tr = (arg_truth_root or "").strip()
    if tr:
        p = Path(tr).expanduser().resolve()
        if not p.is_absolute() or not p.exists() or not p.is_dir():
            raise SystemExit(f"FAIL: --truth_root invalid: {p}")
        return p

    env = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
    if env:
        p = Path(env).expanduser().resolve()
        if not p.is_absolute() or not p.exists() or not p.is_dir():
            raise SystemExit(f"FAIL: C2_TRUTH_ROOT invalid: {p}")
        return p

    if not SLEEVE_REGISTRY.exists():
        raise SystemExit(f"FAIL: missing sleeve registry: {SLEEVE_REGISTRY}")
    reg = _read_json_obj(SLEEVE_REGISTRY)
    sleeves = reg.get("sleeves")
    if not isinstance(sleeves, list):
        raise SystemExit("FAIL: sleeve registry missing sleeves[]")
    for s in sleeves:
        if not isinstance(s, dict):
            continue
        if not bool(s.get("enabled")):
            continue
        if str(s.get("mode") or "").strip().upper() != "PAPER":
            continue
        part = str(s.get("truth_partition") or "").strip()
        if not part:
            continue
        p = (RUNTIME_ROOT / part).resolve()
        if p.exists() and p.is_dir():
            return p
    raise SystemExit("FAIL: no enabled PAPER sleeve truth partition found")


@dataclass
class AttemptVerdict:
    attempt_id: str
    attempt_seq: int
    status: str
    reason_codes: List[str]
    safety_breaches: List[str]
    path: Path


def _load_attempts(truth_root: Path, day: str) -> List[AttemptVerdict]:
    root = (truth_root / "reports" / "orchestrator_run_verdict_v2" / day).resolve()
    if not root.exists() or not root.is_dir():
        return []
    out: List[AttemptVerdict] = []
    for p in sorted(root.glob("*/orchestrator_run_verdict.v2.json")):
        try:
            o = _read_json_obj(p)
            aid = str(o.get("attempt_id") or p.parent.name).strip()
            seq_raw = o.get("attempt_seq")
            try:
                seq = int(seq_raw)
            except Exception:
                seq = -1
            status = str(o.get("status") or "").strip().upper()
            rc = o.get("reason_codes")
            sb = o.get("safety_breaches")
            out.append(
                AttemptVerdict(
                    attempt_id=aid,
                    attempt_seq=seq,
                    status=status,
                    reason_codes=[str(x) for x in rc] if isinstance(rc, list) else [],
                    safety_breaches=[str(x) for x in sb] if isinstance(sb, list) else [],
                    path=p,
                )
            )
        except Exception:
            continue
    out.sort(key=lambda x: (x.attempt_seq, x.attempt_id))
    return out


def _find_status(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return "MISSING"
    try:
        o = _read_json_obj(path)
    except Exception:
        return "INVALID"
    return str(o.get("status") or "").strip().upper() or "MISSING"


def _auth_surface(truth_root: Path, day: str) -> Tuple[int, List[str]]:
    d = (truth_root / "engine_activity_v1" / "authorization_v1" / day).resolve()
    if not d.exists() or not d.is_dir():
        return (0, [])
    statuses: List[str] = []
    n = 0
    for p in sorted(d.glob("*.authorization.v1.json")):
        if not p.is_file():
            continue
        n += 1
        try:
            st = str(_read_json_obj(p).get("status") or "").strip().upper()
        except Exception:
            st = "INVALID"
        statuses.append(st)
    return (n, statuses)


def _phasec_surface(truth_root: Path, day: str) -> Dict[str, int]:
    d = (truth_root / "phaseC_preflight_v1" / day).resolve()
    if not d.exists() or not d.is_dir():
        return {"veto_records": 0, "preflight_decisions": 0}
    veto = len([p for p in d.glob("*.veto_record.v1.json") if p.is_file()])
    decisions = len([p for p in d.rglob("submit_preflight_decision.v1.json") if p.is_file()])
    return {"veto_records": veto, "preflight_decisions": decisions}


def _count_submission_records(truth_root: Path, day: str) -> int:
    d = (truth_root / "execution_evidence_v1" / "submissions" / day).resolve()
    if not d.exists() or not d.is_dir():
        return 0
    return len(list(d.glob("*/broker_submission_record.v2.json")))


def _load_manifest_activity(truth_root: Path, day: str, attempt_id: str) -> Dict[str, Any]:
    p = (
        truth_root
        / "reports"
        / "orchestrator_run_verdict_v2"
        / day
        / attempt_id
        / "orchestrator_attempt_manifest.v2.json"
    ).resolve()
    if not p.exists() or not p.is_file():
        return {"activity": False, "effective_activity": False}
    try:
        obj = _read_json_obj(p)
    except Exception:
        return {"activity": False, "effective_activity": False}
    activity = obj.get("activity") if isinstance(obj.get("activity"), dict) else {}
    return {
        "activity": bool(activity.get("activity")) if isinstance(activity, dict) else False,
        "effective_activity": bool(obj.get("effective_activity")),
        "activity_blob": activity if isinstance(activity, dict) else {},
    }


def _build_health_payload(*, truth_root: Path, day: str, evaluated_at: datetime) -> Dict[str, Any]:
    attempts = _load_attempts(truth_root, day)
    latest_attempt = attempts[-1] if attempts else None
    pass_attempts = [a for a in attempts if a.status == "PASS"]
    authoritative = pass_attempts[-1] if pass_attempts else None

    gate_path = truth_root / "reports" / "gate_stack_verdict_v1" / day / "gate_stack_verdict.v1.json"
    exec_ready_path = truth_root / "reports" / "execution_readiness_gate_v1" / day / "execution_readiness_gate.v1.json"
    alloc_path = truth_root / "allocation_v1" / "capital_authority_allocation_v1" / day / "capital_authority_allocation.v1.json"
    exec_recon_path = truth_root / "reports" / "execution_reconciliation_v1" / day / "execution_reconciliation.v1.json"

    gate_status = _find_status(gate_path)
    exec_ready_status = _find_status(exec_ready_path)
    alloc_status = _find_status(alloc_path)
    exec_recon_status = _find_status(exec_recon_path)

    auth_count, auth_statuses = _auth_surface(truth_root, day)
    phasec = _phasec_surface(truth_root, day)
    submission_records = _count_submission_records(truth_root, day)
    manifest = _load_manifest_activity(truth_root, day, authoritative.attempt_id if authoritative else "")
    expected_activity = bool(manifest.get("effective_activity") or manifest.get("activity"))

    reason_codes: List[str] = []
    supporting_refs: List[str] = []
    degraded = False
    repair_required = False
    blocker_class = "NONE"

    if authoritative is None:
        repair_required = True
        blocker_class = "ORCHESTRATOR_HEALTH_PROOF_MISSING"
        reason_codes.append("NO_CANONICAL_PASS_ATTEMPT")
    else:
        supporting_refs.append(str(authoritative.path))

    if gate_status != "PASS":
        repair_required = True
        blocker_class = "CRITICAL_GATE_FAIL"
        reason_codes.append(f"GATE_STACK_STATUS_{gate_status}")
    else:
        supporting_refs.append(str(gate_path))

    if alloc_status not in ("OK", "PASS", "ACTIVE"):
        repair_required = True
        if blocker_class == "NONE":
            blocker_class = "ALLOCATION_SURFACE_INVALID"
        reason_codes.append(f"ALLOCATION_STATUS_{alloc_status}")
    else:
        supporting_refs.append(str(alloc_path))

    if expected_activity:
        if exec_ready_status != "PASS":
            repair_required = True
            if blocker_class == "NONE":
                blocker_class = "EXECUTION_READINESS_BLOCK"
            reason_codes.append(f"EXECUTION_READINESS_STATUS_{exec_ready_status}")
        if submission_records <= 0:
            repair_required = True
            if blocker_class == "NONE":
                blocker_class = "GOVERNED_SUBMIT_EVIDENCE_MISSING"
            reason_codes.append("SUBMISSION_RECORDS_MISSING_FOR_ACTIVITY_DAY")
        if exec_recon_status not in ("PASS", "OK"):
            repair_required = True
            if blocker_class == "NONE":
                blocker_class = "DOWNSTREAM_RECONCILIATION_BLOCK"
            reason_codes.append(f"EXECUTION_RECON_STATUS_{exec_recon_status}")
        if auth_count <= 0:
            repair_required = True
            if blocker_class == "NONE":
                blocker_class = "AUTHORIZATION_SURFACE_MISSING"
            reason_codes.append("AUTHORIZATION_ARTIFACTS_MISSING")
        if phasec.get("veto_records", 0) <= 0 or phasec.get("preflight_decisions", 0) <= 0:
            repair_required = True
            if blocker_class == "NONE":
                blocker_class = "PHASEC_PREFLIGHT_SURFACE_MISSING"
            reason_codes.append("PHASEC_PREFLIGHT_OR_VETO_MISSING")
    else:
        if exec_ready_status == "MISSING":
            degraded = True
            reason_codes.append("EXECUTION_READINESS_ARTIFACT_MISSING_NON_ACTIVITY")

    if latest_attempt and authoritative:
        if latest_attempt.status == "ABORTED" and latest_attempt.attempt_seq > authoritative.attempt_seq:
            # Explicitly non-blocking: later same-day immutability/cross-sha abort after canonical PASS.
            degraded = True
            reason_codes.append("LATER_ABORT_AFTER_CANONICAL_PASS_NON_BLOCKING")
            reason_codes.append("CROSS_SHA_IMMUTABILITY_ABORT_ADVISORY")

    if auth_count > 0:
        bad_auth = [s for s in auth_statuses if s not in ("AUTHORIZED", "REJECTED")]
        if bad_auth:
            degraded = True
            reason_codes.append("AUTHORIZATION_STATUS_NON_STANDARD")

    if not repair_required:
        supporting_refs.extend([str(exec_ready_path), str(exec_recon_path)])

    if repair_required:
        state = "REPAIR_REQUIRED"
    elif degraded:
        state = "DEGRADED"
        if blocker_class == "NONE":
            blocker_class = "NON_BLOCKING_ADVISORY"
    else:
        state = "READY"

    summary = (
        f"{state}: "
        + (
            "canonical active-day PASS is present and critical surfaces are healthy"
            if state == "READY"
            else "non-blocking advisory conditions present; repair not auto-triggered"
            if state == "DEGRADED"
            else "blocking readiness proof missing or inconsistent for active day"
        )
    )

    payload: Dict[str, Any] = {
        "schema_id": "C2_AUTO_REPAIR_HEALTH_SUPERVISOR_V1",
        "schema_version": 1,
        "evaluated_at_utc": _utc_iso(evaluated_at),
        "target_day_utc": day,
        "state": state,
        "blocker_class": blocker_class,
        "reason_codes": sorted(set(reason_codes)),
        "authoritative_attempt_id": authoritative.attempt_id if authoritative else None,
        "operator_summary": summary,
        "supporting_artifact_refs": sorted(set(supporting_refs)),
        "details": {
            "latest_attempt_id": latest_attempt.attempt_id if latest_attempt else None,
            "latest_attempt_status": latest_attempt.status if latest_attempt else "MISSING",
            "gate_stack_status": gate_status,
            "execution_readiness_status": exec_ready_status,
            "allocation_status": alloc_status,
            "execution_reconciliation_status": exec_recon_status,
            "submission_record_count": int(submission_records),
            "authorization_count": int(auth_count),
            "authorization_statuses": auth_statuses,
            "phasec_veto_record_count": int(phasec.get("veto_records", 0)),
            "phasec_preflight_decision_count": int(phasec.get("preflight_decisions", 0)),
            "effective_activity_expected": bool(expected_activity),
            "activity": manifest.get("activity_blob", {}),
        },
        "producer": {
            "repo": "constellation_2_runtime",
            "module": "ops/tools/run_c2_auto_repair_controller_v1.py",
            "git_sha": _git_sha(),
        },
    }
    validate_against_repo_schema_v1(payload, REPO_ROOT, HEALTH_SCHEMA)
    return payload


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except PermissionError:
        # EPERM means the process exists but is owned by another user.
        return True
    except OSError:
        return False


def _default_state() -> Dict[str, Any]:
    return {
        "schema_id": "C2_AUTO_REPAIR_CONTROLLER_STATE_V1",
        "schema_version": 1,
        "updated_at_utc": "",
        "active_session": None,
        "blockers": {},
        "latest_outcome_by_day": {},
    }


def _load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return _default_state()
    try:
        obj = _read_json_obj(path)
    except Exception:
        return _default_state()
    if str(obj.get("schema_id") or "") != "C2_AUTO_REPAIR_CONTROLLER_STATE_V1":
        return _default_state()
    return obj


def _save_state(path: Path, state: Dict[str, Any]) -> None:
    validate_against_repo_schema_v1(state, REPO_ROOT, STATE_SCHEMA)
    _write_json(path, state)


def _blocker_fingerprint(day: str, blocker_class: str, reason_codes: List[str], refs: List[str]) -> str:
    obj = {
        "day_utc": day,
        "blocker_class": blocker_class,
        "reason_codes": sorted(set(reason_codes)),
        "supporting_refs": sorted(set(refs)),
    }
    return _sha256_bytes((json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8"))


def _build_repair_prompt(context: Dict[str, Any]) -> str:
    ctx_json = json.dumps(context, indent=2, sort_keys=True)
    return f"""You are operating in AUTONOMOUS REPAIR CONTROLLER mode for Constellation.

AUTHORITATIVE REPO ROOT
/home/node/constellation_2_runtime

CURRENT REPAIR CONTEXT (machine-provided)
{ctx_json}

MISSION
- Continuously diagnose, repair, validate, rerun, and reassess until:
  1) proven READY from runtime truth, or
  2) one true human decision gate.

NON-NEGOTIABLE RULES
- Stay inside repo root.
- Respect governance, fail-closed behavior, and immutability.
- Prefer narrow proof first, then relevant end-to-end rerun.
- Do not ask for ordinary repair confirmations.
- Do not claim readiness without artifact proof.
- Cross-SHA same-day aborts must be non-blocking when earlier canonical PASS proves health.

REQUIRED OUTPUT TAIL
At the end of your run, emit exactly one line:
AUTO_REPAIR_OUTCOME: READY
or
AUTO_REPAIR_OUTCOME: DECISION_GATE
or
AUTO_REPAIR_OUTCOME: UNRESOLVED
"""


def _parse_repair_outcome(log_path: Path) -> str:
    if not log_path.exists() or not log_path.is_file():
        return "UNKNOWN"
    txt = log_path.read_text(encoding="utf-8", errors="replace")
    marker = "AUTO_REPAIR_OUTCOME:"
    idx = txt.rfind(marker)
    if idx < 0:
        return "UNRESOLVED"
    tail = txt[idx + len(marker) :].strip().splitlines()[0].strip().upper()
    if tail in ("READY", "DECISION_GATE", "UNRESOLVED"):
        return tail
    return "UNRESOLVED"


def _build_trigger_payload(
    *,
    truth_root: Path,
    day: str,
    evaluated_at: datetime,
    health: Dict[str, Any],
    state: Dict[str, Any],
    cooldown_seconds: int,
    max_relaunch: int,
    codex_cmd: List[str],
    launch_mode: str,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    controller_root = (truth_root / "reports" / "auto_repair_controller_v1").resolve()
    sessions_root = (controller_root / "sessions").resolve()
    packets_root = (controller_root / "repair_packets" / day).resolve()
    sessions_root.mkdir(parents=True, exist_ok=True)
    packets_root.mkdir(parents=True, exist_ok=True)

    decision = "NO_TRIGGER"
    reason_codes: List[str] = []
    operator_summary = ""
    session_state = state.get("active_session")
    active_session = session_state if isinstance(session_state, dict) else None

    if active_session:
        pid = int(active_session.get("pid") or 0)
        if pid > 0 and _pid_alive(pid):
            pass
        else:
            sid = str(active_session.get("session_id") or "").strip()
            log_path = sessions_root / sid / "repair_session.log"
            outcome = _parse_repair_outcome(log_path)
            active_session["status"] = "FINISHED"
            active_session["outcome"] = outcome
            fp = str(active_session.get("blocker_fingerprint") or "").strip()
            if fp:
                b = state.setdefault("blockers", {}).setdefault(fp, {})
                b["last_outcome"] = outcome
                b["last_session_id"] = sid
                b["last_finished_utc"] = _utc_iso(evaluated_at)
                if outcome == "DECISION_GATE":
                    b["decision_gate_suppressed"] = True
            state.setdefault("latest_outcome_by_day", {})[day] = {
                "session_id": sid,
                "outcome": outcome,
                "finished_at_utc": _utc_iso(evaluated_at),
            }
            state["active_session"] = None
            active_session = None

    state_value = str(health.get("state") or "").strip().upper()
    blocker_class = str(health.get("blocker_class") or "").strip()
    reason_set = [str(x) for x in (health.get("reason_codes") or [])]
    refs = [str(x) for x in (health.get("supporting_artifact_refs") or [])]
    fingerprint = _blocker_fingerprint(day, blocker_class, reason_set, refs)
    bmap = state.setdefault("blockers", {})
    bstate = bmap.setdefault(fingerprint, {})

    if state_value == "READY":
        decision = "NO_TRIGGER"
        reason_codes = ["READY_NO_TRIGGER"]
        operator_summary = "READY: no repair trigger."
    elif state_value == "DEGRADED":
        decision = "NO_TRIGGER"
        reason_codes = ["DEGRADED_NO_TRIGGER"]
        operator_summary = "DEGRADED: diagnostics only, repair trigger suppressed by policy."
    else:
        if active_session:
            decision = "SUPPRESSED"
            reason_codes = ["SINGLE_ACTIVE_REPAIR_SESSION"]
            operator_summary = "Repair required but suppressed: active repair session is already running."
        elif bool(bstate.get("decision_gate_suppressed")):
            decision = "SUPPRESSED"
            reason_codes = ["DECISION_GATE_SUPPRESSED_FOR_BLOCKER"]
            operator_summary = "Repair required but suppressed: blocker already reached decision gate."
        else:
            launches = int(bstate.get("launch_count") or 0)
            last_launch = str(bstate.get("last_launch_utc") or "").strip()
            if launches >= max_relaunch:
                decision = "SUPPRESSED"
                reason_codes = ["MAX_RELAUNCH_PER_BLOCKER_REACHED"]
                operator_summary = "Repair required but suppressed: max relaunch count reached."
            else:
                suppress_cooldown = False
                if last_launch:
                    try:
                        last_dt = datetime.fromisoformat(last_launch.replace("Z", "+00:00"))
                        age = int((evaluated_at - last_dt).total_seconds())
                        if age < cooldown_seconds:
                            suppress_cooldown = True
                    except Exception:
                        suppress_cooldown = False
                if suppress_cooldown:
                    decision = "SUPPRESSED"
                    reason_codes = ["COOLDOWN_ACTIVE"]
                    operator_summary = "Repair required but suppressed by cooldown."
                else:
                    decision = "TRIGGERED"
                    reason_codes = ["REPAIR_REQUIRED_TRIGGERED"]
                    session_id = f"{day}__{_ts_compact(evaluated_at)}__{fingerprint[:8]}"
                    session_dir = (sessions_root / session_id).resolve()
                    packet_dir = (packets_root / session_id).resolve()
                    session_dir.mkdir(parents=True, exist_ok=True)
                    packet_dir.mkdir(parents=True, exist_ok=True)

                    context = {
                        "target_day_utc": day,
                        "truth_root": str(truth_root),
                        "health_state": health,
                        "blocker_fingerprint": fingerprint,
                        "cooldown_seconds": int(cooldown_seconds),
                        "max_relaunch_per_blocker_per_day": int(max_relaunch),
                    }
                    context_path = (packet_dir / "repair_context.v1.json").resolve()
                    prompt_path = (packet_dir / "codex_repair_prompt.txt").resolve()
                    _write_json(context_path, context)
                    prompt_path.write_text(_build_repair_prompt(context), encoding="utf-8")

                    log_path = (session_dir / "repair_session.log").resolve()
                    launch_mode_u = launch_mode.upper()
                    pid: Optional[int] = None
                    if launch_mode_u == "LIVE":
                        with prompt_path.open("r", encoding="utf-8") as inp, log_path.open("a", encoding="utf-8") as out:
                            proc = subprocess.Popen(
                                codex_cmd,
                                cwd=str(REPO_ROOT),
                                stdin=inp,
                                stdout=out,
                                stderr=subprocess.STDOUT,
                                text=True,
                            )
                            pid = int(proc.pid)
                        state["active_session"] = {
                            "session_id": session_id,
                            "day_utc": day,
                            "blocker_fingerprint": fingerprint,
                            "started_at_utc": _utc_iso(evaluated_at),
                            "pid": pid,
                            "status": "RUNNING",
                        }
                        operator_summary = f"Repair triggered in LIVE mode (pid={pid})."
                    else:
                        # Deterministic dry-run path for validation/test automation.
                        log_path.write_text("AUTO_REPAIR_OUTCOME: UNRESOLVED\nDRY_RUN_ONLY\n", encoding="utf-8")
                        operator_summary = "Repair trigger dry-run executed (no live Codex launch)."

                    bstate["launch_count"] = launches + 1
                    bstate["last_launch_utc"] = _utc_iso(evaluated_at)
                    bstate["last_session_id"] = session_id
                    bstate["last_outcome"] = "LAUNCHED"
                    bstate["blocker_class"] = blocker_class
                    bstate["reason_codes"] = sorted(set(reason_set))
                    bstate["launch_mode"] = launch_mode_u
                    bstate["packet_path"] = str(context_path)
                    bstate["prompt_path"] = str(prompt_path)
                    bstate["log_path"] = str(log_path)

    state["updated_at_utc"] = _utc_iso(evaluated_at)

    payload: Dict[str, Any] = {
        "schema_id": "C2_AUTO_REPAIR_TRIGGER_DECISION_V1",
        "schema_version": 1,
        "evaluated_at_utc": _utc_iso(evaluated_at),
        "target_day_utc": day,
        "health_state": state_value,
        "blocker_fingerprint": fingerprint,
        "trigger_decision": decision,
        "reason_codes": reason_codes,
        "operator_summary": operator_summary,
        "active_session": state.get("active_session"),
        "cooldown_seconds": int(cooldown_seconds),
        "max_relaunch_per_blocker_per_day": int(max_relaunch),
        "launch_mode": launch_mode.upper(),
        "producer": {
            "repo": "constellation_2_runtime",
            "module": "ops/tools/run_c2_auto_repair_controller_v1.py",
            "git_sha": _git_sha(),
        },
    }
    validate_against_repo_schema_v1(payload, REPO_ROOT, TRIGGER_SCHEMA)
    return payload, state


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_c2_auto_repair_controller_v1")
    ap.add_argument("--day_utc", default="")
    ap.add_argument("--truth_root", default="")
    ap.add_argument("--cooldown_seconds", type=int, default=900)
    ap.add_argument("--max_relaunch_per_blocker_per_day", type=int, default=3)
    ap.add_argument("--launch_mode", choices=["LIVE", "DRY_RUN"], default="LIVE")
    ap.add_argument(
        "--codex_cmd",
        default="codex exec -",
        help="Command used for LIVE launch (shell words, default: 'codex exec -').",
    )
    args = ap.parse_args()

    now = _utc_now()
    day = str(args.day_utc).strip() or now.strftime("%Y-%m-%d")
    truth_root = _resolve_truth_root(args.truth_root)

    controller_root = (truth_root / "reports" / "auto_repair_controller_v1").resolve()
    health_dir = (controller_root / "health" / day).resolve()
    trigger_dir = (controller_root / "trigger" / day).resolve()
    state_path = (controller_root / "state" / "controller_state.v1.json").resolve()
    health_path = (health_dir / f"health_supervisor.{_ts_compact(now)}.v1.json").resolve()
    trigger_path = (trigger_dir / f"repair_trigger.{_ts_compact(now)}.v1.json").resolve()

    health = _build_health_payload(truth_root=truth_root, day=day, evaluated_at=now)
    health_sha = _write_immutable(health_path, health)

    state = _load_state(state_path)
    trigger, state2 = _build_trigger_payload(
        truth_root=truth_root,
        day=day,
        evaluated_at=now,
        health=health,
        state=state,
        cooldown_seconds=int(args.cooldown_seconds),
        max_relaunch=int(args.max_relaunch_per_blocker_per_day),
        codex_cmd=shlex.split(str(args.codex_cmd)),
        launch_mode=str(args.launch_mode).strip().upper(),
    )
    trigger_sha = _write_immutable(trigger_path, trigger)
    _save_state(state_path, state2)

    print(
        "OK: C2_AUTO_REPAIR_CONTROLLER_V1 "
        f"day_utc={day} state={health['state']} trigger_decision={trigger['trigger_decision']} "
        f"health_path={health_path} health_sha256={health_sha} trigger_path={trigger_path} trigger_sha256={trigger_sha} "
        f"state_path={state_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
