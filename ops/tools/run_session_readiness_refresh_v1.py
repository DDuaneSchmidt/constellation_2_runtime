#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry
from ops.tools.run_day_authority_decision_v1 import (
    write_day_authority_decision_from_refresh_report_v1,
)
from constellation_2.common.authority_kernel_v1 import run_day_authority_preflight_v1
from constellation_2.common.constitutional_review_resolution_v1 import (
    iter_constitutional_operator_decision_paths_v1,
)
from constellation_2.common.operator_summary_v1 import write_operator_summary
from constellation_2.common.paper_execution_authority_v1 import (
    resolve_governed_paper_execution_profile,
    resolve_governed_paper_execution_roots,
)

try:
    from constellation_2.common.artifact_lifecycle_incident_v1 import (
        list_unresolved_authority_lifecycle_incidents,
        summarize_lifecycle_incidents,
    )
except ImportError:
    def list_unresolved_authority_lifecycle_incidents(*, truth_root: Path, day_utc: str) -> List[Dict[str, Any]]:
        return []

    def summarize_lifecycle_incidents(incidents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return list(incidents)
from constellation_2.common.trade_submit_readiness_authority_v1 import (
    GovernedSleeveTruthBinding,
    resolve_governed_sleeve_truth_bindings,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.day_artifact_refresh_v1 import write_day_artifact_refreshable_v1
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_operator_statement_path,
    resolve_paper_trading_posture_path,
    resolve_repo_operator_input_root,
    resolve_repo_truth_root,
    resolve_session_readiness_refresh_path,
)


GLOBAL_TRUTH_ROOT = resolve_repo_truth_root(REPO_ROOT)
ACCOUNT_REGISTRY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json").resolve()
SLEEVE_REGISTRY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json").resolve()
BROKER_EVENTS_BOOTSTRAP_TOOL = (REPO_ROOT / "ops/ib/c2_execution_observer_v1.py").resolve()
BROKER_EVENTS_MANIFEST_TOOL = (REPO_ROOT / "ops/ib/run_broker_event_day_manifest_v1.py").resolve()
HANDSHAKE_TOOL = (REPO_ROOT / "ops/tools/run_ib_api_handshake_spine_v1.py").resolve()
READINESS_TOOL = (REPO_ROOT / "ops/tools/run_trade_submit_readiness_c2_v1.py").resolve()
EXECUTION_RECONCILIATION_TOOL = (REPO_ROOT / "ops/tools/run_execution_reconciliation_day_v1.py").resolve()
OPERATOR_STATEMENT_TOOL = (REPO_ROOT / "ops/tools/ensure_cash_ledger_operator_statement_v1.py").resolve()
OPERATOR_STATEMENT_CLIENT_ID = "92"
ACCOUNTING_NAV_TOOL = (REPO_ROOT / "ops/tools/run_accounting_nav_v2_day_v1.py").resolve()
MONITORING_REFRESH_TOOL = (REPO_ROOT / "ops/tools/run_c2_global_monitoring_refresh_v1.py").resolve()
GLOBAL_GATE_REFRESH_TOOL = (REPO_ROOT / "ops/tools/run_gate_authority_plane_v1.py").resolve()
STARTUP_AUTHORIZATION_CONVERGENCE_TOOL = (REPO_ROOT / "ops/tools/run_paper_startup_authorization_convergence_v1.py").resolve()
EXPOSURE_NET_TOOL = (REPO_ROOT / "ops/tools/run_exposure_net_day_v1.py").resolve()
CAPITAL_AUTHORITY_ALLOCATION_TOOL = (REPO_ROOT / "ops/tools/run_capital_authority_allocation_day_v1.py").resolve()
AUTHORIZATION_ARTIFACTS_TOOL = (REPO_ROOT / "ops/tools/run_authorization_artifacts_day_v1.py").resolve()
PAPER_STARTUP_INTENT_INPUT_CONVERGENCE_TOOL = (REPO_ROOT / "ops/tools/run_paper_startup_intent_input_convergence_v1.py").resolve()
STARTUP_MATERIALIZATION_INPUT_CONVERGENCE_TOOL = (REPO_ROOT / "ops/tools/run_startup_materialization_input_convergence_v1.py").resolve()
EXECUTION_POSITIONS_V5_BRIDGE_TOOL = (REPO_ROOT / "ops/tools/run_execution_positions_snapshot_v5_bridge_v1.py").resolve()
POINTER_ATTEMPT_ALLOC_TOOL = (REPO_ROOT / "ops/tools/run_pointer_attempt_alloc_v1.py").resolve()
POINTER_APPEND_TOOL = (REPO_ROOT / "ops/tools/run_pointer_append_v1.py").resolve()
POINTER_HEADS_MATERIALIZE_TOOL = (REPO_ROOT / "ops/tools/run_pointer_heads_materialize_v1.py").resolve()
PAPER_DAY_ORCHESTRATOR_SERVICE_PATH = (REPO_ROOT / "ops/systemd/user/c2-paper-day-orchestrator.service").resolve()
GATE_HIERARCHY_POLICY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/GATE_HIERARCHY_V1.json").resolve()
REGISTRY_COHERENCE_TOOL = (REPO_ROOT / "ops/tools/run_registry_coherence_check_v1.py").resolve()
ARTIFACT_FRESHNESS_SUMMARY_TOOL = (REPO_ROOT / "ops/tools/run_operator_artifact_freshness_summary_v1.py").resolve()
PNL_ATTRIBUTION_TOOL = (REPO_ROOT / "ops/tools/run_pnl_attribution_v1.py").resolve()
TRADING_DAY_STATE_MACHINE_TOOL = (REPO_ROOT / "ops/tools/run_trading_day_state_machine_v1.py").resolve()
TRADING_DAY_INTENT_GENERATION_TOOL = (REPO_ROOT / "ops/tools/run_trading_day_intent_generation_v1.py").resolve()
STARTUP_PROOF_VALIDATION_TOOL = (REPO_ROOT / "ops/tools/run_startup_proof_validation_v1.py").resolve()
STARTUP_MATERIALIZATION_TOOL = (REPO_ROOT / "ops/tools/run_startup_materialization_v1.py").resolve()
GLOBAL_KILL_SWITCH_TOOL = (REPO_ROOT / "ops/tools/run_global_kill_switch_v1.py").resolve()
PAPER_TRADING_POSTURE_TOOL = (REPO_ROOT / "ops/tools/run_paper_trading_posture_v1.py").resolve()
SESSION_AUTHORITY_TOOL = (REPO_ROOT / "ops/tools/run_session_authority_v1.py").resolve()
SUBMIT_BOUNDARY_STATUS_TOOL = (REPO_ROOT / "ops/tools/run_submit_boundary_status_v1.py").resolve()
PAPER_SESSION_LEDGER_TOOL = (REPO_ROOT / "ops/tools/run_paper_session_ledger_v1.py").resolve()
OPERATOR_FUTURE_DAY_OVERRIDE_TOOL = (REPO_ROOT / "ops/tools/run_operator_future_day_override_v1.py").resolve()
SESSION_REPORT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/session_readiness_refresh.v1.schema.json"
CAPITAL_AUTHORITY_ALLOCATION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/ALLOCATION/capital_authority_allocation.v1.schema.json"
PRIMARY_SLEEVE_ID = "PRIMARY"
CANONICAL_SEQUENCE_OWNER = "ops/tools/run_c2_paper_day_orchestrator_v2.py"
OPERATOR_INPUT_TRUTH_ROOT = resolve_repo_operator_input_root(REPO_ROOT)
EXECUTION_OBSERVER_SERVICE_NAME = "c2-execution-observer.service"
EXECUTION_OBSERVER_STATUS_QUERY_TIMEOUT_SECONDS = 5.0
BROKER_EVENT_DAY_MANIFEST_TIMEOUT_SECONDS = 5.0
TRADE_SUBMIT_READINESS_REFRESH_TIMEOUT_SECONDS = 3.0


def _critical_path_python() -> Path:
    return Path(sys.executable)


BROKER_EVENTS_BOOTSTRAP_PYTHON = _critical_path_python()
OPERATOR_STATEMENT_PYTHON = _critical_path_python()


def _read_json(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: top_level_not_object path={path}")
    return obj


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _run(
    cmd: List[str],
    *,
    extra_env: Dict[str, str] | None = None,
    timeout_seconds: float | None = None,
) -> Dict[str, Any]:
    env = os.environ.copy()
    env["C2_TRUTH_ROOT"] = str(GLOBAL_TRUTH_ROOT)
    if extra_env:
        env.update(extra_env)
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            env=env,
            timeout=timeout_seconds if isinstance(timeout_seconds, (int, float)) and float(timeout_seconds) > 0 else None,
        )
    except subprocess.TimeoutExpired as exc:
        timeout_label = (
            f"{float(timeout_seconds):g}"
            if isinstance(timeout_seconds, (int, float)) and float(timeout_seconds) > 0
            else "NONE"
        )
        return {
            "cmd": cmd,
            "returncode": 124,
            "stdout": str(exc.stdout or "").strip(),
            "stderr": (
                "SESSION_READINESS_SUBPROCESS_TIMEOUT:"
                f"timeout_seconds={timeout_label}:command={cmd[0] if cmd else 'UNKNOWN'}"
            ),
        }
    return {
        "cmd": cmd,
        "returncode": int(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def _run_trade_submit_readiness_refreshes(*, day_utc: str, accounts: Dict[str, List[str]]) -> List[Dict[str, Any]]:
    readiness_results: List[Dict[str, Any]] = []
    for env in ("PAPER", "LIVE"):
        for account_id in accounts.get(env, []):
            cmd = [
                sys.executable,
                str(READINESS_TOOL),
                "--day_utc",
                day_utc,
                "--ib_account",
                account_id,
                "--environment",
                env,
            ]
            result = _run(cmd, timeout_seconds=TRADE_SUBMIT_READINESS_REFRESH_TIMEOUT_SECONDS)
            result["environment"] = env
            result["ib_account"] = account_id
            readiness_results.append(result)
    return readiness_results


def _monitoring_refresh_extra_env() -> Dict[str, str]:
    return {
        "C2_SKIP_AUTHORITATIVE_ORCHESTRATOR_BACKFILL": "YES",
        "C2_SKIP_SESSION_AUTHORITY_REENTRY": "YES",
    }


def _parse_json_stdout(result: Dict[str, Any]) -> Dict[str, Any]:
    stdout = str(result.get("stdout") or "").strip()
    if not stdout:
        return {}
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _maybe_write_future_day_override(*, day_utc: str) -> Dict[str, Any]:
    if str(day_utc).strip() <= datetime.now(timezone.utc).date().isoformat():
        return {
            "cmd": [],
            "returncode": 0,
            "stdout": "SKIPPED_NOT_FUTURE_DAY",
            "stderr": "",
            "status": "SKIPPED_NOT_FUTURE_DAY",
        }
    cmd = [
        sys.executable,
        str(OPERATOR_FUTURE_DAY_OVERRIDE_TOOL),
        "--day_utc",
        day_utc,
        "--operator_id",
        "codex",
        "--reason",
        f"Governed PAPER future-day writes for {day_utc} startup readiness repair.",
        "--mode",
        "PAPER",
    ]
    return _run(cmd, extra_env={"C2_MODE": "PAPER"})


def _resolve_broker_events_bootstrap_python() -> Path:
    candidate = BROKER_EVENTS_BOOTSTRAP_PYTHON.expanduser()
    if candidate.exists() and candidate.is_file():
        return candidate
    return _critical_path_python()


def _broker_bootstrap_reason_codes(result: Dict[str, Any]) -> List[str]:
    stderr = str(result.get("stderr") or "")
    stdout = str(result.get("stdout") or "")
    text = f"{stderr}\n{stdout}"
    reason_codes: List[str] = []
    if ("ModuleNotFoundError" in text or "No module named" in text) and "ibapi" in text:
        reason_codes.append("READINESS_BOOTSTRAP_IBAPI_IMPORT_FAILED")
    if reason_codes:
        reason_codes.append(f"READINESS_BOOTSTRAP_PYTHON:{_resolve_broker_events_bootstrap_python()}")
    return sorted(set(reason_codes))


def _read_proc_cmdline(pid: int) -> List[str]:
    if int(pid) <= 0:
        return []
    proc_path = Path("/proc") / str(int(pid)) / "cmdline"
    try:
        raw = proc_path.read_bytes()
    except OSError:
        return []
    return [chunk.decode("utf-8", errors="replace") for chunk in raw.split(b"\0") if chunk]


def _invoked_by_session_authority_build() -> bool:
    parent_cmdline = _read_proc_cmdline(os.getppid())
    if not parent_cmdline:
        return False
    joined = " ".join(parent_cmdline)
    if "ops/tools/run_session_authority_v1.py" not in joined:
        return False
    if "--phase" not in parent_cmdline:
        return False
    try:
        phase_index = parent_cmdline.index("--phase")
    except ValueError:
        return False
    if phase_index + 1 >= len(parent_cmdline):
        return False
    phase_value = str(parent_cmdline[phase_index + 1]).strip().lower()
    return phase_value in {"build", "admit", "all"}


def _observer_service_cmdline_matches(
    *,
    cmdline: List[str],
    execution_truth_root: Path,
    execution_profile: Any,
    sleeve_id: str,
) -> bool:
    required_tokens = [
        str(BROKER_EVENTS_BOOTSTRAP_TOOL),
        "--truth_root",
        str(execution_truth_root),
        "--host",
        str(execution_profile.host),
        "--port",
        str(execution_profile.port),
        "--client-id",
        str(execution_profile.client_id_observer),
        "--environment",
        "PAPER",
        "--sleeve-id",
        str(sleeve_id).strip().upper(),
    ]
    return bool(cmdline) and all(token in cmdline for token in required_tokens)


def _active_execution_observer_service_status(
    *,
    execution_truth_root: Path,
    execution_profile: Any,
    sleeve_id: str,
) -> Dict[str, Any]:
    try:
        show = subprocess.run(
            [
                "systemctl",
                "--user",
                "show",
                EXECUTION_OBSERVER_SERVICE_NAME,
                "--property",
                "ActiveState",
                "--property",
                "MainPID",
                "--property",
                "FragmentPath",
            ],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            check=False,
            timeout=EXECUTION_OBSERVER_STATUS_QUERY_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "service_name": EXECUTION_OBSERVER_SERVICE_NAME,
            "query_returncode": 124,
            "query_stdout": str(exc.stdout or "").strip(),
            "query_stderr": str(exc.stderr or "").strip(),
            "active": False,
            "state": (
                "SYSTEMCTL_QUERY_TIMEOUT:"
                f"timeout_seconds={EXECUTION_OBSERVER_STATUS_QUERY_TIMEOUT_SECONDS:g}"
            ),
        }
    status: Dict[str, Any] = {
        "service_name": EXECUTION_OBSERVER_SERVICE_NAME,
        "query_returncode": int(show.returncode),
        "query_stdout": str(show.stdout).strip(),
        "query_stderr": str(show.stderr).strip(),
        "active": False,
        "state": "UNKNOWN",
    }
    if show.returncode != 0:
        status["state"] = "SYSTEMCTL_QUERY_FAILED"
        return status
    properties: Dict[str, str] = {}
    for line in str(show.stdout).splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        properties[str(key).strip()] = str(value).strip()
    status["active_state"] = properties.get("ActiveState", "")
    status["fragment_path"] = properties.get("FragmentPath", "")
    try:
        main_pid = int(properties.get("MainPID", "0") or "0")
    except ValueError:
        main_pid = 0
    status["main_pid"] = main_pid
    if status["active_state"] != "active" or main_pid <= 0:
        status["state"] = "INACTIVE"
        return status
    cmdline = _read_proc_cmdline(main_pid)
    status["cmdline"] = cmdline
    if not _observer_service_cmdline_matches(
        cmdline=cmdline,
        execution_truth_root=execution_truth_root,
        execution_profile=execution_profile,
        sleeve_id=sleeve_id,
    ):
        status["state"] = "ACTIVE_MISMATCH"
        return status
    status["active"] = True
    status["state"] = "ACTIVE_MATCHED"
    return status


def _write_day_authority_decision_from_report(
    *,
    day_utc: str,
    truth_root: Path,
    report_path: Path,
    producer_git_sha: str,
) -> Dict[str, Any]:
    return write_day_authority_decision_from_refresh_report_v1(
        day_utc=day_utc,
        truth_root=truth_root,
        report_path=report_path,
        producer_git_sha=producer_git_sha,
    )


def _paper_noop_hard_failures_nonblocking(hard_failures: set[str]) -> bool:
    if not hard_failures:
        return False
    allowed_nonblocking_noop = {
        "canonical_intent_publication_v1",
        "canonical_market_data_preopen_prepare_v1",
        "signal_proof_publication_v1",
        "pointer_heads_materialize_v1",
    }
    return hard_failures.issubset(allowed_nonblocking_noop)


def _paper_expected_calendar_no_op(day_utc: str) -> bool:
    posture_path = resolve_paper_trading_posture_path(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day_utc)
    if not posture_path.exists() or not posture_path.is_file():
        return False
    try:
        posture = _read_json(posture_path)
    except Exception:
        return False
    posture_class = str(posture.get("posture_class") or "").strip().upper()
    blocking_family = str(posture.get("blocking_family") or "").strip().upper()
    expected_no_op_today = bool(posture.get("expected_no_op_today") is True)
    blocking_reason_codes = {
        str(code).strip()
        for code in (posture.get("blocking_reason_codes") or [])
        if str(code).strip()
    }
    return (
        posture_class == "PAPER_READY_NO_OP"
        and blocking_family == "EXPECTED_NO_OP"
        and expected_no_op_today
        and "MARKET_CALENDAR_NON_TRADING_SESSION" in blocking_reason_codes
    )


def _paper_global_gate_refresh_nonblocking(result: Dict[str, Any], *, expected_calendar_no_op: bool = False) -> bool:
    if int(result.get("returncode") or 0) == 0:
        return False
    payload = _parse_json_stdout(result)
    hard_failures = {
        str(name).strip()
        for name in (payload.get("hard_failures") or [])
        if str(name).strip()
    }
    if not hard_failures:
        return True
    allowed_nonblocking = {
        "exit_reconciliation_v1",
        "pointer_heads_materialize_v1",
    }
    if hard_failures.issubset(allowed_nonblocking):
        return True
    if expected_calendar_no_op and _paper_noop_hard_failures_nonblocking(hard_failures):
        return True
    return False


def _extract_truth_lifecycle_run_ledger_path(result: Dict[str, Any]) -> str:
    stdout = str(result.get("stdout") or "")
    for token in stdout.replace("\n", " ").split():
        if token.startswith("phase_run_ledger="):
            return str(token.split("=", 1)[1] or "").strip()
    return ""


def _ensure_execution_reconciliation_day_roots(day_utc: str) -> None:
    for rel in (
        f"execution_evidence_v1/submissions/{day_utc}",
        f"fill_ledger_v1/{day_utc}",
        f"execution_stream_v1/{day_utc}",
    ):
        (GLOBAL_TRUTH_ROOT / rel).resolve().mkdir(parents=True, exist_ok=True)


def _git_sha() -> str:
    try:
        return subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT)).decode("utf-8").strip()
    except Exception:
        # Clean runtime roots can be source-derived without .git metadata.
        return "0" * 40


def _extract_fail_reason_code(result: Dict[str, Any]) -> str:
    stderr = str(result.get("stderr") or "").strip()
    if not stderr.startswith("FAIL:"):
        return ""
    detail = stderr[5:].strip()
    if not detail:
        return ""
    code = detail.split(":", 1)[0].strip().upper()
    return code if code else ""


def _refresh_positions_snapshots(*, day_utc: str, producer_git_sha: str, paper_account: str, python_bin: str) -> Dict[str, Dict[str, Any]]:
    v5_cmd = [
        python_bin,
        "-m",
        "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v5",
        "--day_utc",
        day_utc,
        "--producer_git_sha",
        producer_git_sha,
        "--producer_repo",
        REPO_ROOT.name,
        "--truth_root",
        str(GLOBAL_TRUTH_ROOT),
        "--ib_account",
        paper_account,
    ]
    v5_result = _run(v5_cmd)

    v2_cmd = [
        python_bin,
        "-m",
        "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v2",
        "--day_utc",
        day_utc,
        "--producer_git_sha",
        producer_git_sha,
        "--producer_repo",
        REPO_ROOT.name,
    ]
    v2_result: Dict[str, Any] = {
        "cmd": v2_cmd,
        "returncode": 99,
        "stdout": "",
        "stderr": "SKIPPED_POSITIONS_SNAPSHOT_V5_FAILED",
    }
    if int(v5_result.get("returncode") or 0) == 0:
        v2_result = _run(v2_cmd)

    return {"v5": v5_result, "v2": v2_result}


def _resolve_paper_sleeve_truth_bindings(*, paper_account: str) -> List[GovernedSleeveTruthBinding]:
    bindings = resolve_governed_sleeve_truth_bindings(
        repo_root=REPO_ROOT,
        environment="PAPER",
        requested_ib_account=paper_account,
    )
    deduped: List[GovernedSleeveTruthBinding] = []
    seen: set[tuple[str, str]] = set()
    for binding in bindings:
        key = (str(binding.sleeve_id).strip().upper(), str(binding.truth_root.resolve()))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(binding)
    return deduped


def _seed_sleeve_positions_snapshots(*, day_utc: str, paper_account: str) -> Dict[str, Any]:
    seeded_paths: List[str] = []
    bindings = _resolve_paper_sleeve_truth_bindings(paper_account=paper_account)
    canonical_path = (GLOBAL_TRUTH_ROOT / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v5.json").resolve()
    if not canonical_path.exists() or not canonical_path.is_file():
        return {"status": "OK", "seeded_paths": [], "seeded_count": 0}
    for binding in bindings:
        sleeve_snapshot = (binding.truth_root.resolve() / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v5.json").resolve()
        if sleeve_snapshot.exists():
            continue
        bridge = _run(
            [
                sys.executable,
                str(EXECUTION_POSITIONS_V5_BRIDGE_TOOL),
                "--day_utc",
                day_utc,
                "--source_truth_root",
                str(GLOBAL_TRUTH_ROOT),
                "--truth_root",
                str(binding.truth_root.resolve()),
            ]
        )
        if int(bridge.get("returncode") or 0) != 0 or not sleeve_snapshot.exists():
            raise RuntimeError(
                "SLEEVE_POSITIONS_SNAPSHOT_V5_SEED_FAILED:"
                f"sleeve_id={binding.sleeve_id}:day_utc={day_utc}:"
                f"stderr={bridge.get('stderr', '') or bridge.get('stdout', '')}"
            )
        seeded_paths.append(str(sleeve_snapshot))
    return {
        "status": "OK",
        "seeded_paths": sorted(set(seeded_paths)),
        "seeded_count": len(sorted(set(seeded_paths))),
    }


def _annotate_scoped_gate_refresh_result(
    *,
    binding: GovernedSleeveTruthBinding,
    result: Dict[str, Any],
    expected_calendar_no_op: bool,
) -> Dict[str, Any]:
    payload = _parse_json_stdout(result)
    hard_failures = sorted({str(name).strip() for name in (payload.get("hard_failures") or []) if str(name).strip()})
    noop_nonblocking = expected_calendar_no_op and _paper_noop_hard_failures_nonblocking(set(hard_failures))
    annotated = dict(result)
    annotated["sleeve_id"] = str(binding.sleeve_id).strip().upper()
    annotated["environment"] = str(binding.environment).strip().upper()
    annotated["scope_truth_root"] = str(binding.truth_root.resolve())
    annotated["truth_partition"] = str(binding.truth_partition)
    annotated["hard_failures"] = hard_failures
    annotated["nonblocking_override"] = False
    if int(result.get("returncode") or 0) == 0:
        annotated["state"] = "READY"
        annotated["blocking_scope"] = "NONE"
        annotated["reason_codes"] = []
    elif annotated["sleeve_id"] == PRIMARY_SLEEVE_ID and noop_nonblocking:
        annotated["state"] = "READY_WITH_EXPECTED_NO_OP_GAPS"
        annotated["blocking_scope"] = "NONE"
        annotated["reason_codes"] = sorted(set(["EXPECTED_NO_OP_NON_TRADING_SESSION"] + hard_failures))
        annotated["nonblocking_override"] = True
    elif annotated["sleeve_id"] == PRIMARY_SLEEVE_ID:
        annotated["state"] = "BLOCKED"
        annotated["blocking_scope"] = "PRIMARY_SLEEVE"
        annotated["reason_codes"] = hard_failures or [f"SCOPED_GATE_REFRESH_FAILED:{annotated['sleeve_id']}"]
    else:
        annotated["state"] = "BLOCKED"
        annotated["blocking_scope"] = "SLEEVE"
        annotated["reason_codes"] = hard_failures or [f"SCOPED_GATE_REFRESH_FAILED:{annotated['sleeve_id']}"]
    return annotated


def _scope_summary_from_results(scoped_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    blocked_primary: List[str] = []
    blocked_other: List[str] = []
    primary_reason_codes: List[str] = []
    for row in scoped_results:
        if int(row.get("returncode") or 0) == 0 or bool(row.get("nonblocking_override") is True):
            continue
        sleeve_id = str(row.get("sleeve_id") or "").strip().upper()
        row_reason_codes = [str(code).strip() for code in (row.get("reason_codes") or []) if str(code).strip()]
        if sleeve_id == PRIMARY_SLEEVE_ID:
            blocked_primary.append(sleeve_id)
            primary_reason_codes.extend(row_reason_codes)
        elif sleeve_id:
            blocked_other.append(sleeve_id)
    if blocked_primary:
        global_state = "BLOCKED"
    elif blocked_other:
        global_state = "DEGRADED"
    else:
        global_state = "PASS"
    return {
        "primary_sleeve_id": PRIMARY_SLEEVE_ID,
        "global_readiness_state": global_state,
        "primary_ready": not blocked_primary,
        "blocked_primary_sleeve_ids": sorted(set(blocked_primary)),
        "nonblocking_blocked_sleeve_ids": sorted(set(blocked_other)),
        "primary_blocker_reason_codes": sorted(set(primary_reason_codes)),
    }


def _primary_convergence_path(*, day_utc: str) -> Path:
    return (
        GLOBAL_TRUTH_ROOT
        / "reports"
        / "paper_startup_authorization_convergence_v1"
        / day_utc
        / "paper_startup_authorization_convergence.v1.json"
    ).resolve()


def _primary_authorization_path(*, binding: GovernedSleeveTruthBinding, day_utc: str) -> Path:
    return (
        binding.truth_root.resolve()
        / "reports"
        / "authorization_gate_verdict_v1"
        / day_utc
        / "authorization_gate_verdict.v1.json"
    ).resolve()


def _primary_exposure_net_path(*, binding: GovernedSleeveTruthBinding, day_utc: str) -> Path:
    return (
        binding.truth_root.resolve()
        / "risk_v1"
        / "exposure_net_v1"
        / day_utc
        / "exposure_net.v1.json"
    ).resolve()


def _primary_capital_authority_allocation_path(*, binding: GovernedSleeveTruthBinding, day_utc: str) -> Path:
    return (
        binding.truth_root.resolve()
        / "allocation_v1"
        / "capital_authority_allocation_v1"
        / day_utc
        / "capital_authority_allocation.v1.json"
    ).resolve()


def _canonical_capital_authority_allocation_path(*, day_utc: str) -> Path:
    return (
        GLOBAL_TRUTH_ROOT.resolve()
        / "allocation_v1"
        / "capital_authority_allocation_v1"
        / day_utc
        / "capital_authority_allocation.v1.json"
    ).resolve()


def _primary_intents_day_dir(*, binding: GovernedSleeveTruthBinding, day_utc: str) -> Path:
    return (binding.truth_root.resolve() / "intents_v1" / "snapshots" / day_utc).resolve()


def _select_effective_intent_files(*, intents_dir: Path) -> List[Path]:
    if not intents_dir.exists() or not intents_dir.is_dir():
        return []
    by_intent_id: Dict[str, tuple[int, str, Path]] = {}
    passthrough: List[Path] = []
    candidates = sorted(
        [
            p
            for p in intents_dir.iterdir()
            if p.is_file() and p.name.endswith(".json") and p.name != "no_intents_day.v1.json"
        ],
        key=lambda item: item.name,
    )
    for intent_path in candidates:
        try:
            intent_obj = _read_json(intent_path)
        except Exception:
            passthrough.append(intent_path)
            continue
        intent_id = str(intent_obj.get("intent_id") or "").strip()
        if not intent_id:
            passthrough.append(intent_path)
            continue
        stat = intent_path.stat()
        candidate = (int(stat.st_mtime_ns), intent_path.name, intent_path)
        prior = by_intent_id.get(intent_id)
        if prior is None or candidate[:2] >= prior[:2]:
            by_intent_id[intent_id] = candidate
    return sorted(passthrough + [row[2] for row in by_intent_id.values()], key=lambda item: item.name)


def _expected_authorization_paths(*, binding: GovernedSleeveTruthBinding, day_utc: str) -> List[Path]:
    intents_dir = _primary_intents_day_dir(binding=binding, day_utc=day_utc)
    out_dir = (binding.truth_root.resolve() / "engine_activity_v1" / "authorization_v1" / day_utc).resolve()
    return [
        (out_dir / f"{_sha256_file(intent_path)}.authorization.v1.json").resolve()
        for intent_path in _select_effective_intent_files(intents_dir=intents_dir)
    ]


def _collect_authorization_shadow_summary(*, expected_paths: List[Path]) -> Dict[str, Any]:
    proposal_rows: List[Dict[str, Any]] = []
    decision_counts: Dict[str, int] = {}
    missing_fact_count = 0
    dependency_degradation_count = 0
    legacy_constitutional_mismatch_count = 0
    review_required_count = 0
    review_completed_count = 0
    review_action_counts = {"APPROVE": 0, "REJECT": 0, "DEFER": 0}
    captured_count = 0
    for path in expected_paths:
        if not path.exists() or not path.is_file():
            proposal_rows.append(
                {
                    "authorization_path": str(path),
                    "captured": False,
                    "proposal_hash": "",
                    "fact_bundle_hash": "",
                    "decision_enum": "",
                    "comparison_status": "MISSING_ARTIFACT",
                }
            )
            continue
        try:
            payload = _read_json(path)
        except Exception as exc:
            proposal_rows.append(
                {
                    "authorization_path": str(path),
                    "captured": False,
                    "proposal_hash": "",
                    "fact_bundle_hash": "",
                    "decision_enum": "",
                    "comparison_status": f"INVALID_ARTIFACT:{type(exc).__name__}",
                }
            )
            continue
        captured_count += 1
        proposal_hash = str(payload.get("proposal_hash") or "").strip()
        fact_bundle_hash = str(payload.get("fact_bundle_hash") or "").strip()
        decision_enum = str(payload.get("decision_enum") or "").strip()
        comparison = dict(payload.get("legacy_constitutional_comparison") or {})
        comparison_status = str(comparison.get("comparison_status") or "").strip()
        constitutional_shadow = dict(payload.get("constitutional_shadow") or {})
        fact_bundle = dict(constitutional_shadow.get("fact_bundle") or {})
        decision = dict(constitutional_shadow.get("decision") or {})
        review_packet = dict(constitutional_shadow.get("review_packet") or {})
        decision_counts[decision_enum or "UNKNOWN"] = decision_counts.get(decision_enum or "UNKNOWN", 0) + 1
        missing_fact_count += sum(
            1
            for row in (decision.get("negative_evidence") or [])
            if isinstance(row, dict) and str(row.get("type") or "").strip() == "MISSING_FACT"
        )
        dependency_degradation_count += sum(
            1
            for row in (fact_bundle.get("fact_records") or [])
            if isinstance(row, dict)
            and str(row.get("dependency_health") or "").strip().upper() in {"DEGRADED_NON_BLOCKING", "DEGRADED_BLOCKING", "UNAVAILABLE"}
        )
        if comparison_status == "MISMATCH":
            legacy_constitutional_mismatch_count += 1
        operator_decision_paths: List[str] = []
        operator_final_action = ""
        if decision_enum == "REQUIRE_HUMAN_REVIEW":
            review_required_count += 1
            proposal_hash = str(payload.get("proposal_hash") or "").strip().lower()
            day_utc = str(payload.get("day_utc") or "").strip()
            truth_root = path.parents[3]
            candidate_paths = iter_constitutional_operator_decision_paths_v1(
                truth_root=truth_root,
                day_utc=day_utc,
                proposal_hash=proposal_hash,
            )
            operator_decision_paths = [str(item) for item in candidate_paths]
            latest_decision: Dict[str, Any] | None = None
            latest_decided_at = ""
            for candidate in candidate_paths:
                try:
                    operator_payload = _read_json(candidate)
                except Exception:
                    continue
                decided_at = str(operator_payload.get("decided_at") or "").strip()
                if decided_at >= latest_decided_at:
                    latest_decided_at = decided_at
                    latest_decision = operator_payload
            if isinstance(latest_decision, dict):
                review_completed_count += 1
                operator_final_action = str(latest_decision.get("operator_action") or "").strip().upper()
                if operator_final_action in review_action_counts:
                    review_action_counts[operator_final_action] += 1
        proposal_rows.append(
            {
                "authorization_path": str(path),
                "captured": bool(proposal_hash and fact_bundle_hash and decision_enum),
                "proposal_hash": proposal_hash,
                "fact_bundle_hash": fact_bundle_hash,
                "decision_enum": decision_enum,
                "comparison_status": comparison_status,
                "review_required": decision_enum == "REQUIRE_HUMAN_REVIEW",
                "review_packet_hash": str(review_packet.get("packet_hash") or "").strip(),
                "operator_decision_paths": operator_decision_paths,
                "operator_final_action": operator_final_action,
            }
        )
    total = len(expected_paths)
    decision_percentages = {
        key: round((value / total) * 100.0, 2) if total else 0.0
        for key, value in sorted(decision_counts.items())
    }
    approve_rate = round((review_action_counts["APPROVE"] / review_completed_count) * 100.0, 2) if review_completed_count else 0.0
    reject_rate = round((review_action_counts["REJECT"] / review_completed_count) * 100.0, 2) if review_completed_count else 0.0
    defer_rate = round((review_action_counts["DEFER"] / review_completed_count) * 100.0, 2) if review_completed_count else 0.0
    return {
        "proposal_count_expected": total,
        "proposal_count_captured": captured_count,
        "proposal_rows": proposal_rows,
        "decision_counts": dict(sorted(decision_counts.items())),
        "decision_percentages": decision_percentages,
        "missing_fact_count": missing_fact_count,
        "dependency_degradation_count": dependency_degradation_count,
        "legacy_constitutional_mismatch_count": legacy_constitutional_mismatch_count,
        "review_required_count": review_required_count,
        "review_completed_count": review_completed_count,
        "approve_rate": approve_rate,
        "reject_rate": reject_rate,
        "defer_rate": defer_rate,
    }


def _is_refuse_overwrite_existing_file(*, result: Dict[str, Any], path: Path) -> bool:
    stderr = str(result.get("stderr") or "").strip()
    return (
        int(result.get("returncode") or 0) != 0
        and stderr == f"FAIL: REFUSE_OVERWRITE_EXISTING_FILE: {path}"
        and path.exists()
        and path.is_file()
    )


def _initialize_primary_capital_authority_allocation_from_canonical(
    *,
    day_utc: str,
    binding: GovernedSleeveTruthBinding,
) -> Dict[str, Any]:
    canonical_path = _canonical_capital_authority_allocation_path(day_utc=day_utc)
    allocation_path = _primary_capital_authority_allocation_path(binding=binding, day_utc=day_utc)
    if allocation_path.exists() and allocation_path.is_file():
        return {
            "status": "EXISTS",
            "reason_codes": [],
            "canonical_path": str(canonical_path),
            "path": str(allocation_path),
            "sha256": _sha256_file(allocation_path),
        }
    if not canonical_path.exists() or not canonical_path.is_file():
        return {
            "status": "MISSING_CANONICAL",
            "reason_codes": ["PRIMARY_CAPITAL_AUTHORITY_ALLOCATION_ARTIFACT_MISSING"],
            "canonical_path": str(canonical_path),
            "path": str(allocation_path),
        }
    payload = _read_json(canonical_path)
    validate_against_repo_schema_v1(payload, REPO_ROOT, CAPITAL_AUTHORITY_ALLOCATION_SCHEMA)
    if str(payload.get("day_utc") or "").strip() != day_utc:
        raise SystemExit(
            "FAIL: primary_capital_authority_allocation_day_mismatch "
            f"expected_day_utc={day_utc} canonical_day_utc={payload.get('day_utc')!r} path={canonical_path}"
        )
    if str(payload.get("status") or "").strip().upper() != "OK":
        raise SystemExit(
            "FAIL: primary_capital_authority_allocation_not_ok "
            f"status={payload.get('status')!r} path={canonical_path}"
        )
    write_day_artifact_refreshable_v1(
        path=allocation_path,
        data=canonical_json_bytes_v1(payload) + b"\n",
        expected_day_utc=day_utc,
        expected_schema_id="C2_CAPITAL_AUTHORITY_ALLOCATION_V1",
        expected_schema_version=1,
        preserve_statuses=(),
    )
    return {
        "status": "INITIALIZED_FROM_CANONICAL",
        "reason_codes": [],
        "canonical_path": str(canonical_path),
        "path": str(allocation_path),
        "sha256": _sha256_file(allocation_path),
    }


def _refresh_primary_execution_authorization(
    *,
    day_utc: str,
    binding: GovernedSleeveTruthBinding,
) -> Dict[str, Any]:
    sleeve_truth_root = binding.truth_root.resolve()
    exposure_cmd = [
        sys.executable,
        str(EXPOSURE_NET_TOOL),
        "--day_utc",
        day_utc,
    ]
    exposure_result = _run(exposure_cmd, extra_env={"C2_TRUTH_ROOT": str(sleeve_truth_root)})
    exposure_path = _primary_exposure_net_path(binding=binding, day_utc=day_utc)
    exposure_ok = int(exposure_result.get("returncode") or 0) == 0 or _is_refuse_overwrite_existing_file(
        result=exposure_result,
        path=exposure_path,
    )
    if exposure_ok and int(exposure_result.get("returncode") or 0) != 0:
        exposure_result = {
            **exposure_result,
            "normalized_status": "EXISTS_CURRENT_NO_REWRITE",
            "normalized_returncode": 0,
        }

    allocation_cmd = [
        sys.executable,
        str(CAPITAL_AUTHORITY_ALLOCATION_TOOL),
        "--day_utc",
        day_utc,
        "--truth_root",
        str(sleeve_truth_root),
        "--canonical_sequence_owner",
        CANONICAL_SEQUENCE_OWNER,
    ]
    allocation_result = {
        "cmd": allocation_cmd,
        "returncode": 99,
        "stdout": "",
        "stderr": "SKIPPED_EXPOSURE_NET_FAILED",
    }
    allocation_path = _primary_capital_authority_allocation_path(binding=binding, day_utc=day_utc)
    if exposure_ok:
        initialization_result = _initialize_primary_capital_authority_allocation_from_canonical(
            day_utc=day_utc,
            binding=binding,
        )
        if str(initialization_result.get("status") or "").strip() == "INITIALIZED_FROM_CANONICAL":
            allocation_result = {
                "cmd": allocation_cmd,
                "returncode": 0,
                "stdout": (
                    "OK: PRIMARY_CAPITAL_AUTHORITY_ALLOCATION_INITIALIZED_FROM_CANONICAL "
                    f"path={initialization_result['path']} canonical_path={initialization_result['canonical_path']} "
                    f"sha256={initialization_result['sha256']}"
                ),
                "stderr": "",
            }
        else:
            allocation_result = _run(allocation_cmd)

    authorization_cmd = [
        sys.executable,
        str(AUTHORIZATION_ARTIFACTS_TOOL),
        "--day_utc",
        day_utc,
        "--truth_root",
        str(sleeve_truth_root),
    ]
    authorization_result = {
        "cmd": authorization_cmd,
        "returncode": 99,
        "stdout": "",
        "stderr": "SKIPPED_ALLOCATION_FAILED",
    }
    if int(allocation_result.get("returncode") or 0) == 0:
        authorization_result = _run(authorization_cmd)

    expected_paths = _expected_authorization_paths(binding=binding, day_utc=day_utc)
    missing_authorizations = [
        str(path) for path in expected_paths if not path.exists() or not path.is_file()
    ]
    reason_codes: List[str] = []
    if not exposure_ok:
        reason_codes.append("PRIMARY_EXPOSURE_NET_REFRESH_FAILED")
    if not exposure_path.exists() or not exposure_path.is_file():
        reason_codes.append("PRIMARY_EXPOSURE_NET_ARTIFACT_MISSING")
    if int(allocation_result.get("returncode") or 0) != 0:
        reason_codes.append("PRIMARY_CAPITAL_AUTHORITY_ALLOCATION_FAILED")
        nested_fail_code = _extract_fail_reason_code(allocation_result)
        if nested_fail_code:
            reason_codes.append(f"PRIMARY_CAPITAL_AUTHORITY_ALLOCATION_FAILED:{nested_fail_code}")
    if not allocation_path.exists() or not allocation_path.is_file():
        reason_codes.append("PRIMARY_CAPITAL_AUTHORITY_ALLOCATION_ARTIFACT_MISSING")
    if int(authorization_result.get("returncode") or 0) != 0:
        reason_codes.append("PRIMARY_ENGINE_ACTIVITY_AUTHORIZATION_REFRESH_FAILED")
    if missing_authorizations:
        reason_codes.append("PRIMARY_ENGINE_ACTIVITY_AUTHORIZATION_ARTIFACT_MISSING")

    return {
        "status": "OK" if not reason_codes else "ERROR",
        "reason_codes": sorted(set(reason_codes)),
        "exposure_net": exposure_result,
        "exposure_net_path": str(exposure_path),
        "allocation": allocation_result,
        "allocation_path": str(allocation_path),
        "authorization": authorization_result,
        "expected_authorization_paths": [str(path) for path in expected_paths],
        "missing_authorization_paths": sorted(set(missing_authorizations)),
        "constitutional_shadow_summary": _collect_authorization_shadow_summary(expected_paths=expected_paths),
    }

def _convergence_reason_codes(convergence_path: Path) -> List[str]:
    if not convergence_path.exists() or not convergence_path.is_file():
        return ["PRIMARY_STARTUP_AUTHORIZATION_CONVERGENCE_ARTIFACT_MISSING"]
    try:
        payload = _read_json(convergence_path)
    except Exception as exc:
        return [f"PRIMARY_STARTUP_AUTHORIZATION_CONVERGENCE_ARTIFACT_INVALID:{type(exc).__name__}"]
    reason_codes: List[str] = []
    for row in (payload.get("blocker_chain") or []):
        if not isinstance(row, dict):
            continue
        code = str(row.get("blocker_code") or row.get("artifact_id") or "").strip()
        if code:
            reason_codes.append(code)
    return sorted(set(reason_codes))


def _refresh_pointer_truth_root_for_authorization(
    *,
    day_utc: str,
    authorization_path: Path,
    pointer_truth_root: Path,
    git_sha: str,
    scope_label: str,
) -> Dict[str, Any]:
    cfg_hash = _sha256_file(PAPER_DAY_ORCHESTRATOR_SERVICE_PATH)
    policy_hash = _sha256_file(GATE_HIERARCHY_POLICY_PATH)
    attempt_result = _run(
        [
            sys.executable,
            str(POINTER_ATTEMPT_ALLOC_TOOL),
            '--day_utc',
            day_utc,
            '--mode',
            'PAPER',
            '--orchestrator_config_hash',
            cfg_hash,
            '--git_sha',
            git_sha,
            '--truth_root',
            str(pointer_truth_root),
        ]
    )
    if int(attempt_result.get('returncode') or 0) != 0:
        return {
            'status': 'ERROR',
            'reason_codes': [f'PRIMARY_{scope_label}_POINTER_ATTEMPT_ALLOC_FAILED'],
            'pointer_truth_root': str(pointer_truth_root),
            'details': attempt_result,
        }
    attempt_payload = _parse_json_stdout(attempt_result)
    attempt_id = str(attempt_payload.get('attempt_id') or '').strip()
    attempt_seq = int(attempt_payload.get('attempt_seq') or 0)
    if not attempt_id or attempt_seq <= 0:
        return {
            'status': 'ERROR',
            'reason_codes': [f'PRIMARY_{scope_label}_POINTER_ATTEMPT_ALLOC_INVALID'],
            'pointer_truth_root': str(pointer_truth_root),
            'details': attempt_result,
        }

    append_result = _run(
        [
            sys.executable,
            str(POINTER_APPEND_TOOL),
            '--day_utc',
            day_utc,
            '--attempt_id',
            attempt_id,
            '--attempt_seq',
            str(attempt_seq),
            '--mode',
            'PAPER',
            '--status',
            'PASS',
            '--authoritative',
            'YES',
            '--policy_hash',
            policy_hash,
            '--orchestrator_config_hash',
            cfg_hash,
            '--produced_utc',
            f'{day_utc}T00:00:00Z',
            '--points_to',
            str(authorization_path),
            '--git_sha',
            git_sha,
            '--truth_root',
            str(pointer_truth_root),
        ]
    )
    if int(append_result.get('returncode') or 0) != 0:
        return {
            'status': 'ERROR',
            'reason_codes': [f'PRIMARY_{scope_label}_POINTER_APPEND_FAILED'],
            'pointer_truth_root': str(pointer_truth_root),
            'details': append_result,
        }

    heads_result = _run(
        [
            sys.executable,
            str(POINTER_HEADS_MATERIALIZE_TOOL),
            '--fail_if_no_authority_head',
            'YES',
            '--truth_root',
            str(pointer_truth_root),
        ]
    )
    if int(heads_result.get('returncode') or 0) != 0:
        return {
            'status': 'ERROR',
            'reason_codes': [f'PRIMARY_{scope_label}_POINTER_HEADS_MATERIALIZE_FAILED'],
            'pointer_truth_root': str(pointer_truth_root),
            'details': heads_result,
        }

    return {
        'status': 'OK',
        'reason_codes': [],
        'pointer_truth_root': str(pointer_truth_root),
        'attempt': attempt_result,
        'append': append_result,
        'heads': heads_result,
    }


def _refresh_primary_authority_pointer(
    *,
    day_utc: str,
    binding: GovernedSleeveTruthBinding,
    git_sha: str,
) -> Dict[str, Any]:
    pointer_truth_root = GLOBAL_TRUTH_ROOT.resolve()
    authorization_path = _primary_authorization_path(binding=binding, day_utc=day_utc)
    if not authorization_path.exists() or not authorization_path.is_file():
        return {
            'status': 'ERROR',
            'reason_codes': ['PRIMARY_AUTHORIZATION_VERDICT_MISSING'],
            'authorization_path': str(authorization_path),
            'pointer_truth_root': str(pointer_truth_root),
        }
    authorization_payload = _read_json(authorization_path)
    authorization_status = str(authorization_payload.get('status') or '').strip().upper()
    if authorization_status not in {'PASS', 'BOOTSTRAP_PASS'}:
        return {
            'status': 'ERROR',
            "reason_codes": [f"PRIMARY_AUTHORIZATION_VERDICT_NOT_READY:{authorization_status or UNKNOWN}"],
            'authorization_path': str(authorization_path),
            'pointer_truth_root': str(pointer_truth_root),
        }

    scoped_pointer_truth_root = binding.truth_root.resolve()
    canonical_pointer_refresh = _refresh_pointer_truth_root_for_authorization(
        day_utc=day_utc,
        authorization_path=authorization_path,
        pointer_truth_root=pointer_truth_root,
        git_sha=git_sha,
        scope_label='CANONICAL',
    )
    scoped_pointer_refresh = _refresh_pointer_truth_root_for_authorization(
        day_utc=day_utc,
        authorization_path=authorization_path,
        pointer_truth_root=scoped_pointer_truth_root,
        git_sha=git_sha,
        scope_label='SCOPED',
    )
    reason_codes = sorted(
        set(
            [
                str(code).strip()
                for row in (canonical_pointer_refresh, scoped_pointer_refresh)
                for code in (row.get('reason_codes') or [])
                if str(code).strip()
            ]
        )
    )

    return {
        'status': 'OK' if not reason_codes else 'ERROR',
        'reason_codes': reason_codes,
        'authorization_path': str(authorization_path),
        'pointer_truth_root': str(pointer_truth_root),
        'scoped_pointer_truth_root': str(scoped_pointer_truth_root),
        'canonical_pointer_refresh': canonical_pointer_refresh,
        'scoped_pointer_refresh': scoped_pointer_refresh,
    }



def _run_primary_startup_authorization_refresh(
    *,
    day_utc: str,
    paper_account: str,
    binding: GovernedSleeveTruthBinding,
    git_sha: str,
) -> Dict[str, Any]:
    cmd = [
        sys.executable,
        str(STARTUP_AUTHORIZATION_CONVERGENCE_TOOL),
        "--day_utc",
        day_utc,
        "--truth_root",
        str(GLOBAL_TRUTH_ROOT),
        "--environment",
        "PAPER",
        "--ib_account",
        paper_account,
    ]
    result = _run(cmd)
    pointer_refresh = {"status": "SKIPPED", "reason_codes": []}
    execution_authorization_refresh = {"status": "SKIPPED", "reason_codes": []}
    if int(result.get("returncode") or 0) == 0:
        pointer_refresh = _refresh_primary_authority_pointer(
            day_utc=day_utc,
            binding=binding,
            git_sha=git_sha,
        )
        if pointer_refresh.get("status") != "ERROR":
            execution_authorization_refresh = _refresh_primary_execution_authorization(
                day_utc=day_utc,
                binding=binding,
            )
    reason_codes = _convergence_reason_codes(_primary_convergence_path(day_utc=day_utc))
    if pointer_refresh.get("status") == "ERROR":
        reason_codes.extend([str(code).strip() for code in (pointer_refresh.get("reason_codes") or []) if str(code).strip()])
    if execution_authorization_refresh.get("status") == "ERROR":
        reason_codes.extend([str(code).strip() for code in (execution_authorization_refresh.get("reason_codes") or []) if str(code).strip()])
    reason_codes = sorted(set(reason_codes))
    final_rc = 0 if int(result.get("returncode") or 0) == 0 and pointer_refresh.get("status") != "ERROR" and execution_authorization_refresh.get("status") != "ERROR" else 2
    stderr_parts = [str(result.get("stderr") or "").strip()]
    if pointer_refresh.get("status") == "ERROR":
        stderr_parts.append(json.dumps(pointer_refresh, sort_keys=True))
    if execution_authorization_refresh.get("status") == "ERROR":
        stderr_parts.append(json.dumps(execution_authorization_refresh, sort_keys=True))
    return {
        "cmd": cmd,
        "returncode": final_rc,
        "stdout": str(result.get("stdout") or "").strip(),
        "stderr": "\n".join(part for part in stderr_parts if part),
        "sleeve_id": str(binding.sleeve_id).strip().upper(),
        "environment": str(binding.environment).strip().upper(),
        "scope_truth_root": str(binding.truth_root.resolve()),
        "truth_partition": str(binding.truth_partition),
        "hard_failures": reason_codes,
        "nonblocking_override": False,
        "state": "READY" if final_rc == 0 else "BLOCKED",
        "blocking_scope": "NONE" if final_rc == 0 else "PRIMARY_SLEEVE",
        "reason_codes": reason_codes if final_rc != 0 else [],
        "convergence_artifact_path": str(_primary_convergence_path(day_utc=day_utc)),
        "pointer_refresh": pointer_refresh,
        "execution_authorization_refresh": execution_authorization_refresh,
    }


def _write_session_report(*, day_utc: str, results: Dict[str, Any], failures: List[str], truth_root: Path) -> tuple[Path, str]:
    produced_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    core_failure_prefixes = {
        "operator_future_day_override",
        "operator_statement",
        "positions_snapshot_refresh",
        "positions_snapshot_v5_refresh",
        "sleeve_positions_snapshot_seed",
        "cash_ledger_snapshot_refresh",
        "accounting_nav_refresh",
        "broker_events_bootstrap",
        "broker_event_day_manifest",
        "ib_api_handshake",
        "global_gate_refresh",
        "global_kill_switch_refresh",
        "trade_submit_readiness",
        "artifact_lifecycle_incidents",
        "scoped_gate_refresh:PRIMARY",
        "trading_day_state_machine",
    }
    monitoring_failure_prefixes = (
        "monitoring_refresh",
        "registry_coherence_check",
        "operator_artifact_freshness_summary",
        "pnl_attribution",
        "startup_proof_validation",
    )
    scope_summary = results.get("scope_summary") if isinstance(results.get("scope_summary"), dict) else {}
    nonblocking_blocked_sleeves = [
        str(item).strip().upper()
        for item in (scope_summary.get("nonblocking_blocked_sleeve_ids") or [])
        if str(item).strip()
    ]
    monitoring_failure = any(any(item.startswith(prefix) for prefix in monitoring_failure_prefixes) for item in failures) or bool(nonblocking_blocked_sleeves)
    core_failures = [item for item in failures if any(item == prefix or item.startswith(f"{prefix}:") for prefix in core_failure_prefixes)]
    ledger_result = results.get("paper_session_ledger")
    ledger_payload = _parse_json_stdout(ledger_result) if isinstance(ledger_result, dict) else {}
    ledger_authority_status = str(ledger_payload.get("authority_status") or "").strip().upper() or "UNKNOWN"
    if ledger_authority_status != "GRANTED":
        core_failures.append("paper_session_ledger_denied")
    status = "FAIL" if core_failures else ("OK_WITH_MONITORING_GAPS" if monitoring_failure else "OK")
    monitoring_failure_count = len([item for item in failures if any(item.startswith(prefix) for prefix in monitoring_failure_prefixes)]) + len(nonblocking_blocked_sleeves)
    report: Dict[str, Any] = {
        "schema_id": "C2_SESSION_READINESS_REFRESH_V1",
        "schema_version": 1,
        "day_utc": day_utc,
        "produced_utc": produced_utc,
        "producer": {
            "repo": REPO_ROOT.name,
            "git_sha": _git_sha(),
            "module": "ops/tools/run_session_readiness_refresh_v1.py",
        },
        "status": status,
        "overall_refresh_verdict": status,
        "core_failure_count": len(core_failures),
        "monitoring_failure_count": monitoring_failure_count,
        "failures": sorted(set(failures)),
        "results": {
            **results,
            "paper_session_ledger_gate": {
                "authority_scope": "DERIVED_ONLY_VIEW",
                "ledger_path": str(ledger_payload.get("path") or ""),
                "ledger_id": str(ledger_payload.get("ledger_id") or ""),
                "authority_status": ledger_authority_status,
            },
        },
        "canonical_json_hash": "",
    }
    report = _canonical_json_safe_value(report)
    report["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(report)
    validate_against_repo_schema_v1(report, REPO_ROOT, SESSION_REPORT_SCHEMA)
    out_path = resolve_session_readiness_refresh_path(truth_root=truth_root, day_utc=day_utc)
    write_day_artifact_refreshable_v1(
        path=out_path,
        data=canonical_json_bytes_v1(report) + b"\n",
        expected_day_utc=day_utc,
        expected_schema_id="C2_SESSION_READINESS_REFRESH_V1",
        expected_schema_version=1,
        preserve_statuses=(),
    )
    return out_path, status


def _canonical_json_safe_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _canonical_json_safe_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_canonical_json_safe_value(item) for item in value]
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("SESSION_READINESS_REFRESH_NONFINITE_FLOAT")
        return format(value, ".15g")
    return value


def _load_accounts() -> Dict[str, List[str]]:
    sleeve_reg = _read_json(SLEEVE_REGISTRY_PATH)
    sleeve_rows = sleeve_reg.get("sleeves")
    if not isinstance(sleeve_rows, list):
        raise SystemExit("FAIL: sleeve_registry_sleeves_not_list")
    active_accounts_by_env: Dict[str, set[str]] = {"PAPER": set(), "LIVE": set()}
    for row in sleeve_rows:
        if not isinstance(row, dict):
            continue
        if row.get("enabled") is not True:
            continue
        env = str(row.get("mode") or "").strip().upper()
        account_id = str(row.get("ib_account") or "").strip()
        if env in active_accounts_by_env and account_id:
            active_accounts_by_env[env].add(account_id)

    reg = _read_json(ACCOUNT_REGISTRY_PATH)
    rows = reg.get("accounts")
    if not isinstance(rows, list):
        raise SystemExit("FAIL: account_registry_accounts_not_list")
    out: Dict[str, List[str]] = {"PAPER": [], "LIVE": []}
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("enabled_for_submission") is not True:
            continue
        env = str(row.get("environment") or "").strip().upper()
        account_id = str(row.get("account_id") or "").strip()
        if env in out and account_id and account_id in active_accounts_by_env.get(env, set()):
            out[env].append(account_id)
    for env in out:
        out[env] = sorted(set(out[env]))
    return out


def _authority_lifecycle_result(day_utc: str, truth_root: Path) -> Dict[str, Any]:
    incidents = list_unresolved_authority_lifecycle_incidents(truth_root=truth_root, day_utc=day_utc)
    return {
        "status": "FAIL" if incidents else "OK",
        "incident_count": len(incidents),
        "incidents": summarize_lifecycle_incidents(incidents),
    }


def _write_operator_summary_safe(*, day_utc: str, truth_root: Path) -> Dict[str, Any]:
    try:
        ref = write_operator_summary(summary_kind="preopen", day_utc=day_utc, truth_root=truth_root)
        return {"status": "OK", "path": str(ref.path), "sha256": ref.sha256}
    except BaseException as exc:
        return {"status": "ERROR", "error": str(exc)}


def _skipped_missing_tool_result(*, cmd: List[str], tool_path: Path, status: str) -> Dict[str, Any]:
    return {
        "cmd": cmd,
        "returncode": 0,
        "stdout": "",
        "stderr": "",
        "status": status,
        "reason_codes": [f"OPTIONAL_TOOL_MISSING:{tool_path}"],
    }


def _run_build_context_fast_path(*, day_utc: str, truth_root: Path) -> int:
    decision_path = (
        truth_root
        / "reports"
        / "day_authority_decision_v1"
        / day_utc
        / "day_authority_decision.v1.json"
    ).resolve()
    decision_state_observed = ""
    reason_codes: List[str] = []
    if decision_path.exists() and decision_path.is_file():
        try:
            payload = _read_json(decision_path)
            decision_state_observed = str(payload.get("decision_state") or "").strip().upper()
        except Exception as exc:
            reason_codes.append(f"DAY_AUTHORITY_DECISION_INVALID:{type(exc).__name__}")
    else:
        reason_codes.append("DAY_AUTHORITY_DECISION_MISSING")
    decision_open = decision_state_observed in {"OPEN", "OPEN_SUBMIT_CAPABLE", "READY_NOW"}
    output = {
        "day_utc": day_utc,
        "truth_root": str(truth_root),
        "results": {
            "authority_kernel_validation": {
                "validation_summary": {
                    "validation_state": "PASS" if decision_open else "FAIL",
                }
            },
            "scope_summary": {
                "primary_ready": decision_open,
            },
            "day_authority_decision": {
                "decision_state": "OPEN" if decision_open else "BLOCKED",
                "observed_state": decision_state_observed,
                "path": str(decision_path),
            },
        },
        "failures": reason_codes,
        "status": "SESSION_READINESS_BUILD_FAST_PATH",
    }
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0 if decision_open else 2


def main() -> int:
    global GLOBAL_TRUTH_ROOT
    ap = argparse.ArgumentParser(prog="run_session_readiness_refresh_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    ap.add_argument("--build_fast_path", choices=["AUTO", "YES", "NO"], default="AUTO")
    args = ap.parse_args()

    if str(args.truth_root).strip():
        GLOBAL_TRUTH_ROOT = Path(str(args.truth_root)).resolve()

    day = str(args.day_utc).strip()
    build_fast_path_mode = str(args.build_fast_path or "AUTO").strip().upper()
    if build_fast_path_mode == "YES" or (
        build_fast_path_mode == "AUTO" and _invoked_by_session_authority_build()
    ):
        return _run_build_context_fast_path(day_utc=day, truth_root=GLOBAL_TRUTH_ROOT)

    produced_utc = f"{day}T00:00:00Z"
    paper_account = resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    accounts = _load_accounts()
    git_sha = _git_sha()
    baseline_b2_python = str(Path(sys.executable).resolve())
    operator_statement_path = resolve_operator_statement_path(
        operator_input_root=OPERATOR_INPUT_TRUTH_ROOT,
        day_utc=day,
    )

    results: Dict[str, Any] = {}
    failures: List[str] = []
    posture_cmd = [
        sys.executable,
        str(PAPER_TRADING_POSTURE_TOOL),
        "--day_utc",
        day,
        "--truth_root",
        str(GLOBAL_TRUTH_ROOT),
    ]
    results["paper_trading_posture"] = _run(posture_cmd)
    if results["paper_trading_posture"]["returncode"] != 0:
        failures.append("paper_trading_posture")
    expected_calendar_no_op = _paper_expected_calendar_no_op(day)
    results["operator_future_day_override"] = _maybe_write_future_day_override(day_utc=day)
    if results["operator_future_day_override"]["returncode"] != 0:
        failures.append("operator_future_day_override")

    operator_cmd = [
        str(OPERATOR_STATEMENT_PYTHON if OPERATOR_STATEMENT_PYTHON.exists() else _critical_path_python()),
        str(OPERATOR_STATEMENT_TOOL),
        "--day_utc",
        day,
        "--truth_root",
        str(OPERATOR_INPUT_TRUTH_ROOT),
        "--ib_account",
        paper_account,
        "--mode",
        "SEED_100K",
        "--allow_create",
        "YES",
    ]
    if "operator_statement" not in results:
        results["operator_statement"] = _run(operator_cmd)
        if results["operator_statement"]["returncode"] != 0:
            failures.append("operator_statement")

    intent_convergence_cmd = [
        sys.executable,
        str(PAPER_STARTUP_INTENT_INPUT_CONVERGENCE_TOOL),
        "--day_utc",
        day,
        "--truth_root",
        str(GLOBAL_TRUTH_ROOT),
        "--environment",
        "PAPER",
        "--ib_account",
        paper_account,
    ]
    results["paper_startup_intent_input_convergence"] = _run(intent_convergence_cmd)
    if results["paper_startup_intent_input_convergence"]["returncode"] != 0:
        failures.append("paper_startup_intent_input_convergence")

    intent_generation_cmd = [
        sys.executable,
        str(TRADING_DAY_INTENT_GENERATION_TOOL),
        "--day_utc",
        day,
        "--truth_root",
        str(GLOBAL_TRUTH_ROOT),
    ]
    results["trading_day_intent_generation"] = _run(intent_generation_cmd)
    if results["trading_day_intent_generation"]["returncode"] != 0:
        failures.append("trading_day_intent_generation")

    startup_materialization_input_convergence_cmd = [
        sys.executable,
        str(STARTUP_MATERIALIZATION_INPUT_CONVERGENCE_TOOL),
        "--day_utc",
        day,
        "--truth_root",
        str(GLOBAL_TRUTH_ROOT),
        "--ib_account",
        paper_account,
    ]
    results["startup_materialization_input_convergence"] = _run(startup_materialization_input_convergence_cmd)
    if results["startup_materialization_input_convergence"]["returncode"] != 0:
        failures.append("startup_materialization_input_convergence")

    startup_materialization_cmd = [
        sys.executable,
        str(STARTUP_MATERIALIZATION_TOOL),
        "--day_utc",
        day,
        "--truth_root",
        str(GLOBAL_TRUTH_ROOT),
    ]
    results["startup_materialization"] = _run(startup_materialization_cmd)
    if results["startup_materialization"]["returncode"] != 0:
        failures.append("startup_materialization")
        authority_result = run_day_authority_preflight_v1(
            truth_root=GLOBAL_TRUTH_ROOT,
            day_utc=day,
            startup_materialization_result=results["startup_materialization"],
            handshake_result=None,
            global_gate_result=None,
            scope_summary=None,
            scoped_gate_results=[],
            producer_module="ops/tools/run_session_readiness_refresh_v1.py",
            producer_git_sha=git_sha,
            global_gate_nonblocking=False,
        )
        results["authority_kernel_validation"] = authority_result
        report_path, report_status = _write_session_report(day_utc=day, results=results, failures=failures, truth_root=GLOBAL_TRUTH_ROOT)
        results["session_readiness_report"] = {"path": str(report_path)}
        results["day_authority_decision"] = _write_day_authority_decision_from_report(
            day_utc=day,
            truth_root=GLOBAL_TRUTH_ROOT,
            report_path=report_path,
            producer_git_sha=git_sha,
        )
        results["operator_summary"] = _write_operator_summary_safe(day_utc=day, truth_root=GLOBAL_TRUTH_ROOT)

    operator_cmd = [
        str(OPERATOR_STATEMENT_PYTHON if OPERATOR_STATEMENT_PYTHON.exists() else _critical_path_python()),
        str(OPERATOR_STATEMENT_TOOL),
        "--day_utc",
        day,
        "--truth_root",
        str(OPERATOR_INPUT_TRUTH_ROOT),
        "--ib_account",
        paper_account,
        "--mode",
        "SEED_100K",
        "--allow_create",
        "YES",
    ]
    results["operator_statement"] = _run(operator_cmd)
    if results["operator_statement"]["returncode"] != 0:
        failures.append("operator_statement")

    positions_refresh = _refresh_positions_snapshots(
        day_utc=day,
        producer_git_sha=git_sha,
        paper_account=paper_account,
        python_bin=baseline_b2_python,
    )
    results["positions_snapshot_v5_refresh"] = positions_refresh["v5"]
    if results["positions_snapshot_v5_refresh"]["returncode"] != 0:
        failures.append("positions_snapshot_v5_refresh")
    results["positions_snapshot_refresh"] = positions_refresh["v2"]
    if results["positions_snapshot_refresh"]["returncode"] != 0:
        failures.append("positions_snapshot_refresh")

    cash_ledger_snapshot_cmd = [
        baseline_b2_python,
        "-m",
        "constellation_2.phaseF.cash_ledger.run.run_cash_ledger_snapshot_day_v1",
        "--day_utc",
        day,
        "--operator_statement_json",
        str(operator_statement_path),
        "--producer_repo",
        REPO_ROOT.name,
        "--producer_git_sha",
        git_sha,
    ]
    results["cash_ledger_snapshot_refresh"] = _run(cash_ledger_snapshot_cmd)
    if results["cash_ledger_snapshot_refresh"]["returncode"] != 0:
        failures.append("cash_ledger_snapshot_refresh")

    accounting_nav_cmd = [
        baseline_b2_python,
        str(ACCOUNTING_NAV_TOOL),
        "--day_utc",
        day,
        "--producer_repo",
        REPO_ROOT.name,
        "--producer_git_sha",
        git_sha,
    ]
    results["accounting_nav_refresh"] = _run(accounting_nav_cmd)
    if results["accounting_nav_refresh"]["returncode"] != 0:
        failures.append("accounting_nav_refresh")

    try:
        results["sleeve_positions_snapshot_seed"] = _seed_sleeve_positions_snapshots(day_utc=day, paper_account=paper_account)
    except BaseException as exc:
        results["sleeve_positions_snapshot_seed"] = {"status": "ERROR", "error": str(exc)}
        failures.append("sleeve_positions_snapshot_seed")

    execution_profile = resolve_governed_paper_execution_profile(
        repo_root=REPO_ROOT,
        environment="PAPER",
        ib_account=paper_account,
        sleeve_id=PRIMARY_SLEEVE_ID,
    )
    execution_roots = resolve_governed_paper_execution_roots(
        repo_root=REPO_ROOT,
        environment="PAPER",
        ib_account=paper_account,
        sleeve_id=PRIMARY_SLEEVE_ID,
    )
    execution_truth_root = execution_roots.execution_root_path.resolve()

    broker_events_cmd = [
        str(_resolve_broker_events_bootstrap_python()),
        str(BROKER_EVENTS_BOOTSTRAP_TOOL),
        "--truth_root",
        str(execution_truth_root),
        "--host",
        execution_profile.host,
        "--port",
        str(execution_profile.port),
        "--client-id",
        str(execution_profile.client_id_observer),
        "--poll-seconds",
        "10",
        "--environment",
        "PAPER",
        "--sleeve-id",
        PRIMARY_SLEEVE_ID,
        "--day-utc",
        day,
        "--bootstrap-handshake-only",
        "--handshake-timeout-seconds",
        "15",
    ]
    observer_service_status = _active_execution_observer_service_status(
        execution_truth_root=execution_truth_root,
        execution_profile=execution_profile,
        sleeve_id=PRIMARY_SLEEVE_ID,
    )
    results["execution_observer_service"] = observer_service_status
    if bool(observer_service_status.get("active")):
        results["broker_events_bootstrap"] = {
            "cmd": broker_events_cmd,
            "returncode": 0,
            "stdout": (
                "SKIPPED: ACTIVE_CANONICAL_OBSERVER_SERVICE "
                f"service={observer_service_status.get('service_name', '')} "
                f"pid={observer_service_status.get('main_pid', 0)}"
            ),
            "stderr": "",
            "reason_codes": [],
            "status": "SKIPPED_ACTIVE_CANONICAL_OBSERVER_SERVICE",
        }
    else:
        broker_events_cmd.append("--disable-legacy-log")
        results["broker_events_bootstrap"] = _run(broker_events_cmd)
        if results["broker_events_bootstrap"]["returncode"] != 0:
            results["broker_events_bootstrap"]["reason_codes"] = _broker_bootstrap_reason_codes(results["broker_events_bootstrap"])
            failures.append("broker_events_bootstrap")
        else:
            results["broker_events_bootstrap"]["reason_codes"] = []

    broker_events_manifest_cmd = [
        sys.executable,
        str(BROKER_EVENTS_MANIFEST_TOOL),
        "--day_utc",
        day,
        "--truth_root",
        str(execution_truth_root),
    ]
    results["broker_event_day_manifest"] = _run(
        broker_events_manifest_cmd,
        timeout_seconds=BROKER_EVENT_DAY_MANIFEST_TIMEOUT_SECONDS,
    )
    if results["broker_event_day_manifest"]["returncode"] != 0:
        failures.append("broker_event_day_manifest")

    handshake_cmd = [
        sys.executable,
        str(HANDSHAKE_TOOL),
        "--day_utc",
        day,
        "--truth_root",
        str(execution_truth_root),
    ]
    results["ib_api_handshake"] = _run(handshake_cmd)
    if results["ib_api_handshake"]["returncode"] != 0:
        failures.append("ib_api_handshake")

    global_gate_refresh_cmd = [
        sys.executable,
        str(GLOBAL_GATE_REFRESH_TOOL),
        "--day_utc",
        day,
        "--truth_root",
        str(GLOBAL_TRUTH_ROOT),
        "--produced_utc",
        produced_utc,
        "--mode",
        "PAPER",
    ]
    results["global_gate_refresh"] = _run(global_gate_refresh_cmd)
    results["global_gate_refresh"]["truth_lifecycle_run_ledger_path"] = _extract_truth_lifecycle_run_ledger_path(
        results["global_gate_refresh"]
    )
    if results["global_gate_refresh"]["returncode"] != 0 and not _paper_global_gate_refresh_nonblocking(
        results["global_gate_refresh"],
        expected_calendar_no_op=expected_calendar_no_op,
    ):
        failures.append("global_gate_refresh")

    global_kill_switch_cmd = [
        sys.executable,
        str(GLOBAL_KILL_SWITCH_TOOL),
        "--day_utc",
        day,
    ]
    results["global_kill_switch_refresh"] = _run(global_kill_switch_cmd)
    if results["global_kill_switch_refresh"]["returncode"] != 0:
        failures.append("global_kill_switch_refresh")

    scoped_gate_refresh_results: List[Dict[str, Any]] = []
    try:
        scoped_bindings = _resolve_paper_sleeve_truth_bindings(paper_account=paper_account)
    except ValueError as exc:
        scoped_gate_refresh_results.append(
            {
                "scope": "PAPER_SLEEVE_BINDINGS",
                "returncode": 2,
                "error": str(exc),
                "state": "BLOCKED",
                "blocking_scope": "GLOBAL",
                "reason_codes": [str(exc)],
            }
        )
        failures.append("scoped_gate_refresh")
        scoped_bindings = []

    for binding in scoped_bindings:
        if str(binding.sleeve_id).strip().upper() == PRIMARY_SLEEVE_ID:
            annotated = _run_primary_startup_authorization_refresh(
                day_utc=day,
                paper_account=paper_account,
                binding=binding,
                git_sha=git_sha,
            )
        else:
            cmd = [
                sys.executable,
                str(GLOBAL_GATE_REFRESH_TOOL),
                "--day_utc",
                day,
                "--truth_root",
                str(binding.truth_root.resolve()),
                "--mode",
                "PAPER",
            ]
            annotated = _annotate_scoped_gate_refresh_result(
                binding=binding,
                result=_run(cmd),
                expected_calendar_no_op=expected_calendar_no_op,
            )
        scoped_gate_refresh_results.append(annotated)
        if (
            int(annotated.get("returncode") or 0) != 0
            and str(annotated.get("sleeve_id") or "") == PRIMARY_SLEEVE_ID
            and not bool(annotated.get("nonblocking_override") is True)
        ):
            failures.append(f"scoped_gate_refresh:PRIMARY:{annotated['scope_truth_root']}")
    results["scoped_gate_refresh"] = scoped_gate_refresh_results
    results["scope_summary"] = _scope_summary_from_results(scoped_gate_refresh_results)

    authority_result = run_day_authority_preflight_v1(
        truth_root=GLOBAL_TRUTH_ROOT,
        day_utc=day,
        startup_materialization_result=results.get("startup_materialization"),
        handshake_result=results.get("ib_api_handshake"),
        global_gate_result=results.get("global_gate_refresh"),
        scope_summary=results.get("scope_summary"),
        scoped_gate_results=scoped_gate_refresh_results,
        producer_module="ops/tools/run_session_readiness_refresh_v1.py",
        producer_git_sha=git_sha,
        global_gate_nonblocking=_paper_global_gate_refresh_nonblocking(
            results["global_gate_refresh"],
            expected_calendar_no_op=expected_calendar_no_op,
        ),
    )
    results["authority_kernel_validation"] = authority_result

    in_session_authority_build_context = _invoked_by_session_authority_build()
    if in_session_authority_build_context:
        validation_summary = (
            authority_result.get("validation_summary")
            if isinstance(authority_result.get("validation_summary"), dict)
            else {}
        )
        validation_state = str(validation_summary.get("validation_state") or "").strip().upper()
        primary_ready = bool((results.get("scope_summary") or {}).get("primary_ready") is True)
        day_authority_decision_state = "OPEN" if validation_state == "PASS" and primary_ready else "BLOCKED"
        fast_payload = {
            "day_utc": day,
            "truth_root": str(GLOBAL_TRUTH_ROOT),
            "results": {
                "authority_kernel_validation": authority_result,
                "scope_summary": results.get("scope_summary") if isinstance(results.get("scope_summary"), dict) else {},
                "day_authority_decision": {
                    "decision_state": day_authority_decision_state,
                    "source": "SESSION_READINESS_BUILD_FAST_PATH",
                },
            },
            "failures": list(failures),
            "status": "SESSION_READINESS_BUILD_FAST_PATH",
        }
        print(json.dumps(fast_payload, indent=2, sort_keys=True))
        return 0 if day_authority_decision_state == "OPEN" else 2

    # Seed readiness for upstream consumers first, then rerun after canonical
    # build/admission refresh because that path rewrites day_authority_decision_v1.
    if in_session_authority_build_context:
        skip_reason = "SKIPPED_REENTRY_CALLER_SESSION_AUTHORITY_BUILD"
        results["trade_submit_readiness"] = [
            {
                "cmd": [],
                "returncode": 0,
                "stdout": skip_reason,
                "stderr": "",
                "status": skip_reason,
            }
        ]
    else:
        readiness_results_prebuild = _run_trade_submit_readiness_refreshes(day_utc=day, accounts=accounts)
        results["trade_submit_readiness_prebuild"] = readiness_results_prebuild
        for result in readiness_results_prebuild:
            if result["returncode"] != 0:
                failures.append(f"trade_submit_readiness_prebuild:{result['environment']}:{result['ib_account']}")
    if in_session_authority_build_context:
        skip_reason = "SKIPPED_REENTRY_CALLER_SESSION_AUTHORITY_BUILD"
        results["target_day_build_refresh"] = {
            "cmd": [],
            "returncode": 0,
            "stdout": skip_reason,
            "stderr": "",
            "status": skip_reason,
        }
        results["target_day_admission_refresh"] = {
            "cmd": [],
            "returncode": 0,
            "stdout": skip_reason,
            "stderr": "",
            "status": skip_reason,
        }
        results["active_session_refresh"] = {
            "cmd": [],
            "returncode": 0,
            "stdout": skip_reason,
            "stderr": "",
            "status": skip_reason,
        }
    else:
        session_authority_build_cmd = [
            sys.executable,
            str(SESSION_AUTHORITY_TOOL),
            "--target_day",
            day,
            "--truth_root",
            str(GLOBAL_TRUTH_ROOT),
            "--environment",
            "PAPER",
            "--ib_account",
            paper_account,
            "--phase",
            "build",
        ]
        results["target_day_build_refresh"] = _run(
            session_authority_build_cmd,
            extra_env=_monitoring_refresh_extra_env(),
        )
        if results["target_day_build_refresh"]["returncode"] != 0:
            failures.append("target_day_build_refresh")
        session_authority_admit_cmd = [
            sys.executable,
            str(SESSION_AUTHORITY_TOOL),
            "--target_day",
            day,
            "--truth_root",
            str(GLOBAL_TRUTH_ROOT),
            "--environment",
            "PAPER",
            "--ib_account",
            paper_account,
            "--phase",
            "admit",
        ]
        results["target_day_admission_refresh"] = _run(
            session_authority_admit_cmd,
            extra_env=_monitoring_refresh_extra_env(),
        )
        if results["target_day_admission_refresh"]["returncode"] != 0:
            failures.append("target_day_admission_refresh")
        session_authority_activate_cmd = [
            sys.executable,
            str(SESSION_AUTHORITY_TOOL),
            "--target_day",
            day,
            "--truth_root",
            str(GLOBAL_TRUTH_ROOT),
            "--environment",
            "PAPER",
            "--ib_account",
            paper_account,
            "--phase",
            "activate",
        ]
        results["active_session_refresh"] = _run(
            session_authority_activate_cmd,
            extra_env=_monitoring_refresh_extra_env(),
        )
        if results["active_session_refresh"]["returncode"] != 0:
            failures.append("active_session_refresh")
    if not in_session_authority_build_context:
        readiness_results = _run_trade_submit_readiness_refreshes(day_utc=day, accounts=accounts)
        for result in readiness_results:
            if result["returncode"] != 0:
                failures.append(f"trade_submit_readiness:{result['environment']}:{result['ib_account']}")
        results["trade_submit_readiness"] = readiness_results
    else:
        readiness_results = list(results.get("trade_submit_readiness") or [])
    submit_boundary_cmd = [
        sys.executable,
        str(SUBMIT_BOUNDARY_STATUS_TOOL),
        "--day_utc",
        day,
        "--truth_root",
        str(GLOBAL_TRUTH_ROOT),
    ]
    results["submit_boundary_status"] = _run(submit_boundary_cmd)
    if results["submit_boundary_status"]["returncode"] != 0:
        failures.append("submit_boundary_status")
    ledger_cmd = [
        sys.executable,
        str(PAPER_SESSION_LEDGER_TOOL),
        "--day_utc",
        day,
        "--truth_root",
        str(GLOBAL_TRUTH_ROOT),
    ]
    results["paper_session_ledger"] = _run(ledger_cmd)
    if results["paper_session_ledger"]["returncode"] != 0:
        failures.append("paper_session_ledger")

    results["artifact_lifecycle_incidents"] = _authority_lifecycle_result(day, GLOBAL_TRUTH_ROOT)
    if results["artifact_lifecycle_incidents"]["status"] != "OK":
        failures.append("artifact_lifecycle_incidents")

    if results["ib_api_handshake"]["returncode"] == 0 and all(r["returncode"] == 0 for r in readiness_results):
        _ensure_execution_reconciliation_day_roots(day)
        execution_reconciliation_cmd = [sys.executable, str(EXECUTION_RECONCILIATION_TOOL), "--day_utc", day]
        results["execution_reconciliation"] = _run(execution_reconciliation_cmd)
        if results["execution_reconciliation"]["returncode"] != 0:
            failures.append("execution_reconciliation")
    else:
        results["execution_reconciliation"] = {
            "cmd": [sys.executable, str(EXECUTION_RECONCILIATION_TOOL), "--day_utc", day],
            "returncode": 0,
            "stdout": "",
            "stderr": "",
            "status": "SKIPPED_READINESS_NOT_GREEN",
        }

    nav_path = (GLOBAL_TRUTH_ROOT / "accounting_v2" / "nav" / day / "nav.v2.json").resolve()
    pnl_cmd = [sys.executable, str(PNL_ATTRIBUTION_TOOL), "--day_utc", day]
    if not PNL_ATTRIBUTION_TOOL.exists():
        results["pnl_attribution"] = _skipped_missing_tool_result(
            cmd=pnl_cmd,
            tool_path=PNL_ATTRIBUTION_TOOL,
            status="SKIPPED_OPTIONAL_TOOL_MISSING",
        )
    elif nav_path.exists():
        results["pnl_attribution"] = _run(pnl_cmd)
        if results["pnl_attribution"]["returncode"] != 0:
            failures.append("pnl_attribution")
    else:
        results["pnl_attribution"] = {
            "cmd": pnl_cmd,
            "returncode": 0,
            "stdout": "",
            "stderr": "",
            "status": "PENDING_NAV_NOT_AVAILABLE",
            "nav_path": str(nav_path),
        }

    monitoring_refresh_cmd = [sys.executable, str(MONITORING_REFRESH_TOOL), "--day_utc", day, "--truth_root", str(GLOBAL_TRUTH_ROOT)]
    results["monitoring_refresh"] = _run(
        monitoring_refresh_cmd,
        extra_env=_monitoring_refresh_extra_env(),
    )
    if results["monitoring_refresh"]["returncode"] != 0:
        failures.append("monitoring_refresh")

    registry_coherence_cmd = [sys.executable, str(REGISTRY_COHERENCE_TOOL), "--day_utc", day, "--truth_root", str(GLOBAL_TRUTH_ROOT)]
    if REGISTRY_COHERENCE_TOOL.exists():
        results["registry_coherence_check"] = _run(registry_coherence_cmd)
        if results["registry_coherence_check"]["returncode"] != 0:
            failures.append("registry_coherence_check")
    else:
        results["registry_coherence_check"] = _skipped_missing_tool_result(
            cmd=registry_coherence_cmd,
            tool_path=REGISTRY_COHERENCE_TOOL,
            status="SKIPPED_OPTIONAL_TOOL_MISSING",
        )

    artifact_freshness_cmd = [sys.executable, str(ARTIFACT_FRESHNESS_SUMMARY_TOOL), "--day_utc", day, "--truth_root", str(GLOBAL_TRUTH_ROOT)]
    if ARTIFACT_FRESHNESS_SUMMARY_TOOL.exists():
        results["operator_artifact_freshness_summary"] = _run(artifact_freshness_cmd)
        if results["operator_artifact_freshness_summary"]["returncode"] != 0:
            failures.append("operator_artifact_freshness_summary")
    else:
        results["operator_artifact_freshness_summary"] = _skipped_missing_tool_result(
            cmd=artifact_freshness_cmd,
            tool_path=ARTIFACT_FRESHNESS_SUMMARY_TOOL,
            status="SKIPPED_OPTIONAL_TOOL_MISSING",
        )

    report_path, report_status = _write_session_report(
        day_utc=day,
        results=results,
        failures=failures,
        truth_root=GLOBAL_TRUTH_ROOT,
    )
    results["session_readiness_report"] = {"path": str(report_path)}

    startup_proof_cmd = [sys.executable, str(STARTUP_PROOF_VALIDATION_TOOL), "--day_utc", day, "--truth_root", str(GLOBAL_TRUTH_ROOT)]
    results["startup_proof_validation"] = _run(startup_proof_cmd)
    if results["startup_proof_validation"]["returncode"] != 0:
        failures.append("startup_proof_validation")
    trading_day_state_machine_cmd = [
        sys.executable,
        str(TRADING_DAY_STATE_MACHINE_TOOL),
        "--day_utc",
        day,
        "--truth_root",
        str(GLOBAL_TRUTH_ROOT),
    ]
    results["trading_day_state_machine"] = _run(trading_day_state_machine_cmd)
    if results["trading_day_state_machine"]["returncode"] != 0:
        failures.append("trading_day_state_machine")

    report_path, report_status = _write_session_report(day_utc=day, results=results, failures=failures, truth_root=GLOBAL_TRUTH_ROOT)
    results["session_readiness_report"] = {"path": str(report_path)}
    results["day_authority_decision"] = _write_day_authority_decision_from_report(
        day_utc=day,
        truth_root=GLOBAL_TRUTH_ROOT,
        report_path=report_path,
        producer_git_sha=git_sha,
    )
    results["operator_summary"] = _write_operator_summary_safe(day_utc=day, truth_root=GLOBAL_TRUTH_ROOT)

    print(
        json.dumps(
            {
                "day_utc": day,
                "truth_root": str(GLOBAL_TRUTH_ROOT),
                "results": results,
                "failures": failures,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report_status in {"OK", "OK_WITH_MONITORING_GAPS"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
