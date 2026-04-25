#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping

from constellation_2.common.next_day_readiness_probe_v1 import resolve_next_day_readiness_probe_path
from constellation_2.common.paper_session_fact_plane_v1 import read_validated_surface_v1, sha256_file_v1
from constellation_2.common.paper_session_path_alignment_v1 import resolve_deployment_state_machine_path
from constellation_2.common.session_authority_v1 import (
    CLOSURE_STATUS_CLOSED,
    SessionAuthorityRefV1,
    derive_active_session_payload_v1,
    derive_target_day_admission_payload_v1,
    read_active_session_ref_v1,
    read_target_day_admission_ref_v1,
    read_target_day_build_ref_v1,
    write_active_session_v1,
    write_target_day_admission_v1,
)
from constellation_2.common.session_promotion_gate_v1 import (
    PROMOTION_STATE_PROMOTED,
    derive_session_promotion_decision_payload_v1,
    read_session_promotion_decision_ref_v1,
    write_session_promotion_decision_v1,
)
from constellation_2.common.trade_submit_readiness_authority_v1 import resolve_governed_sleeve_truth_bindings
from ops.tools.run_session_authority_control_plane_v1 import read_optional_pre_open_bundle_surface_v1


ACCOUNTING_NAV_V2_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/ACCOUNTING/accounting_nav.v2.schema.json"
ALLOCATION_SUMMARY_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/ALLOCATION/allocation_summary.v1.schema.json"
RECONCILIATION_REPORT_V3_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/reconciliation_report.v3.schema.json"
EXIT_RECONCILIATION_V1_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/EXIT_OBLIGATIONS/exit_reconciliation.v1.schema.json"
CAPITAL_RISK_ENVELOPE_V2_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/capital_risk_envelope.v2.schema.json"
LIQUIDITY_SLIPPAGE_GATE_V1_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/liquidity_slippage_gate.v1.schema.json"
OPERATOR_DAILY_GATE_V3_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/operator_daily_gate.v3.schema.json"
GATE_STACK_VERDICT_V1_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/gate_stack_verdict.v1.schema.json"
LIFECYCLE_STATE_AUTHORITY_V1_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/lifecycle_state_authority.v1.schema.json"
)
AUTHORIZATION_GATE_VERDICT_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/authorization_gate_verdict.v1.schema.json"
)
ECONOMIC_HEALTH_GATE_VERDICT_V1_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/economic_health_gate_verdict.v1.schema.json"
)
GATE_DECISION_LEDGER_V1_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/gate_decision_ledger.v1.schema.json"
BROKER_FACT_SPINE_AUDIT_V1_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/broker_fact_spine_audit.v1.schema.json"
)

SESSION_AUTHORITY_TOOL_TIMEOUT_ENV = "C2_SESSION_AUTHORITY_TOOL_TIMEOUT_SECONDS"
SESSION_AUTHORITY_TOOL_TIMEOUT_DEFAULT_SECONDS = 15.0
SESSION_AUTHORITY_TOOL_MIN_TIMEOUT_SECONDS_BY_SCRIPT: Dict[str, float] = {
    "ops/tools/run_broker_fact_spine_v1.py": 45.0,
}


def _extract_payload_day(payload: Mapping[str, Any]) -> str:
    for field in ("target_day", "target_day_utc", "day_utc", "trading_day", "session_date"):
        value = str(payload.get(field) or "").strip()
        if value:
            return value
    return ""


def _resolve_primary_binding(*, repo_root: Path, environment: str, ib_account: str):
    bindings = resolve_governed_sleeve_truth_bindings(
        repo_root=repo_root,
        environment=environment,
        requested_ib_account=ib_account,
    )
    for binding in bindings:
        if str(binding.sleeve_id).strip().upper() == "PRIMARY":
            return binding
    return bindings[0]


def build_tool_command_v1(repo_root: Path, script_relpath: str, *args: str) -> List[str]:
    return [sys.executable, str((repo_root / script_relpath).resolve()), *args]


def _parse_tool_stdout_json(stdout: str) -> Dict[str, Any]:
    text = str(stdout or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _session_readiness_refresh_startup_ready(result: Mapping[str, Any]) -> bool:
    payload = _parse_tool_stdout_json(str(result.get("stdout") or ""))
    results = payload.get("results") if isinstance(payload.get("results"), dict) else {}
    authority_validation = (
        results.get("authority_kernel_validation", {}).get("validation_summary", {})
        if isinstance(results.get("authority_kernel_validation"), dict)
        else {}
    )
    scope_summary = results.get("scope_summary") if isinstance(results.get("scope_summary"), dict) else {}
    day_authority_decision = (
        results.get("day_authority_decision") if isinstance(results.get("day_authority_decision"), dict) else {}
    )
    return (
        str(authority_validation.get("validation_state") or "").strip().upper() == "PASS"
        and bool(scope_summary.get("primary_ready") is True)
        and str(day_authority_decision.get("decision_state") or "").strip().upper() == "OPEN"
    )


def _annotate_source_ref_for_closure(
    *,
    result: Dict[str, Any],
    downstream_build_cycle_scripts: Iterable[str],
) -> Dict[str, Any]:
    annotated = dict(result)
    downstream = set(str(item).strip() for item in downstream_build_cycle_scripts)
    annotated["required_for_closure"] = True
    script = str(result.get("script") or "").strip()
    if script in downstream:
        annotated["required_for_closure"] = False
        annotated["nonblocking_reason"] = "DOWNSTREAM_BUILD_CYCLE_CONSUMER"
        return annotated
    if script != "ops/tools/run_session_readiness_refresh_v1.py":
        return annotated
    startup_ready = _session_readiness_refresh_startup_ready(result)
    annotated["startup_authority_ready"] = startup_ready
    if startup_ready:
        annotated["required_for_closure"] = False
        annotated["nonblocking_reason"] = "STARTUP_AUTHORITY_READY_MONITORING_GAPS_ALLOWED"
    return annotated


def source_ref_blocks_build_v1(*, ref: Mapping[str, Any], downstream_build_cycle_scripts: Iterable[str]) -> bool:
    if not isinstance(ref, Mapping):
        return False
    script = str(ref.get("script") or "").strip()
    downstream = set(str(item).strip() for item in downstream_build_cycle_scripts)
    if script in {"", "source_ref"}:
        return False
    if script in downstream:
        return False
    if bool(ref.get("required_for_closure") is False):
        return False
    return int(ref.get("return_code") or 0) != 0


def run_tool_v1(
    *,
    repo_root: Path,
    script_relpath: str,
    downstream_build_cycle_scripts: Iterable[str],
    args: Iterable[str] = (),
) -> Dict[str, Any]:
    cmd = build_tool_command_v1(repo_root, script_relpath, *tuple(args))
    timeout_seconds = _resolve_timeout_seconds_for_script(script_relpath)
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
        return_code = int(proc.returncode)
        stdout = proc.stdout.strip()
        stderr = proc.stderr.strip()
        timed_out = False
    except subprocess.TimeoutExpired as exc:
        return_code = 124
        stdout = str(exc.stdout or "").strip()
        timeout_detail = (
            f"SESSION_AUTHORITY_TOOL_TIMEOUT:"
            f"script={script_relpath}:timeout_seconds={_format_timeout_seconds(timeout_seconds)}"
        )
        timeout_stderr = str(exc.stderr or "").strip()
        stderr = f"{timeout_stderr}\n{timeout_detail}".strip() if timeout_stderr else timeout_detail
        timed_out = True
    return _annotate_source_ref_for_closure(
        result={
            "script": script_relpath,
            "command": cmd,
            "return_code": return_code,
            "stdout": stdout,
            "stderr": stderr,
            "timed_out": timed_out,
            "timeout_seconds": _format_timeout_seconds(timeout_seconds),
        },
        downstream_build_cycle_scripts=downstream_build_cycle_scripts,
    )


def _error_detail_from_process(*, return_code: int, stdout: str, stderr: str) -> str:
    if int(return_code) == 0:
        return ""
    for text in (str(stderr or "").strip(), str(stdout or "").strip()):
        if text:
            return text
    return f"return_code={int(return_code)}"


def _nested_producer_artifact_specs(*, sleeve_truth_root: Path, day_utc: str) -> Dict[str, List[Dict[str, str]]]:
    return {
        "ops/tools/run_accounting_nav_v2_day_v1.py": [
            {
                "artifact_id": "accounting_nav_v2",
                "artifact_path": str((sleeve_truth_root / "accounting_v2" / "nav" / day_utc / "nav.v2.json").resolve()),
                "schema_ref": ACCOUNTING_NAV_V2_SCHEMA_RELPATH,
            }
        ],
        "constellation_2/phaseG/allocation/run/run_allocation_day_v2.py": [
            {
                "artifact_id": "allocation_summary_v1",
                "artifact_path": str((sleeve_truth_root / "allocation_v1" / "summary" / day_utc / "summary.json").resolve()),
                "schema_ref": ALLOCATION_SUMMARY_SCHEMA_RELPATH,
            }
        ],
        "ops/tools/run_broker_fact_spine_v1.py": [
            {
                "artifact_id": "broker_fact_spine_audit_v1",
                "artifact_path": str(
                    (sleeve_truth_root / "reports" / "broker_fact_spine_audit_v1" / day_utc / "broker_fact_spine_audit.v1.json").resolve()
                ),
                "schema_ref": BROKER_FACT_SPINE_AUDIT_V1_SCHEMA_RELPATH,
            }
        ],
        "ops/tools/run_reconciliation_report_v3.py": [
            {
                "artifact_id": "reconciliation_report_v3",
                "artifact_path": str(
                    (sleeve_truth_root / "reports" / "reconciliation_report_v3" / day_utc / "reconciliation_report.v3.json").resolve()
                ),
                "schema_ref": RECONCILIATION_REPORT_V3_SCHEMA_RELPATH,
            }
        ],
        "constellation_2/phaseI/exit_reconciliation/run/run_exit_reconciliation_day_v1.py": [
            {
                "artifact_id": "exit_reconciliation_v1",
                "artifact_path": str(
                    (sleeve_truth_root / "exit_reconciliation_v1" / day_utc / "exit_reconciliation.v1.json").resolve()
                ),
                "schema_ref": EXIT_RECONCILIATION_V1_SCHEMA_RELPATH,
            }
        ],
        "ops/tools/run_c2_capital_risk_envelope_gate_v2.py": [
            {
                "artifact_id": "capital_risk_envelope_v2",
                "artifact_path": str(
                    (sleeve_truth_root / "reports" / "capital_risk_envelope_v2" / day_utc / "capital_risk_envelope.v2.json").resolve()
                ),
                "schema_ref": CAPITAL_RISK_ENVELOPE_V2_SCHEMA_RELPATH,
            }
        ],
        "ops/tools/run_liquidity_slippage_gate_v1.py": [
            {
                "artifact_id": "liquidity_slippage_gate_v1",
                "artifact_path": str(
                    (sleeve_truth_root / "reports" / "liquidity_slippage_gate_v1" / day_utc / "liquidity_slippage_gate.v1.json").resolve()
                ),
                "schema_ref": LIQUIDITY_SLIPPAGE_GATE_V1_SCHEMA_RELPATH,
            }
        ],
        "ops/tools/run_operator_daily_gate_v3.py": [
            {
                "artifact_id": "operator_daily_gate_v3",
                "artifact_path": str(
                    (sleeve_truth_root / "reports" / "operator_daily_gate_v3" / day_utc / "operator_daily_gate.v3.json").resolve()
                ),
                "schema_ref": OPERATOR_DAILY_GATE_V3_SCHEMA_RELPATH,
            }
        ],
        "ops/tools/run_gate_stack_verdict_v1.py": [
            {
                "artifact_id": "gate_stack_verdict_v1",
                "artifact_path": str(
                    (sleeve_truth_root / "reports" / "gate_stack_verdict_v1" / day_utc / "gate_stack_verdict.v1.json").resolve()
                ),
                "schema_ref": GATE_STACK_VERDICT_V1_SCHEMA_RELPATH,
            }
        ],
        "ops/tools/run_gate_authority_plane_v1.py": [
            {
                "artifact_id": "lifecycle_state_authority_v1",
                "artifact_path": str(
                    (sleeve_truth_root / "reports" / "lifecycle_state_authority_v1" / day_utc / "lifecycle_state_authority.v1.json").resolve()
                ),
                "schema_ref": LIFECYCLE_STATE_AUTHORITY_V1_SCHEMA_RELPATH,
            },
            {
                "artifact_id": "authorization_gate_verdict_v1",
                "artifact_path": str(
                    (sleeve_truth_root / "reports" / "authorization_gate_verdict_v1" / day_utc / "authorization_gate_verdict.v1.json").resolve()
                ),
                "schema_ref": AUTHORIZATION_GATE_VERDICT_SCHEMA_RELPATH,
            },
            {
                "artifact_id": "economic_health_gate_verdict_v1",
                "artifact_path": str(
                    (sleeve_truth_root / "reports" / "economic_health_gate_verdict_v1" / day_utc / "economic_health_gate_verdict.v1.json").resolve()
                ),
                "schema_ref": ECONOMIC_HEALTH_GATE_VERDICT_V1_SCHEMA_RELPATH,
            },
            {
                "artifact_id": "gate_decision_ledger_v1",
                "artifact_path": str(
                    (sleeve_truth_root / "reports" / "gate_decision_ledger_v1" / day_utc / "gate_decision_ledger.v1.json").resolve()
                ),
                "schema_ref": GATE_DECISION_LEDGER_V1_SCHEMA_RELPATH,
            },
        ],
    }


def _validation_status_for_nested_artifact(*, artifact_path: Path, schema_relpath: str, expected_day_utc: str) -> Dict[str, Any]:
    if not artifact_path.exists() or not artifact_path.is_file():
        return {
            "exists": False,
            "artifact_sha256": "",
            "validation_status": "MISSING",
            "error_detail": "",
        }
    try:
        ref = read_validated_surface_v1(path=artifact_path, schema_relpath=schema_relpath)
    except Exception as exc:
        return {
            "exists": True,
            "artifact_sha256": sha256_file_v1(artifact_path),
            "validation_status": "INVALID",
            "error_detail": f"{type(exc).__name__}: {exc}",
        }
    observed_day = _extract_payload_day(ref.payload)
    if observed_day != expected_day_utc:
        return {
            "exists": True,
            "artifact_sha256": ref.sha256,
            "validation_status": "DAY_MISMATCH",
            "error_detail": f"expected_day={expected_day_utc} observed_day={observed_day}",
        }
    return {
        "exists": True,
        "artifact_sha256": ref.sha256,
        "validation_status": "VALID",
        "error_detail": "",
    }


def _aggregate_nested_validation_status(produced_artifacts: Iterable[Mapping[str, Any]]) -> str:
    statuses = [str(item.get("validation_status") or "").strip().upper() for item in produced_artifacts]
    statuses = [status for status in statuses if status]
    if not statuses:
        return "UNKNOWN"
    if any(status in {"INVALID", "DAY_MISMATCH"} for status in statuses):
        return "INVALID"
    if all(status == "VALID" for status in statuses):
        return "VALID"
    if all(status == "MISSING" for status in statuses):
        return "MISSING"
    if "VALID" in statuses and "MISSING" in statuses:
        return "PARTIAL"
    return statuses[0]


def _format_timeout_seconds(timeout_seconds: float) -> str:
    if float(timeout_seconds).is_integer():
        return str(int(timeout_seconds))
    return str(timeout_seconds)


def _resolve_tool_timeout_seconds() -> float:
    raw_value = str(os.environ.get(SESSION_AUTHORITY_TOOL_TIMEOUT_ENV) or "").strip()
    if not raw_value:
        return SESSION_AUTHORITY_TOOL_TIMEOUT_DEFAULT_SECONDS
    try:
        parsed = float(raw_value)
    except ValueError:
        return SESSION_AUTHORITY_TOOL_TIMEOUT_DEFAULT_SECONDS
    if parsed <= 0:
        return SESSION_AUTHORITY_TOOL_TIMEOUT_DEFAULT_SECONDS
    return parsed


def _resolve_timeout_seconds_for_script(script_relpath: str) -> float:
    timeout_seconds = _resolve_tool_timeout_seconds()
    script_min_timeout = float(SESSION_AUTHORITY_TOOL_MIN_TIMEOUT_SECONDS_BY_SCRIPT.get(str(script_relpath), 0.0))
    if script_min_timeout > timeout_seconds:
        return script_min_timeout
    return timeout_seconds


def _run_nested_tool_with_artifacts(
    *,
    repo_root: Path,
    script_relpath: str,
    cmd: List[str],
    sleeve_truth_root: Path,
    expected_day_utc: str,
) -> Dict[str, Any]:
    timeout_seconds = _resolve_timeout_seconds_for_script(script_relpath)
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
        return_code = int(proc.returncode)
        stdout = proc.stdout.strip()
        stderr = proc.stderr.strip()
        timed_out = False
    except subprocess.TimeoutExpired as exc:
        return_code = 124
        stdout = str(exc.stdout or "").strip()
        timeout_detail = (
            f"SESSION_AUTHORITY_TOOL_TIMEOUT:"
            f"script={script_relpath}:timeout_seconds={_format_timeout_seconds(timeout_seconds)}"
        )
        timeout_stderr = str(exc.stderr or "").strip()
        stderr = f"{timeout_stderr}\n{timeout_detail}".strip() if timeout_stderr else timeout_detail
        timed_out = True
    artifact_specs = _nested_producer_artifact_specs(
        sleeve_truth_root=sleeve_truth_root,
        day_utc=expected_day_utc,
    ).get(script_relpath, [])
    produced_artifacts: List[Dict[str, Any]] = []
    for spec in artifact_specs:
        artifact_path = Path(str(spec["artifact_path"])).resolve()
        validation = _validation_status_for_nested_artifact(
            artifact_path=artifact_path,
            schema_relpath=str(spec["schema_ref"]),
            expected_day_utc=expected_day_utc,
        )
        produced_artifacts.append(
            {
                "artifact_id": str(spec["artifact_id"]),
                "artifact_path": str(artifact_path),
                "schema_ref": str(spec["schema_ref"]),
                "exists": bool(validation["exists"]),
                "artifact_sha256": str(validation["artifact_sha256"]),
                "validation_status": str(validation["validation_status"]),
                "error_detail": str(validation["error_detail"]),
            }
        )
    return {
        "script": script_relpath,
        "command": cmd,
        "return_code": return_code,
        "stdout": stdout,
        "stderr": stderr,
        "required_for_closure": False,
        "timed_out": timed_out,
        "timeout_seconds": _format_timeout_seconds(timeout_seconds),
        "produced_artifacts": produced_artifacts,
        "validation_status": _aggregate_nested_validation_status(produced_artifacts),
        "error_detail": _error_detail_from_process(
            return_code=return_code,
            stdout=stdout,
            stderr=stderr,
        ),
    }


def _artifact_producer_git_sha(*, repo_root: Path) -> str:
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(repo_root))
        git_sha = out.decode("utf-8").strip()
    except Exception:
        git_sha = "0" * 40
    return git_sha if len(git_sha) == 40 else ("0" * 40)


def _resolve_primary_sleeve_positions_snapshot_path(*, sleeve_truth_root: Path, day_utc: str) -> Path:
    candidates = (
        sleeve_truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v3.json",
        sleeve_truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v2.json",
        sleeve_truth_root / "positions_snapshot_v2" / "snapshots" / day_utc / "positions_snapshot.v2.json",
    )
    for path in candidates:
        if path.exists() and path.is_file():
            return path.resolve()
    return candidates[1].resolve()


def run_primary_sleeve_capability_initialization_v1(
    *,
    repo_root: Path,
    target_day: str,
    environment: str,
    ib_account: str,
) -> List[Dict[str, Any]]:
    binding = _resolve_primary_binding(repo_root=repo_root, environment=environment, ib_account=ib_account)
    sleeve_truth_root = Path(str(binding.truth_root)).resolve()
    produced_utc = f"{target_day}T00:00:00Z"
    producer_git_sha = _artifact_producer_git_sha(repo_root=repo_root)
    producer_repo = "constellation"
    positions_snapshot_path = _resolve_primary_sleeve_positions_snapshot_path(
        sleeve_truth_root=sleeve_truth_root,
        day_utc=target_day,
    )
    commands = [
        (
            "ops/tools/run_accounting_nav_v2_day_v1.py",
            build_tool_command_v1(
                repo_root,
                "ops/tools/run_accounting_nav_v2_day_v1.py",
                "--day_utc",
                target_day,
                "--truth_root",
                str(sleeve_truth_root),
                "--producer_repo",
                producer_repo,
                "--producer_git_sha",
                producer_git_sha,
            ),
        ),
        (
            "constellation_2/phaseG/allocation/run/run_allocation_day_v2.py",
            build_tool_command_v1(
                repo_root,
                "constellation_2/phaseG/allocation/run/run_allocation_day_v2.py",
                "--day_utc",
                target_day,
                "--producer_git_sha",
                producer_git_sha,
                "--producer_repo",
                producer_repo,
                "--truth_root",
                str(sleeve_truth_root),
            ),
        ),
        (
            "ops/tools/run_broker_fact_spine_v1.py",
            build_tool_command_v1(
                repo_root,
                "ops/tools/run_broker_fact_spine_v1.py",
                "--day_utc",
                target_day,
                "--truth_root",
                str(sleeve_truth_root),
                "--environment",
                environment,
                "--sleeve_id",
                "PRIMARY",
                "--evaluation_utc",
                produced_utc,
                "--json",
            ),
        ),
        (
            "ops/tools/run_reconciliation_report_v3.py",
            build_tool_command_v1(
                repo_root,
                "ops/tools/run_reconciliation_report_v3.py",
                "--day_utc",
                target_day,
                "--truth_root",
                str(sleeve_truth_root),
            ),
        ),
        (
            "constellation_2/phaseI/exit_reconciliation/run/run_exit_reconciliation_day_v1.py",
            build_tool_command_v1(
                repo_root,
                "constellation_2/phaseI/exit_reconciliation/run/run_exit_reconciliation_day_v1.py",
                "--day_utc",
                target_day,
                "--truth_root",
                str(sleeve_truth_root),
                "--positions_snapshot_path",
                str(positions_snapshot_path),
            ),
        ),
        (
            "ops/tools/run_c2_capital_risk_envelope_gate_v2.py",
            build_tool_command_v1(
                repo_root,
                "ops/tools/run_c2_capital_risk_envelope_gate_v2.py",
                "--out_day_utc",
                target_day,
                "--input_day_utc",
                target_day,
                "--truth_root",
                str(sleeve_truth_root),
                "--produced_utc",
                produced_utc,
            ),
        ),
        (
            "ops/tools/run_liquidity_slippage_gate_v1.py",
            build_tool_command_v1(
                repo_root,
                "ops/tools/run_liquidity_slippage_gate_v1.py",
                "--day_utc",
                target_day,
                "--truth_root",
                str(sleeve_truth_root),
            ),
        ),
        (
            "ops/tools/run_operator_daily_gate_v3.py",
            build_tool_command_v1(
                repo_root,
                "ops/tools/run_operator_daily_gate_v3.py",
                "--day_utc",
                target_day,
                "--truth_root",
                str(sleeve_truth_root),
                "--produced_utc",
                produced_utc,
                "--mode",
                environment,
            ),
        ),
        (
            "ops/tools/run_gate_stack_verdict_v1.py",
            build_tool_command_v1(
                repo_root,
                "ops/tools/run_gate_stack_verdict_v1.py",
                "--day_utc",
                target_day,
                "--truth_root",
                str(sleeve_truth_root),
                "--produced_utc",
                produced_utc,
                "--mode",
                environment,
            ),
        ),
        (
            "ops/tools/run_gate_authority_plane_v1.py",
            build_tool_command_v1(
                repo_root,
                "ops/tools/run_gate_authority_plane_v1.py",
                "--day_utc",
                target_day,
                "--truth_root",
                str(sleeve_truth_root),
                "--produced_utc",
                produced_utc,
                "--mode",
                environment,
            ),
        ),
    ]
    source_refs: List[Dict[str, Any]] = []
    for script_relpath, cmd in commands:
        source_refs.append(
            _run_nested_tool_with_artifacts(
                repo_root=repo_root,
                script_relpath=script_relpath,
                cmd=cmd,
                sleeve_truth_root=sleeve_truth_root,
                expected_day_utc=target_day,
            )
        )
    return source_refs


def write_promoted_active_session_v1(
    *,
    truth_root: Path,
    target_day: str,
    environment: str,
    admission_ref: SessionAuthorityRefV1,
    prior_active_session_ref: SessionAuthorityRefV1 | None,
) -> SessionAuthorityRefV1:
    pre_open_bundle_ref = read_optional_pre_open_bundle_surface_v1(
        truth_root=truth_root,
        target_day=target_day,
    )
    promotion_payload = derive_session_promotion_decision_payload_v1(
        truth_root=truth_root,
        target_day=target_day,
        pre_open_bundle_ref=pre_open_bundle_ref,
        target_day_admission_ref=admission_ref,
        prior_active_session_ref=prior_active_session_ref,
        owner_tool="ops/tools/run_session_authority_v1.py",
        allow_pre_open_incomplete_if_admitted=str(environment or "").strip().upper() == "PAPER",
    )
    promotion_ref = write_session_promotion_decision_v1(
        truth_root=truth_root,
        payload=promotion_payload,
    )
    active_payload = derive_active_session_payload_v1(
        truth_root=truth_root,
        target_day=target_day,
        admission_ref=admission_ref,
        prior_active_session_ref=prior_active_session_ref,
        promotion_ref=promotion_ref,
    )
    return write_active_session_v1(truth_root=truth_root, payload=active_payload)


def refresh_active_session_for_final_convergence_v1(
    *,
    truth_root: Path,
    target_day: str,
    environment: str = "PAPER",
) -> SessionAuthorityRefV1:
    admission_ref = read_target_day_admission_ref_v1(truth_root=truth_root, target_day=target_day)
    try:
        prior_active_ref = read_active_session_ref_v1(truth_root=truth_root)
    except Exception:
        prior_active_ref = None
    return write_promoted_active_session_v1(
        truth_root=truth_root,
        target_day=target_day,
        environment=environment,
        admission_ref=admission_ref,
        prior_active_session_ref=prior_active_ref,
    )


def finalize_session_authority_cli_result_v1(
    *,
    truth_root: Path,
    target_day: str,
    phase: str,
    environment: str,
    ib_account: str,
    build_ref: SessionAuthorityRefV1 | None,
    admission_ref: SessionAuthorityRefV1 | None,
    active_ref: SessionAuthorityRefV1 | None,
) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "truth_root": str(truth_root),
        "target_day": target_day,
        "phase": phase,
        "environment": environment,
        "ib_account": ib_account,
    }
    if build_ref is not None:
        summary["target_day_build"] = {
            "path": str(build_ref.path),
            "sha256": build_ref.sha256,
            "build_status": str(build_ref.payload.get("build_status") or "").strip(),
            "completeness_result": str(build_ref.payload.get("completeness_result") or "").strip(),
            "closure_status": str(build_ref.payload.get("closure_status") or "").strip(),
            "hidden_dependency_check_status": str(
                (build_ref.payload.get("hidden_dependency_check_result") or {}).get("status") or ""
            ).strip(),
        }
    if admission_ref is not None:
        summary["target_day_admission"] = {
            "path": str(admission_ref.path),
            "sha256": admission_ref.sha256,
            "admission_status": str(admission_ref.payload.get("admission_status") or "").strip(),
            "closure_status": str(admission_ref.payload.get("closure_status") or "").strip(),
            "blocking_reason_codes": list(admission_ref.payload.get("blocking_reason_codes") or []),
        }
    if active_ref is not None:
        summary["active_session"] = {
            "path": str(active_ref.path),
            "sha256": active_ref.sha256,
            "active_day": str(active_ref.payload.get("active_day") or "").strip(),
            "rollover_status": str(active_ref.payload.get("rollover_status") or "").strip(),
            "rollover_reason_code": str(active_ref.payload.get("rollover_reason_code") or "").strip(),
            "target_day_admission_status": str(active_ref.payload.get("target_day_admission_status") or "").strip(),
            "promotion_state": str(active_ref.payload.get("promotion_state") or "").strip(),
            "promotion_decision_ref": str(active_ref.payload.get("promotion_decision_ref") or "").strip(),
        }

    try:
        promotion_ref = read_session_promotion_decision_ref_v1(truth_root=truth_root, target_day=target_day)
    except Exception:
        promotion_ref = None
    if promotion_ref is not None:
        summary["session_promotion_decision"] = {
            "path": str(promotion_ref.path),
            "sha256": promotion_ref.sha256,
            "promotion_state": str(promotion_ref.payload.get("promotion_state") or "").strip(),
            "blocked_reason_codes": [
                str(code).strip()
                for code in (promotion_ref.payload.get("blocked_reason_codes") or [])
                if str(code).strip()
            ],
        }

    if admission_ref is not None:
        admission_ok = str(admission_ref.payload.get("admission_status") or "").strip().upper() == "ADMIT"
        promotion_ok = promotion_ref is None or str(promotion_ref.payload.get("promotion_state") or "").strip().upper() == PROMOTION_STATE_PROMOTED
        exit_code = 0 if admission_ok and promotion_ok else 2
    elif build_ref is not None:
        exit_code = 0 if str(build_ref.payload.get("closure_status") or "").strip().upper() == CLOSURE_STATUS_CLOSED else 2
    else:
        exit_code = 0

    return {"summary": summary, "exit_code": exit_code}


def run_session_authority_phase_flow_v1(
    *,
    truth_root: Path,
    target_day: str,
    environment: str,
    ib_account: str,
    phase: str,
    repo_root: Path,
    build_fn: Callable[[], SessionAuthorityRefV1],
    post_build_alignment_fn: Callable[[Path, str], None],
    reporting_fn: Callable[[Path, str, bool], None],
) -> Dict[str, SessionAuthorityRefV1 | None]:
    prior_active_ref = None
    try:
        prior_active_ref = read_active_session_ref_v1(truth_root=truth_root)
    except Exception:
        prior_active_ref = None

    build_ref: SessionAuthorityRefV1 | None = None
    admission_ref: SessionAuthorityRefV1 | None = None
    active_ref: SessionAuthorityRefV1 | None = None

    if phase in {"build", "all"}:
        build_ref = build_fn()
    else:
        build_ref = read_target_day_build_ref_v1(truth_root=truth_root, target_day=target_day)

    if phase in {"admit", "all"}:
        admission_payload = derive_target_day_admission_payload_v1(
            truth_root=truth_root,
            target_day=target_day,
            build_ref=build_ref,
            prior_active_session_ref=prior_active_ref,
            enforce_consistency_gate=True,
            environment=environment,
            repo_root=repo_root,
        )
        admission_ref = write_target_day_admission_v1(truth_root=truth_root, payload=admission_payload)
    elif phase == "activate":
        admission_ref = read_target_day_admission_ref_v1(truth_root=truth_root, target_day=target_day)

    if phase in {"activate", "all"}:
        if admission_ref is None:
            admission_ref = read_target_day_admission_ref_v1(truth_root=truth_root, target_day=target_day)
        active_ref = write_promoted_active_session_v1(
            truth_root=truth_root,
            target_day=target_day,
            environment=environment,
            admission_ref=admission_ref,
            prior_active_session_ref=prior_active_ref,
        )

    if phase == "all" and admission_ref is not None:
        admission_is_admit = str(admission_ref.payload.get("admission_status") or "").strip().upper() == "ADMIT"
        if admission_is_admit:
            post_build_alignment_fn(truth_root, target_day)
        reporting_fn(truth_root, target_day, admission_is_admit)

    return {
        "build_ref": build_ref,
        "admission_ref": admission_ref,
        "active_ref": active_ref,
    }


def collect_target_day_build_source_refs_v1(
    *,
    repo_root: Path,
    truth_root: Path,
    target_day: str,
    environment: str,
    ib_account: str,
    downstream_build_cycle_scripts: Iterable[str],
    run_primary_sleeve_capability_initialization_fn: Callable[..., Iterable[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    source_refs: List[Dict[str, Any]] = []
    skip_startup_materialization_reentry = (
        str(os.environ.get("C2_SKIP_STARTUP_MATERIALIZATION_REENTRY") or "").strip().upper() == "YES"
    )
    skip_session_readiness_reentry = (
        str(os.environ.get("C2_SKIP_SESSION_AUTHORITY_REENTRY") or "").strip().upper() == "YES"
    )

    for result in (
        run_tool_v1(
            repo_root=repo_root,
            script_relpath="ops/tools/run_paper_startup_intent_input_convergence_v1.py",
            downstream_build_cycle_scripts=downstream_build_cycle_scripts,
            args=(
                "--day_utc",
                target_day,
                "--truth_root",
                str(truth_root),
                "--environment",
                environment,
                "--ib_account",
                ib_account,
            ),
        ),
        run_tool_v1(
            repo_root=repo_root,
            script_relpath="ops/tools/run_trading_day_intent_generation_v1.py",
            downstream_build_cycle_scripts=downstream_build_cycle_scripts,
            args=("--day_utc", target_day, "--truth_root", str(truth_root)),
        ),
        run_tool_v1(
            repo_root=repo_root,
            script_relpath="ops/tools/run_startup_materialization_input_convergence_v1.py",
            downstream_build_cycle_scripts=downstream_build_cycle_scripts,
            args=(
                "--day_utc",
                target_day,
                "--truth_root",
                str(truth_root),
                "--ib_account",
                ib_account,
            ),
        ),
        run_tool_v1(
            repo_root=repo_root,
            script_relpath="ops/tools/run_bod_execution_environment_proof_v1.py",
            downstream_build_cycle_scripts=downstream_build_cycle_scripts,
            args=("--day_utc", target_day, "--truth_root", str(truth_root)),
        ),
        run_tool_v1(
            repo_root=repo_root,
            script_relpath="ops/tools/run_phasec_risk_inputs_prep_v1.py",
            downstream_build_cycle_scripts=downstream_build_cycle_scripts,
            args=("--day_utc", target_day, "--truth_root", str(truth_root)),
        ),
        *(
            []
            if skip_startup_materialization_reentry
            else [
                run_tool_v1(
                    repo_root=repo_root,
                    script_relpath="ops/tools/run_startup_materialization_v1.py",
                    downstream_build_cycle_scripts=downstream_build_cycle_scripts,
                    args=("--day_utc", target_day, "--truth_root", str(truth_root)),
                )
            ]
        ),
        run_tool_v1(
            repo_root=repo_root,
            script_relpath="ops/tools/run_paper_trading_posture_v1.py",
            downstream_build_cycle_scripts=downstream_build_cycle_scripts,
            args=("--day_utc", target_day, "--truth_root", str(truth_root)),
        ),
        run_tool_v1(
            repo_root=repo_root,
            script_relpath="ops/tools/run_paper_startup_authorization_convergence_v1.py",
            downstream_build_cycle_scripts=downstream_build_cycle_scripts,
            args=(
                "--day_utc",
                target_day,
                "--truth_root",
                str(truth_root),
                "--environment",
                environment,
                "--ib_account",
                ib_account,
            ),
        ),
        run_tool_v1(
            repo_root=repo_root,
            script_relpath="ops/tools/run_day_authority_decision_v1.py",
            downstream_build_cycle_scripts=downstream_build_cycle_scripts,
            args=("--day_utc", target_day, "--truth_root", str(truth_root)),
        ),
    ):
        source_refs.append(result)

    if skip_session_readiness_reentry:
        source_refs.append(
            {
                "script": "ops/tools/run_session_readiness_refresh_v1.py",
                "command": build_tool_command_v1(
                    repo_root,
                    "ops/tools/run_session_readiness_refresh_v1.py",
                    "--day_utc",
                    target_day,
                    "--truth_root",
                    str(truth_root),
                ),
                "return_code": 0,
                "stdout": "SKIPPED: C2_SKIP_SESSION_AUTHORITY_REENTRY=YES",
                "stderr": "",
                "required_for_closure": False,
                "nonblocking_reason": "SESSION_AUTHORITY_REENTRY_SKIPPED_BY_CALLER",
            }
        )
    else:
        source_refs.append(
            run_tool_v1(
                repo_root=repo_root,
                script_relpath="ops/tools/run_session_readiness_refresh_v1.py",
                downstream_build_cycle_scripts=downstream_build_cycle_scripts,
                args=(
                    "--day_utc",
                    target_day,
                    "--truth_root",
                    str(truth_root),
                    "--build_fast_path",
                    "YES",
                ),
            )
        )

    source_refs.extend(
        run_primary_sleeve_capability_initialization_fn(
            repo_root=repo_root,
            target_day=target_day,
            environment=environment,
            ib_account=ib_account,
        )
    )

    for result in (
        run_tool_v1(
            repo_root=repo_root,
            script_relpath="ops/tools/run_capability_state_v1.py",
            downstream_build_cycle_scripts=downstream_build_cycle_scripts,
            args=(
                "--day_utc",
                target_day,
                "--truth_root",
                str(truth_root),
                "--environment",
                environment,
                "--ib_account",
                ib_account,
            ),
        ),
        run_tool_v1(
            repo_root=repo_root,
            script_relpath="ops/tools/run_paper_policy_verdict_v1.py",
            downstream_build_cycle_scripts=downstream_build_cycle_scripts,
            args=("--day_utc", target_day, "--truth_root", str(truth_root)),
        ),
        run_tool_v1(
            repo_root=repo_root,
            script_relpath="ops/tools/run_submit_boundary_status_v1.py",
            downstream_build_cycle_scripts=downstream_build_cycle_scripts,
            args=("--day_utc", target_day, "--truth_root", str(truth_root)),
        ),
        run_tool_v1(
            repo_root=repo_root,
            script_relpath="ops/tools/run_paper_session_ledger_v1.py",
            downstream_build_cycle_scripts=downstream_build_cycle_scripts,
            args=("--day_utc", target_day, "--truth_root", str(truth_root)),
        ),
        run_tool_v1(
            repo_root=repo_root,
            script_relpath="ops/tools/run_startup_proof_validation_v1.py",
            downstream_build_cycle_scripts=downstream_build_cycle_scripts,
            args=("--day_utc", target_day, "--truth_root", str(truth_root)),
        ),
        run_tool_v1(
            repo_root=repo_root,
            script_relpath="ops/tools/run_trading_day_state_machine_v1.py",
            downstream_build_cycle_scripts=downstream_build_cycle_scripts,
            args=("--day_utc", target_day, "--truth_root", str(truth_root)),
        ),
    ):
        source_refs.append(result)

    for path in (
        resolve_next_day_readiness_probe_path(truth_root=truth_root, target_day_utc=target_day),
        resolve_deployment_state_machine_path(truth_root=truth_root, day_utc=target_day),
    ):
        source_refs.append(
            {
                "script": "source_ref",
                "command": [],
                "return_code": 0,
                "stdout": "",
                "stderr": "",
                "artifact_path": str(path),
                "exists": bool(path.exists() and path.is_file()),
            }
        )

    return source_refs


def finalize_target_day_build_v1(
    *,
    repo_root: Path,
    truth_root: Path,
    target_day: str,
    environment: str,
    ib_account: str,
    source_refs: Iterable[Mapping[str, Any]],
    collect_target_day_build_artifact_rows_fn: Callable[..., list[dict[str, Any]]],
    compute_hidden_dependency_check_result_fn: Callable[..., Dict[str, Any]],
    derive_target_day_build_payload_fn: Callable[..., Dict[str, Any]],
    write_target_day_build_fn: Callable[..., SessionAuthorityRefV1],
    downstream_build_cycle_scripts: Iterable[str],
) -> SessionAuthorityRefV1:
    artifact_results = collect_target_day_build_artifact_rows_fn(
        truth_root=truth_root,
        target_day=target_day,
        environment=environment,
        ib_account=ib_account,
    )

    hidden_dependency_check_result = compute_hidden_dependency_check_result_fn(
        repo_root=repo_root,
        artifact_results=artifact_results,
        source_refs=source_refs,
        downstream_build_cycle_scripts=downstream_build_cycle_scripts,
    )
    payload = derive_target_day_build_payload_fn(
        truth_root=truth_root,
        target_day=target_day,
        artifact_results=artifact_results,
        source_refs=source_refs,
        hidden_dependency_check_result=hidden_dependency_check_result,
    )
    payload["hidden_dependency_check_result"] = {
        **dict(payload.get("hidden_dependency_check_result") or {}),
        "declared_inventory_artifacts": list(hidden_dependency_check_result.get("declared_inventory_artifacts") or []),
        "observed_dependency_artifacts": list(hidden_dependency_check_result.get("observed_dependency_artifacts") or []),
        "undeclared_dependency_artifacts": list(hidden_dependency_check_result.get("undeclared_dependency_artifacts") or []),
        "failing_producers": list(hidden_dependency_check_result.get("failing_producers") or []),
        "summary": str(hidden_dependency_check_result.get("summary") or ""),
        "status": str(
            hidden_dependency_check_result.get("status")
            or payload.get("hidden_dependency_check_result", {}).get("status")
            or ""
        ),
        "blocking_reason_code": str(
            hidden_dependency_check_result.get("blocking_reason_code")
            or payload.get("hidden_dependency_check_result", {}).get("blocking_reason_code")
            or ""
        ),
    }
    return write_target_day_build_fn(truth_root=truth_root, payload=payload)


def run_post_build_alignment_tools_v1(
    *,
    repo_root: Path,
    truth_root: Path,
    target_day: str,
    downstream_build_cycle_scripts: Iterable[str],
) -> None:
    for script_relpath in (
        "ops/tools/run_submit_boundary_status_v1.py",
        "ops/tools/run_paper_session_ledger_v1.py",
        "ops/tools/run_startup_proof_validation_v1.py",
        "ops/tools/run_trading_day_state_machine_v1.py",
    ):
        run_tool_v1(
            repo_root=repo_root,
            script_relpath=script_relpath,
            downstream_build_cycle_scripts=downstream_build_cycle_scripts,
            args=("--day_utc", target_day, "--truth_root", str(truth_root)),
        )


def run_authority_reporting_tools_v1(
    *,
    repo_root: Path,
    truth_root: Path,
    target_day: str,
    environment: str,
    include_control_plane: bool,
    downstream_build_cycle_scripts: Iterable[str],
) -> None:
    refresh_active_session_for_final_convergence_v1(
        truth_root=truth_root,
        target_day=target_day,
        environment=environment,
    )
    if include_control_plane:
        run_tool_v1(
            repo_root=repo_root,
            script_relpath="ops/tools/run_paper_day_control_plane_v1.py",
            downstream_build_cycle_scripts=downstream_build_cycle_scripts,
            args=("--day_utc", target_day, "--truth_root", str(truth_root)),
        )
    run_tool_v1(
        repo_root=repo_root,
        script_relpath="ops/tools/run_session_authority_status_v1.py",
        downstream_build_cycle_scripts=downstream_build_cycle_scripts,
        args=(
            "--truth_root",
            str(truth_root),
            "--environment",
            "PAPER",
            "--mode",
            "WRITE",
            "--json",
        ),
    )
    run_tool_v1(
        repo_root=repo_root,
        script_relpath="ops/tools/run_day_open_trigger_v1.py",
        downstream_build_cycle_scripts=downstream_build_cycle_scripts,
        args=(
            "--day_utc",
            target_day,
            "--truth_root",
            str(truth_root),
            "--environment",
            "PAPER",
        ),
    )
    run_tool_v1(
        repo_root=repo_root,
        script_relpath="ops/tools/run_operator_day_authority_summary_v1.py",
        downstream_build_cycle_scripts=downstream_build_cycle_scripts,
        args=("--day_utc", target_day, "--truth_root", str(truth_root)),
    )
    run_tool_v1(
        repo_root=repo_root,
        script_relpath="ops/tools/run_day_failure_causality_v1.py",
        downstream_build_cycle_scripts=downstream_build_cycle_scripts,
        args=("--day_utc", target_day, "--truth_root", str(truth_root)),
    )


def run_final_convergence_tools_v1(
    *,
    repo_root: Path,
    truth_root: Path,
    target_day: str,
    environment: str,
    downstream_build_cycle_scripts: Iterable[str],
) -> None:
    run_authority_reporting_tools_v1(
        repo_root=repo_root,
        truth_root=truth_root,
        target_day=target_day,
        environment=environment,
        include_control_plane=True,
        downstream_build_cycle_scripts=downstream_build_cycle_scripts,
    )
