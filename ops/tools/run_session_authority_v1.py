#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry
from constellation_2.common.runtime_path_authority_v1 import (
    classify_runtime_path_v1,
    resolve_decision_truth_root_v1,
)
from constellation_2.common.session_authority_v1 import (
    SessionAuthorityRefV1,
    derive_target_day_build_payload_v1,
    resolve_session_authority_target_day_v1,
    write_target_day_build_v1,
)
from constellation_2.common.trade_submit_readiness_authority_v1 import (
    resolve_governed_account_binding,
    resolve_governed_sleeve_truth_bindings,
)


_BASE_DOWNSTREAM_BUILD_CYCLE_SCRIPTS = {
    "ops/tools/run_submit_boundary_status_v1.py",
    "ops/tools/run_paper_session_ledger_v1.py",
    "ops/tools/run_startup_proof_validation_v1.py",
    "ops/tools/run_trading_day_state_machine_v1.py",
}

_PAPER_OPEN_NONBLOCKING_BUILD_SCRIPTS = {
    "ops/tools/run_paper_startup_authorization_convergence_v1.py",
    "ops/tools/run_paper_policy_verdict_v1.py",
    "ops/tools/run_startup_materialization_v1.py",
}


def _run_tool(
    script_relpath: str,
    *args: str,
    downstream_build_cycle_scripts: Iterable[str] | None = None,
) -> Dict[str, Any]:
    from ops.tools.run_session_authority_orchestration_v1 import run_tool_v1

    return run_tool_v1(
        repo_root=REPO_ROOT,
        script_relpath=script_relpath,
        downstream_build_cycle_scripts=(
            tuple(downstream_build_cycle_scripts)
            if downstream_build_cycle_scripts is not None
            else _downstream_build_cycle_scripts_for_environment_v1("PAPER")
        ),
        args=args,
    )


def _downstream_build_cycle_scripts_for_environment_v1(environment: str) -> set[str]:
    env = str(environment or "").strip().upper()
    scripts = set(_BASE_DOWNSTREAM_BUILD_CYCLE_SCRIPTS)
    if env == "PAPER":
        scripts.update(_PAPER_OPEN_NONBLOCKING_BUILD_SCRIPTS)
    return scripts


def _annotate_source_ref_for_closure(
    result: Mapping[str, Any],
    downstream_build_cycle_scripts: Iterable[str] | None = None,
) -> Dict[str, Any]:
    from ops.tools.run_session_authority_orchestration_v1 import _annotate_source_ref_for_closure as _impl

    return _impl(
        result=dict(result),
        downstream_build_cycle_scripts=(
            tuple(downstream_build_cycle_scripts)
            if downstream_build_cycle_scripts is not None
            else _downstream_build_cycle_scripts_for_environment_v1("PAPER")
        ),
    )


def _source_ref_blocks_build(
    ref: Mapping[str, Any],
    downstream_build_cycle_scripts: Iterable[str] | None = None,
) -> bool:
    from ops.tools.run_session_authority_orchestration_v1 import source_ref_blocks_build_v1

    return source_ref_blocks_build_v1(
        ref=ref,
        downstream_build_cycle_scripts=(
            tuple(downstream_build_cycle_scripts)
            if downstream_build_cycle_scripts is not None
            else _downstream_build_cycle_scripts_for_environment_v1("PAPER")
        ),
    )


def _compute_hidden_dependency_check_result(
    *,
    artifact_results: Iterable[Mapping[str, Any]],
    source_refs: Iterable[Mapping[str, Any]],
    downstream_build_cycle_scripts: Iterable[str] | None = None,
) -> Dict[str, Any]:
    from ops.tools.run_session_authority_diagnostic_v1 import compute_hidden_dependency_check_result_v1

    return compute_hidden_dependency_check_result_v1(
        repo_root=REPO_ROOT,
        artifact_results=artifact_results,
        source_refs=source_refs,
        downstream_build_cycle_scripts=(
            tuple(downstream_build_cycle_scripts)
            if downstream_build_cycle_scripts is not None
            else _downstream_build_cycle_scripts_for_environment_v1("PAPER")
        ),
    )


def _extract_timestamp(payload: Mapping[str, Any]) -> str:
    from ops.tools.run_session_authority_control_plane_v1 import _extract_timestamp as _impl

    return _impl(payload)


def _freshness_status(*, payload: Mapping[str, Any], target_day: str, observed_day: str, freshness_rule: str) -> str:
    from ops.tools.run_session_authority_control_plane_v1 import _freshness_status as _impl

    return _impl(payload=payload, target_day=target_day, observed_day=observed_day, freshness_rule=freshness_rule)


def _normalize_scoped_dependency_type(dependency_type: str) -> str:
    from ops.tools.run_session_authority_control_plane_v1 import _normalize_scoped_dependency_type as _impl

    return _impl(dependency_type)


def _payload_dependencies(artifact_id: str, payload: Mapping[str, Any]) -> List[str]:
    from ops.tools.run_session_authority_control_plane_v1 import _payload_dependencies as _impl

    return _impl(artifact_id, payload)


def _capability_artifact_status(payload: Dict[str, Any]) -> tuple[str, bool, List[str]]:
    from ops.tools.run_session_authority_control_plane_v1 import _capability_artifact_status as _impl

    return _impl(payload)


def _collect_pre_open_bundle_rows(*, truth_root: Path, target_day: str) -> List[Dict[str, Any]]:
    from ops.tools.run_session_authority_control_plane_v1 import _pre_open_bundle_rows as _impl

    return _impl(truth_root=truth_root, target_day=target_day, required=True)


def _collect_market_calendar_row(*, truth_root: Path, target_day: str) -> Dict[str, Any]:
    from ops.tools.run_session_authority_control_plane_v1 import _market_calendar_row as _impl

    return _impl(truth_root=truth_root, target_day=target_day)


def _collect_day_authority_row(*, truth_root: Path, target_day: str, environment: str = "PAPER") -> Dict[str, Any]:
    from ops.tools.run_session_authority_control_plane_v1 import collect_target_day_build_artifact_rows_v1

    rows = collect_target_day_build_artifact_rows_v1(
        truth_root=truth_root,
        target_day=target_day,
        environment=environment,
        ib_account="",
    )
    for row in rows:
        if str(row.get("artifact_id") or "").strip() == "day_authority_decision_v1":
            return row
    raise KeyError("day_authority_decision_v1 row not found")


def _collect_primary_scoped_authorization_row(
    *,
    truth_root: Path,
    target_day: str,
    environment: str = "PAPER",
    ib_account: str = "",
) -> Dict[str, Any]:
    from ops.tools.run_session_authority_control_plane_v1 import collect_target_day_build_artifact_rows_v1

    rows = collect_target_day_build_artifact_rows_v1(
        truth_root=truth_root,
        target_day=target_day,
        environment=environment,
        ib_account=ib_account,
    )
    for row in rows:
        if str(row.get("artifact_id") or "").strip() == "primary_scoped_authorization_gate_verdict_v1":
            return row
    raise KeyError("primary_scoped_authorization_gate_verdict_v1 row not found")


def _collect_previous_day_economic_rows_from_readiness(
    *,
    truth_root: Path,
    target_day: str,
    environment: str = "PAPER",
    ib_account: str = "",
) -> List[Dict[str, Any]]:
    from ops.tools.run_session_authority_control_plane_v1 import _previous_day_economic_rows as _impl

    return _impl(
        truth_root=truth_root,
        target_day=target_day,
        environment=environment,
        ib_account=ib_account,
    )


def _read_surface_row(
    *,
    artifact_id: str,
    path: Path,
    target_day: str,
    required: bool = True,
    role_class: str = "REQUIRED_DERIVED_GATE",
    classification: str = "TEST",
) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {
            "artifact_id": artifact_id,
            "artifact_name": artifact_id,
            "required": required,
            "role_class": role_class,
            "classification": classification,
            "authority_path": str(path.resolve()),
            "observed_status": "MISSING",
            "result_status": "FAIL",
            "blocking_reason_code": "TARGET_DAY_ARTIFACT_MISSING",
            "blocker_codes": [f"{artifact_id.upper()}_MISSING"],
            "schema_status": "MISSING",
            "schema_ref": "",
            "freshness_rule": "TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
            "freshness_status": "STALE",
            "target_day_expected": target_day,
            "target_day_observed": "",
            "date_binding_status": "MISSING",
            "provenance_required": True,
            "provenance_summary": {"required": True, "present": False, "fields_present": [], "source": ""},
            "closure_status": "OPEN",
            "producer": {"module": "", "git_sha": ""},
            "source_refs": [],
            "observed_dependency_artifacts": [],
        }

    payload = json.loads(path.read_text(encoding="utf-8"))
    observed_day = str(payload.get("day_utc") or payload.get("target_day") or "").strip()
    status = str(
        payload.get("status")
        or payload.get("boundary_status")
        or payload.get("authority_status")
        or payload.get("final_start_decision")
        or payload.get("admission_status")
        or ""
    ).strip().upper()
    status = status or "UNKNOWN"
    is_pass = status in {"PASS", "OK", "AUTHORIZED", "ADMIT", "READY_NOW", "GRANTED", "COMPLETE"}
    return {
        "artifact_id": artifact_id,
        "artifact_name": artifact_id,
        "required": required,
        "role_class": role_class,
        "classification": classification,
        "authority_path": str(path.resolve()),
        "observed_status": status,
        "result_status": "PASS" if is_pass else "FAIL",
        "blocking_reason_code": "" if is_pass else "REQUIRED_GATE_FAIL",
        "blocker_codes": [] if is_pass else [f"{artifact_id.upper()}_NOT_PASS"],
        "schema_status": "VALID",
        "schema_ref": "",
        "freshness_rule": "TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        "freshness_status": "CURRENT" if observed_day == target_day else "STALE",
        "target_day_expected": target_day,
        "target_day_observed": observed_day,
        "date_binding_status": "MATCH" if observed_day == target_day else "MISMATCH",
        "provenance_required": True,
        "provenance_summary": {"required": True, "present": True, "fields_present": ["path"], "source": "compat"},
        "closure_status": "CLOSED" if is_pass else "OPEN",
        "producer": {"module": "ops/tools/run_session_authority_v1.py", "git_sha": ""},
        "source_refs": [{"artifact_path": str(path.resolve()), "artifact_sha256": ""}],
        "observed_dependency_artifacts": [],
    }


def _collect_handshake_rows(
    *,
    truth_root: Path,
    environment: str,
    ib_account: str,
    target_day: str,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    pointer_path = (truth_root / "ib_api_handshake" / "latest_pointer.v1.json").resolve()
    handshake_day_path = (truth_root / "ib_api_handshake" / target_day / "ib_api_handshake.v1.json").resolve()

    pointer_row = _read_surface_row(
        artifact_id="ib_api_handshake_latest_pointer_v1",
        path=pointer_path,
        target_day=target_day,
        required=True,
        role_class="REQUIRED_BINDING_INPUT",
        classification="UPSTREAM_MATERIALIZATION",
    )
    if pointer_path.exists() and pointer_path.is_file():
        pointer_obj = json.loads(pointer_path.read_text(encoding="utf-8"))
        pointer_day = str(pointer_obj.get("day_utc") or "").strip()
        pointer_target = str(((pointer_obj.get("pointers") or {}).get("handshake_path")) or "").strip()
        if pointer_day and pointer_day != target_day and handshake_day_path.exists() and handshake_day_path.is_file():
            pointer_row["target_day_observed"] = target_day
            pointer_row["date_binding_status"] = "MATCH"
            pointer_row["freshness_status"] = "CURRENT"
            pointer_row["blocking_reason_code"] = ""
            pointer_row["blocker_codes"] = []
            pointer_row["result_status"] = "PASS"
            if pointer_target:
                pointer_row["source_refs"].append({"artifact_path": str(Path(pointer_target).resolve()), "artifact_sha256": ""})

    handshake_row = _read_surface_row(
        artifact_id="ib_api_handshake_v1",
        path=handshake_day_path,
        target_day=target_day,
        required=True,
        role_class="REQUIRED_BINDING_INPUT",
        classification="UPSTREAM_MATERIALIZATION",
    )
    return pointer_row, handshake_row


def _trade_submit_path(
    *,
    truth_root: Path,
    target_day: str,
    environment: str,
    ib_account: str,
) -> Path:
    bindings = resolve_governed_sleeve_truth_bindings(
        repo_root=REPO_ROOT,
        environment=environment,
        requested_ib_account=ib_account,
    )
    primary_binding = None
    for binding in bindings:
        sleeve_id = str(getattr(binding, "sleeve_id", "") or "").strip().upper()
        if sleeve_id == "PRIMARY":
            primary_binding = binding
            break
    binding = primary_binding if primary_binding is not None else bindings[0]
    execution_truth_root = Path(str(binding.truth_root)).resolve()
    return (
        execution_truth_root
        / "trade_submit_readiness_c2_v1"
        / "_history"
        / str(environment).strip().upper()
        / str(ib_account).strip()
        / str(target_day).strip()
        / "status.json"
    ).resolve()


def _resolve_primary_binding(*, environment: str, ib_account: str):
    bindings = resolve_governed_sleeve_truth_bindings(
        repo_root=REPO_ROOT,
        environment=environment,
        requested_ib_account=ib_account,
    )
    for binding in bindings:
        if str(binding.sleeve_id).strip().upper() == "PRIMARY":
            return binding
    return bindings[0]


def _artifact_producer_git_sha() -> str:
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        git_sha = out.decode("utf-8").strip()
    except Exception:
        git_sha = "0" * 40
    return git_sha if len(git_sha) == 40 else ("0" * 40)


def _run_primary_sleeve_capability_initialization(
    *,
    target_day: str,
    environment: str,
    ib_account: str,
) -> List[Dict[str, Any]]:
    from ops.tools.run_session_authority_orchestration_v1 import (
        _aggregate_nested_validation_status,
        _error_detail_from_process,
        _nested_producer_artifact_specs,
        _validation_status_for_nested_artifact,
        build_tool_command_v1,
    )

    binding = _resolve_primary_binding(environment=environment, ib_account=ib_account)
    sleeve_truth_root = Path(str(binding.truth_root)).resolve()
    produced_utc = f"{target_day}T00:00:00Z"
    producer_git_sha = _artifact_producer_git_sha()
    producer_repo = "constellation"
    positions_snapshot_path = (
        sleeve_truth_root / "positions_v1" / "snapshots" / target_day / "positions_snapshot.v2.json"
    ).resolve()

    commands = [
        (
            "ops/tools/run_accounting_nav_v2_day_v1.py",
            build_tool_command_v1(
                REPO_ROOT,
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
                REPO_ROOT,
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
                REPO_ROOT,
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
                REPO_ROOT,
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
                REPO_ROOT,
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
                REPO_ROOT,
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
                REPO_ROOT,
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
                REPO_ROOT,
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
                REPO_ROOT,
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
                REPO_ROOT,
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

    artifact_specs = _nested_producer_artifact_specs(
        sleeve_truth_root=sleeve_truth_root,
        day_utc=target_day,
    )
    source_refs: List[Dict[str, Any]] = []
    for script_relpath, cmd in commands:
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(REPO_ROOT),
                capture_output=True,
                text=True,
            )
            return_code = int(proc.returncode)
            stdout = str(proc.stdout or "").strip()
            stderr = str(proc.stderr or "").strip()
        except Exception as exc:
            return_code = 2
            stdout = ""
            stderr = f"{type(exc).__name__}: {exc}"

        produced_artifacts: List[Dict[str, Any]] = []
        for artifact_spec in artifact_specs.get(script_relpath, []):
            artifact_path = Path(str(artifact_spec.get("artifact_path") or "")).resolve()
            validation = _validation_status_for_nested_artifact(
                artifact_path=artifact_path,
                schema_relpath=str(artifact_spec.get("schema_ref") or ""),
                expected_day_utc=target_day,
            )
            produced_artifacts.append(
                {
                    "artifact_id": str(artifact_spec.get("artifact_id") or "").strip(),
                    "artifact_path": str(artifact_path),
                    "schema_ref": str(artifact_spec.get("schema_ref") or "").strip(),
                    **validation,
                }
            )
        source_refs.append(
            {
                "script": script_relpath,
                "command": cmd,
                "return_code": return_code,
                "stdout": stdout,
                "stderr": stderr,
                "required_for_closure": False,
                "nonblocking_reason": "NESTED_PRODUCER_OBSERVABILITY_ONLY",
                "produced_artifacts": produced_artifacts,
                "validation_status": _aggregate_nested_validation_status(produced_artifacts),
                "error_detail": _error_detail_from_process(
                    return_code=return_code,
                    stdout=stdout,
                    stderr=stderr,
                ),
            }
        )
    return source_refs


def _session_readiness_refresh_startup_ready(result: Mapping[str, Any]) -> bool:
    from ops.tools.run_session_authority_orchestration_v1 import _session_readiness_refresh_startup_ready as _impl

    return _impl(result)


def _run_post_build_alignment_tools(*, truth_root: Path, target_day: str) -> None:
    for script_relpath in (
        "ops/tools/run_submit_boundary_status_v1.py",
        "ops/tools/run_paper_session_ledger_v1.py",
        "ops/tools/run_startup_proof_validation_v1.py",
        "ops/tools/run_trading_day_state_machine_v1.py",
    ):
        _run_tool(script_relpath, "--day_utc", target_day, "--truth_root", str(truth_root))


def _refresh_active_session_for_final_convergence(*, truth_root: Path, target_day: str):
    from ops.tools.run_session_authority_orchestration_v1 import refresh_active_session_for_final_convergence_v1

    return refresh_active_session_for_final_convergence_v1(truth_root=truth_root, target_day=target_day, environment="PAPER")


def _run_final_convergence_tools(*, truth_root: Path, target_day: str) -> None:
    _refresh_active_session_for_final_convergence(truth_root=truth_root, target_day=target_day)
    _run_tool("ops/tools/run_paper_day_control_plane_v1.py", "--day_utc", target_day, "--truth_root", str(truth_root))
    _run_tool(
        "ops/tools/run_session_authority_status_v1.py",
        "--truth_root",
        str(truth_root),
        "--environment",
        "PAPER",
        "--mode",
        "WRITE",
        "--json",
    )
    _run_tool(
        "ops/tools/run_day_open_trigger_v1.py",
        "--day_utc",
        target_day,
        "--truth_root",
        str(truth_root),
        "--environment",
        "PAPER",
    )
    _run_tool("ops/tools/run_operator_day_authority_summary_v1.py", "--day_utc", target_day, "--truth_root", str(truth_root))
    _run_tool("ops/tools/run_day_failure_causality_v1.py", "--day_utc", target_day, "--truth_root", str(truth_root))


def _run_target_day_build(
    *,
    truth_root: Path,
    target_day: str,
    environment: str,
    ib_account: str,
) -> SessionAuthorityRefV1:
    source_refs: List[Dict[str, Any]] = []
    scripts = _downstream_build_cycle_scripts_for_environment_v1(environment)
    for script_relpath, script_args in (
        (
            "ops/tools/run_day_authority_decision_v1.py",
            ("--day_utc", target_day, "--truth_root", str(truth_root)),
        ),
    ):
        source_refs.append(
            _annotate_source_ref_for_closure(
                _run_tool(script_relpath, *script_args),
                downstream_build_cycle_scripts=scripts,
            )
        )

    if str(os.environ.get("C2_SKIP_SESSION_AUTHORITY_REENTRY") or "").strip().upper() != "YES":
        source_refs.append(
            _annotate_source_ref_for_closure(
                _run_tool(
                    "ops/tools/run_session_readiness_refresh_v1.py",
                    "--day_utc",
                    target_day,
                    "--truth_root",
                    str(truth_root),
                    "--build_fast_path",
                    "YES",
                ),
                downstream_build_cycle_scripts=scripts,
            )
        )

    source_refs.extend(_run_primary_sleeve_capability_initialization(target_day=target_day, environment=environment, ib_account=ib_account))

    from ops.tools.run_session_authority_control_plane_v1 import collect_target_day_build_artifact_rows_v1

    artifact_results = collect_target_day_build_artifact_rows_v1(
        truth_root=truth_root,
        target_day=target_day,
        environment=environment,
        ib_account=ib_account,
    )
    hidden_dependency_check_result = _compute_hidden_dependency_check_result(
        artifact_results=artifact_results,
        source_refs=source_refs,
        downstream_build_cycle_scripts=scripts,
    )
    payload = derive_target_day_build_payload_v1(
        truth_root=truth_root,
        target_day=target_day,
        artifact_results=artifact_results,
        source_refs=source_refs,
        hidden_dependency_check_result=hidden_dependency_check_result,
    )
    return write_target_day_build_v1(truth_root=truth_root, payload=payload)


def _run_target_day_build_pipeline_v1(
    *,
    truth_root: Path,
    target_day: str,
    environment: str,
    ib_account: str,
    downstream_build_cycle_scripts: set[str],
) -> SessionAuthorityRefV1:
    from ops.tools.run_session_authority_control_plane_v1 import (
        collect_target_day_build_artifact_rows_v1,
    )
    from ops.tools.run_session_authority_diagnostic_v1 import (
        compute_hidden_dependency_check_result_v1,
    )
    from ops.tools.run_session_authority_orchestration_v1 import (
        collect_target_day_build_source_refs_v1,
        finalize_target_day_build_v1,
        run_primary_sleeve_capability_initialization_v1,
    )

    source_refs = collect_target_day_build_source_refs_v1(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        target_day=target_day,
        environment=environment,
        ib_account=ib_account,
        downstream_build_cycle_scripts=downstream_build_cycle_scripts,
        run_primary_sleeve_capability_initialization_fn=run_primary_sleeve_capability_initialization_v1,
    )

    return finalize_target_day_build_v1(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        target_day=target_day,
        environment=environment,
        ib_account=ib_account,
        source_refs=source_refs,
        collect_target_day_build_artifact_rows_fn=collect_target_day_build_artifact_rows_v1,
        compute_hidden_dependency_check_result_fn=compute_hidden_dependency_check_result_v1,
        derive_target_day_build_payload_fn=derive_target_day_build_payload_v1,
        write_target_day_build_fn=write_target_day_build_v1,
        downstream_build_cycle_scripts=downstream_build_cycle_scripts,
    )


def _resolve_ib_account(environment: str, requested_ib_account: str) -> str:
    env = str(environment).strip().upper()
    requested = str(requested_ib_account or "").strip()
    if env == "PAPER" and not requested:
        return resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    binding = resolve_governed_account_binding(
        repo_root=REPO_ROOT,
        environment=env,
        requested_ib_account=requested,
    )
    return binding.ib_account


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_session_authority_v1")
    ap.add_argument("--target_day", default="")
    ap.add_argument("--truth_root", default="")
    ap.add_argument("--environment", default="PAPER", choices=["PAPER", "LIVE"])
    ap.add_argument("--ib_account", default="")
    ap.add_argument("--phase", default="all", choices=["build", "admit", "activate", "all"])
    args = ap.parse_args(argv)

    from ops.tools.run_session_authority_orchestration_v1 import (
        finalize_session_authority_cli_result_v1,
        run_authority_reporting_tools_v1,
        run_post_build_alignment_tools_v1,
        run_session_authority_phase_flow_v1,
    )

    target_day = resolve_session_authority_target_day_v1(args.target_day)
    truth_root = resolve_decision_truth_root_v1(args.truth_root or "", repo_root=REPO_ROOT)
    environment = str(args.environment).strip().upper()
    ib_account = _resolve_ib_account(environment, str(args.ib_account or "").strip())
    downstream_build_cycle_scripts = _downstream_build_cycle_scripts_for_environment_v1(environment)

    phase_bundle = run_session_authority_phase_flow_v1(
        truth_root=truth_root,
        target_day=target_day,
        environment=environment,
        ib_account=ib_account,
        phase=args.phase,
        repo_root=REPO_ROOT,
        build_fn=lambda: _run_target_day_build_pipeline_v1(
            truth_root=truth_root,
            target_day=target_day,
            environment=environment,
            ib_account=ib_account,
            downstream_build_cycle_scripts=downstream_build_cycle_scripts,
        ),
        post_build_alignment_fn=lambda root, day: run_post_build_alignment_tools_v1(
            repo_root=REPO_ROOT,
            truth_root=root,
            target_day=day,
            downstream_build_cycle_scripts=downstream_build_cycle_scripts,
        ),
        reporting_fn=lambda root, day, include_control_plane: run_authority_reporting_tools_v1(
            repo_root=REPO_ROOT,
            truth_root=root,
            target_day=day,
            environment=environment,
            include_control_plane=include_control_plane,
            downstream_build_cycle_scripts=downstream_build_cycle_scripts,
        ),
    )

    cli_result = finalize_session_authority_cli_result_v1(
        truth_root=truth_root,
        target_day=target_day,
        phase=args.phase,
        environment=environment,
        ib_account=ib_account,
        build_ref=phase_bundle["build_ref"],
        admission_ref=phase_bundle["admission_ref"],
        active_ref=phase_bundle["active_ref"],
    )
    print(json.dumps(dict(cli_result["summary"]), sort_keys=True))
    return int(cli_result["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
