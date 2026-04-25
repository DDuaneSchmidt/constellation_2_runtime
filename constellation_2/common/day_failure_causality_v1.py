from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict

from constellation_2.common.capability_state_v1 import resolve_capability_state_path, resolve_paper_policy_verdict_path
from constellation_2.common.operator_day_authority_summary_v1 import build_operator_day_authority_summary_payload
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    now_utc_iso_v1,
    producer_block_v1,
    read_json_object_v1,
    resolve_fact_plane_truth_root_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_bod_execution_environment_proof_path,
    resolve_day_failure_causality_path,
    resolve_day_open_attempt_path,
    resolve_day_open_trigger_path,
    resolve_paper_session_ledger_path,
    resolve_phasec_risk_inputs_prep_path,
    resolve_report_artifact_path,
    resolve_startup_materialization_path,
    resolve_submit_boundary_status_path,
    resolve_trading_day_state_machine_path,
)
from constellation_2.common.session_authority_monitor_v1 import resolve_session_authority_status_path
from constellation_2.common.session_authority_v1 import resolve_target_day_admission_path, resolve_target_day_build_path


SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/day_failure_causality.v1.schema.json"


def _artifact_status_ok(payload: Dict[str, Any], pass_values: set[str], status_field: str) -> bool:
    value = str(payload.get(status_field) or "").strip().upper()
    return value in pass_values


def _startup_ok(payload: Dict[str, Any]) -> bool:
    return str(payload.get("status") or "").strip().upper() == "SUCCESS"


def _submit_boundary_ok(payload: Dict[str, Any]) -> bool:
    return (
        str(payload.get("boundary_status") or "").strip().upper() == "AUTHORIZED"
        and bool(payload.get("submission_authorized") is True)
    )


def _ledger_ok(payload: Dict[str, Any]) -> bool:
    return str(
        payload.get("authority_status")
        or ((payload.get("control_state") or {}).get("authority_status") if isinstance(payload.get("control_state"), dict) else "")
        or ""
    ).strip().upper() == "GRANTED"


def _build_ok(payload: Dict[str, Any]) -> bool:
    return (
        str(payload.get("build_status") or "").strip().upper() == "COMPLETE"
        and str(payload.get("closure_status") or "").strip().upper() == "CLOSED"
        and not list(payload.get("blocker_chain") or [])
    )


def _admission_ok(payload: Dict[str, Any]) -> bool:
    return str(payload.get("admission_status") or "").strip().upper() == "ADMIT"


def _session_authority_ok(payload: Dict[str, Any]) -> bool:
    return str(payload.get("rollover_status") or "").strip().upper() in {"ROLLOVER_COMPLETED", "ACTIVE_SESSION_CONFIRMED"}


def _state_machine_ok(payload: Dict[str, Any]) -> bool:
    return str(payload.get("final_start_decision") or "").strip().upper() == "READY_NOW"


def _day_open_trigger_ok(payload: Dict[str, Any]) -> bool:
    status = str(payload.get("trigger_status") or "").strip().upper()
    return status in {"EMITTED", "SUPPRESSED_PRE_OPEN", "SUPPRESSED_AUTHORITY_NOT_GRANTED", "SUPPRESSED_ACTIVE_SESSION_NOT_BOUND", "SUPPRESSED_ADMISSION_NOT_ADMIT"}


def _day_open_attempt_ok(payload: Dict[str, Any]) -> bool:
    classification = str(payload.get("final_classification") or "").strip().upper()
    return classification not in {"OPEN_FAILED", "OPEN_MISSED"}


def _first_code_from_list(values: Any) -> str:
    if isinstance(values, list):
        for item in values:
            text = str(item).strip()
            if text:
                return text
    return ""


def _extract_artifact_details(artifact_id: str, payload: Dict[str, Any]) -> tuple[str, str, int | None, str]:
    if artifact_id == "bod_execution_environment_proof_v1":
        probe = payload.get("bridge_import_probe") if isinstance(payload.get("bridge_import_probe"), dict) else {}
        return (
            _first_code_from_list(payload.get("blocking_codes")),
            "status",
            int(probe["returncode"]) if isinstance(probe.get("returncode"), int) else None,
            str(probe.get("stderr") or "").strip(),
        )
    if artifact_id == "phasec_risk_inputs_prep_v1":
        bridge = payload.get("bridge_result") if isinstance(payload.get("bridge_result"), dict) else {}
        return (
            _first_code_from_list(payload.get("blocking_codes")),
            "blocking_codes",
            int(bridge["returncode"]) if isinstance(bridge.get("returncode"), int) else None,
            str(bridge.get("stderr") or "").strip(),
        )
    if artifact_id == "startup_materialization_v1":
        result = payload.get("phasec_materializer_result") if isinstance(payload.get("phasec_materializer_result"), dict) else {}
        return (
            _first_code_from_list(payload.get("blocking_codes")),
            "blocking_codes",
            int(result["returncode"]) if isinstance(result.get("returncode"), int) else None,
            str(result.get("stderr") or "").strip(),
        )
    if artifact_id == "capability_state_v1":
        capabilities = payload.get("capabilities") if isinstance(payload.get("capabilities"), list) else []
        for row in capabilities:
            if not isinstance(row, dict):
                continue
            if str(row.get("status") or "").strip().upper() == "FAIL":
                code = _first_code_from_list(row.get("reason_codes")) or f"CAPABILITY_FAIL:{str(row.get('capability_id') or '').strip()}"
                return (code, f"capabilities[{str(row.get('capability_id') or '').strip()}]", None, "")
        return ("", "", None, "")
    if artifact_id == "paper_policy_verdict_v1":
        blocking_items = payload.get("blocking_items") if isinstance(payload.get("blocking_items"), list) else []
        for row in blocking_items:
            if not isinstance(row, dict):
                continue
            code = _first_code_from_list(row.get("reason_codes")) or f"PAPER_POLICY_NOT_PASS:{str(row.get('capability_id') or '').strip()}"
            return (code, "blocking_items", None, "")
        return ("", "", None, "")
    if artifact_id == "submit_boundary_status_v1":
        return (_first_code_from_list(payload.get("blocking_codes")), "blocking_codes", None, "")
    if artifact_id == "paper_session_ledger_v1":
        return (_first_code_from_list(payload.get("reason_codes")), "reason_codes", None, "")
    if artifact_id == "target_day_build_v1":
        blocker_chain = payload.get("blocker_chain") if isinstance(payload.get("blocker_chain"), list) else []
        if blocker_chain and isinstance(blocker_chain[0], dict):
            return (
                str(blocker_chain[0].get("blocker_code") or "").strip(),
                "blocker_chain[0].blocker_code",
                None,
                "",
            )
        return (str((payload.get("hidden_dependency_check_result") or {}).get("blocking_reason_code") or "").strip(), "hidden_dependency_check_result.blocking_reason_code", None, "")
    if artifact_id == "target_day_admission_v1":
        return (_first_code_from_list(payload.get("blocking_reason_codes")), "blocking_reason_codes", None, "")
    if artifact_id == "session_authority_status_v1":
        return (
            str(payload.get("first_real_blocker_code") or payload.get("rollover_reason_code") or "").strip(),
            "first_real_blocker_code",
            None,
            "",
        )
    if artifact_id == "trading_day_state_machine_v1":
        first_true_blocker = payload.get("first_true_blocker") if isinstance(payload.get("first_true_blocker"), dict) else {}
        return (
            str(first_true_blocker.get("first_true_blocker_code") or "").strip(),
            "first_true_blocker.first_true_blocker_code",
            None,
            "",
        )
    if artifact_id == "day_open_trigger_v1":
        return (
            str(payload.get("trigger_reason_code") or "").strip(),
            "trigger_reason_code",
            None,
            "",
        )
    if artifact_id == "day_open_attempt_v1":
        return (
            _first_code_from_list(payload.get("reason_codes")) or str(payload.get("result_code") or "").strip(),
            "reason_codes",
            None,
            "",
        )
    return ("", "", None, "")


def _artifact_entry(
    *,
    artifact_id: str,
    path: Path,
    owner_tool: str,
    owner_contract: str,
    ok_fn: Callable[[Dict[str, Any]], bool],
) -> Dict[str, Any]:
    resolved = Path(path).resolve()
    if not resolved.exists() or not resolved.is_file():
        missing_code = f"{artifact_id.upper()}_MISSING"
        return {
            "artifact_id": artifact_id,
            "artifact_path": str(resolved),
            "owner_tool": owner_tool,
            "owner_contract": owner_contract,
            "ok": False,
            "observed_status": "MISSING",
            "failing_field": "",
            "failing_code": missing_code,
            "returncode": None,
            "stderr": "",
        }
    payload = read_json_object_v1(resolved)
    failing_code, failing_field, returncode, stderr = _extract_artifact_details(artifact_id, payload)
    return {
        "artifact_id": artifact_id,
        "artifact_path": str(resolved),
        "owner_tool": owner_tool,
        "owner_contract": owner_contract,
        "ok": bool(ok_fn(payload)),
        "observed_status": str(
            payload.get("status")
            or payload.get("overall_status")
            or payload.get("admission_status")
            or payload.get("build_status")
            or payload.get("final_start_decision")
            or payload.get("rollover_status")
            or payload.get("boundary_status")
            or payload.get("authority_status")
            or ""
        ).strip(),
        "failing_field": failing_field,
        "failing_code": failing_code,
        "returncode": returncode,
        "stderr": stderr,
    }


def build_day_failure_causality_payload(*, truth_root: Path, day_utc: str) -> Dict[str, Any]:
    root = resolve_fact_plane_truth_root_v1(truth_root)
    operator_summary = build_operator_day_authority_summary_payload(truth_root=root, day_utc=day_utc)
    final_operator_classification = str(operator_summary.get("startup_open_status") or "").strip()
    if final_operator_classification not in {"BLOCKED", "FAILED", "OPEN_MISSED", "OPEN_FAILED", "LATE_OPEN_EXHAUSTED"}:
        return {
            "schema_id": "day_failure_causality",
            "schema_version": "v1",
            "day_utc": str(day_utc).strip(),
            "produced_at_utc": now_utc_iso_v1(),
            "producer": producer_block_v1(module="constellation_2/common/day_failure_causality_v1.py"),
            "status": "NO_FAILURE_DETECTED",
            "final_operator_classification": final_operator_classification,
            "first_failing_artifact_id": "",
            "first_failing_artifact_path": "",
            "first_failing_field": "",
            "first_failing_code": "",
            "first_failing_owner_tool": "",
            "first_failing_owner_contract": "",
            "direct_returncode": None,
            "direct_stderr": "",
            "propagation_chain": [],
            "operator_message": str(operator_summary.get("operator_message") or "").strip(),
        }

    pipeline: list[Dict[str, Any]] = []
    trigger_path = resolve_day_open_trigger_path(truth_root=root, day_utc=day_utc)
    attempt_path = resolve_day_open_attempt_path(truth_root=root, day_utc=day_utc)
    if final_operator_classification in {"OPEN_MISSED", "OPEN_FAILED", "LATE_OPEN_EXHAUSTED"} or trigger_path.exists():
        pipeline.append(
            _artifact_entry(
                artifact_id="day_open_trigger_v1",
                path=trigger_path,
                owner_tool="ops/tools/run_day_open_trigger_v1.py",
                owner_contract="governance/05_CONTRACTS/C2/day_open_trigger_v1.contract.md",
                ok_fn=_day_open_trigger_ok,
            )
        )
    if final_operator_classification in {"OPEN_MISSED", "OPEN_FAILED", "LATE_OPEN_EXHAUSTED"} or attempt_path.exists():
        pipeline.append(
            _artifact_entry(
                artifact_id="day_open_attempt_v1",
                path=attempt_path,
                owner_tool="ops/tools/run_day_open_attempt_v1.py",
                owner_contract="governance/05_CONTRACTS/C2/day_open_attempt_v1.contract.md",
                ok_fn=_day_open_attempt_ok,
            )
        )
    pipeline.extend([
        _artifact_entry(
            artifact_id="bod_execution_environment_proof_v1",
            path=resolve_bod_execution_environment_proof_path(truth_root=root, day_utc=day_utc),
            owner_tool="ops/tools/run_bod_execution_environment_proof_v1.py",
            owner_contract="governance/05_CONTRACTS/C2/bod_execution_environment_proof_v1.contract.md",
            ok_fn=lambda payload: _artifact_status_ok(payload, {"PASS"}, "status"),
        ),
        _artifact_entry(
            artifact_id="phasec_risk_inputs_prep_v1",
            path=resolve_phasec_risk_inputs_prep_path(truth_root=root, day_utc=day_utc),
            owner_tool="ops/tools/run_phasec_risk_inputs_prep_v1.py",
            owner_contract="governance/05_CONTRACTS/C2/phasec_risk_inputs_prep_v1.contract.md",
            ok_fn=lambda payload: _artifact_status_ok(payload, {"PASS"}, "status"),
        ),
        _artifact_entry(
            artifact_id="startup_materialization_v1",
            path=resolve_startup_materialization_path(truth_root=root, day_utc=day_utc),
            owner_tool="ops/tools/run_startup_materialization_v1.py",
            owner_contract="governance/05_CONTRACTS/C2/startup_materialization_v1.contract.md",
            ok_fn=_startup_ok,
        ),
        _artifact_entry(
            artifact_id="capability_state_v1",
            path=resolve_capability_state_path(truth_root=root, day_utc=day_utc),
            owner_tool="ops/tools/run_capability_state_v1.py",
            owner_contract="governance/05_CONTRACTS/C2/capability_state_v1.contract.md",
            ok_fn=lambda payload: _artifact_status_ok(payload, {"PASS"}, "overall_status"),
        ),
        _artifact_entry(
            artifact_id="paper_policy_verdict_v1",
            path=resolve_paper_policy_verdict_path(truth_root=root, day_utc=day_utc),
            owner_tool="ops/tools/run_paper_policy_verdict_v1.py",
            owner_contract="governance/05_CONTRACTS/C2/paper_policy_verdict_v1.contract.md",
            ok_fn=lambda payload: _artifact_status_ok(payload, {"PASS"}, "overall_status"),
        ),
        _artifact_entry(
            artifact_id="submit_boundary_status_v1",
            path=resolve_submit_boundary_status_path(truth_root=root, day_utc=day_utc),
            owner_tool="ops/tools/run_submit_boundary_status_v1.py",
            owner_contract="governance/05_CONTRACTS/C2/submit_boundary_status_v1.contract.md",
            ok_fn=_submit_boundary_ok,
        ),
        _artifact_entry(
            artifact_id="paper_session_ledger_v1",
            path=resolve_paper_session_ledger_path(truth_root=root, day_utc=day_utc),
            owner_tool="ops/tools/run_paper_session_ledger_v1.py",
            owner_contract="governance/05_CONTRACTS/C2/paper_session_ledger_v1.contract.md",
            ok_fn=_ledger_ok,
        ),
        _artifact_entry(
            artifact_id="target_day_build_v1",
            path=resolve_target_day_build_path(truth_root=root, target_day=day_utc),
            owner_tool="ops/tools/run_session_authority_v1.py",
            owner_contract="governance/05_CONTRACTS/C2/target_day_build_v1.contract.md",
            ok_fn=_build_ok,
        ),
        _artifact_entry(
            artifact_id="target_day_admission_v1",
            path=resolve_target_day_admission_path(truth_root=root, target_day=day_utc),
            owner_tool="ops/tools/run_session_authority_v1.py",
            owner_contract="governance/05_CONTRACTS/C2/target_day_admission_v1.contract.md",
            ok_fn=_admission_ok,
        ),
        _artifact_entry(
            artifact_id="session_authority_status_v1",
            path=resolve_session_authority_status_path(truth_root=root),
            owner_tool="ops/tools/run_session_authority_status_v1.py",
            owner_contract="governance/05_CONTRACTS/C2/session_authority_v1.contract.md",
            ok_fn=_session_authority_ok,
        ),
        _artifact_entry(
            artifact_id="trading_day_state_machine_v1",
            path=resolve_trading_day_state_machine_path(truth_root=root, day_utc=day_utc),
            owner_tool="ops/tools/run_trading_day_state_machine_v1.py",
            owner_contract="governance/05_CONTRACTS/C2/trading_day_state_machine_v1.contract.md",
            ok_fn=_state_machine_ok,
        ),
    ])
    first_failure = next((row for row in pipeline if not row["ok"]), None)
    propagation_chain = [
        {
            "artifact_id": row["artifact_id"],
            "artifact_path": row["artifact_path"],
            "observed_status": row["observed_status"],
            "failing_code": row["failing_code"],
        }
        for row in pipeline
        if not row["ok"]
    ]
    return {
        "schema_id": "day_failure_causality",
        "schema_version": "v1",
        "day_utc": str(day_utc).strip(),
        "produced_at_utc": now_utc_iso_v1(),
        "producer": producer_block_v1(module="constellation_2/common/day_failure_causality_v1.py"),
        "status": "NO_FAILURE_DETECTED" if first_failure is None else "FAILURE_DETECTED",
        "final_operator_classification": final_operator_classification,
        "first_failing_artifact_id": str(first_failure["artifact_id"]) if first_failure else "",
        "first_failing_artifact_path": str(first_failure["artifact_path"]) if first_failure else "",
        "first_failing_field": str(first_failure["failing_field"]) if first_failure else "",
        "first_failing_code": str(first_failure["failing_code"]) if first_failure else "",
        "first_failing_owner_tool": str(first_failure["owner_tool"]) if first_failure else "",
        "first_failing_owner_contract": str(first_failure["owner_contract"]) if first_failure else "",
        "direct_returncode": first_failure["returncode"] if first_failure else None,
        "direct_stderr": str(first_failure["stderr"]) if first_failure else "",
        "propagation_chain": propagation_chain,
        "operator_message": str(operator_summary.get("operator_message") or "").strip(),
    }


def write_day_failure_causality_v1(*, truth_root: Path, payload: Dict[str, Any]) -> SurfaceRefV1:
    root = resolve_fact_plane_truth_root_v1(truth_root)
    return atomic_write_idempotent_validated_json_v1(
        path=resolve_day_failure_causality_path(
            truth_root=root,
            day_utc=str(payload.get("day_utc") or "").strip(),
        ),
        payload=payload,
        schema_relpath=SCHEMA_RELPATH,
        volatile_field_names=("produced_at_utc",),
    )
