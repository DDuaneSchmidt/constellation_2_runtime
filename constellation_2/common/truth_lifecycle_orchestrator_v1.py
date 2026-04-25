from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Mapping

from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_truth_day_run_ledger_path,
    resolve_truth_lifecycle_phase_result_path,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


PHASE_GRAPH_REGISTRY_RELPATH = "governance/02_REGISTRIES/C2_TRUTH_LIFECYCLE_PHASE_GRAPH_V1.json"
PHASE_RESULT_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/truth_lifecycle_phase_result.v1.schema.json"
DAY_RUN_LEDGER_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/truth_day_run_ledger.v1.schema.json"

VALID_PHASE_STATUSES = {"PASS", "FAIL", "BLOCKED", "SKIPPED", "BOOTSTRAP"}


@dataclass(frozen=True)
class LifecycleRunContextV1:
    repo_root: Path
    truth_root: Path
    day_utc: str
    produced_utc: str
    mode: str


@dataclass(frozen=True)
class PhaseRunResultV1:
    status: str
    first_blocker_code: str = ""
    first_blocker_artifact_path: str = ""
    upstream_dependency_id: str = ""
    upstream_dependency_artifact_path: str = ""
    reason_codes: tuple[str, ...] = ()
    produced_artifacts: tuple[str, ...] = ()
    producer_invocations: tuple[dict[str, Any], ...] = ()
    bootstrap_applied: bool = False
    bootstrap_reason: str = ""
    phase_payload: Mapping[str, Any] = field(default_factory=dict)


PhaseRunnerV1 = Callable[[LifecycleRunContextV1, dict[str, Any]], PhaseRunResultV1]


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _atomic_write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    os.replace(tmp, path)


def _as_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_reason_codes(reason_codes: Iterable[Any]) -> list[str]:
    return [code for code in (_as_text(item) for item in reason_codes) if code]


def _normalize_phase_result(raw: PhaseRunResultV1) -> PhaseRunResultV1:
    status = _as_text(raw.status).upper()
    if status not in VALID_PHASE_STATUSES:
        raise RuntimeError(f"LIFECYCLE_PHASE_STATUS_INVALID:{status or 'EMPTY'}")
    reason_codes = tuple(_normalize_reason_codes(raw.reason_codes))
    produced_artifacts = tuple(_as_text(path) for path in raw.produced_artifacts if _as_text(path))
    producer_invocations = tuple(dict(item) for item in raw.producer_invocations)
    first_blocker_code = _as_text(raw.first_blocker_code)
    if status in {"FAIL", "BLOCKED"} and not first_blocker_code:
        if reason_codes:
            first_blocker_code = reason_codes[0]
        else:
            first_blocker_code = f"PHASE_{status}"
    return PhaseRunResultV1(
        status=status,
        first_blocker_code=first_blocker_code,
        first_blocker_artifact_path=_as_text(raw.first_blocker_artifact_path),
        upstream_dependency_id=_as_text(raw.upstream_dependency_id),
        upstream_dependency_artifact_path=_as_text(raw.upstream_dependency_artifact_path),
        reason_codes=reason_codes,
        produced_artifacts=produced_artifacts,
        producer_invocations=producer_invocations,
        bootstrap_applied=bool(raw.bootstrap_applied),
        bootstrap_reason=_as_text(raw.bootstrap_reason),
        phase_payload=dict(raw.phase_payload or {}),
    )


def _blocked_skip_result(
    *,
    blocked_phase_id: str,
    first_blocker_code: str,
    first_blocker_artifact_path: str,
    upstream_dependency_id: str,
    upstream_dependency_artifact_path: str,
) -> PhaseRunResultV1:
    reason_codes = [f"UPSTREAM_PHASE_BLOCKED:{blocked_phase_id}"]
    if first_blocker_code:
        reason_codes.append(first_blocker_code)
    return PhaseRunResultV1(
        status="SKIPPED",
        first_blocker_code=first_blocker_code,
        first_blocker_artifact_path=first_blocker_artifact_path,
        upstream_dependency_id=upstream_dependency_id,
        upstream_dependency_artifact_path=upstream_dependency_artifact_path,
        reason_codes=tuple(reason_codes),
    )


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"REGISTRY_TOP_LEVEL_NOT_OBJECT:path={path}")
    return payload


def _read_optional_json(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _phase_graph_registry_path(repo_root: Path) -> Path:
    return (Path(repo_root).resolve() / PHASE_GRAPH_REGISTRY_RELPATH).resolve()


def load_truth_lifecycle_phase_graph_v1(*, repo_root: Path) -> dict[str, Any]:
    path = _phase_graph_registry_path(repo_root)
    payload = _read_json(path)
    phase_graph_id = _as_text(payload.get("phase_graph_id"))
    if phase_graph_id != "C2_TRUTH_LIFECYCLE_PHASE_GRAPH_V1":
        raise RuntimeError("PHASE_GRAPH_ID_MISMATCH")
    phases = payload.get("phases")
    if not isinstance(phases, list) or not phases:
        raise RuntimeError("PHASE_GRAPH_PHASES_EMPTY")
    seen: set[str] = set()
    for row in phases:
        if not isinstance(row, dict):
            raise RuntimeError("PHASE_GRAPH_PHASE_ROW_INVALID")
        phase_id = _as_text(row.get("phase_id")).upper()
        if not phase_id:
            raise RuntimeError("PHASE_GRAPH_PHASE_ID_MISSING")
        if phase_id in seen:
            raise RuntimeError(f"PHASE_GRAPH_PHASE_ID_DUPLICATE:{phase_id}")
        seen.add(phase_id)
    return payload


def _phase_payload(
    *,
    context: LifecycleRunContextV1,
    phase: dict[str, Any],
    result: PhaseRunResultV1,
    phase_result_path: Path,
) -> dict[str, Any]:
    phase_id = _as_text(phase.get("phase_id")).upper()
    payload = {
        "schema_id": "truth_lifecycle_phase_result",
        "schema_version": "v1",
        "day_utc": context.day_utc,
        "phase_id": phase_id,
        "description": _as_text(phase.get("description")),
        "truth_root": str(context.truth_root.resolve()),
        "produced_utc": context.produced_utc,
        "status": result.status,
        "first_blocker_code": result.first_blocker_code,
        "first_blocker_artifact_path": result.first_blocker_artifact_path,
        "upstream_dependency_id": result.upstream_dependency_id,
        "upstream_dependency_artifact_path": result.upstream_dependency_artifact_path,
        "reason_codes": list(result.reason_codes),
        "required_inputs": list(phase.get("required_inputs") or []),
        "producer_entrypoints": list(phase.get("producer_entrypoints") or []),
        "produced_artifacts": list(result.produced_artifacts),
        "declared_artifacts": list(phase.get("produced_artifacts") or []),
        "blocker_semantics": _as_text(phase.get("blocker_semantics")),
        "skippable": bool(phase.get("skippable") is True),
        "supports_genesis_bootstrap": bool(phase.get("supports_genesis_bootstrap") is True),
        "bootstrap_applied": bool(result.bootstrap_applied),
        "bootstrap_reason": result.bootstrap_reason,
        "producer_invocations": list(result.producer_invocations),
        "phase_payload": dict(result.phase_payload or {}),
        "phase_result_path": str(phase_result_path),
    }
    validate_against_repo_schema_v1(payload, context.repo_root, PHASE_RESULT_SCHEMA_RELPATH)
    return payload


def _ledger_payload(
    *,
    context: LifecycleRunContextV1,
    phase_graph: dict[str, Any],
    phase_result_records: list[dict[str, Any]],
    phase_result_paths: list[Path],
    producer_invocations: list[dict[str, Any]],
    first_blocker: dict[str, str],
    run_ledger_path: Path,
) -> dict[str, Any]:
    by_phase = {str(row.get("phase_id")): row for row in phase_result_records}
    authorization_status = _as_text(by_phase.get("AUTHORIZATION", {}).get("status"))
    execution_readiness_status = _as_text(by_phase.get("EXECUTION_READINESS", {}).get("status"))
    execution_status = _as_text(by_phase.get("EXECUTION", {}).get("status"))
    day_close_status = _as_text(by_phase.get("DAY_CLOSE", {}).get("status"))

    effective_first_blocker = dict(first_blocker)

    def _set_blocker_once(code: str, artifact_path: str, dependency_id: str) -> None:
        if effective_first_blocker.get("first_blocker_code"):
            return
        effective_first_blocker["first_blocker_code"] = str(code or "").strip()
        effective_first_blocker["first_blocker_artifact_path"] = str(artifact_path or "").strip()
        effective_first_blocker["upstream_dependency_id"] = str(dependency_id or "").strip()
        effective_first_blocker["upstream_dependency_artifact_path"] = str(artifact_path or "").strip()

    # Workstream 1 + 2 coherence: lifecycle run ledger must not report PASS while canonical
    # day admission/build surfaces are blocked or while truth/sleeve continuity diverges.
    build_path = (context.truth_root / "target_day_build_v1" / f"{context.day_utc}.json").resolve()
    build_obj = _read_optional_json(build_path)
    if isinstance(build_obj, dict):
        build_status = str(build_obj.get("build_status") or "").strip().upper()
        closure_status = str(build_obj.get("closure_status") or "").strip().upper()
        if build_status != "COMPLETE" or closure_status != "CLOSED":
            _set_blocker_once(
                "TARGET_DAY_BUILD_NOT_CLOSED",
                str(build_path),
                "target_day_build_v1",
            )

    admission_path = (context.truth_root / "target_day_admission_v1" / f"{context.day_utc}.json").resolve()
    admission_obj = _read_optional_json(admission_path)
    if isinstance(admission_obj, dict):
        admission_status = str(admission_obj.get("admission_status") or "").strip().upper()
        if admission_status != "ADMIT":
            _set_blocker_once(
                "TARGET_DAY_ADMISSION_NOT_ADMIT",
                str(admission_path),
                "target_day_admission_v1",
            )

    primary_sleeve_root = (context.truth_root.parent / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()
    if primary_sleeve_root.exists() and primary_sleeve_root.is_dir():
        truth_recon = (
            context.truth_root
            / "reports"
            / "reconciliation_report_v3"
            / context.day_utc
            / "reconciliation_report.v3.json"
        ).resolve()
        sleeve_recon = (
            primary_sleeve_root
            / "reports"
            / "reconciliation_report_v3"
            / context.day_utc
            / "reconciliation_report.v3.json"
        ).resolve()
        if sleeve_recon.exists() and not truth_recon.exists():
            _set_blocker_once(
                "TRUTH_SLEEVE_DIVERGENCE_RECONCILIATION_REPORT_V3",
                str(truth_recon),
                "reconciliation_report_v3",
            )

        truth_exit = (
            context.truth_root
            / "exit_reconciliation_v1"
            / context.day_utc
            / "exit_reconciliation.v1.json"
        ).resolve()
        sleeve_exit = (
            primary_sleeve_root
            / "exit_reconciliation_v1"
            / context.day_utc
            / "exit_reconciliation.v1.json"
        ).resolve()
        if sleeve_exit.exists() and not truth_exit.exists():
            _set_blocker_once(
                "TRUTH_SLEEVE_DIVERGENCE_EXIT_RECONCILIATION_V1",
                str(truth_exit),
                "exit_reconciliation_v1",
            )

    exec_control_path = (
        context.truth_root
        / "reports"
        / "trading_day_execution_control_plane_v1"
        / context.day_utc
        / "trading_day_execution_control_plane.v1.json"
    ).resolve()
    exec_control_obj = _read_optional_json(exec_control_path)
    if isinstance(exec_control_obj, dict):
        final_start_decision = str(exec_control_obj.get("final_start_decision") or "").strip().upper()
        if final_start_decision and final_start_decision != "READY_NOW":
            _set_blocker_once(
                f"TRADING_DAY_EXECUTION_CONTROL_PLANE_NOT_READY:{final_start_decision}",
                str(exec_control_path),
                "trading_day_execution_control_plane_v1",
            )

    overall_status = "PASS"
    if effective_first_blocker.get("first_blocker_code"):
        overall_status = "BLOCKED"

    phase_summary = [
        {
            "phase_id": str(row.get("phase_id") or ""),
            "status": str(row.get("status") or ""),
            "phase_result_path": str(path),
            "first_blocker_code": str(row.get("first_blocker_code") or ""),
            "first_blocker_artifact_path": str(row.get("first_blocker_artifact_path") or ""),
            "bootstrap_applied": bool(row.get("bootstrap_applied") is True),
        }
        for row, path in zip(phase_result_records, phase_result_paths, strict=True)
    ]
    structural_readiness_reached = authorization_status == "PASS" and overall_status == "PASS"
    execution_readiness_reached = execution_readiness_status in {"PASS", "BOOTSTRAP"} and overall_status == "PASS"
    execution_reached = execution_status == "PASS"
    day_close_reached = day_close_status == "PASS"
    business_no_trade = (
        structural_readiness_reached
        and execution_readiness_reached
        and execution_status == "SKIPPED"
        and day_close_reached
    )

    registry_path = _phase_graph_registry_path(context.repo_root)
    payload = {
        "schema_id": "truth_day_run_ledger",
        "schema_version": "v1",
        "day_utc": context.day_utc,
        "mode": context.mode,
        "truth_root": str(context.truth_root.resolve()),
        "produced_utc": context.produced_utc,
        "phase_graph_id": _as_text(phase_graph.get("phase_graph_id")),
        "phase_graph_version": _as_text(phase_graph.get("phase_graph_version")),
        "phase_graph_ref": {
            "path": str(registry_path),
            "sha256": _sha256_file(registry_path),
        },
        "overall_status": overall_status,
        "first_blocker_code": effective_first_blocker.get("first_blocker_code", ""),
        "first_blocker_artifact_path": effective_first_blocker.get("first_blocker_artifact_path", ""),
        "upstream_dependency_id": effective_first_blocker.get("upstream_dependency_id", ""),
        "upstream_dependency_artifact_path": effective_first_blocker.get("upstream_dependency_artifact_path", ""),
        "bootstrap_applied": any(bool(row.get("bootstrap_applied") is True) for row in phase_result_records),
        "phase_results": phase_summary,
        "producer_invocations": producer_invocations,
        "structural_readiness_reached": structural_readiness_reached,
        "business_no_trade": business_no_trade,
        "execution_readiness_reached": execution_readiness_reached,
        "execution_reached": execution_reached,
        "day_close_reached": day_close_reached,
        "run_ledger_path": str(run_ledger_path),
    }
    validate_against_repo_schema_v1(payload, context.repo_root, DAY_RUN_LEDGER_SCHEMA_RELPATH)
    return payload


def run_truth_lifecycle_orchestrator_v1(
    *,
    context: LifecycleRunContextV1,
    phase_runners: Mapping[str, PhaseRunnerV1],
) -> dict[str, Any]:
    phase_graph = load_truth_lifecycle_phase_graph_v1(repo_root=context.repo_root)
    phases = list(phase_graph.get("phases") or [])

    blocked_phase_id = ""
    first_blocker = {
        "first_blocker_code": "",
        "first_blocker_artifact_path": "",
        "upstream_dependency_id": "",
        "upstream_dependency_artifact_path": "",
    }
    phase_result_records: list[dict[str, Any]] = []
    phase_result_paths: list[Path] = []
    producer_invocations: list[dict[str, Any]] = []

    for phase in phases:
        phase_id = _as_text(phase.get("phase_id")).upper()
        phase_result_path = resolve_truth_lifecycle_phase_result_path(
            truth_root=context.truth_root,
            day_utc=context.day_utc,
            phase_id=phase_id,
        )
        if blocked_phase_id:
            result = _blocked_skip_result(
                blocked_phase_id=blocked_phase_id,
                first_blocker_code=first_blocker["first_blocker_code"],
                first_blocker_artifact_path=first_blocker["first_blocker_artifact_path"],
                upstream_dependency_id=first_blocker["upstream_dependency_id"],
                upstream_dependency_artifact_path=first_blocker["upstream_dependency_artifact_path"],
            )
        else:
            runner = phase_runners.get(phase_id)
            if runner is None:
                result = PhaseRunResultV1(
                    status="BLOCKED",
                    first_blocker_code="PHASE_RUNNER_MISSING",
                    reason_codes=(f"PHASE_RUNNER_MISSING:{phase_id}",),
                )
            else:
                try:
                    result = _normalize_phase_result(runner(context, phase))
                except Exception as exc:  # noqa: BLE001
                    code = f"PHASE_EXCEPTION:{phase_id}"
                    result = PhaseRunResultV1(
                        status="BLOCKED",
                        first_blocker_code=code,
                        reason_codes=(code, repr(exc)),
                    )
            result = _normalize_phase_result(result)
            if result.status in {"FAIL", "BLOCKED"} and not blocked_phase_id:
                blocked_phase_id = phase_id
                first_blocker = {
                    "first_blocker_code": result.first_blocker_code,
                    "first_blocker_artifact_path": result.first_blocker_artifact_path,
                    "upstream_dependency_id": result.upstream_dependency_id or phase_id,
                    "upstream_dependency_artifact_path": result.upstream_dependency_artifact_path,
                }

        phase_payload = _phase_payload(
            context=context,
            phase=phase,
            result=result,
            phase_result_path=phase_result_path,
        )
        _atomic_write(phase_result_path, phase_payload)
        phase_result_records.append(phase_payload)
        phase_result_paths.append(phase_result_path)
        producer_invocations.extend(list(result.producer_invocations))

    run_ledger_path = resolve_truth_day_run_ledger_path(
        truth_root=context.truth_root,
        day_utc=context.day_utc,
    )
    ledger_payload = _ledger_payload(
        context=context,
        phase_graph=phase_graph,
        phase_result_records=phase_result_records,
        phase_result_paths=phase_result_paths,
        producer_invocations=producer_invocations,
        first_blocker=first_blocker,
        run_ledger_path=run_ledger_path,
    )
    _atomic_write(run_ledger_path, ledger_payload)
    return {
        "phase_results": phase_result_records,
        "phase_result_paths": [str(path) for path in phase_result_paths],
        "run_ledger_path": str(run_ledger_path),
        "run_ledger": ledger_payload,
    }
