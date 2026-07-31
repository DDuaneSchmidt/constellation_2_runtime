from __future__ import annotations

import json
import os
import socket
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from typing import Any, Iterator

from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT

from .artifact_store import ArtifactStore, DEFAULT_STORE_ROOT
from .failure_observatory import record_exception, record_failure, record_worker_failure
from .orchestrator_models import OrchestratorError, OrchestratorRunRecord, OrchestratorStatus, TriggerType
from .orchestrator_reports import write_orchestrator_report
from .run_ledger import write_run_ledger
from .stage_controller import select_backlog_items, validate_run_eligibility
from .research_backlog import ResearchBacklog
from .worker_adapters import register_default_worker_adapters
from .worker_connection_rules import is_worker_connection_allowed
from .worker_governance import validate_worker_execution
from .worker_registry import get_worker


def run_once(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    trigger_type: str = TriggerType.MANUAL.value,
    trigger_source: dict[str, Any] | None = None,
    truth_root: str | Path = DEFAULT_TRUTH_ROOT,
    day: str | None = None,
    seed: int | None = None,
) -> dict[str, Any]:
    return _execute(
        root,
        mode="run_once",
        trigger_type=trigger_type,
        trigger_source=trigger_source or {"source": "orchestrator_run_once"},
        truth_root=truth_root,
        day=day,
        seed=seed,
    )


def dry_run(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    trigger_type: str = TriggerType.MANUAL.value,
    trigger_source: dict[str, Any] | None = None,
    truth_root: str | Path = DEFAULT_TRUTH_ROOT,
    day: str | None = None,
    seed: int | None = None,
) -> dict[str, Any]:
    return _execute(
        root,
        mode="dry_run",
        trigger_type=trigger_type,
        trigger_source=trigger_source or {"source": "orchestrator_dry_run"},
        truth_root=truth_root,
        day=day,
        seed=seed,
    )


def audit_only(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    trigger_type: str = TriggerType.MANUAL.value,
    trigger_source: dict[str, Any] | None = None,
    truth_root: str | Path = DEFAULT_TRUTH_ROOT,
    day: str | None = None,
) -> dict[str, Any]:
    return _execute(
        root,
        mode="audit_only",
        trigger_type=trigger_type,
        trigger_source=trigger_source or {"source": "orchestrator_audit_only"},
        truth_root=truth_root,
        day=day,
    )


def _execute(
    root: str | Path,
    *,
    mode: str,
    trigger_type: str,
    trigger_source: dict[str, Any],
    truth_root: str | Path,
    day: str | None,
    seed: int | None = None,
) -> dict[str, Any]:
    started_monotonic = monotonic()
    started_at = _now()
    run_id = _new_run_id(started_at, trigger_type)
    root_path = Path(root)
    runtime_truth = _load_runtime_truth(truth_root, day)
    verified_graph = _load_verified_graph(truth_root, day)
    try:
        with _local_lock(root_path, run_id, trigger_type, trigger_source) as acquired:
            if not acquired:
                record = _record(
                    run_id=run_id,
                    started_at=started_at,
                    started_monotonic=started_monotonic,
                    status=OrchestratorStatus.SKIPPED_ALREADY_RUNNING.value,
                    trigger_type=trigger_type,
                    trigger_source=trigger_source,
                    skip_reason="LOCK_HELD",
                )
                _persist(root_path, record)
                return record
            selected = [] if mode == "audit_only" else select_backlog_items(root_path, limit=1, seed=seed)
            eligibility = validate_run_eligibility(root_path, selected_backlog_items=selected, runtime_truth=runtime_truth, verified_graph=verified_graph, require_backlog_item=mode != "audit_only")
            worker_invocation = {"workers_invoked": [], "artifacts_created": [], "errors": []}
            if mode in {"run_once", "dry_run"} and eligibility["eligible"] and selected:
                worker_invocation = _invoke_connected_worker(root_path, selected, mode=mode, run_id=run_id)
            if worker_invocation["errors"]:
                status = OrchestratorStatus.FAILED_SAFETY_GATE.value
            elif mode == "dry_run":
                status = OrchestratorStatus.DRY_RUN_COMPLETED.value
            elif mode == "audit_only":
                status = OrchestratorStatus.AUDIT_ONLY_COMPLETED.value
            elif eligibility["eligible"]:
                status = OrchestratorStatus.COMPLETED.value
            else:
                status = OrchestratorStatus.FAILED_SAFETY_GATE.value if eligibility["safety_gate_results"] else OrchestratorStatus.FAILED.value
            errors = []
            if status == OrchestratorStatus.FAILED_SAFETY_GATE.value:
                if worker_invocation["errors"]:
                    errors.extend(worker_invocation["errors"])
                else:
                    errors.append(OrchestratorError(stage="validate_eligibility", code=str(eligibility.get("skip_reason") or "SAFETY_GATE_FAILED"), message="Orchestrator failed closed before worker invocation.", recoverable=True).to_dict())
            record = _record(
                run_id=run_id,
                started_at=started_at,
                started_monotonic=started_monotonic,
                status=status,
                trigger_type=trigger_type,
                trigger_source=trigger_source,
                selected_backlog_items=eligibility["selected_backlog_items"],
                safety_gate_results=eligibility["safety_gate_results"],
                lineage_validation_result=eligibility["lineage_validation_result"],
                workers_invoked=worker_invocation["workers_invoked"],
                artifacts_created=worker_invocation["artifacts_created"],
                errors=errors,
                skip_reason=eligibility.get("skip_reason"),
            )
            _record_orchestrator_failures(root_path, record)
            _persist(root_path, record)
            return record
    except Exception as exc:
        record = _record(
            run_id=run_id,
            started_at=started_at,
            started_monotonic=started_monotonic,
            status=OrchestratorStatus.FAILED.value,
            trigger_type=trigger_type,
            trigger_source=trigger_source,
            errors=[OrchestratorError(stage="orchestrator", code=exc.__class__.__name__, message=str(exc), recoverable=False).to_dict()],
        )
        record_exception(exc, root=root_path, component="ORCHESTRATOR", run_id=run_id, recoverable=False)
        _persist(root_path, record)
        return record


def _record_orchestrator_failures(root: Path, record: dict[str, Any]) -> None:
    for error in record.get("errors", []) or []:
        component = "WORKER" if str(error.get("stage") or "").startswith("worker") else "ORCHESTRATOR"
        if component == "WORKER":
            record_worker_failure(root=root, error_type=str(error.get("code") or "WORKER_FAILURE"), error_message=str(error.get("message") or ""), run_id=str(record.get("run_id") or ""), recoverable=bool(error.get("recoverable")), metadata={"stage": error.get("stage")})
        else:
            record_failure(root=root, component="ORCHESTRATOR", severity="ERROR", error_type=str(error.get("code") or "ORCHESTRATOR_FAILURE"), error_message=str(error.get("message") or ""), run_id=str(record.get("run_id") or ""), recoverable=bool(error.get("recoverable")), metadata={"stage": error.get("stage")})


def _invoke_connected_worker(root: Path, selected: list[dict[str, Any]], *, mode: str, run_id: str) -> dict[str, Any]:
    register_default_worker_adapters(replace=True)
    store = ArtifactStore(root)
    backlog = ResearchBacklog(root)
    item_id = str(selected[0].get("backlog_item_id") or "")
    item = backlog.get(item_id) if item_id else None
    input_ids = [str(item_id) for item_id in (item or {}).get("source_artifact_ids", []) if item_id]
    input_artifacts = []
    errors: list[dict[str, Any]] = []
    for artifact_id in input_ids:
        try:
            input_artifacts.append(store.get_artifact(artifact_id))
        except Exception as exc:
            errors.append(OrchestratorError(stage="worker_input", code="INPUT_ARTIFACT_MISSING", message=str(exc), recoverable=True).to_dict())
    if errors:
        return {"workers_invoked": [], "artifacts_created": [], "errors": errors}
    worker = _select_connected_worker(input_artifacts)
    if worker is None:
        return {
            "workers_invoked": [],
            "artifacts_created": [],
            "errors": [OrchestratorError(stage="worker_registry", code="NO_CONNECTED_WORKER", message="No connected worker accepts the selected backlog inputs.", recoverable=True).to_dict()],
        }
    try:
        if mode == "dry_run":
            result = worker.dry_run(input_artifacts, metadata={"root": str(root), "orchestrator_run_id": run_id})
        else:
            result = worker.run(input_artifacts, metadata={"root": str(root), "orchestrator_run_id": run_id})
    except Exception as exc:
        return {
            "workers_invoked": [],
            "artifacts_created": [],
            "errors": [OrchestratorError(stage="worker_execution", code=exc.__class__.__name__, message=str(exc), recoverable=False).to_dict()],
        }
    governance = validate_worker_execution(worker, result)
    artifacts_created = []
    if mode != "dry_run":
        for artifact_id in result.output_artifact_ids:
            try:
                artifacts_created.append(store.get_artifact(artifact_id))
            except Exception as exc:
                errors.append(OrchestratorError(stage="worker_output", code="OUTPUT_ARTIFACT_MISSING", message=str(exc), recoverable=False).to_dict())
    if governance.status != "PASS" or result.status == "VALIDATION_FAILED":
        errors.append(
            OrchestratorError(
                stage="worker_governance",
                code="WORKER_VALIDATION_FAILED",
                message="Connected worker validation failed.",
                recoverable=True,
            ).to_dict()
        )
    return {"workers_invoked": [result.to_dict()], "artifacts_created": artifacts_created, "errors": errors}


def _select_connected_worker(input_artifacts: list[dict[str, Any]]):
    input_types = {str(item.get("artifact_type")) for item in input_artifacts}
    preferred = [
        "atlas_v2_claim_worker_adapter",
        "atlas_v2_hypothesis_worker_adapter",
        "atlas_v2_experiment_design_worker_adapter",
        "atlas_v2_learning_worker_adapter",
        "atlas_v2_evaluation_worker_adapter",
    ]
    for worker_id in preferred:
        worker = get_worker(worker_id)
        if not is_worker_connection_allowed(worker):
            continue
        supported = set(worker.supported_input_artifact_types)
        if input_types and input_types.issubset(supported):
            return worker
    for worker_id in preferred:
        worker = get_worker(worker_id)
        if is_worker_connection_allowed(worker) and any(item in set(worker.supported_input_artifact_types) for item in input_types):
            return worker
    return None


def _record(
    *,
    run_id: str,
    started_at: str,
    started_monotonic: float,
    status: str,
    trigger_type: str,
    trigger_source: dict[str, Any],
    selected_backlog_items: list[dict[str, Any]] | None = None,
    safety_gate_results: list[dict[str, Any]] | None = None,
    lineage_validation_result: dict[str, Any] | None = None,
    errors: list[dict[str, Any]] | None = None,
    workers_invoked: list[dict[str, Any]] | None = None,
    artifacts_created: list[dict[str, Any]] | None = None,
    skip_reason: str | None = None,
) -> dict[str, Any]:
    completed_at = _now()
    return OrchestratorRunRecord(
        run_id=run_id,
        started_at=started_at,
        completed_at=completed_at,
        status=status,
        trigger_type=trigger_type,
        trigger_source=trigger_source,
        selected_backlog_items=list(selected_backlog_items or []),
        workers_invoked=list(workers_invoked or []),
        artifacts_created=list(artifacts_created or []),
        safety_gate_results=list(safety_gate_results or []),
        lineage_validation_result=dict(lineage_validation_result or {}),
        errors=list(errors or []),
        skip_reason=skip_reason,
        duration_seconds=round(monotonic() - started_monotonic, 6),
    ).to_dict()


def _persist(root: Path, record: dict[str, Any]) -> None:
    write_run_ledger(record, root)
    write_orchestrator_report(record, root)


@contextmanager
def _local_lock(root: Path, run_id: str, trigger_type: str, trigger_source: dict[str, Any]) -> Iterator[bool]:
    lock_path = root / "orchestrator" / "orchestrator.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_payload = {
        "lock_id": f"lock-{run_id}",
        "owner_pid": os.getpid(),
        "owner_host": socket.gethostname(),
        "acquired_at": _now(),
        "heartbeat_at": _now(),
        "intended_trigger": {"trigger_type": trigger_type, "trigger_source": trigger_source},
        "lease_expires_at": "",
    }
    fd: int | None = None
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        yield False
        return
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fd = None
            fh.write(json.dumps(lock_payload, indent=2, sort_keys=True) + "\n")
        yield True
    finally:
        if fd is not None:
            os.close(fd)
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass


def _load_runtime_truth(truth_root: str | Path, day: str | None) -> dict[str, Any] | None:
    day_value = day or datetime.now(UTC).date().isoformat()
    path = Path(truth_root) / "reports" / "aegis_runtime_truth_kernel_v1" / day_value / "runtime_truth_kernel.v1.json"
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload.setdefault("path", str(path))
    return payload


def _load_verified_graph(truth_root: str | Path, day: str | None) -> dict[str, Any] | None:
    day_value = day or datetime.now(UTC).date().isoformat()
    path = Path(truth_root) / "reports" / "aegis_verified_runtime_graph_v1" / day_value / "verified_runtime_graph.v1.json"
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload.setdefault("path", str(path))
    return payload


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _new_run_id(started_at: str, trigger_type: str) -> str:
    safe_time = started_at.replace(":", "").replace("-", "").replace("Z", "Z")
    return f"orchestrator-{safe_time}-{trigger_type.lower()}-{uuid.uuid4().hex[:10]}"
