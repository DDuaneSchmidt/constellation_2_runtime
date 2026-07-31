from __future__ import annotations

from collections import OrderedDict
from typing import Any

from .artifact_models import FORBIDDEN_ARTIFACT_TYPES
from .worker_interfaces import ResearchOSWorker


class WorkerRegistryError(ValueError):
    pass


_WORKERS: OrderedDict[str, ResearchOSWorker] = OrderedDict()


def register_worker(worker: ResearchOSWorker, *, replace: bool = False) -> ResearchOSWorker:
    ok, failures = validate_worker_contract(worker)
    if not ok:
        raise WorkerRegistryError("; ".join(failures))
    if worker.worker_id in _WORKERS and not replace:
        raise WorkerRegistryError(f"worker already registered: {worker.worker_id}")
    _WORKERS[worker.worker_id] = worker
    return worker


def get_worker(worker_id: str) -> ResearchOSWorker:
    try:
        return _WORKERS[worker_id]
    except KeyError as exc:
        raise WorkerRegistryError(f"unknown worker: {worker_id}") from exc


def list_workers() -> list[dict[str, Any]]:
    return [
        {
            "worker_id": worker.worker_id,
            "worker_type": worker.worker_type,
            "supported_input_artifact_types": list(worker.supported_input_artifact_types),
            "supported_output_artifact_types": list(worker.supported_output_artifact_types),
            "adapter_status": getattr(worker, "adapter_status", ""),
            "source_component": getattr(worker, "source_component", ""),
        }
        for worker in _WORKERS.values()
    ]


def validate_worker_contract(worker: object) -> tuple[bool, list[str]]:
    failures: list[str] = []
    for attr in ("worker_id", "worker_type", "supported_input_artifact_types", "supported_output_artifact_types"):
        if not getattr(worker, attr, None):
            failures.append(f"missing {attr}")
    for method in ("run", "dry_run", "validate_inputs", "validate_outputs"):
        if not callable(getattr(worker, method, None)):
            failures.append(f"missing callable {method}")
    output_types = [str(item) for item in getattr(worker, "supported_output_artifact_types", [])]
    forbidden = sorted(set(output_types) & FORBIDDEN_ARTIFACT_TYPES)
    if forbidden:
        failures.append(f"worker declares forbidden output artifact types: {forbidden}")
    return not failures, failures


def clear_worker_registry_for_tests() -> None:
    _WORKERS.clear()
