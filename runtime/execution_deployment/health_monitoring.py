from __future__ import annotations

from pathlib import Path

from .broker_adapters import adapter_readiness
from .execution_state_machine import OPEN_STATES, current_state
from .schemas import content_hash, utc_now
from .types import HealthCheckResult
from ..meta_governance.api import _store, get_active_snapshot, resolve_active_runtime_authority


def _health(component_name: str, status: str, checked_at: str, *, details: dict, blocking: bool) -> HealthCheckResult:
    payload = {
        "component_name": component_name,
        "status": status,
        "checked_at": checked_at,
        "details": details,
        "blocking": blocking,
    }
    artifact_hash = content_hash(payload)
    return HealthCheckResult(
        health_check_id=f"health-check-{component_name}-{artifact_hash[:12]}",
        component_name=component_name,
        status=status,
        checked_at=checked_at,
        details=details,
        blocking=blocking,
        artifact_hash=artifact_hash,
    )


def run_health_checks(
    *,
    runtime_mode: str,
    adapter_name: str,
    recovery_ref: str | None = None,
    store_root: str | Path | None = None,
    checked_at: str | None = None,
) -> tuple[HealthCheckResult, ...]:
    store = _store(store_root)
    at = checked_at or utc_now()
    checks: list[HealthCheckResult] = []
    try:
        authority = resolve_active_runtime_authority(store_root=store.root)
        checks.append(_health("startup_gate", "healthy", at, details={"snapshot_id": authority["snapshot"]["snapshot_id"]}, blocking=False))
        checks.append(_health("authority_resolution", "healthy", at, details={"graph_hash": authority["snapshot"]["graph_hash"]}, blocking=False))
    except ValueError as exc:
        checks.append(_health("startup_gate", "failed", at, details={"reason": str(exc)}, blocking=True))
        checks.append(_health("authority_resolution", "failed", at, details={"reason": str(exc)}, blocking=True))
    artifact_store_ok = store.root.exists() and all((store.root / dirname).exists() for dirname in ("state", "audit_events"))
    checks.append(_health("artifact_store", "healthy" if artifact_store_ok else "failed", at, details={"root": str(store.root)}, blocking=not artifact_store_ok))
    audit_dir_ok = store.stream_path("governed_audit").parent.exists()
    checks.append(_health("audit_stream", "healthy" if audit_dir_ok else "failed", at, details={"stream_path": str(store.stream_path("governed_audit"))}, blocking=not audit_dir_ok))
    readiness = adapter_readiness(adapter_name=adapter_name, runtime_mode=runtime_mode)
    checks.append(_health("adapter_readiness", readiness["status"], at, details={"reason": readiness["reason"], "runtime_mode": runtime_mode, "adapter_name": adapter_name}, blocking=bool(readiness["blocking"])))
    if recovery_ref:
        recovery = store.read("execution_recovery_records", recovery_ref)["record"]
        status = "degraded" if recovery["unresolved_refs"] else "healthy"
        checks.append(_health("recovery_backlog", status, at, details={"unresolved_refs": recovery["unresolved_refs"]}, blocking=False))
    else:
        checks.append(_health("recovery_backlog", "unknown", at, details={"reason": "recovery_not_run"}, blocking=False))
    open_submissions = [ref for ref in store.list_ids("execution_submissions") if current_state(store, ref) in OPEN_STATES]
    status = "degraded" if open_submissions else "healthy"
    checks.append(_health("unresolved_critical_execution_items", status, at, details={"open_submission_refs": tuple(sorted(open_submissions))}, blocking=False))
    return tuple(checks)
