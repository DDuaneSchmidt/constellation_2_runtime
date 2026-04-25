from __future__ import annotations

from pathlib import Path

from .schemas import content_hash, utc_now
from .types import DeploymentState
from ..meta_governance.api import _store, get_active_snapshot
from ..meta_governance.interpreter_version import get_interpreter_version


def build_deployment_state(
    *,
    runtime_mode: str,
    health_check_refs: tuple[str, ...],
    recovery_ref: str | None = None,
    as_of: str | None = None,
    store_root: str | Path | None = None,
) -> DeploymentState:
    store = _store(store_root)
    active_snapshot = get_active_snapshot(store_root=store.root)
    checks = [store.read("health_check_results", ref)["record"] for ref in health_check_refs]
    unhealthy = tuple(sorted(check["component_name"] for check in checks if check["status"] in {"degraded", "failed", "unknown"} and check["component_name"] != "recovery_backlog"))
    blocking_reasons = [f"{check['component_name']}:{check['status']}" for check in checks if check["blocking"]]
    startup_gate_status = "pass" if active_snapshot and not any(check["component_name"] == "startup_gate" and check["status"] == "failed" for check in checks) else "fail"
    if runtime_mode in {"live_disabled", "live_ready", "live_active"}:
        blocking_reasons.append("LIVE_EXECUTION_UNPROVEN")
    if active_snapshot is None:
        service_state = "starting"
    elif blocking_reasons:
        service_state = "blocked"
    elif recovery_ref and store.read("execution_recovery_records", recovery_ref)["record"]["unresolved_refs"]:
        service_state = "recovering"
    elif any(check["status"] in {"degraded", "unknown"} for check in checks):
        service_state = "degraded"
    else:
        service_state = "healthy"
    recorded_at = as_of or utc_now()
    payload = {
        "as_of": recorded_at,
        "runtime_mode": runtime_mode,
        "service_state": service_state,
        "startup_gate_status": startup_gate_status,
        "active_snapshot_id": active_snapshot["record"]["snapshot_id"] if active_snapshot else None,
        "interpreter_version": get_interpreter_version(),
        "unhealthy_components": unhealthy,
        "blocked_execution_reasons": tuple(sorted(set(blocking_reasons))),
    }
    artifact_hash = content_hash(payload)
    return DeploymentState(
        deployment_state_id=f"deployment-state-{artifact_hash[:12]}",
        as_of=recorded_at,
        runtime_mode=runtime_mode,
        service_state=service_state,
        startup_gate_status=startup_gate_status,
        active_snapshot_id=payload["active_snapshot_id"],
        interpreter_version=get_interpreter_version(),
        unhealthy_components=unhealthy,
        blocked_execution_reasons=tuple(sorted(set(blocking_reasons))),
        artifact_hash=artifact_hash,
    )
