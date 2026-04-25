from __future__ import annotations

from pathlib import Path

from .deployment_state import build_deployment_state
from .health_monitoring import run_health_checks
from .operator_actions import build_execution_operator_view
from .schemas import utc_now
from ..meta_governance.api import _store


def supervise_runtime(
    *,
    runtime_mode: str,
    adapter_name: str,
    recovery_ref: str | None = None,
    as_of: str | None = None,
    store_root: str | Path | None = None,
) -> dict[str, str | tuple[str, ...]]:
    store = _store(store_root)
    recorded_at = as_of or utc_now()
    health_records = run_health_checks(runtime_mode=runtime_mode, adapter_name=adapter_name, recovery_ref=recovery_ref, checked_at=recorded_at, store_root=store.root)
    health_refs: list[str] = []
    for record in health_records:
        store.write_immutable("health_check_results", record.health_check_id, record, artifact_type="HealthCheckResult", created_at=record.checked_at)
        health_refs.append(record.health_check_id)
    deployment = build_deployment_state(runtime_mode=runtime_mode, health_check_refs=tuple(health_refs), recovery_ref=recovery_ref, as_of=recorded_at, store_root=store.root)
    store.write_immutable("deployment_states", deployment.deployment_state_id, deployment, artifact_type="DeploymentState", created_at=deployment.as_of)
    operator_view = build_execution_operator_view(store, deployment_state_ref=deployment.deployment_state_id, recovery_refs=(recovery_ref,) if recovery_ref else (), as_of=recorded_at)
    store.write_immutable("execution_operator_views", operator_view.operator_view_id, operator_view, artifact_type="ExecutionOperatorView", created_at=operator_view.as_of)
    return {
        "health_check_refs": tuple(health_refs),
        "deployment_state_ref": deployment.deployment_state_id,
        "operator_view_ref": operator_view.operator_view_id,
    }
