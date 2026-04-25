from __future__ import annotations

from pathlib import Path

from ..meta_governance.api import _store
from ..meta_governance.startup_gate import validate_runtime_authority
from .schemas import content_hash
from .types import AuthorityEvaluation


def _scope_for_policy(policy: dict) -> dict:
    return {
        "allowed_actions": tuple(policy.get("autonomy", {}).get("allowed_actions", ())),
        "allowed_scopes": tuple(policy.get("execution", {}).get("allowed_scopes", ())),
        "policy_version": policy.get("version"),
        "has_tax_surface": "tax" in policy.get("weights", {}),
        "has_risk_surface": "risk" in policy.get("weights", {}),
    }


def evaluate_authority(
    candidate_action_id: str,
    *,
    expected_graph_hash: str | None = None,
    expected_interpreter_version: str | None = None,
    store_root: str | Path | None = None,
) -> AuthorityEvaluation:
    store = _store(store_root)
    blocking_conditions: list[str] = []
    snapshot_id = ""
    graph_hash = ""
    interpreter_version = ""
    authority_scope: dict = {}
    try:
        authority = validate_runtime_authority(store, actor="autonomous_authority", event_type="autonomous_authority_blocked")
        snapshot = authority["snapshot"]
        snapshot_id = snapshot["snapshot_id"]
        graph_hash = snapshot["graph_hash"]
        interpreter_version = snapshot["interpreter_version"]
        authority_scope = _scope_for_policy(authority["compiled_policy"])
    except ValueError as exc:
        blocking_conditions.append(str(exc))
    if expected_graph_hash and graph_hash and expected_graph_hash != graph_hash:
        blocking_conditions.append("GRAPH_HASH_MISMATCH")
    if expected_interpreter_version and interpreter_version and expected_interpreter_version != interpreter_version:
        blocking_conditions.append("INTERPRETER_VERSION_MISMATCH")
    allowed = not blocking_conditions
    payload = {
        "candidate_action_id": candidate_action_id,
        "active_snapshot_id": snapshot_id,
        "graph_hash": graph_hash,
        "interpreter_version": interpreter_version,
        "authority_scope": authority_scope,
        "blocking_conditions": tuple(sorted(blocking_conditions)),
        "allowed": allowed,
    }
    artifact_hash = content_hash(payload)
    return AuthorityEvaluation(
        candidate_action_id=candidate_action_id,
        active_snapshot_id=snapshot_id,
        graph_hash=graph_hash,
        interpreter_version=interpreter_version,
        authority_scope=authority_scope,
        blocking_conditions=tuple(sorted(blocking_conditions)),
        allowed=allowed,
        artifact_hash=artifact_hash,
    )
