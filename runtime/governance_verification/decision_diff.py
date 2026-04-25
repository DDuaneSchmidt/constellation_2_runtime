from __future__ import annotations

from typing import Any

from ..decision_generators import allocation_generator, risk_generator
from ..invariant_gate import validate
from ..scoring_engine import score_decisions
from ..state_rebuilder import rebuild_state
from ..meta_governance.startup_gate import validate_snapshot_authority
from ..meta_governance.store import ArtifactStore
from .dataset_registry import get_dataset_document
from .schemas import content_hash
from .types import DecisionDelta, DecisionDiffArtifact
from .verification_context import get_verification_context


def _resolve_snapshot_policy(store: ArtifactStore, snapshot_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    authority = validate_snapshot_authority(store, snapshot_id, actor="verification", event_type="verification_blocked")
    snapshot = authority["snapshot"]
    graph = authority["graph"]
    return snapshot, graph["compiled_policy"]


def _events_from_datasets(dataset_docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_type = {doc["record"]["ref"]["dataset_type"]: doc["record"]["dataset"] for doc in dataset_docs}
    if "historical_decision_inputs" in by_type:
        events = by_type["historical_decision_inputs"]
        if isinstance(events, dict) and "events" in events:
            return list(events["events"])
        if isinstance(events, list):
            return list(events)
    events: list[dict[str, Any]] = []
    positions = by_type.get("historical_position_state", [])
    if isinstance(positions, dict):
        positions = positions.get("positions", [])
    if positions:
        events.append({"timestamp": "2026-01-01T00:00:00Z", "ts_epoch": 0, "event": {"type": "POSITION_SNAPSHOT", "positions": positions}})
    market = by_type.get("historical_market_context")
    if market:
        events.append({"timestamp": "2026-01-01T00:00:01Z", "ts_epoch": 1, "event": {"type": "MARKET_SNAPSHOT", "market": market}})
    tax_lots = by_type.get("historical_tax_lot_state")
    if tax_lots:
        events.append({"timestamp": "2026-01-01T00:00:02Z", "ts_epoch": 2, "event": {"type": "TAX_LOT_SNAPSHOT", "tax_lots": tax_lots}})
    return events


def _decision_key(decision: dict[str, Any]) -> str:
    return f"{decision.get('position_id', 'global')}::{decision.get('action', 'UNKNOWN')}"


def _decision_domains(decision: dict[str, Any]) -> tuple[str, ...]:
    scores = decision.get("scores", {})
    domains = {domain for domain, value in scores.items() if value}
    if decision.get("action"):
        domains.add("execution")
    return tuple(sorted(domains))


def _invalid_reasons(decisions: list[dict[str, Any]], policy: dict[str, Any], state: dict[str, Any]) -> tuple[str, ...]:
    reasons: list[str] = []
    if len(decisions) > policy["limits"]["max_actions_per_cycle"]:
        reasons.append("max_actions_per_cycle_exceeded")
    if any(d["total_score"] < policy["limits"]["min_decision_score"] for d in decisions):
        reasons.append("min_decision_score_failed")
    seen: set[tuple[str, str]] = set()
    for decision in decisions:
        key = (decision["position_id"], decision["action"])
        if key in seen:
            reasons.append("duplicate_action")
            break
        seen.add(key)
    action_map: dict[str, set[str]] = {}
    for decision in decisions:
        action_map.setdefault(decision["position_id"], set()).add(decision["action"])
    if any(len(actions) > 1 for actions in action_map.values()):
        reasons.append("conflicting_actions")
    return tuple(sorted(set(reasons)))


def _replay_policy(policy: dict[str, Any], events: list[dict[str, Any]]) -> dict[str, Any]:
    state = rebuild_state(events)
    generated = list(risk_generator(state, policy))
    generated.extend(allocation_generator(state))
    scored = score_decisions(generated, policy)
    valid = validate(scored, policy, state)
    ordered = [_normalize_decision(index, decision) for index, decision in enumerate(scored)]
    return {
        "state": state,
        "generated": generated,
        "scored": scored,
        "decisions": ordered if valid else [],
        "valid": valid,
        "invalid_reasons": _invalid_reasons(scored, policy, state) if not valid else (),
    }


def _normalize_decision(index: int, decision: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        "position_id": decision.get("position_id"),
        "action": decision.get("action"),
        "scores": dict(decision.get("scores", {})),
        "total_score": decision.get("total_score"),
        "rank": index,
    }
    if "quantity" in decision:
        normalized["quantity"] = decision["quantity"]
    return normalized


def _delta_materiality(delta_type: str) -> str:
    if delta_type in {"blocked", "newly_allowed", "newly_forbidden"}:
        return "critical"
    if delta_type in {"added", "removed", "resized", "accelerated"}:
        return "material"
    return "minor"


def _build_delta(
    decision_key: str,
    baseline_decision: dict[str, Any] | None,
    candidate_decision: dict[str, Any] | None,
    delta_type: str,
    explanation: str,
) -> DecisionDelta:
    affected_domains = sorted(set(_decision_domains(baseline_decision or {})) | set(_decision_domains(candidate_decision or {})))
    return DecisionDelta(
        decision_key=decision_key,
        baseline_decision=baseline_decision,
        candidate_decision=candidate_decision,
        delta_type=delta_type,
        affected_domains=tuple(affected_domains),
        materiality_class=_delta_materiality(delta_type),
        explanation=explanation,
    )


def _decision_deltas(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
) -> tuple[DecisionDelta, ...]:
    if baseline["valid"] and not candidate["valid"]:
        explanation = f"candidate_invalid:{','.join(candidate['invalid_reasons']) or 'unknown'}"
        return tuple(
            _build_delta(_decision_key(decision), decision, None, "blocked", explanation)
            for decision in baseline["decisions"]
        )
    if not baseline["valid"] and candidate["valid"]:
        explanation = f"baseline_invalid:{','.join(baseline['invalid_reasons']) or 'unknown'}"
        return tuple(
            _build_delta(_decision_key(decision), None, decision, "newly_allowed", explanation)
            for decision in candidate["decisions"]
        )
    if not baseline["valid"] and not candidate["valid"]:
        return ()
    baseline_map = {_decision_key(decision): decision for decision in baseline["decisions"]}
    candidate_map = {_decision_key(decision): decision for decision in candidate["decisions"]}
    all_keys = sorted(set(baseline_map) | set(candidate_map))
    deltas: list[DecisionDelta] = []
    for key in all_keys:
        left = baseline_map.get(key)
        right = candidate_map.get(key)
        if left is None and right is not None:
            deltas.append(_build_delta(key, None, right, "added", "candidate introduces new governed decision"))
            continue
        if left is not None and right is None:
            deltas.append(_build_delta(key, left, None, "removed", "candidate removes governed decision"))
            continue
        assert left is not None and right is not None
        if left.get("quantity") != right.get("quantity"):
            deltas.append(_build_delta(key, left, right, "resized", "decision quantity changed"))
            continue
        if left["rank"] != right["rank"]:
            direction = "accelerated" if right["rank"] < left["rank"] else "deferred"
            deltas.append(_build_delta(key, left, right, direction, "decision ordering changed"))
            continue
        if left["total_score"] != right["total_score"]:
            deltas.append(_build_delta(key, left, right, "reprioritized", "decision score changed under candidate snapshot"))
    return tuple(deltas)


def compute_decision_diff(store: ArtifactStore, verification_id: str) -> str:
    context = get_verification_context(store, verification_id)["record"]
    baseline_snapshot, baseline_policy = _resolve_snapshot_policy(store, context["active_snapshot_id"])
    candidate_snapshot, candidate_policy = _resolve_snapshot_policy(store, context["candidate_snapshot_id"])
    dataset_docs = [get_dataset_document(store, dataset_ref) for dataset_ref in context["dataset_refs"]]
    events = _events_from_datasets(dataset_docs)
    baseline_run = _replay_policy(baseline_policy, events)
    candidate_run = _replay_policy(candidate_policy, events)
    deltas = _decision_deltas(baseline_run, candidate_run)
    baseline_keys = {_decision_key(decision) for decision in baseline_run["decisions"]}
    candidate_keys = {_decision_key(decision) for decision in candidate_run["decisions"]}
    changed_keys = {delta.decision_key for delta in deltas}
    unchanged_count = len((baseline_keys & candidate_keys) - changed_keys)
    artifact_hash = content_hash(
        {
            "verification_id": verification_id,
            "baseline_snapshot_id": baseline_snapshot["snapshot_id"],
            "candidate_snapshot_id": candidate_snapshot["snapshot_id"],
            "decision_deltas": deltas,
            "decision_count_changed": len(deltas),
            "decision_count_unchanged": unchanged_count,
        }
    )
    artifact = DecisionDiffArtifact(
        verification_id=verification_id,
        baseline_snapshot_id=baseline_snapshot["snapshot_id"],
        candidate_snapshot_id=candidate_snapshot["snapshot_id"],
        decision_deltas=deltas,
        decision_count_changed=len(deltas),
        decision_count_unchanged=unchanged_count,
        artifact_hash=artifact_hash,
    )
    store.write_immutable("decision_diff_artifacts", verification_id, artifact, artifact_type="DecisionDiffArtifact")
    return verification_id


def replay_decisions(store: ArtifactStore, verification_id: str) -> dict[str, Any]:
    context = get_verification_context(store, verification_id)["record"]
    dataset_docs = [get_dataset_document(store, dataset_ref) for dataset_ref in context["dataset_refs"]]
    events = _events_from_datasets(dataset_docs)
    _, baseline_policy = _resolve_snapshot_policy(store, context["active_snapshot_id"])
    _, candidate_policy = _resolve_snapshot_policy(store, context["candidate_snapshot_id"])
    return {
        "baseline": _replay_policy(baseline_policy, events),
        "candidate": _replay_policy(candidate_policy, events),
        "events": events,
    }
