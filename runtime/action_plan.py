from typing import List, Dict, Any
from event_log import append_event


def _require_governed_decision_context(decision: Dict[str, Any]) -> None:
    if not decision.get("snapshot_id"):
        raise ValueError("SNAPSHOT_ID_REQUIRED")
    if not decision.get("graph_hash"):
        raise ValueError("GRAPH_HASH_REQUIRED")
    if not decision.get("decision_id"):
        raise ValueError("DECISION_ID_REQUIRED")


def compile_plan(scored_decisions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    plan = []

    for d in scored_decisions:
        _require_governed_decision_context(d)
        action = {
            "position_id": d["position_id"],
            "action": d["action"],
            "decision_id": d.get("decision_id"),
            "snapshot_id": d.get("snapshot_id"),
            "graph_hash": d.get("graph_hash"),
        }

        plan.append(action)

        append_event({
            "type": "POSITION_ACTION_DECIDED",
            "position_id": d["position_id"],
            "action": d["action"],
            "decision_id": d.get("decision_id"),
            "snapshot_id": d.get("snapshot_id"),
            "graph_hash": d.get("graph_hash"),
        })

    return plan
