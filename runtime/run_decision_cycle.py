import hashlib
import json

from action_plan import compile_plan
from decision_generators import allocation_generator, risk_generator
from event_log import append_event, read_events
from invariant_gate import validate
from meta_governance.api import record_decision_lineage, resolve_active_runtime_authority
from scoring_engine import score_decisions
from state_rebuilder import rebuild_state


def compute_input_hash(events, policy):
    payload = {
        "events": events,
        "policy": policy
    }
    encoded = json.dumps(payload, sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def attach_governance_context(scored, snapshot, input_hash):
    governed = []
    for index, decision in enumerate(scored):
        decision_key = {
            "snapshot_id": snapshot["snapshot_id"],
            "graph_hash": snapshot["graph_hash"],
            "position_id": decision["position_id"],
            "action": decision["action"],
            "index": index,
            "input_hash": input_hash,
        }
        decision_id = hashlib.sha256(
            json.dumps(decision_key, sort_keys=True).encode("utf-8")
        ).hexdigest()[:16]
        governed_decision = dict(decision)
        governed_decision["decision_id"] = decision_id
        governed_decision["snapshot_id"] = snapshot["snapshot_id"]
        governed_decision["graph_hash"] = snapshot["graph_hash"]
        record_decision_lineage(decision_id, snapshot["snapshot_id"])
        governed.append(governed_decision)
    return governed


def run_cycle():
    events = read_events()
    state = rebuild_state(events)
    authority = resolve_active_runtime_authority()
    snapshot = authority["snapshot"]
    policy = authority["compiled_policy"]

    input_hash = compute_input_hash(events, policy)

    append_event({
        "type": "DECISION_CYCLE_STARTED",
        "input_hash": input_hash,
        "policy_version": policy["version"],
        "snapshot_id": snapshot["snapshot_id"],
        "graph_hash": snapshot["graph_hash"],
    })

    decisions = []
    decisions += risk_generator(state, policy)
    decisions += allocation_generator(state)
    append_event({
        "type": "DECISIONS_GENERATED",
        "decisions": decisions,
        "snapshot_id": snapshot["snapshot_id"],
        "graph_hash": snapshot["graph_hash"],
    })

    scored = score_decisions(decisions, policy)
    scored = attach_governance_context(scored, snapshot, input_hash)
    append_event({
        "type": "DECISIONS_SCORED",
        "scored": scored,
        "snapshot_id": snapshot["snapshot_id"],
        "graph_hash": snapshot["graph_hash"],
    })

    if not validate(scored, policy, state):
        raise Exception("Invariant violation — fail closed")

    plan = compile_plan(scored)
    append_event({
        "type": "ACTION_PLAN_CREATED",
        "plan": plan,
        "snapshot_id": snapshot["snapshot_id"],
        "graph_hash": snapshot["graph_hash"],
    })

    return plan

if __name__ == "__main__":
    print(run_cycle())
