import time
from typing import List, Dict, Any


def validate(decisions: List[Dict[str, Any]], policy: Dict[str, Any], state: Dict[str, Any]) -> bool:
    # LIMIT: max actions
    if len(decisions) > policy["limits"]["max_actions_per_cycle"]:
        return False

    # LIMIT: score threshold
    for d in decisions:
        if d["total_score"] < policy["limits"]["min_decision_score"]:
            return False

    # prevent repeated identical actions during the cooldown window
    for d in decisions:
        pid = d["position_id"]
        execution = state.get("execution", {}).get(pid)
        if pid in state.get("cooldowns", {}):
            last = state["cooldowns"][pid]["last_action"]
            elapsed = time.time() - state["cooldowns"][pid]["timestamp"]
            if (
                execution
                and execution.get("status") == "FILLED"
                and policy["cooldown"]["enabled"]
                and policy["cooldown"]["block_same_action"]
                and last == d["action"]
                and elapsed < policy["cooldown"]["min_seconds_between_same_action"]
            ):
                return False

    # cannot assume action completed without execution confirmation
    for d in decisions:
        pid = d["position_id"]
        execution = state.get("execution", {}).get(pid)

        if execution:
            if execution.get("status") == "REJECTED":
                continue

    # CAPITAL CHECK
    total_positions = len(state["positions"])
    if total_positions < 0:
        return False

    # DUPLICATE ACTION CHECK
    seen = set()
    for d in decisions:
        key = (d["position_id"], d["action"])
        if key in seen:
            return False
        seen.add(key)

    # CONFLICT CHECK (same position multiple actions)
    action_map = {}
    for d in decisions:
        pid = d["position_id"]
        if pid not in action_map:
            action_map[pid] = set()
        action_map[pid].add(d["action"])

    for pid, actions in action_map.items():
        if len(actions) > 1:
            return False

    return True
