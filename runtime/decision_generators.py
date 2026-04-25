import time
from typing import Any, Dict, List


def is_in_cooldown(pid, state, policy):
    if not policy["cooldown"]["enabled"]:
        return False

    if pid not in state.get("cooldowns", {}):
        return False

    last = state["cooldowns"][pid]
    elapsed = time.time() - last["timestamp"]

    if elapsed < policy["cooldown"]["min_seconds_between_same_action"]:
        return True

    return False


def risk_generator(state: Dict[str, Any], policy: Dict[str, Any]) -> List[Dict[str, Any]]:
    decisions = []

    for pid, pos in state["positions"].items():
        if pos.get("quantity", 0) <= 0:
            continue

        execution = state.get("execution", {}).get(pid)

        # ONLY enforce cooldown if execution confirmed
        if execution and execution.get("status") == "FILLED":
            if is_in_cooldown(pid, state, policy):
                continue

        # if last action not filled, retry allowed
        if execution:
            if execution.get("status") != "FILLED":
                pass

        if (
            execution
            and execution.get("status") == "FILLED"
            and policy["cooldown"]["block_same_action"]
            and pid in state.get("cooldowns", {})
        ):
            last = state["cooldowns"][pid]["last_action"]
            if last == "EXIT":
                cooldown_elapsed = time.time() - state["cooldowns"][pid]["timestamp"]
                if cooldown_elapsed < policy["cooldown"]["min_seconds_between_same_action"]:
                    continue

        if pos.get("risk_flag") == "CRITICAL":
            decisions.append({
                "position_id": pid,
                "action": "EXIT",
                "scores": {
                    "risk": 1.0,
                    "tax": -0.3,
                    "allocation": 0.1
                }
            })

    return decisions


def allocation_generator(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    return []
