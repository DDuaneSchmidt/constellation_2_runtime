from datetime import datetime, timezone
from typing import Dict, Any, List


def _record_epoch(record: Dict[str, Any]) -> float:
    dt = datetime.fromisoformat(record["timestamp"])
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    parsed_epoch = dt.timestamp()

    ts_epoch = record.get("ts_epoch")
    if ts_epoch is None:
        return parsed_epoch

    if abs(ts_epoch - parsed_epoch) > 1:
        return parsed_epoch

    return ts_epoch


def rebuild_state(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    state = {
        "positions": {},
        "intent": {},
        "market": {},
        "cooldowns": {},
        "execution": {}
    }

    for record in events:
        event = record["event"]

        if event["type"] == "POSITION_SNAPSHOT":
            for pos in event["positions"]:
                state["positions"][pos["position_id"]] = dict(pos)

        elif event["type"] == "INTENT_SNAPSHOT":
            state["intent"] = event["intent"]

        elif event["type"] == "MARKET_SNAPSHOT":
            state["market"] = event["market"]

        elif event["type"] == "POSITION_ACTION_DECIDED":
            pid = event["position_id"]
            state["cooldowns"][pid] = {
                "last_action": event["action"],
                "timestamp": _record_epoch(record)
            }

        elif event["type"] == "EXECUTION_RESULT":
            pid = event["position_id"]
            filled = event.get("filled_qty", 0)

            state["execution"][pid] = {
                "action": event["action"],
                "status": event["status"],
                "filled_qty": filled
            }

            if pid in state["positions"]:
                state["positions"][pid]["quantity"] -= filled

                if state["positions"][pid]["quantity"] <= 0:
                    del state["positions"][pid]

        elif event["type"] == "POSITION_CLOSED":
            if event["position_id"] in state["positions"]:
                del state["positions"][event["position_id"]]

    return state
