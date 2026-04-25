import uuid
from typing import Dict, Any, List
from event_log import append_event


def ingest_position_snapshot(positions: List[Dict[str, Any]]) -> None:
    append_event({
        "type": "POSITION_SNAPSHOT",
        "snapshot_id": str(uuid.uuid4()),
        "positions": positions
    })


def ingest_intent_snapshot(intent: Dict[str, Any]) -> None:
    append_event({
        "type": "INTENT_SNAPSHOT",
        "snapshot_id": str(uuid.uuid4()),
        "intent": intent
    })


def ingest_market_snapshot(market: Dict[str, Any]) -> None:
    append_event({
        "type": "MARKET_SNAPSHOT",
        "snapshot_id": str(uuid.uuid4()),
        "market": market
    })


def ingest_execution_result(position_id: str, action: str, status: str, filled_qty: float):
    append_event({
        "type": "EXECUTION_RESULT",
        "position_id": position_id,
        "action": action,
        "status": status,
        "filled_qty": filled_qty
    })


def commit_snapshot() -> None:
    append_event({
        "type": "SNAPSHOT_COMMITTED"
    })
