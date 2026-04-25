import json
import os
from typing import Any, Dict, List

from meta_governance.api import _store
from meta_governance.audit_log import (
    append_runtime_event,
    read_canonical_audit_stream,
    reset_canonical_audit_stream,
)

EVENT_LOG_PATH = "/home/node/constellation/runtime/event_log.jsonl"


def append_event(event: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(EVENT_LOG_PATH), exist_ok=True)
    append_runtime_event(_store(), event)


def read_events() -> List[Dict[str, Any]]:
    records = []
    for row in read_canonical_audit_stream(_store()):
        if row.get("stream_type") != "runtime_event":
            continue
        records.append(
            {
                "timestamp": row["timestamp"],
                "ts_epoch": row.get("ts_epoch"),
                "event": row["event"],
            }
        )
    return records


def reset_event_log():
    reset_canonical_audit_stream(_store())
    with open(EVENT_LOG_PATH, "w", encoding="utf-8") as f:
        f.write("")
