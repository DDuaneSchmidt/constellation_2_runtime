from __future__ import annotations

from typing import Any

from ..meta_governance.api import _store
from ..meta_governance.startup_gate import validate_runtime_authority
from ..meta_governance.store import ArtifactStore
from ..state_rebuilder import rebuild_state
from .schemas import REALITY_SCHEMA_VERSION, content_hash
from .types import InternalRealitySnapshot


SUPPORTED_LIVE_SURFACES = {"positions", "executions", "accounts"}
OPTIONAL_RUNTIME_EVENTS = {
    "taxlots": "TAX_LOT_SNAPSHOT",
    "cash": "CASH_SNAPSHOT",
    "valuations": "VALUATION_SNAPSHOT",
    "pnl": "PNL_SNAPSHOT",
    "accounts": "ACCOUNT_SNAPSHOT",
}


def _runtime_events(store: ArtifactStore) -> list[dict[str, Any]]:
    path = store.stream_path("governed_audit")
    if not path.exists():
        return []
    events: list[dict[str, Any]] = []
    for row in path.read_text(encoding="utf-8").splitlines():
        if not row.strip():
            continue
        import json

        payload = json.loads(row)
        if payload.get("stream_type") != "runtime_event":
            continue
        events.append(
            {
                "timestamp": payload["timestamp"],
                "ts_epoch": payload.get("ts_epoch"),
                "event": payload["event"],
            }
        )
    return events


def _latest_records(events: list[dict[str, Any]], event_type: str, field: str) -> tuple[dict[str, Any], ...]:
    latest: list[dict[str, Any]] = []
    for row in events:
        event = row["event"]
        if event.get("type") != event_type:
            continue
        payload = event.get(field, ())
        if isinstance(payload, list):
            latest = [dict(item) for item in payload]
        elif isinstance(payload, dict):
            latest = [dict(payload)]
    return tuple(latest)


def _execution_records(events: list[dict[str, Any]]) -> tuple[dict[str, Any], ...]:
    records: list[dict[str, Any]] = []
    for row in events:
        event = row["event"]
        if event.get("type") != "EXECUTION_RESULT":
            continue
        execution_key = event.get("execution_key") or f"{event.get('position_id', 'unknown')}::{event.get('action', 'UNKNOWN')}::{row['timestamp']}"
        record = dict(event)
        record.setdefault("execution_key", execution_key)
        record.setdefault("captured_at", row["timestamp"])
        records.append(record)
    return tuple(records)


def _account_state(position_state: tuple[dict[str, Any], ...], execution_state: tuple[dict[str, Any], ...], account_records: tuple[dict[str, Any], ...]) -> tuple[dict[str, Any], ...]:
    if account_records:
        return account_records
    account_ids = {
        record["account_id"]
        for record in list(position_state) + list(execution_state)
        if record.get("account_id")
    }
    return tuple({"account_id": account_id} for account_id in sorted(account_ids))


def _capture_time(events: list[dict[str, Any]], snapshot_active_at: str) -> str:
    if not events:
        return snapshot_active_at
    return max(row["timestamp"] for row in events)


def _unsupported_surface(surface: str) -> tuple[dict[str, Any], ...]:
    return (
        {
            "surface": surface,
            "status": "UNKNOWN",
            "reason": "RUNTIME_SURFACE_UNPROVEN",
        },
    )


def capture_internal_snapshot(
    *,
    store_root: str | None = None,
    runtime_snapshot_id: str | None = None,
    required_surfaces: tuple[str, ...] = ("positions", "executions"),
    supplemental_state: dict[str, list[dict[str, Any]]] | None = None,
) -> str:
    store = _store(store_root)
    authority = validate_runtime_authority(store, actor="reality_capture", event_type="reality_capture_blocked")
    snapshot = authority["snapshot"]
    if runtime_snapshot_id is not None and runtime_snapshot_id != snapshot["snapshot_id"]:
        raise ValueError("INTERNAL_CAPTURE_REQUIRES_ACTIVE_RUNTIME_SNAPSHOT")
    events = _runtime_events(store)
    state = rebuild_state(events)
    supplemental = supplemental_state or {}
    position_state = tuple(dict(record) for record in state["positions"].values())
    execution_state = _execution_records(events)
    account_records = tuple(dict(record) for record in supplemental.get("accounts", ())) or _latest_records(events, OPTIONAL_RUNTIME_EVENTS["accounts"], "accounts")
    taxlot_state = tuple(dict(record) for record in supplemental.get("taxlots", ())) or _latest_records(events, OPTIONAL_RUNTIME_EVENTS["taxlots"], "tax_lots")
    cash_state = tuple(dict(record) for record in supplemental.get("cash", ())) or _latest_records(events, OPTIONAL_RUNTIME_EVENTS["cash"], "cash")
    valuation_state = tuple(dict(record) for record in supplemental.get("valuations", ())) or _latest_records(events, OPTIONAL_RUNTIME_EVENTS["valuations"], "valuations")
    pnl_state = tuple(dict(record) for record in supplemental.get("pnl", ())) or _latest_records(events, OPTIONAL_RUNTIME_EVENTS["pnl"], "pnl")
    surfaces = {
        "positions": position_state,
        "executions": execution_state,
        "accounts": _account_state(position_state, execution_state, account_records),
        "taxlots": taxlot_state or _unsupported_surface("taxlots"),
        "cash": cash_state or _unsupported_surface("cash"),
        "valuations": valuation_state or _unsupported_surface("valuations"),
        "pnl": pnl_state or _unsupported_surface("pnl"),
    }
    for surface in required_surfaces:
        if surface not in surfaces:
            raise ValueError(f"UNKNOWN_INTERNAL_SURFACE:{surface}")
        if surface not in SUPPORTED_LIVE_SURFACES and not supplemental.get(surface):
            raise ValueError(f"UNSUPPORTED_INTERNAL_SURFACE:{surface}")
    captured_at = _capture_time(events, snapshot["active_at"])
    lineage_refs = [snapshot["snapshot_id"], snapshot["graph_hash"]]
    for lineage_id in store.list_ids("lineage_records"):
        lineage = store.read("lineage_records", lineage_id)
        if lineage["record"]["snapshot_id"] == snapshot["snapshot_id"]:
            lineage_refs.append(lineage_id)
    payload = {
        "runtime_snapshot_id": snapshot["snapshot_id"],
        "captured_at": captured_at,
        "account_state": surfaces["accounts"],
        "position_state": surfaces["positions"],
        "taxlot_state": surfaces["taxlots"],
        "execution_state": surfaces["executions"],
        "cash_state": surfaces["cash"],
        "valuation_state": surfaces["valuations"],
        "pnl_state": surfaces["pnl"],
        "lineage_refs": tuple(sorted(set(lineage_refs))),
    }
    snapshot_hash = content_hash(payload)
    internal_snapshot = InternalRealitySnapshot(
        internal_snapshot_id=f"internal-reality-{snapshot_hash[:12]}",
        runtime_snapshot_id=snapshot["snapshot_id"],
        captured_at=captured_at,
        schema_version=REALITY_SCHEMA_VERSION,
        content_hash=snapshot_hash,
        account_state=tuple(payload["account_state"]),
        position_state=tuple(payload["position_state"]),
        taxlot_state=tuple(payload["taxlot_state"]),
        execution_state=tuple(payload["execution_state"]),
        cash_state=tuple(payload["cash_state"]),
        valuation_state=tuple(payload["valuation_state"]),
        pnl_state=tuple(payload["pnl_state"]),
        lineage_refs=tuple(sorted(set(lineage_refs))),
    )
    store.write_immutable(
        "internal_reality_snapshots",
        internal_snapshot.internal_snapshot_id,
        internal_snapshot,
        artifact_type="InternalRealitySnapshot",
        created_at=captured_at,
    )
    return internal_snapshot.internal_snapshot_id
