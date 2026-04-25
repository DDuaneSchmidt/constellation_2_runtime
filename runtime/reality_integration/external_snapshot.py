from __future__ import annotations

from typing import Any

from .schemas import REALITY_SCHEMA_VERSION, content_hash, require_fields
from .types import ExternalRealitySnapshot
from ..meta_governance.store import ArtifactStore


SUPPORTED_SOURCE_TYPES = {
    "broker_statement",
    "broker_api_snapshot",
    "imported_holdings_snapshot",
    "tax_lot_export",
    "execution_fill_export",
    "valuation_feed_snapshot",
}


def _tuple_records(records: Any) -> tuple[dict[str, Any], ...]:
    if records is None:
        return ()
    if not isinstance(records, list):
        raise ValueError("SNAPSHOT_RECORDS_MUST_BE_LIST")
    normalized: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            raise ValueError("SNAPSHOT_RECORD_MUST_BE_OBJECT")
        normalized.append(dict(record))
    return tuple(normalized)


def validate_external_snapshot_payload(payload: dict[str, Any]) -> None:
    require_fields(payload, ("source_type", "source_name", "captured_at"), "EXTERNAL_REALITY_SNAPSHOT")
    if payload["source_type"] not in SUPPORTED_SOURCE_TYPES:
        raise ValueError(f"UNSUPPORTED_EXTERNAL_SOURCE_TYPE:{payload['source_type']}")
    for field in (
        "account_refs",
        "position_records",
        "taxlot_records",
        "execution_records",
        "cash_records",
        "valuation_records",
        "pnl_records",
    ):
        if field in payload and payload[field] is not None and not isinstance(payload[field], list):
            raise ValueError(f"EXTERNAL_SNAPSHOT_FIELD_MUST_BE_LIST:{field}")


def ingest_external_snapshot(
    store: ArtifactStore,
    *,
    source_type: str,
    source_name: str,
    captured_at: str,
    account_refs: list[dict[str, Any]] | None = None,
    position_records: list[dict[str, Any]] | None = None,
    taxlot_records: list[dict[str, Any]] | None = None,
    execution_records: list[dict[str, Any]] | None = None,
    cash_records: list[dict[str, Any]] | None = None,
    valuation_records: list[dict[str, Any]] | None = None,
    pnl_records: list[dict[str, Any]] | None = None,
) -> str:
    payload = {
        "source_type": source_type,
        "source_name": source_name,
        "captured_at": captured_at,
        "account_refs": account_refs or [],
        "position_records": position_records or [],
        "taxlot_records": taxlot_records or [],
        "execution_records": execution_records or [],
        "cash_records": cash_records or [],
        "valuation_records": valuation_records or [],
        "pnl_records": pnl_records or [],
    }
    validate_external_snapshot_payload(payload)
    snapshot_hash = content_hash(payload)
    snapshot = ExternalRealitySnapshot(
        external_snapshot_id=f"external-reality-{snapshot_hash[:12]}",
        source_type=source_type,
        source_name=source_name,
        captured_at=captured_at,
        schema_version=REALITY_SCHEMA_VERSION,
        content_hash=snapshot_hash,
        account_refs=_tuple_records(account_refs or []),
        position_records=_tuple_records(position_records or []),
        taxlot_records=_tuple_records(taxlot_records or []),
        execution_records=_tuple_records(execution_records or []),
        cash_records=_tuple_records(cash_records or []),
        valuation_records=_tuple_records(valuation_records or []),
        pnl_records=_tuple_records(pnl_records or []),
    )
    store.write_immutable(
        "external_reality_snapshots",
        snapshot.external_snapshot_id,
        snapshot,
        artifact_type="ExternalRealitySnapshot",
        created_at=captured_at,
    )
    return snapshot.external_snapshot_id
