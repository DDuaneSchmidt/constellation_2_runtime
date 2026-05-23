from __future__ import annotations

from dataclasses import dataclass
from typing import Any


FORBIDDEN_CANONICAL_MARKET_DATA_TABLES = {"prices", "ohlcv", "market_bars", "historical_prices"}


@dataclass(frozen=True)
class AegisReferenceTable:
    name: str
    fields: tuple[str, ...]
    purpose: str
    stores_canonical_ohlcv_rows: bool = False


AEGIS_RESEARCH_REFERENCE_TABLES: tuple[AegisReferenceTable, ...] = (
    AegisReferenceTable(
        name="dataset_snapshot_refs",
        fields=(
            "id",
            "dataset_snapshot_id",
            "dataset_type",
            "provider",
            "interval",
            "universe_snapshot_id",
            "start_date",
            "end_date",
            "symbol_count",
            "quality_status",
            "storage_uri",
            "content_hash",
            "bar_policy_version",
            "created_at",
            "registered_at",
            "status",
            "notes",
        ),
        purpose="Aegis-side reference to immutable Research Store dataset snapshots.",
    ),
    AegisReferenceTable(
        name="evidence_package_refs",
        fields=(
            "id",
            "evidence_package_id",
            "hypothesis_id",
            "research_plan_id",
            "dataset_snapshot_id",
            "storage_uri",
            "manifest_hash",
            "summary_status",
            "created_at",
            "registered_at",
            "lifecycle_state",
            "review_status",
            "notes",
        ),
        purpose="Aegis-side reference to immutable Research Store evidence packages.",
    ),
    AegisReferenceTable(
        name="research_audit_events",
        fields=(
            "id",
            "event_id",
            "timestamp",
            "actor",
            "entity_type",
            "entity_id",
            "action",
            "previous_state_hash",
            "new_state_hash",
            "reason",
            "metadata_json",
        ),
        purpose="Aegis-side audit event reference metadata; detailed source records remain append-only.",
    ),
)


def as_reference_model_manifest() -> dict[str, Any]:
    return {
        "boundary": "Aegis stores references, lifecycle state, decisions, and audit metadata only.",
        "research_store_owns": [
            "historical OHLCV snapshots",
            "universe snapshots",
            "data quality reports",
            "evidence packages",
            "large result artifacts",
            "derived/event tables",
        ],
        "forbidden_canonical_market_data_tables": sorted(FORBIDDEN_CANONICAL_MARKET_DATA_TABLES),
        "reference_tables": [
            {
                "name": table.name,
                "fields": list(table.fields),
                "purpose": table.purpose,
                "stores_canonical_ohlcv_rows": table.stores_canonical_ohlcv_rows,
            }
            for table in AEGIS_RESEARCH_REFERENCE_TABLES
        ],
    }
