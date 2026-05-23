from __future__ import annotations

from ops.aegis.research_lab.research_store_refs_v1 import (
    AEGIS_RESEARCH_REFERENCE_TABLES,
    FORBIDDEN_CANONICAL_MARKET_DATA_TABLES,
    as_reference_model_manifest,
)


def test_aegis_reference_boundary_has_no_canonical_ohlcv_table() -> None:
    manifest = as_reference_model_manifest()
    names = {row["name"] for row in manifest["reference_tables"]}

    assert names == {"dataset_snapshot_refs", "evidence_package_refs", "research_audit_events"}
    assert not names.intersection(FORBIDDEN_CANONICAL_MARKET_DATA_TABLES)
    assert all(row["stores_canonical_ohlcv_rows"] is False for row in manifest["reference_tables"])


def test_aegis_reference_fields_store_metadata_only() -> None:
    all_fields = {field for table in AEGIS_RESEARCH_REFERENCE_TABLES for field in table.fields}

    assert "dataset_snapshot_id" in all_fields
    assert "evidence_package_id" in all_fields
    assert "content_hash" in all_fields
    assert not {"open", "high", "low", "close", "volume"}.intersection(all_fields)
