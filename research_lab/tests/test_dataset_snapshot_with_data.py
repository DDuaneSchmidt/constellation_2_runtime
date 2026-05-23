from __future__ import annotations

from ops.aegis.research_lab.research_store_refs_v1 import as_reference_model_manifest


def test_aegis_still_stores_only_references_not_ohlcv_rows() -> None:
    manifest = as_reference_model_manifest()
    field_names = {field for table in manifest["reference_tables"] for field in table["fields"]}

    assert {"open", "high", "low", "close", "adj_close", "volume"}.isdisjoint(field_names)
    assert all(table["stores_canonical_ohlcv_rows"] is False for table in manifest["reference_tables"])
