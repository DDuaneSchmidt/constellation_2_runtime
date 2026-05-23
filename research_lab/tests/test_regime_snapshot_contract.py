from __future__ import annotations

from research_lab.contracts.schemas import validate_contract
from research_lab.regimes.regime_snapshot import regime_snapshot_content_hash


def test_regime_snapshot_schema_validates() -> None:
    snapshot = {
        "regime_snapshot_id": "rs_spy_fixture",
        "dataset_snapshot_id": "ds_fixture",
        "benchmark_symbol": "SPY",
        "created_at": "2024-01-01T00:00:00Z",
        "created_by": "pytest",
        "regime_model_version": "spy_daily_regime_v1",
        "inputs": {"dataset_snapshot_hash": "abc123abc123"},
        "labels": {"row_count": 1, "labels_hash": "abc123abc123"},
        "storage_uri": "research://regimes/rs_spy_fixture",
        "content_hash": "",
        "schema_version": "regime_snapshot.v1",
    }
    snapshot["content_hash"] = regime_snapshot_content_hash(snapshot)

    validate_contract("regime_snapshot", snapshot)
