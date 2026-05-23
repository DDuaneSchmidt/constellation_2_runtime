from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab import SCHEMA_VERSION
from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, sha256_hex, short_hash, utc_now_iso
from research_lab.storage.paths import dataset_uri
from research_lab.universes.universe_registry import load_universe_snapshot


DATASET_HASH_EXCLUDE = {
    "dataset_snapshot_id",
    "created_at",
    "content_hash",
    "source_hash",
    "storage_uri",
    "quality_report_uri",
}


def _compact_date(value: str) -> str:
    return str(value).strip().replace("-", "")


def build_empty_dataset_snapshot(
    *,
    dataset_type: str,
    provider: str,
    interval: str,
    bar_policy_version: str,
    universe_snapshot_id: str,
    start_date: str,
    end_date: str,
    provider_version: str = "placeholder_v1",
    created_by: str = "Aegis",
    created_at: str | None = None,
    store_root: Path | None = None,
) -> dict[str, Any]:
    universe = load_universe_snapshot(universe_snapshot_id, store_root=store_root)
    symbols = [str(row["symbol"]).upper() for row in universe["symbols"]]
    base: dict[str, Any] = {
        "dataset_snapshot_id": "",
        "dataset_type": str(dataset_type).strip(),
        "provider": str(provider).strip(),
        "provider_version": str(provider_version).strip(),
        "interval": str(interval).strip(),
        "bar_policy_version": str(bar_policy_version).strip(),
        "universe_snapshot_id": universe_snapshot_id,
        "start_date": str(start_date).strip(),
        "end_date": str(end_date).strip(),
        "symbol_count": len(symbols),
        "symbols": symbols,
        "storage_uri": "",
        "canonical_format": f"{dataset_type}.{interval}.parquet.v1",
        "row_count": 0,
        "quality_status": "pending",
        "quality_report_uri": "",
        "content_hash": "",
        "source_hash": sha256_hex(
            {
                "universe_snapshot_id": universe_snapshot_id,
                "universe_content_hash": universe["content_hash"],
                "provider": provider,
                "provider_version": provider_version,
            }
        ),
        "created_at": created_at or utc_now_iso(),
        "created_by": created_by,
        "schema_version": SCHEMA_VERSION,
    }
    digest = content_hash(base, exclude=DATASET_HASH_EXCLUDE, sort_lists=False)
    universe_name = str(universe["universe_name"])
    snapshot_id = (
        f"ds_{base['dataset_type']}_{base['interval']}_{universe_name}_"
        f"{_compact_date(base['start_date'])}_{_compact_date(base['end_date'])}_{short_hash(digest)}"
    )
    base["dataset_snapshot_id"] = snapshot_id
    base["storage_uri"] = dataset_uri(snapshot_id)
    base["quality_report_uri"] = f"{dataset_uri(snapshot_id)}/quality_report.json"
    base["content_hash"] = digest
    validate_contract("dataset_snapshot", base)
    return base


def recompute_dataset_content_hash(snapshot: dict[str, Any]) -> str:
    return content_hash(snapshot, exclude=DATASET_HASH_EXCLUDE, sort_lists=False)
