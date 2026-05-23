from __future__ import annotations

from pathlib import Path

from research_lab.storage.hashing import sha256_hex
from research_lab.storage.paths import resolve_research_uri


def test_canonical_hash_sorts_dict_keys() -> None:
    assert sha256_hex({"b": 2, "a": 1}) == sha256_hex({"a": 1, "b": 2})


def test_canonical_hash_normalizes_utc_timestamps() -> None:
    assert sha256_hex({"created_at": "2026-05-18T12:00:00+00:00"}) == sha256_hex({"created_at": "2026-05-18T12:00:00Z"})


def test_research_uri_resolver_maps_store_paths(tmp_path: Path) -> None:
    store = tmp_path / "store"

    assert resolve_research_uri("research://universes/us_1", store_root=store) == store.resolve() / "universes" / "us_1"
    assert resolve_research_uri("research://datasets/ds_1/quality_report.json", store_root=store) == store.resolve() / "datasets" / "ds_1" / "quality_report.json"
    assert resolve_research_uri("research://evidence/ep_1", store_root=store) == store.resolve() / "evidence_packages" / "ep_1"
    assert resolve_research_uri("research://registries/datasets", store_root=store) == store.resolve() / "registries" / "dataset_snapshots.jsonl"
    assert resolve_research_uri("research://audit/2026-05-18", store_root=store) == store.resolve() / "audit_log" / "audit_events.jsonl"
