from __future__ import annotations

from pathlib import Path

from research_lab.contracts.schemas import validate_contract
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout, evidence_uri


def _registry_path(store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "registries" / "evidence_packages.jsonl"


def evidence_manifest_path(evidence_package_id: str, *, store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "evidence_packages" / evidence_package_id / "evidence_manifest.json"


def store_evidence_package_manifest(manifest: dict, *, store_root: Path | None = None) -> dict:
    validate_contract("evidence_package", manifest)
    package_id = manifest["evidence_package_id"]
    write_json(evidence_manifest_path(package_id, store_root=store_root), manifest, overwrite=False)
    registry_row = {
        "evidence_package_id": package_id,
        "hypothesis_id": manifest["hypothesis_id"],
        "research_plan_id": manifest["research_plan_id"],
        "dataset_snapshot_id": manifest["dataset_snapshot_id"],
        "universe_snapshot_id": manifest["universe_snapshot_id"],
        "manifest_hash": manifest["manifest_hash"],
        "storage_uri": evidence_uri(package_id),
        "created_at": manifest["created_at"],
        "schema_version": manifest["schema_version"],
    }
    append_jsonl(_registry_path(store_root), registry_row)
    return registry_row


def append_evidence_registry_entry(manifest: dict, *, store_root: Path | None = None) -> dict:
    validate_contract("evidence_package", manifest)
    registry_row = {
        "evidence_package_id": manifest["evidence_package_id"],
        "research_plan_id": manifest["research_plan_id"],
        "hypothesis_id": manifest["hypothesis_id"],
        "dataset_snapshot_id": manifest["dataset_snapshot_id"],
        "storage_uri": evidence_uri(manifest["evidence_package_id"]),
        "event_count": manifest.get("event_count"),
        "evidence_quality": manifest.get("evidence_quality"),
        "created_at": manifest["created_at"],
        "content_hash": manifest["manifest_hash"],
        "manifest_hash": manifest["manifest_hash"],
        "runner_name": manifest["runner_name"],
        "runner_version": manifest["runner_version"],
        "schema_version": manifest["schema_version"],
    }
    append_jsonl(_registry_path(store_root), registry_row)
    return registry_row


def list_evidence_packages(*, store_root: Path | None = None) -> list[dict]:
    return read_jsonl(_registry_path(store_root))


def load_evidence_package_manifest(evidence_package_id: str, *, store_root: Path | None = None) -> dict:
    return read_json(evidence_manifest_path(evidence_package_id, store_root=store_root))
