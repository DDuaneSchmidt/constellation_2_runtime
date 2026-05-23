from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.candidates.candidate_registry import candidate_batch_manifest_path
from research_lab.costs.cost_model_registry import cost_model_snapshot_path
from research_lab.datasets.dataset_registry import dataset_snapshot_path
from research_lab.evidence.evidence_registry import evidence_manifest_path
from research_lab.regimes.regime_registry import regime_snapshot_path
from research_lab.sleeves.sleeve_definition import validate_sleeve_definition, validate_sleeve_version
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout, sleeve_uri


def sleeve_dir(sleeve_id: str, *, store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "sleeves" / sleeve_id


def sleeve_definition_path(sleeve_id: str, *, store_root: Path | None = None) -> Path:
    return sleeve_dir(sleeve_id, store_root=store_root) / "sleeve_definition.json"


def sleeve_version_path(sleeve_id: str, sleeve_version_id: str, *, store_root: Path | None = None) -> Path:
    return sleeve_dir(sleeve_id, store_root=store_root) / "versions" / f"{sleeve_version_id}.json"


def sleeve_health_path(sleeve_id: str, sleeve_health_snapshot_id: str, *, store_root: Path | None = None) -> Path:
    return sleeve_dir(sleeve_id, store_root=store_root) / "health" / f"{sleeve_health_snapshot_id}.json"


def sleeve_challenge_path(sleeve_id: str, sleeve_challenge_id: str, *, store_root: Path | None = None) -> Path:
    return sleeve_dir(sleeve_id, store_root=store_root) / "challenges" / f"{sleeve_challenge_id}.json"


def sleeve_review_path(sleeve_id: str, sleeve_review_id: str, *, store_root: Path | None = None) -> Path:
    return sleeve_dir(sleeve_id, store_root=store_root) / "reviews" / f"{sleeve_review_id}.json"


def _registry(name: str, store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "registries" / name


def _require(path: Path, label: str) -> None:
    if not path.exists():
        raise RuntimeError(f"{label} missing: {path}")


def store_sleeve_definition(definition: dict[str, Any], *, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    validate_sleeve_definition(definition)
    store = ensure_store_layout(store_root)
    path = sleeve_definition_path(definition["sleeve_id"], store_root=store)
    write_json(path, definition, overwrite=False)
    registry_row = {
        "sleeve_id": definition["sleeve_id"],
        "name": definition["name"],
        "hypothesis_id": definition["hypothesis_id"],
        "sleeve_type": definition["sleeve_type"],
        "status": definition["status"],
        "content_hash": definition["content_hash"],
        "created_at": definition["created_at"],
        "storage_uri": sleeve_uri(definition["sleeve_id"]),
        "schema_version": definition["schema_version"],
    }
    append_jsonl(_registry("sleeve_definitions.jsonl", store), registry_row)
    write_audit_event(actor=actor, entity_type="sleeve", entity_id=definition["sleeve_id"], action="sleeve_created", new_state_hash=definition["content_hash"], reason="Created governance-only sleeve definition.", metadata={"registry_row": registry_row}, store_root=store)
    return registry_row


def load_sleeve_definition(sleeve_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    return read_json(sleeve_definition_path(sleeve_id, store_root=store_root))


def store_sleeve_version(version: dict[str, Any], *, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    validate_sleeve_version(version)
    store = ensure_store_layout(store_root)
    _require(sleeve_definition_path(version["sleeve_id"], store_root=store), "Sleeve definition")
    _require(evidence_manifest_path(version["linked_event_study_evidence_package_id"], store_root=store), "Event-study evidence package")
    _require(evidence_manifest_path(version["linked_backtest_evidence_package_id"], store_root=store), "Backtest evidence package")
    for batch_id in version["linked_candidate_batch_ids"]:
        _require(candidate_batch_manifest_path(batch_id, store_root=store), "Candidate batch")
    _require(dataset_snapshot_path(version["dataset_snapshot_id"], store_root=store), "Dataset snapshot")
    _require(regime_snapshot_path(version["regime_snapshot_id"], store_root=store), "Regime snapshot")
    _require(cost_model_snapshot_path(version["cost_model_snapshot_id"], store_root=store), "Cost model snapshot")
    write_json(sleeve_version_path(version["sleeve_id"], version["sleeve_version_id"], store_root=store), version, overwrite=False)
    registry_row = {
        "sleeve_version_id": version["sleeve_version_id"],
        "sleeve_id": version["sleeve_id"],
        "version": version["version"],
        "hypothesis_id": version["hypothesis_id"],
        "linked_evidence_package_ids": version["linked_evidence_package_ids"],
        "linked_candidate_batch_ids": version["linked_candidate_batch_ids"],
        "content_hash": version["content_hash"],
        "created_at": version["created_at"],
        "schema_version": version["schema_version"],
    }
    append_jsonl(_registry("sleeve_versions.jsonl", store), registry_row)
    write_audit_event(actor=actor, entity_type="sleeve_version", entity_id=version["sleeve_version_id"], action="sleeve_version_created", new_state_hash=version["content_hash"], reason="Created immutable governance sleeve version.", metadata={"registry_row": registry_row}, store_root=store)
    return registry_row


def load_sleeve_version(sleeve_id: str, sleeve_version_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    return read_json(sleeve_version_path(sleeve_id, sleeve_version_id, store_root=store_root))


def append_registry_row(name: str, row: dict[str, Any], *, store_root: Path | None = None) -> None:
    append_jsonl(_registry(name, store_root), row)


def read_registry(name: str, *, store_root: Path | None = None) -> list[dict[str, Any]]:
    return read_jsonl(_registry(name, store_root))

