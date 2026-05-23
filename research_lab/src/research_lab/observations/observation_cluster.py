from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash
from research_lab.storage.manifest_io import read_json, read_jsonl
from research_lab.storage.paths import ensure_store_layout


SCHEMA_VERSION = "observation_cluster.v1"
RESEARCH_LABEL = "RESEARCH_ONLY"
ACTIONABILITY_STATUS = "non_actionable_research_only"
CLUSTER_VERSION = "observation_clustering.v1"


def cluster_dir(store: Path) -> Path:
    return store / "observation_clusters"


def cluster_path(store: Path, observation_cluster_id: str) -> Path:
    return cluster_dir(store) / f"{observation_cluster_id}.json"


def cluster_registry_path(store: Path) -> Path:
    return store / "registries" / "observation_cluster_registry.json"


def build_observation_cluster(
    *,
    generated_at: str,
    cluster_status: str,
    cluster_family: str,
    cluster_label: str,
    cluster_summary: str,
    observation_candidate_ids: list[str],
    observation_candidate_batch_ids: list[str],
    observation_type_counts: dict[str, int],
    severity_counts: dict[str, int],
    first_seen_at: str,
    last_seen_at: str,
    recurrence_count: int,
    unique_symbol_or_asset_count: int,
    universe_ids: list[str],
    source_scanner_ids: list[str],
    source_artifact_ids: dict[str, Any],
    latest_integrity_report_id: str,
    latest_research_os_status_report_id: str,
    cluster_score: float,
    novelty_score: float,
    persistence_score: float,
    severity_score: float,
    research_priority_score: float,
    research_status: str,
    blockers: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    payload = {
        "observation_cluster_id": "",
        "generated_at": generated_at,
        "cluster_version": CLUSTER_VERSION,
        "cluster_status": cluster_status,
        "cluster_family": cluster_family,
        "cluster_label": cluster_label,
        "cluster_summary": cluster_summary,
        "observation_candidate_ids": observation_candidate_ids,
        "observation_candidate_batch_ids": observation_candidate_batch_ids,
        "observation_type_counts": observation_type_counts,
        "severity_counts": severity_counts,
        "first_seen_at": first_seen_at,
        "last_seen_at": last_seen_at,
        "recurrence_count": int(recurrence_count),
        "unique_symbol_or_asset_count": int(unique_symbol_or_asset_count),
        "universe_ids": sorted(set(universe_ids)),
        "source_scanner_ids": sorted(set(source_scanner_ids)),
        "source_artifact_ids": source_artifact_ids,
        "latest_integrity_report_id": latest_integrity_report_id,
        "latest_research_os_status_report_id": latest_research_os_status_report_id,
        "cluster_score": float(cluster_score),
        "novelty_score": float(novelty_score),
        "persistence_score": float(persistence_score),
        "severity_score": float(severity_score),
        "research_priority_score": float(research_priority_score),
        "research_status": research_status,
        "actionability_status": ACTIONABILITY_STATUS,
        "non_actionable_reason": "Observation cluster is research-only. It is not a hypothesis, trading signal, recommendation, order, allocation, sleeve, paper trial, or proposal.",
        "blockers": blockers or [],
        "schema_version": SCHEMA_VERSION,
        "research_label": RESEARCH_LABEL,
        "immutable_hash": "",
        "content_hash": "",
    }
    fingerprint = content_hash(payload, exclude={"observation_cluster_id", "generated_at", "immutable_hash", "content_hash"}, sort_lists=True)
    payload["observation_cluster_id"] = f"ocl_{short_hash(content_hash({'fingerprint': fingerprint, 'generated_at': generated_at}), 16)}"
    payload["immutable_hash"] = fingerprint
    payload["content_hash"] = fingerprint
    validate_contract("observation_cluster", payload)
    return payload


def load_observation_cluster(observation_cluster_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    return read_json(cluster_path(store, observation_cluster_id))


def list_observation_clusters(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    store = ensure_store_layout(store_root)
    return read_jsonl(cluster_registry_path(store))


def latest_observation_cluster(*, store_root: Path | None = None) -> dict[str, Any] | None:
    rows = list_observation_clusters(store_root=store_root)
    if not rows:
        return None
    return load_observation_cluster(str(rows[-1]["observation_cluster_id"]), store_root=store_root)

