from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.contracts.schemas import validate_contract
from research_lab.observations.observation_cluster import cluster_path, cluster_registry_path, load_observation_cluster
from research_lab.observations.observation_clustering import CLUSTERING_VERSION, cluster_observations, counts_by
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout


SCHEMA_VERSION = "observation_cluster_batch.v1"
RESEARCH_LABEL = "RESEARCH_ONLY"


def cluster_batch_dir(store: Path) -> Path:
    return store / "observation_cluster_batches"


def cluster_batch_path(store: Path, observation_cluster_batch_id: str) -> Path:
    return cluster_batch_dir(store) / f"{observation_cluster_batch_id}.json"


def cluster_batch_registry_path(store: Path) -> Path:
    return store / "registries" / "observation_cluster_batch_registry.json"


def build_observation_cluster_batch(
    *,
    clusters: list[dict[str, Any]],
    source_batches: list[dict[str, Any]],
    generated_at: str,
    clustering_run_id: str,
) -> dict[str, Any]:
    highest = sorted(clusters, key=lambda row: (-float(row.get("research_priority_score") or 0.0), str(row.get("observation_cluster_id") or "")))[:5]
    highest_fingerprint_ids = [
        str(row.get("immutable_hash") or "")
        for row in sorted(clusters, key=lambda row: (-float(row.get("research_priority_score") or 0.0), str(row.get("immutable_hash") or "")))[:5]
    ]
    latest_integrity = next((str(row.get("latest_integrity_report_id") or "") for row in clusters if row.get("latest_integrity_report_id")), "")
    latest_status = next((str(row.get("latest_research_os_status_report_id") or "") for row in clusters if row.get("latest_research_os_status_report_id")), "")
    if not latest_integrity:
        latest_integrity = next((str(batch.get("latest_integrity_report_id") or "") for batch in source_batches if batch.get("latest_integrity_report_id")), "")
    if not latest_status:
        latest_status = next((str(batch.get("latest_research_os_status_report_id") or "") for batch in source_batches if batch.get("latest_research_os_status_report_id")), "")
    payload = {
        "observation_cluster_batch_id": "",
        "generated_at": generated_at,
        "clustering_run_id": clustering_run_id,
        "clustering_version": CLUSTERING_VERSION,
        "source_observation_candidate_batch_ids": [str(batch.get("observation_candidate_batch_id") or "") for batch in source_batches if batch.get("observation_candidate_batch_id")],
        "cluster_count": len(clusters),
        "observation_cluster_ids": [row["observation_cluster_id"] for row in clusters],
        "cluster_family_counts": counts_by(clusters, "cluster_family"),
        "cluster_status_counts": counts_by(clusters, "cluster_status"),
        "highest_priority_cluster_ids": [row["observation_cluster_id"] for row in highest],
        "latest_integrity_report_id": latest_integrity,
        "latest_research_os_status_report_id": latest_status,
        "scanner_statuses": [status for batch in source_batches for status in batch.get("scanner_statuses") or []],
        "non_actionable_research_only_assertion": {
            "research_only": True,
            "not_hypotheses": True,
            "not_trading_signals": True,
            "buy_sell_hold_recommendations_allowed": False,
            "hypothesis_created": False,
            "challenger_created": False,
            "sleeve_created": False,
            "paper_trial_created": False,
            "paper_trial_proposal_created": False,
            "candidate_ledger_mutated": False,
            "trade_order_created": False,
            "capital_allocation_created": False,
            "integrity_repaired": False,
        },
        "schema_version": SCHEMA_VERSION,
        "research_label": RESEARCH_LABEL,
        "immutable_hash": "",
        "content_hash": "",
    }
    fingerprint_payload = dict(payload)
    fingerprint_payload["observation_cluster_ids"] = [row["immutable_hash"] for row in clusters]
    fingerprint_payload["highest_priority_cluster_ids"] = highest_fingerprint_ids
    fingerprint = content_hash(fingerprint_payload, exclude={"observation_cluster_batch_id", "generated_at", "clustering_run_id", "immutable_hash", "content_hash"}, sort_lists=True)
    payload["observation_cluster_batch_id"] = f"oclb_{short_hash(content_hash({'fingerprint': fingerprint, 'generated_at': generated_at}), 16)}"
    payload["immutable_hash"] = fingerprint
    payload["content_hash"] = fingerprint
    validate_contract("observation_cluster_batch", payload)
    return payload


def write_observation_cluster_batch(
    *,
    store_root: Path | None = None,
    latest_only: bool = True,
    window_days: int = 30,
    max_clusters: int = 50,
    generated_at: str | None = None,
    actor: str = "Aegis",
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    generated_at = generated_at or utc_now_iso()
    clusters, source_batches = cluster_observations(store_root=store, latest_only=latest_only, generated_at=generated_at, window_days=window_days, max_clusters=max_clusters)
    cluster_results = []
    for cluster in clusters:
        path = cluster_path(store, cluster["observation_cluster_id"])
        if path.exists():
            raise FileExistsError(f"Refusing to overwrite immutable observation cluster: {path}")
        write_json(path, cluster, overwrite=False)
        row = {
            "observation_cluster_id": cluster["observation_cluster_id"],
            "generated_at": cluster["generated_at"],
            "cluster_family": cluster["cluster_family"],
            "cluster_status": cluster["cluster_status"],
            "research_status": cluster["research_status"],
            "research_priority_score": cluster["research_priority_score"],
            "immutable_hash": cluster["immutable_hash"],
        }
        append_jsonl(cluster_registry_path(store), row)
        audit = write_audit_event(
            actor=actor,
            entity_type="observation_cluster",
            entity_id=cluster["observation_cluster_id"],
            action="observation_cluster_generated",
            new_state_hash=cluster["immutable_hash"],
            reason="Generated read-only observation cluster.",
            metadata={
                "observation_cluster_id": cluster["observation_cluster_id"],
                "cluster_family": cluster["cluster_family"],
                "cluster_status": cluster["cluster_status"],
                "recurrence_count": cluster["recurrence_count"],
                "research_priority_score": cluster["research_priority_score"],
                "latest_integrity_report_id": cluster["latest_integrity_report_id"],
                "latest_research_os_status_report_id": cluster["latest_research_os_status_report_id"],
                "immutable_hash": cluster["immutable_hash"],
            },
            store_root=store,
        )
        cluster_results.append({"cluster": cluster, "registry_row": row, "audit_event": audit, "json_path": str(path)})
    batch = build_observation_cluster_batch(clusters=clusters, source_batches=source_batches, generated_at=generated_at, clustering_run_id=f"oclr_{short_hash(content_hash({'generated_at': generated_at, 'latest_only': latest_only}), 16)}")
    path = cluster_batch_path(store, batch["observation_cluster_batch_id"])
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite immutable observation cluster batch: {path}")
    write_json(path, batch, overwrite=False)
    row = {
        "observation_cluster_batch_id": batch["observation_cluster_batch_id"],
        "generated_at": batch["generated_at"],
        "clustering_run_id": batch["clustering_run_id"],
        "clustering_version": batch["clustering_version"],
        "cluster_count": batch["cluster_count"],
        "immutable_hash": batch["immutable_hash"],
    }
    append_jsonl(cluster_batch_registry_path(store), row)
    audit = write_audit_event(
        actor=actor,
        entity_type="observation_cluster_batch",
        entity_id=batch["observation_cluster_batch_id"],
        action="observation_cluster_batch_generated",
        new_state_hash=batch["immutable_hash"],
        reason="Generated read-only observation cluster batch.",
        metadata={
            "observation_cluster_batch_id": batch["observation_cluster_batch_id"],
            "cluster_count": batch["cluster_count"],
            "highest_priority_cluster_ids": batch["highest_priority_cluster_ids"],
            "latest_integrity_report_id": batch["latest_integrity_report_id"],
            "latest_research_os_status_report_id": batch["latest_research_os_status_report_id"],
            "immutable_hash": batch["immutable_hash"],
        },
        store_root=store,
    )
    return {"batch": batch, "clusters": cluster_results, "registry_row": row, "audit_event": audit, "json_path": str(path)}


def load_observation_cluster_batch(observation_cluster_batch_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    return read_json(cluster_batch_path(store, observation_cluster_batch_id))


def list_observation_cluster_batches(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    store = ensure_store_layout(store_root)
    return read_jsonl(cluster_batch_registry_path(store))


def latest_observation_cluster_batch(*, store_root: Path | None = None) -> dict[str, Any] | None:
    rows = list_observation_cluster_batches(store_root=store_root)
    if not rows:
        return None
    return load_observation_cluster_batch(str(rows[-1]["observation_cluster_batch_id"]), store_root=store_root)


def latest_observation_clusters_for_batch(*, store_root: Path | None = None, limit: int = 20) -> list[dict[str, Any]]:
    batch = latest_observation_cluster_batch(store_root=store_root)
    if not batch:
        return []
    return [load_observation_cluster(str(cluster_id), store_root=store_root) for cluster_id in batch.get("observation_cluster_ids", [])[:limit]]
