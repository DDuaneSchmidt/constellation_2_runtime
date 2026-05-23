from __future__ import annotations

from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso

SCHEMA_VERSION = "event_cluster.v1"
DATA_REQUIREMENT_STATUSES = {"available", "partially_available", "missing_or_future", "unknown"}

def recompute_event_cluster_hash(cluster: dict[str, Any]) -> str:
    return content_hash(cluster, exclude={"event_cluster_id", "created_at", "content_hash"}, sort_lists=False)

def build_event_cluster(
    *,
    event_family: dict[str, Any],
    observations: list[dict[str, Any]],
    cluster_title: str,
    cluster_description: str = "",
    created_at: str | None = None,
) -> dict[str, Any]:
    if not observations:
        raise ValueError("cluster has zero observations")
    family_id = str(event_family["event_family_id"])
    observation_ids: list[str] = []
    symbols: set[str] = set()
    mechanisms: set[str] = set()
    for observation in observations:
        if observation.get("event_family_id") != family_id:
            raise ValueError("observation event family mismatch")
        observation_ids.append(str(observation["event_observation_id"]))
        symbols.update(str(symbol).upper() for symbol in observation.get("symbols_mentioned", []))
        mechanism = str(observation.get("suspected_mechanism") or "").strip()
        if mechanism:
            mechanisms.add(mechanism)
    data_status = str(event_family.get("default_data_requirement_status") or "unknown")
    if data_status not in DATA_REQUIREMENT_STATUSES:
        data_status = "unknown"
    payload = {
        "event_cluster_id": "",
        "event_family_id": family_id,
        "observation_ids": sorted(observation_ids),
        "cluster_title": str(cluster_title).strip(),
        "cluster_description": str(cluster_description or cluster_title).strip(),
        "common_symbols": sorted(symbols),
        "common_mechanisms": sorted(mechanisms),
        "candidate_intents": list(event_family.get("default_research_intents") or []),
        "sample_observation_count": len(observation_ids),
        "data_requirement_status": data_status,
        "created_at": created_at or utc_now_iso(),
        "schema_version": SCHEMA_VERSION,
        "research_label": "RESEARCH_ONLY",
        "content_hash": "",
    }
    fingerprint = recompute_event_cluster_hash(payload)
    payload["event_cluster_id"] = f"ecl_{short_hash(content_hash({'fingerprint': fingerprint, 'created_at': payload['created_at']}), 16)}"
    payload["content_hash"] = fingerprint
    validate_event_cluster(payload)
    return payload

def validate_event_cluster(cluster: dict[str, Any]) -> None:
    validate_contract("event_cluster", cluster)
    if int(cluster.get("sample_observation_count") or 0) <= 0:
        raise ValueError("cluster has zero observations")
    actual = recompute_event_cluster_hash(cluster)
    if actual != cluster.get("content_hash"):
        raise ValueError(f"EventCluster content_hash mismatch: expected {cluster.get('content_hash')}, got {actual}")
