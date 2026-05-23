from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash
from research_lab.storage.manifest_io import read_json, read_jsonl
from research_lab.storage.paths import ensure_store_layout


SCHEMA_VERSION = "observation_candidate.v1"
RESEARCH_LABEL = "RESEARCH_ONLY"
ACTIONABILITY_STATUS = "non_actionable_research_only"


def candidate_dir(store: Path) -> Path:
    return store / "observation_candidates"


def candidate_path(store: Path, observation_candidate_id: str) -> Path:
    return candidate_dir(store) / f"{observation_candidate_id}.json"


def candidate_registry_path(store: Path) -> Path:
    return store / "registries" / "observation_candidate_registry.json"


def build_observation_candidate(
    *,
    generated_at: str,
    scanner_id: str,
    scanner_version: str,
    observation_type: str,
    observation_family: str,
    symbol_or_asset_id: str,
    universe_id: str,
    event_date: str,
    observation_summary: str,
    trigger_conditions: list[dict[str, Any]],
    measured_values: dict[str, Any],
    baseline_values: dict[str, Any],
    anomaly_score: float,
    novelty_score: float,
    repeat_count: int,
    severity: str,
    research_status: str,
    non_actionable_reason: str,
    source_artifact_ids: dict[str, Any],
    source_data_refs: list[dict[str, Any]],
    latest_integrity_report_id: str,
    latest_research_os_status_report_id: str,
    blockers: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    payload = {
        "observation_candidate_id": "",
        "generated_at": generated_at,
        "scanner_id": scanner_id,
        "scanner_version": scanner_version,
        "observation_type": observation_type,
        "observation_family": observation_family,
        "symbol_or_asset_id": symbol_or_asset_id,
        "universe_id": universe_id,
        "event_date": event_date,
        "observation_summary": observation_summary,
        "trigger_conditions": trigger_conditions,
        "measured_values": measured_values,
        "baseline_values": baseline_values,
        "anomaly_score": float(anomaly_score),
        "novelty_score": float(novelty_score),
        "repeat_count": int(repeat_count),
        "severity": severity,
        "research_status": research_status,
        "actionability_status": ACTIONABILITY_STATUS,
        "non_actionable_reason": non_actionable_reason,
        "source_artifact_ids": source_artifact_ids,
        "source_data_refs": source_data_refs,
        "latest_integrity_report_id": latest_integrity_report_id,
        "latest_research_os_status_report_id": latest_research_os_status_report_id,
        "blockers": blockers or [],
        "schema_version": SCHEMA_VERSION,
        "research_label": RESEARCH_LABEL,
        "immutable_hash": "",
        "content_hash": "",
    }
    fingerprint = content_hash(payload, exclude={"observation_candidate_id", "generated_at", "immutable_hash", "content_hash"}, sort_lists=True)
    payload["observation_candidate_id"] = f"obc_{short_hash(content_hash({'fingerprint': fingerprint, 'generated_at': generated_at}), 16)}"
    payload["immutable_hash"] = fingerprint
    payload["content_hash"] = fingerprint
    validate_contract("observation_candidate", payload)
    return payload


def load_observation_candidate(observation_candidate_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    return read_json(candidate_path(store, observation_candidate_id))


def list_observation_candidates(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    store = ensure_store_layout(store_root)
    return read_jsonl(candidate_registry_path(store))


def latest_observation_candidate(*, store_root: Path | None = None) -> dict[str, Any] | None:
    rows = list_observation_candidates(store_root=store_root)
    if not rows:
        return None
    return load_observation_candidate(str(rows[-1]["observation_candidate_id"]), store_root=store_root)

