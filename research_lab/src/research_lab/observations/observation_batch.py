from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.contracts.schemas import validate_contract
from research_lab.observations.observation_candidate import candidate_path, candidate_registry_path, list_observation_candidates, load_observation_candidate
from research_lab.observations.observation_scanner import SCANNER_VERSION, counts_by, run_observation_scanners
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout


SCHEMA_VERSION = "observation_candidate_batch.v1"
RESEARCH_LABEL = "RESEARCH_ONLY"


def batch_dir(store: Path) -> Path:
    return store / "observation_candidate_batches"


def batch_path(store: Path, observation_candidate_batch_id: str) -> Path:
    return batch_dir(store) / f"{observation_candidate_batch_id}.json"


def batch_registry_path(store: Path) -> Path:
    return store / "registries" / "observation_candidate_batch_registry.json"


def build_observation_candidate_batch(
    *,
    observations: list[dict[str, Any]],
    scanner_statuses: list[dict[str, Any]],
    generated_at: str,
    scanner_run_id: str,
) -> dict[str, Any]:
    latest_integrity_report_id = next((str(row.get("latest_integrity_report_id") or "") for row in observations if row.get("latest_integrity_report_id")), "")
    latest_research_os_status_report_id = next((str(row.get("latest_research_os_status_report_id") or "") for row in observations if row.get("latest_research_os_status_report_id")), "")
    source_artifact_ids: dict[str, Any] = {}
    for row in observations:
        for key, value in (row.get("source_artifact_ids") or {}).items():
            if value:
                source_artifact_ids.setdefault(key, value)
    payload = {
        "observation_candidate_batch_id": "",
        "generated_at": generated_at,
        "scanner_run_id": scanner_run_id,
        "scanner_version": SCANNER_VERSION,
        "observation_count": len(observations),
        "observation_candidate_ids": [row["observation_candidate_id"] for row in observations],
        "observation_type_counts": counts_by(observations, "observation_type"),
        "severity_counts": counts_by(observations, "severity"),
        "scanner_statuses": scanner_statuses,
        "source_artifact_ids": source_artifact_ids,
        "latest_integrity_report_id": latest_integrity_report_id,
        "latest_research_os_status_report_id": latest_research_os_status_report_id,
        "non_actionable_research_only_assertion": {
            "research_only": True,
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
    fingerprint_payload["observation_candidate_ids"] = [row["immutable_hash"] for row in observations]
    fingerprint = content_hash(fingerprint_payload, exclude={"observation_candidate_batch_id", "generated_at", "scanner_run_id", "immutable_hash", "content_hash"}, sort_lists=True)
    payload["observation_candidate_batch_id"] = f"ocb_{short_hash(content_hash({'fingerprint': fingerprint, 'generated_at': generated_at}), 16)}"
    payload["immutable_hash"] = fingerprint
    payload["content_hash"] = fingerprint
    validate_contract("observation_candidate_batch", payload)
    return payload


def write_observation_candidate_batch(
    *,
    store_root: Path | None = None,
    scanners: list[str] | None = None,
    max_observations: int = 50,
    generated_at: str | None = None,
    actor: str = "Aegis",
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    generated_at = generated_at or utc_now_iso()
    scanner_run_id = f"obsrun_{short_hash(content_hash({'generated_at': generated_at, 'scanners': scanners or ['all']}), 16)}"
    observations, statuses = run_observation_scanners(store_root=store, scanners=scanners, generated_at=generated_at, max_observations=max_observations)
    candidate_results = []
    for candidate in observations:
        path = candidate_path(store, candidate["observation_candidate_id"])
        if path.exists():
            raise FileExistsError(f"Refusing to overwrite immutable observation candidate: {path}")
        write_json(path, candidate, overwrite=False)
        row = {
            "observation_candidate_id": candidate["observation_candidate_id"],
            "generated_at": candidate["generated_at"],
            "scanner_id": candidate["scanner_id"],
            "observation_type": candidate["observation_type"],
            "observation_family": candidate["observation_family"],
            "severity": candidate["severity"],
            "research_status": candidate["research_status"],
            "actionability_status": candidate["actionability_status"],
            "immutable_hash": candidate["immutable_hash"],
        }
        append_jsonl(candidate_registry_path(store), row)
        audit = write_audit_event(
            actor=actor,
            entity_type="observation_candidate",
            entity_id=candidate["observation_candidate_id"],
            action="observation_candidate_generated",
            new_state_hash=candidate["immutable_hash"],
            reason="Generated read-only research observation candidate.",
            metadata={
                "observation_candidate_id": candidate["observation_candidate_id"],
                "observation_type": candidate["observation_type"],
                "observation_family": candidate["observation_family"],
                "severity": candidate["severity"],
                "latest_integrity_report_id": candidate["latest_integrity_report_id"],
                "latest_research_os_status_report_id": candidate["latest_research_os_status_report_id"],
                "immutable_hash": candidate["immutable_hash"],
            },
            store_root=store,
        )
        candidate_results.append({"candidate": candidate, "registry_row": row, "audit_event": audit, "json_path": str(path)})
    batch = build_observation_candidate_batch(observations=observations, scanner_statuses=statuses, generated_at=generated_at, scanner_run_id=scanner_run_id)
    path = batch_path(store, batch["observation_candidate_batch_id"])
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite immutable observation candidate batch: {path}")
    write_json(path, batch, overwrite=False)
    row = {
        "observation_candidate_batch_id": batch["observation_candidate_batch_id"],
        "generated_at": batch["generated_at"],
        "scanner_run_id": batch["scanner_run_id"],
        "scanner_version": batch["scanner_version"],
        "observation_count": batch["observation_count"],
        "immutable_hash": batch["immutable_hash"],
    }
    append_jsonl(batch_registry_path(store), row)
    audit = write_audit_event(
        actor=actor,
        entity_type="observation_candidate_batch",
        entity_id=batch["observation_candidate_batch_id"],
        action="observation_candidate_batch_generated",
        new_state_hash=batch["immutable_hash"],
        reason="Generated read-only research observation candidate batch.",
        metadata={
            "observation_candidate_batch_id": batch["observation_candidate_batch_id"],
            "observation_count": batch["observation_count"],
            "latest_integrity_report_id": batch["latest_integrity_report_id"],
            "latest_research_os_status_report_id": batch["latest_research_os_status_report_id"],
            "immutable_hash": batch["immutable_hash"],
        },
        store_root=store,
    )
    return {"batch": batch, "candidates": candidate_results, "registry_row": row, "audit_event": audit, "json_path": str(path)}


def load_observation_candidate_batch(observation_candidate_batch_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    return read_json(batch_path(store, observation_candidate_batch_id))


def list_observation_candidate_batches(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    store = ensure_store_layout(store_root)
    return read_jsonl(batch_registry_path(store))


def latest_observation_candidate_batch(*, store_root: Path | None = None) -> dict[str, Any] | None:
    rows = list_observation_candidate_batches(store_root=store_root)
    if not rows:
        return None
    return load_observation_candidate_batch(str(rows[-1]["observation_candidate_batch_id"]), store_root=store_root)


def latest_observation_candidates_for_batch(*, store_root: Path | None = None, limit: int = 20) -> list[dict[str, Any]]:
    batch = latest_observation_candidate_batch(store_root=store_root)
    if not batch:
        return []
    return [load_observation_candidate(str(candidate_id), store_root=store_root) for candidate_id in batch.get("observation_candidate_ids", [])[:limit]]
