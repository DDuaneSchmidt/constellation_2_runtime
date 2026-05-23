from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.contracts.schemas import validate_contract
from research_lab.sleeves.sleeve_health import load_sleeve_health_snapshot
from research_lab.sleeves.sleeve_registry import append_registry_row, sleeve_challenge_path
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import read_json, write_json
from research_lab.storage.paths import ensure_store_layout


def validate_sleeve_challenge(challenge: dict[str, Any]) -> None:
    validate_contract("sleeve_challenge", challenge)


def build_sleeve_challenge(
    *,
    sleeve_id: str,
    sleeve_version_id: str,
    sleeve_health_snapshot_id: str,
    created_by: str = "Aegis",
    store_root: Path | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    health = load_sleeve_health_snapshot(sleeve_id, sleeve_health_snapshot_id, store_root=store_root)
    if health["backtest_health"] == "watch":
        challenge_type = "underperformance"
        recommended = "continue_research"
        reason = health["health_reasons"]["backtest_health"]
    elif health["candidate_health"] == "watch":
        challenge_type = "candidate_drought"
        recommended = "collect_more_candidates"
        reason = health["health_reasons"]["candidate_health"]
    elif health["attribution_health"] in {"watch", "insufficient_data"}:
        challenge_type = "poor_attribution"
        recommended = "manual_review_required"
        reason = health["health_reasons"]["attribution_health"]
    else:
        challenge_type = "manual_review"
        recommended = "manual_review_required"
        reason = f"Sleeve health is {health['overall_health']} and requires governance review."
    payload = {
        "sleeve_id": sleeve_id,
        "sleeve_version_id": sleeve_version_id,
        "sleeve_health_snapshot_id": sleeve_health_snapshot_id,
        "challenge_reason": reason,
        "challenge_type": challenge_type,
        "recommended_action": recommended,
        "evidence_refs": {
            "linked_evidence_package_ids": health["linked_evidence_package_ids"],
            "linked_candidate_batch_ids": health["linked_candidate_batch_ids"],
            "linked_attribution_report_ids": health["linked_attribution_report_ids"],
        },
        "created_at": created_at or utc_now_iso(),
        "created_by": created_by,
        "schema_version": "sleeve_challenge.v1",
        "compliance_label": "Challenge is governance review only. It does not approve live operation or mutate sleeve logic.",
    }
    seed_hash = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    payload["sleeve_challenge_id"] = f"slvc_{sleeve_id}_{short_hash(seed_hash, 12)}"
    payload["content_hash"] = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    validate_sleeve_challenge(payload)
    return payload


def store_sleeve_challenge(challenge: dict[str, Any], *, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    validate_sleeve_challenge(challenge)
    store = ensure_store_layout(store_root)
    write_json(sleeve_challenge_path(challenge["sleeve_id"], challenge["sleeve_challenge_id"], store_root=store), challenge, overwrite=False)
    row = {
        "sleeve_challenge_id": challenge["sleeve_challenge_id"],
        "sleeve_id": challenge["sleeve_id"],
        "sleeve_version_id": challenge["sleeve_version_id"],
        "sleeve_health_snapshot_id": challenge["sleeve_health_snapshot_id"],
        "challenge_type": challenge["challenge_type"],
        "recommended_action": challenge["recommended_action"],
        "content_hash": challenge["content_hash"],
        "created_at": challenge["created_at"],
        "schema_version": challenge["schema_version"],
    }
    append_registry_row("sleeve_challenges.jsonl", row, store_root=store)
    write_audit_event(actor=actor, entity_type="sleeve_challenge", entity_id=challenge["sleeve_challenge_id"], action="sleeve_challenge_created", new_state_hash=challenge["content_hash"], reason="Created deterministic sleeve challenge.", metadata={"registry_row": row}, store_root=store)
    return row


def load_sleeve_challenge(sleeve_id: str, sleeve_challenge_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    return read_json(sleeve_challenge_path(sleeve_id, sleeve_challenge_id, store_root=store_root))

