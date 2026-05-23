from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import append_jsonl, read_jsonl
from research_lab.storage.paths import ensure_store_layout


DECISION_SCHEMA_VERSION = "operator_decision.v1"
VALID_DECISIONS = {"approve", "ignore", "defer", "reject"}


def decisions_registry_path(store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "registries" / "operator_decisions.jsonl"


def validate_operator_decision(decision: dict[str, Any]) -> None:
    validate_contract("operator_decision", decision)


def load_operator_decisions(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    return read_jsonl(decisions_registry_path(store_root))


def latest_decisions_by_candidate(*, store_root: Path | None = None) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for row in load_operator_decisions(store_root=store_root):
        candidate_id = row["candidate_id"]
        if candidate_id not in latest or row["decided_at"] >= latest[candidate_id]["decided_at"]:
            latest[candidate_id] = row
    return latest


def build_operator_decision(
    *,
    candidate_id: str,
    candidate_batch_id: str,
    decision: str,
    decision_reason: str,
    decided_by: str,
    portfolio_context_snapshot_id: str = "",
    notes: str = "",
    decided_at: str | None = None,
) -> dict[str, Any]:
    normalized = decision.strip().lower()
    if normalized not in VALID_DECISIONS:
        raise RuntimeError(f"Unsupported operator decision: {decision}")
    if not decision_reason.strip():
        raise RuntimeError("Operator decision reason is required")
    if not decided_by.strip():
        raise RuntimeError("Decision operator is required")
    payload = {
        "candidate_id": candidate_id,
        "candidate_batch_id": candidate_batch_id,
        "decision": normalized,
        "decision_reason": decision_reason,
        "decided_by": decided_by,
        "decided_at": decided_at or utc_now_iso(),
        "portfolio_context_snapshot_id": portfolio_context_snapshot_id,
        "notes": notes,
        "schema_version": DECISION_SCHEMA_VERSION,
    }
    payload["content_hash"] = content_hash(payload)
    payload["operator_decision_id"] = f"od_{short_hash(payload['content_hash'], 16)}"
    payload["content_hash"] = content_hash(payload)
    validate_operator_decision(payload)
    return payload


def append_operator_decision(decision: dict[str, Any], *, store_root: Path | None = None, actor: str = "operator") -> dict[str, Any]:
    validate_operator_decision(decision)
    store = ensure_store_layout(store_root)
    append_jsonl(decisions_registry_path(store), decision)
    write_audit_event(
        actor=actor,
        entity_type="operator_decision",
        entity_id=decision["operator_decision_id"],
        action="operator_decision_recorded",
        new_state_hash=decision["content_hash"],
        reason="Recorded append-only candidate operator decision.",
        metadata={
            "candidate_id": decision["candidate_id"],
            "candidate_batch_id": decision["candidate_batch_id"],
            "decision": decision["decision"],
        },
        store_root=store,
    )
    return decision

