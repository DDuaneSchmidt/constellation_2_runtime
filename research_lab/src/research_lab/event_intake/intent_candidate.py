from __future__ import annotations

from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso

SCHEMA_VERSION = "intent_candidate.v1"
ALLOWED_EDGE_TYPES = {"mean_reversion", "momentum_continuation", "volatility_expansion", "volatility_normalization", "regime_filter", "risk_off_exhaustion", "gap_fill", "gap_continuation", "lottery_dislocation"}
ALLOWED_FRAGILITY = {"low", "medium", "high", "extreme"}

def recompute_intent_candidate_hash(intent: dict[str, Any]) -> str:
    return content_hash(intent, exclude={"intent_candidate_id", "created_at", "content_hash"}, sort_lists=False)

def build_intent_candidate(
    *,
    event_cluster: dict[str, Any],
    template: dict[str, Any],
    created_at: str | None = None,
) -> dict[str, Any]:
    edge_type = str(template["candidate_edge_type"])
    if edge_type not in ALLOWED_EDGE_TYPES:
        raise ValueError(f"invalid candidate_edge_type: {edge_type}")
    fragility = str(template.get("expected_fragility") or "unknown")
    if fragility not in ALLOWED_FRAGILITY:
        raise ValueError(f"invalid expected_fragility: {fragility}")
    governance_notes = [str(item) for item in template.get("governance_notes", [])]
    if edge_type == "lottery_dislocation" and (fragility != "extreme" or "stricter_review_required" not in governance_notes):
        raise ValueError("lottery event must use extreme fragility and stricter_review_required governance")
    payload = {
        "intent_candidate_id": "",
        "event_cluster_id": str(event_cluster["event_cluster_id"]),
        "event_family_id": str(event_cluster["event_family_id"]),
        "intent_name": str(template["intent_name"]),
        "intent_description": str(template["intent_description"]),
        "candidate_edge_type": edge_type,
        "expected_holding_period": str(template["expected_holding_period"]),
        "expected_frequency": str(template["expected_frequency"]),
        "expected_fragility": fragility,
        "default_test_type": str(template["default_test_type"]),
        "required_data": [str(item) for item in template.get("required_data", [])],
        "governance_notes": governance_notes,
        "created_at": created_at or utc_now_iso(),
        "schema_version": SCHEMA_VERSION,
        "research_label": "RESEARCH_ONLY",
        "content_hash": "",
    }
    fingerprint = recompute_intent_candidate_hash(payload)
    payload["intent_candidate_id"] = f"icand_{short_hash(content_hash({'fingerprint': fingerprint, 'created_at': payload['created_at']}), 16)}"
    payload["content_hash"] = fingerprint
    validate_intent_candidate(payload)
    return payload

def validate_intent_candidate(intent: dict[str, Any]) -> None:
    validate_contract("intent_candidate", intent)
    if intent.get("candidate_edge_type") == "lottery_dislocation":
        if intent.get("expected_fragility") != "extreme" or "stricter_review_required" not in (intent.get("governance_notes") or []):
            raise ValueError("lottery event must use extreme fragility and stricter_review_required governance")
    actual = recompute_intent_candidate_hash(intent)
    if actual != intent.get("content_hash"):
        raise ValueError(f"IntentCandidate content_hash mismatch: expected {intent.get('content_hash')}, got {actual}")
