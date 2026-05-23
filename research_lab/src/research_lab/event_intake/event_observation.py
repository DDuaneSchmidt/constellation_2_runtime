from __future__ import annotations

from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso

SCHEMA_VERSION = "event_observation.v1"
ALLOWED_SOURCE_TYPES = {"operator_observation", "news_article", "social_media_claim", "market_data_scan", "research_note", "manual_import", "recovered_artifact"}
ALLOWED_CONFIDENCE_LEVELS = {"low", "medium", "high", "unknown"}
ALLOWED_RESEARCH_PRIORITIES = {"low", "medium", "high", "watchlist"}

def recompute_event_observation_hash(observation: dict[str, Any]) -> str:
    return content_hash(observation, exclude={"event_observation_id", "observed_at", "content_hash"}, sort_lists=False)

def build_event_observation(
    *,
    event_family_id: str,
    title: str,
    description: str,
    observed_by: str = "Aegis",
    source_type: str,
    source_ref: str = "",
    symbols_mentioned: list[str] | None = None,
    market_context: dict[str, Any] | None = None,
    suspected_mechanism: str = "",
    confidence_level: str | None = None,
    research_priority: str = "medium",
    notes: str = "",
    observed_at: str | None = None,
) -> dict[str, Any]:
    source = str(source_type).strip()
    if source not in ALLOWED_SOURCE_TYPES:
        raise ValueError(f"invalid source_type: {source_type}")
    confidence = str(confidence_level or ("low" if source == "social_media_claim" else "unknown")).strip()
    if confidence not in ALLOWED_CONFIDENCE_LEVELS:
        raise ValueError(f"invalid confidence_level: {confidence_level}")
    if source == "social_media_claim" and confidence != "low":
        raise ValueError("social_media_claim observations must be low confidence")
    priority = str(research_priority).strip()
    if priority not in ALLOWED_RESEARCH_PRIORITIES:
        raise ValueError(f"invalid research_priority: {research_priority}")
    cleaned_notes = str(notes or "").strip()
    if source == "social_media_claim" and "unverified_claim" not in cleaned_notes:
        cleaned_notes = f"{cleaned_notes}; unverified_claim" if cleaned_notes else "unverified_claim"
    symbols = sorted({str(symbol).strip().upper() for symbol in symbols_mentioned or [] if str(symbol).strip()})
    payload = {
        "event_observation_id": "",
        "event_family_id": str(event_family_id).strip().lower(),
        "title": str(title).strip(),
        "description": str(description).strip(),
        "observed_at": observed_at or utc_now_iso(),
        "observed_by": str(observed_by).strip() or "Aegis",
        "source_type": source,
        "source_ref": str(source_ref or ""),
        "symbols_mentioned": symbols,
        "market_context": market_context or {},
        "suspected_mechanism": str(suspected_mechanism or ""),
        "confidence_level": confidence,
        "research_priority": priority,
        "notes": cleaned_notes,
        "schema_version": SCHEMA_VERSION,
        "research_label": "RESEARCH_ONLY",
        "content_hash": "",
    }
    fingerprint = recompute_event_observation_hash(payload)
    payload["event_observation_id"] = f"eobs_{short_hash(content_hash({'fingerprint': fingerprint, 'observed_at': payload['observed_at']}), 16)}"
    payload["content_hash"] = fingerprint
    validate_event_observation(payload)
    return payload

def validate_event_observation(observation: dict[str, Any]) -> None:
    validate_contract("event_observation", observation)
    if observation.get("source_type") == "social_media_claim":
        if observation.get("confidence_level") != "low":
            raise ValueError("social_media_claim observations must be low confidence")
        if "unverified_claim" not in str(observation.get("notes") or ""):
            raise ValueError("social_media_claim observations must include unverified_claim in notes")
    actual = recompute_event_observation_hash(observation)
    if actual != observation.get("content_hash"):
        raise ValueError(f"EventObservation content_hash mismatch: expected {observation.get('content_hash')}, got {actual}")
