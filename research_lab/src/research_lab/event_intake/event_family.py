from __future__ import annotations

from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash

SCHEMA_VERSION = "event_family.v1"

def recompute_event_family_hash(family: dict[str, Any]) -> str:
    return content_hash(family, exclude={"content_hash"}, sort_lists=False)

def build_event_family(spec: dict[str, Any]) -> dict[str, Any]:
    symbols = [str(symbol).strip().upper() for symbol in spec.get("default_symbols", []) if str(symbol).strip()]
    windows = sorted({int(window) for window in spec.get("default_forward_windows", [])})
    if not symbols:
        raise ValueError("EventFamily requires default_symbols")
    if not windows:
        raise ValueError("EventFamily requires default_forward_windows")
    payload = {
        "event_family_id": str(spec["event_family_id"]).strip().lower(),
        "name": str(spec["name"]).strip(),
        "description": str(spec["description"]).strip(),
        "asset_focus": [str(item).strip() for item in spec.get("asset_focus", []) if str(item).strip()],
        "default_symbols": symbols,
        "default_event_definitions": list(spec.get("default_event_definitions", [])),
        "default_forward_windows": windows,
        "default_regime_dimensions": [str(item).strip() for item in spec.get("default_regime_dimensions", []) if str(item).strip()],
        "default_research_intents": [str(item).strip() for item in spec.get("default_research_intents", []) if str(item).strip()],
        "default_data_requirement_status": str(spec.get("default_data_requirement_status") or "unknown"),
        "risk_notes": [str(item).strip() for item in spec.get("risk_notes", []) if str(item).strip()],
        "schema_version": SCHEMA_VERSION,
        "content_hash": "",
    }
    payload["content_hash"] = recompute_event_family_hash(payload)
    validate_event_family(payload)
    return payload

def validate_event_family(family: dict[str, Any]) -> None:
    validate_contract("event_family", family)
    actual = recompute_event_family_hash(family)
    if actual != family.get("content_hash"):
        raise ValueError(f"EventFamily content_hash mismatch: expected {family.get('content_hash')}, got {actual}")
