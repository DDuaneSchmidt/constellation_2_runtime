from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


EVENT_TRIGGERS = (
    "BREADTH_COLLAPSE",
    "DEFENSIVE_ROTATION",
    "FAILED_BREAKOUT",
    "MACRO_EVENT_REACTION",
    "PANIC_EXHAUSTION",
    "RECOVERY_FAILURE",
    "VOLATILITY_SPIKE",
)

REGIME_TRIGGERS = (
    "volatility_regime",
    "trend_regime",
    "liquidity_regime",
    "breadth_regime",
    "event_regime",
)

DEFAULT_SLEEVE_REGISTRY = Path("governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json")


def build_event_regime_trigger_registry_v1(*, repo_root: Path, day_utc: str) -> dict[str, Any]:
    repo_root = Path(repo_root).resolve()
    sleeve_registry_path = repo_root / DEFAULT_SLEEVE_REGISTRY
    confirmed_sleeves = confirmed_sleeve_ids_v1(repo_root=repo_root)
    mappings: list[dict[str, Any]] = []
    for trigger in EVENT_TRIGGERS:
        mappings.append(_mapping(trigger_key=trigger, trigger_type="EVENT", sleeve_ids=confirmed_sleeves))
    for trigger in REGIME_TRIGGERS:
        mappings.append(_mapping(trigger_key=trigger, trigger_type="REGIME", sleeve_ids=confirmed_sleeves))
    return {
        "schema_id": "aegis_event_regime_trigger_registry",
        "schema_version": "v1",
        "artifact_id": "aegis_event_regime_trigger_registry_v1",
        "generated_at": _now(),
        "day_utc": day_utc,
        "sleeve_registry_path": str(sleeve_registry_path),
        "confirmed_sleeve_ids": confirmed_sleeves,
        "mapping_status": "CONFIRMED" if confirmed_sleeves else "MISSING_SLEEVE_MAPPING",
        "mappings": mappings,
        "safety": {
            "advisory_only": True,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "automatic_sleeve_mutation_allowed": False,
        },
    }


def confirmed_sleeve_ids_v1(*, repo_root: Path) -> list[str]:
    path = Path(repo_root).resolve() / DEFAULT_SLEEVE_REGISTRY
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    sleeves = payload.get("sleeves") if isinstance(payload.get("sleeves"), list) else []
    out: list[str] = []
    for row in sleeves:
        if not isinstance(row, dict):
            continue
        sleeve_id = str(row.get("sleeve_id") or "").strip().upper()
        if sleeve_id and bool(row.get("enabled", False)):
            out.append(sleeve_id)
    return sorted(set(out))


def mapping_by_trigger_v1(registry: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = registry.get("mappings") if isinstance(registry.get("mappings"), list) else []
    return {str(row.get("trigger_key") or ""): row for row in rows if isinstance(row, dict)}


def _mapping(*, trigger_key: str, trigger_type: str, sleeve_ids: list[str]) -> dict[str, Any]:
    return {
        "trigger_key": trigger_key,
        "trigger_type": trigger_type,
        "condition": _condition(trigger_key=trigger_key, trigger_type=trigger_type),
        "sleeve_ids": sleeve_ids,
        "mapping_status": "CONFIRMED" if sleeve_ids else "MISSING_SLEEVE_MAPPING",
        "reason": (
            f"{trigger_type} trigger {trigger_key} maps to confirmed sleeves for advisory review."
            if sleeve_ids
            else f"{trigger_type} trigger {trigger_key} has no confirmed sleeve_id in the sleeve registry."
        ),
        "advisory_only": True,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "enabled": True,
        "minimum_evidence_quality": "LOW",
        "max_runs_per_day": 3,
        "cooldown_minutes": 15,
    }


def _condition(*, trigger_key: str, trigger_type: str) -> str:
    if trigger_type == "EVENT":
        return f"event monitor triggered event_type={trigger_key}"
    return f"regime context contains evaluated {trigger_key} classification other than UNKNOWN"


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
