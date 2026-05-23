from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout


GENERATED_AT = "1970-01-01T00:00:00Z"
RESEARCH_LABEL = "RESEARCH_ONLY"
SCHEMA_VERSION = "challenger_variant.v1"


def _variant_dir(store: Path) -> Path:
    return store / "challenger_variants"


def _variant_path(store: Path, challenger_variant_id: str) -> Path:
    return _variant_dir(store) / f"{challenger_variant_id}.json"


def _registry_path(store: Path) -> Path:
    return store / "registries" / "challenger_variants.jsonl"


def build_challenger_variant(
    *,
    track: dict[str, Any],
    hypothesis: dict[str, Any],
    source_artifact_ids: dict[str, Any],
    generated_at: str = GENERATED_AT,
) -> dict[str, Any]:
    delta = dict(hypothesis.get("deterministic_rule_delta") or {})
    payload = {
        "challenger_variant_id": "",
        "challenger_track_id": track["challenger_track_id"],
        "incumbent_sleeve_id": track["incumbent_sleeve_id"],
        "incumbent_sleeve_version_id": track["incumbent_sleeve_version_id"],
        "hypothesis_id": hypothesis["challenger_hypothesis_id"],
        "variant_name": hypothesis["variant_name"],
        "generated_at": generated_at,
        "schema_version": SCHEMA_VERSION,
        "research_label": RESEARCH_LABEL,
        "parameter_deltas": {key: value for key, value in delta.items() if key in {"holding_period", "signal_threshold"}},
        "rule_deltas": {key: value for key, value in delta.items() if key not in {"holding_period", "signal_threshold"}},
        "regime_constraints": {"risk_regime_filter": delta.get("risk_regime_filter", "")},
        "source_artifact_ids": source_artifact_ids,
        "status": "materialized",
        "governance_constraints": {
            "research_only": True,
            "incumbent_sleeve_mutated": False,
            "paper_trial_mutated": False,
            "candidate_ledger_mutated": False,
            "trading_authorized": False,
            "automatic_promotion_allowed": False,
        },
        "immutable_hash": "",
        "content_hash": "",
    }
    seed = content_hash(payload, exclude={"challenger_variant_id", "immutable_hash", "content_hash", "generated_at"}, sort_lists=True)
    payload["challenger_variant_id"] = f"chv_{short_hash(seed, 16)}"
    payload["immutable_hash"] = content_hash(payload, exclude={"immutable_hash", "content_hash", "generated_at"}, sort_lists=True)
    payload["content_hash"] = payload["immutable_hash"]
    validate_contract("challenger_variant", payload)
    return payload


def write_challenger_variant(
    variant: dict[str, Any],
    *,
    store_root: Path | None = None,
    actor: str = "Aegis",
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    path = _variant_path(store, str(variant["challenger_variant_id"]))
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite immutable challenger variant: {path}")
    validate_contract("challenger_variant", variant)
    write_json(path, variant, overwrite=False)
    row = {
        "challenger_variant_id": variant["challenger_variant_id"],
        "challenger_track_id": variant["challenger_track_id"],
        "incumbent_sleeve_id": variant["incumbent_sleeve_id"],
        "incumbent_sleeve_version_id": variant["incumbent_sleeve_version_id"],
        "hypothesis_id": variant["hypothesis_id"],
        "variant_name": variant["variant_name"],
        "status": variant["status"],
        "research_label": variant["research_label"],
        "content_hash": variant["content_hash"],
        "generated_at": variant["generated_at"],
        "schema_version": variant["schema_version"],
    }
    append_jsonl(_registry_path(store), row)
    if not read_jsonl(_registry_path(store)) or read_jsonl(_registry_path(store))[-1] != row:
        raise RuntimeError("challenger variant registry append failed")
    audit = write_audit_event(
        actor=actor,
        entity_type="challenger_variant",
        entity_id=variant["challenger_variant_id"],
        action="challenger_variant_created",
        new_state_hash=variant["content_hash"],
        reason="Created immutable research-only challenger variant definition.",
        metadata={"registry_row": row, "governance_constraints": variant["governance_constraints"]},
        store_root=store,
    )
    return {"variant": variant, "registry_row": row, "audit_event": audit, "json_path": str(path)}


def load_challenger_variant(challenger_variant_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    return read_json(_variant_path(store, challenger_variant_id))


def list_challenger_variants(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    store = ensure_store_layout(store_root)
    return read_jsonl(_registry_path(store))
