from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from research_lab import SCHEMA_VERSION
from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso


UNIVERSE_HASH_EXCLUDE = {"universe_snapshot_id", "created_at", "content_hash"}


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Universe input must be a mapping: {path}")
    return payload


def _normalize_symbol(row: dict[str, Any]) -> dict[str, Any]:
    symbol = str(row.get("symbol") or "").strip().upper()
    if not symbol:
        raise ValueError("Universe symbol entry is missing symbol")
    return {
        "symbol": symbol,
        "asset_type": str(row.get("asset_type") or "").strip() or "UNKNOWN",
        "category": str(row.get("category") or "").strip() or "UNKNOWN",
        "active": bool(row.get("active")),
        "min_start_date": str(row.get("min_start_date") or "").strip() or "UNKNOWN",
        "notes": str(row.get("notes") or "").strip(),
    }


def build_universe_snapshot(
    *,
    name: str,
    version: str,
    input_path: Path,
    created_by: str = "Aegis",
    created_at: str | None = None,
) -> dict[str, Any]:
    source = _load_yaml(input_path)
    raw_symbols = source.get("symbols")
    if not isinstance(raw_symbols, list) or not raw_symbols:
        raise ValueError("Universe input requires a non-empty symbols list")
    symbols = sorted((_normalize_symbol(dict(row)) for row in raw_symbols), key=lambda row: row["symbol"])
    payload: dict[str, Any] = {
        "universe_snapshot_id": "",
        "universe_name": str(name).strip(),
        "universe_version": str(version).strip(),
        "created_at": created_at or utc_now_iso(),
        "created_by": str(source.get("created_by") or created_by),
        "symbols": symbols,
        "symbol_count": len(symbols),
        "selection_policy": str(source.get("selection_policy") or "UNKNOWN").strip(),
        "source_notes": str(source.get("source_notes") or "").strip(),
        "content_hash": "",
        "schema_version": SCHEMA_VERSION,
    }
    digest = content_hash(payload, exclude=UNIVERSE_HASH_EXCLUDE, sort_lists=False)
    payload["content_hash"] = digest
    payload["universe_snapshot_id"] = f"us_{payload['universe_name']}_{payload['universe_version']}_{short_hash(digest)}"
    validate_contract("universe_snapshot", payload)
    return payload


def recompute_universe_content_hash(snapshot: dict[str, Any]) -> str:
    return content_hash(snapshot, exclude=UNIVERSE_HASH_EXCLUDE, sort_lists=False)
