from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_scalar(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    return value


def normalize(value: Any) -> Any:
    if is_dataclass(value):
        value = asdict(value)
    if isinstance(value, dict):
        return {str(key): normalize(val) for key, val in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, tuple):
        return [normalize(item) for item in value]
    if isinstance(value, list):
        return [normalize(item) for item in value]
    if isinstance(value, set):
        normalized_items = [normalize(item) for item in value]
        return sorted(normalized_items, key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")))
    return _normalize_scalar(value)


def canonical_json_bytes(value: Any) -> bytes:
    normalized = normalize(value)
    return (json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n").encode("utf-8")


def content_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def build_envelope(artifact_type: str, record: Any, created_at: str | None = None) -> dict[str, Any]:
    created = created_at or utc_now()
    normalized_record = normalize(record)
    base = {
        "artifact_type": artifact_type,
        "schema_version": SCHEMA_VERSION,
        "created_at": created,
        "record": normalized_record,
    }
    return {
        **base,
        "content_hash": content_hash(base),
    }


def require_fields(payload: dict[str, Any], fields: tuple[str, ...], label: str) -> None:
    missing = [field for field in fields if field not in payload]
    if missing:
        raise ValueError(f"{label}_FIELDS_MISSING:{','.join(sorted(missing))}")
