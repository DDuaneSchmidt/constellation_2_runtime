from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any


VOLATILE_HASH_FIELDS = {
    "content_hash",
    "source_hash",
    "manifest_hash",
    "input_hash",
    "output_hash",
    "previous_state_hash",
    "new_state_hash",
}


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_timestamp(value: str) -> str:
    text = str(value).strip()
    if not text:
        return text
    try:
        if text.endswith("Z"):
            parsed = datetime.fromisoformat(text[:-1] + "+00:00")
        else:
            parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except ValueError:
        return text


def canonicalize(value: Any, *, sort_lists: bool = False) -> Any:
    if isinstance(value, dict):
        return {
            str(key): canonicalize(item, sort_lists=sort_lists)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, list):
        items = [canonicalize(item, sort_lists=sort_lists) for item in value]
        if sort_lists:
            return sorted(items, key=lambda item: canonical_json(item))
        return items
    if isinstance(value, str):
        return normalize_timestamp(value)
    return value


def canonical_json(value: Any, *, sort_lists: bool = False) -> str:
    return json.dumps(canonicalize(value, sort_lists=sort_lists), sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256_hex(value: Any, *, sort_lists: bool = False) -> str:
    return hashlib.sha256(canonical_json(value, sort_lists=sort_lists).encode("utf-8")).hexdigest()


def content_hash_payload(payload: dict[str, Any], *, exclude: set[str] | None = None, sort_lists: bool = False) -> dict[str, Any]:
    excluded = set(VOLATILE_HASH_FIELDS)
    if exclude:
        excluded.update(exclude)
    clone = deepcopy(payload)
    for field in excluded:
        clone.pop(field, None)
    return canonicalize(clone, sort_lists=sort_lists)


def content_hash(payload: dict[str, Any], *, exclude: set[str] | None = None, sort_lists: bool = False) -> str:
    return sha256_hex(content_hash_payload(payload, exclude=exclude, sort_lists=sort_lists), sort_lists=sort_lists)


def short_hash(value: str, length: int = 6) -> str:
    return str(value)[:length]
