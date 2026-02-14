"""
canon_json_v1.py

Constellation 2.0 Phase A
Offline canonical JSON + SHA-256 hashing utilities.

Design authority:
- constellation_2/governance/C2_DETERMINISM_STANDARD.md

Hard constraints (Phase A):
- Offline only
- Deterministic output (byte-identical for identical logical input)
- Fail-closed: raise CanonJsonError for any violation

Notes on numeric determinism:
- C2 determinism standard requires explicit numeric rules, but precision rules are not
  defined in the current Design Pack. Therefore, this module does NOT attempt to
  reformat numeric strings from input artifacts.
- For any DERIVED numeric values (computed by mapper), formatting must be governed by
  explicit tick/precision policy in the mapper. If unknown, mapper must VETO.

This module canonicalizes JSON according to the C2 baseline rule (subset):
- json.dumps(sort_keys=True, separators=(",", ":"), ensure_ascii=False)
- UTF-8 bytes
- SHA-256 lowercase hex digest
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Tuple


class CanonJsonError(Exception):
    """Raised when canonicalization/hashing fails (fail-closed)."""


@dataclass(frozen=True)
class CanonicalJsonResult:
    canonical_json: str
    canonical_bytes: bytes
    sha256_hex: str


def _ensure_json_obj(value: Any) -> Any:
    # We allow any JSON-serializable value, but we fail-closed if json.dumps cannot
    # serialize it deterministically with the chosen parameters.
    return value


def canonicalize_json_obj(obj: Any) -> str:
    """
    Canonicalize an in-memory JSON object.

    Returns canonical JSON string (no trailing newline).
    Fail-closed on serialization failure.
    """
    try:
        safe = _ensure_json_obj(obj)
        return json.dumps(
            safe,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
    except Exception as e:  # noqa: BLE001 (explicit fail-closed)
        raise CanonJsonError(f"Canonicalization failed: {e}") from e


def sha256_hex_utf8(s: str) -> str:
    """
    SHA-256 over UTF-8 bytes of a string. Returns lowercase hex.
    """
    try:
        b = s.encode("utf-8")
    except Exception as e:  # noqa: BLE001
        raise CanonJsonError(f"UTF-8 encode failed: {e}") from e
    return hashlib.sha256(b).hexdigest()


def canonicalize_and_hash(obj: Any) -> CanonicalJsonResult:
    """
    Canonicalize JSON and compute SHA-256 hash of canonical UTF-8 bytes.
    """
    canon = canonicalize_json_obj(obj)
    b = canon.encode("utf-8")
    h = hashlib.sha256(b).hexdigest()
    return CanonicalJsonResult(canonical_json=canon, canonical_bytes=b, sha256_hex=h)


def load_json_file(path: Path) -> Any:
    """
    Load JSON from disk (UTF-8). Fail-closed on any parse/IO error.
    """
    try:
        raw = path.read_text(encoding="utf-8")
    except Exception as e:  # noqa: BLE001
        raise CanonJsonError(f"Failed to read JSON file {path}: {e}") from e
    try:
        return json.loads(raw)
    except Exception as e:  # noqa: BLE001
        raise CanonJsonError(f"Failed to parse JSON in {path}: {e}") from e


def canonicalize_and_hash_file(path: Path) -> CanonicalJsonResult:
    """
    Load JSON file, canonicalize, and hash.
    """
    obj = load_json_file(path)
    return canonicalize_and_hash(obj)


def inject_canonical_hash_field(
    obj: Dict[str, Any],
    field_name: str = "canonical_json_hash",
) -> Tuple[Dict[str, Any], str]:
    """
    Returns a NEW dict with `field_name` set to the canonical hash of the object
    *with that field set to null* (to avoid self-referential hashing ambiguity).

    Contract:
    - If the field exists, it is treated as nullable and will be forced to None
      during hash computation.
    - If the object is not a dict, fail-closed.
    """
    if not isinstance(obj, dict):
        raise CanonJsonError("inject_canonical_hash_field requires a JSON object (dict).")

    working = dict(obj)
    working[field_name] = None
    res = canonicalize_and_hash(working)
    out = dict(obj)
    out[field_name] = res.sha256_hex
    return out, res.sha256_hex
