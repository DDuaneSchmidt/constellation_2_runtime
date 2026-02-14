# constellation_2/phaseB/lib/canon_json_v1.py
#
# Canonical JSON + hashing for Constellation 2.0 Phase B.
#
# Governance alignment:
# - C2_DETERMINISM_STANDARD.md:
#   * sorted keys
#   * no insignificant whitespace
#   * UTF-8
#   * SHA-256 lowercase hex
#   * forbid environment/locale dependent behavior
# - We explicitly HARD FAIL on floats to avoid non-deterministic numeric encodings.

from __future__ import annotations

import copy
import hashlib
import json
from typing import Any, Dict, Iterable, List, Tuple


class CanonicalizationError(Exception):
    pass


def _walk_assert_no_floats(x: Any, path: str) -> None:
    if isinstance(x, float):
        raise CanonicalizationError(f"FLOAT_FORBIDDEN at {path}")
    if isinstance(x, dict):
        for k, v in x.items():
            if not isinstance(k, str):
                raise CanonicalizationError(f"NON_STRING_KEY_FORBIDDEN at {path}")
            _walk_assert_no_floats(v, f"{path}.{k}")
        return
    if isinstance(x, list):
        for i, v in enumerate(x):
            _walk_assert_no_floats(v, f"{path}[{i}]")
        return
    if isinstance(x, tuple):
        for i, v in enumerate(x):
            _walk_assert_no_floats(v, f"{path}({i})")
        return
    # primitives ok: None/bool/int/str


def canonical_json_bytes_v1(obj: Any) -> bytes:
    """
    Deterministic canonical JSON serialization:
    - UTF-8
    - sorted keys
    - separators to remove insignificant whitespace
    - allow_nan=False (reject NaN/Infinity)
    - ensure_ascii=False (UTF-8 content preserved)
    """
    _walk_assert_no_floats(obj, "$")
    try:
        s = json.dumps(
            obj,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as e:
        raise CanonicalizationError(f"JSON_CANONICALIZE_FAILED: {e}") from e
    return s.encode("utf-8")


def sha256_hex_v1(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_sha256_hex_v1(obj: Any) -> str:
    """
    Hash of canonical form.
    """
    return sha256_hex_v1(canonical_json_bytes_v1(obj))


def canonical_hash_excluding_fields_v1(obj: Dict[str, Any], fields: Iterable[str]) -> str:
    """
    Compute canonical SHA-256 over a deep-copied object with specified fields set to None
    (not removed), to avoid self-referential hashing.
    """
    if not isinstance(obj, dict):
        raise CanonicalizationError("EXCLUDING_FIELDS_REQUIRES_OBJECT")
    cp = copy.deepcopy(obj)
    for f in fields:
        if f in cp:
            cp[f] = None
    return canonical_sha256_hex_v1(cp)


def canonical_hash_for_c2_artifact_v1(obj: Dict[str, Any]) -> str:
    """
    C2 convention for artifacts that include 'canonical_json_hash':
    hash the canonical JSON with canonical_json_hash forced to null.
    """
    return canonical_hash_excluding_fields_v1(obj, fields=("canonical_json_hash",))
