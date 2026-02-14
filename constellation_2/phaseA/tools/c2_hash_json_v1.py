"""
c2_hash_json_v1.py

Offline helper: canonicalize JSON and print SHA-256 hash of canonical UTF-8 bytes.

Usage:
  python3 constellation_2/phaseA/tools/c2_hash_json_v1.py /path/to/file.json

Rules:
- Forces canonical_json_hash field (if present) to null before hashing, so the hash
  represents the canonical content excluding self-hash.
- Deterministic: uses Phase A canonicalization (sorted keys, no whitespace).
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict

from constellation_2.phaseA.lib.canon_json_v1 import CanonJsonError, canonicalize_and_hash, load_json_file


def _force_canon_hash_null(obj: Any) -> Any:
    if isinstance(obj, dict) and "canonical_json_hash" in obj:
        out: Dict[str, Any] = dict(obj)
        out["canonical_json_hash"] = None
        return out
    return obj


def main() -> int:
    if len(sys.argv) != 2:
        print("ERR: expected exactly one argument: path to JSON file", file=sys.stderr)
        return 2
    p = Path(sys.argv[1]).expanduser().resolve()
    try:
        obj = load_json_file(p)
        obj2 = _force_canon_hash_null(obj)
        res = canonicalize_and_hash(obj2)
        print(res.sha256_hex)
        return 0
    except CanonJsonError as e:
        print(f"ERR: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
