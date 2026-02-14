"""
c2_validate_v1.py

Constellation 2.0 Phase A
Offline CLI: validate a JSON file against a named C2 schema.

Usage:
  python3 -m constellation_2.phaseA.tools.c2_validate_v1 <schema_name> <json_path>

Example:
  python3 -m constellation_2.phaseA.tools.c2_validate_v1 options_intent.v2 constellation_2/acceptance/samples/sample_options_intent.v2.json
"""

from __future__ import annotations

import sys
from pathlib import Path

from constellation_2.phaseA.lib.canon_json_v1 import CanonJsonError, canonicalize_and_hash_file
from constellation_2.phaseA.lib.schema_loader_v1 import SchemaLoaderError, SCHEMA_NAME_TO_FILE
from constellation_2.phaseA.lib.validate_json_against_schema_v1 import (
    JsonSchemaValidationBoundaryError,
    validate_obj_against_schema,
)
from constellation_2.phaseA.lib.canon_json_v1 import load_json_file


def main() -> int:
    if len(sys.argv) != 3:
        print("ERR: expected args: <schema_name> <json_path>", file=sys.stderr)
        print(f"Known schemas: {sorted(SCHEMA_NAME_TO_FILE.keys())}", file=sys.stderr)
        return 2

    schema_name = sys.argv[1].strip()
    p = Path(sys.argv[2]).expanduser().resolve()

    try:
        obj = load_json_file(p)
    except CanonJsonError as e:
        print(f"ERR: cannot read/parse JSON: {e}", file=sys.stderr)
        return 2

    try:
        res = validate_obj_against_schema(schema_name, obj)
    except (SchemaLoaderError, JsonSchemaValidationBoundaryError) as e:
        print(f"ERR: validation boundary failure: {e}", file=sys.stderr)
        return 2

    if not res.ok:
        print(f"FAIL: schema={schema_name} file={p}")
        print(res.error or "validation failed")
        return 1

    # Also print canonical hash (computed with self-hash null if present, because tool c2_hash_json_v1 does so)
    try:
        h = canonicalize_and_hash_file(p).sha256_hex
    except CanonJsonError as e:
        print(f"ERR: canonical hash failed: {e}", file=sys.stderr)
        return 2

    print(f"OK: schema={schema_name} file={p}")
    print(f"CANON_SHA256={h}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
