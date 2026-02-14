from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict

from constellation_2.phaseD.lib.validate_against_schema_v1 import (
    SchemaValidationError,
    validate_against_repo_schema_v1,
)

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()


def load_json_object_strict(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise ValueError(f"INPUT_FILE_MISSING: {str(path)}")
    if not path.is_file():
        raise ValueError(f"INPUT_PATH_NOT_FILE: {str(path)}")
    try:
        with path.open("r", encoding="utf-8") as f:
            obj = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"INPUT_JSON_INVALID: {str(path)}: {e}") from e
    if not isinstance(obj, dict):
        raise ValueError("TOP_LEVEL_JSON_NOT_OBJECT")
    return obj


def validate_file_against_repo_schema_or_exit(path: Path, schema_relpath: str) -> None:
    obj = load_json_object_strict(path)
    try:
        validate_against_repo_schema_v1(obj, REPO_ROOT, schema_relpath)
    except SchemaValidationError as e:
        print(f"FAIL_SCHEMA_VIOLATION: {schema_relpath}: {e}", file=sys.stderr)
        raise SystemExit(2)
    print("OK")
