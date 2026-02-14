# constellation_2/phaseB/lib/validate_against_schema_v1.py
#
# jsonschema validation helper for Constellation 2.0 Phase B.
#
# Fail-closed rules:
# - Missing schema file => HARD FAIL (raise)
# - Invalid JSON instance => HARD FAIL (raise)
# - Floats are forbidden anywhere in the instance (determinism standard)
#
# Schema standard:
# - JSON Schema draft 2020-12 (matches schema files)

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import jsonschema
from jsonschema import Draft202012Validator

from .canon_json_v1 import CanonicalizationError, _walk_assert_no_floats


class SchemaValidationError(Exception):
    pass


def _read_json_file_strict(path: Path) -> Any:
    if not path.exists():
        raise SchemaValidationError(f"SCHEMA_FILE_MISSING: {str(path)}")
    if not path.is_file():
        raise SchemaValidationError(f"SCHEMA_PATH_NOT_FILE: {str(path)}")
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise SchemaValidationError(f"SCHEMA_JSON_INVALID: {str(path)}") from e


def load_schema_v1(repo_root: Path, schema_relpath: str) -> Dict[str, Any]:
    schema_path = repo_root / schema_relpath
    schema = _read_json_file_strict(schema_path)
    if not isinstance(schema, dict):
        raise SchemaValidationError(f"SCHEMA_NOT_OBJECT: {schema_relpath}")
    return schema


def validate_instance_against_schema_v1(instance: Any, schema: Dict[str, Any], schema_name: str) -> None:
    # Determinism guard: forbid floats anywhere before validation.
    try:
        _walk_assert_no_floats(instance, "$")
    except CanonicalizationError as e:
        raise SchemaValidationError(f"INSTANCE_NONDETERMINISTIC_FLOAT: {schema_name}: {e}") from e

    try:
        Draft202012Validator.check_schema(schema)
    except jsonschema.exceptions.SchemaError as e:
        raise SchemaValidationError(f"SCHEMA_INVALID_DRAFT202012: {schema_name}") from e

    v = Draft202012Validator(schema)
    errors = sorted(v.iter_errors(instance), key=lambda e: (list(e.path), e.message))
    if errors:
        # Provide first few errors to keep output operator-friendly but still useful.
        lines = []
        for e in errors[:10]:
            loc = "$"
            for p in list(e.path):
                if isinstance(p, int):
                    loc += f"[{p}]"
                else:
                    loc += f".{p}"
            lines.append(f"{loc}: {e.message}")
        raise SchemaValidationError(f"SCHEMA_VALIDATION_FAILED: {schema_name}\n" + "\n".join(lines))


def validate_against_repo_schema_v1(instance: Any, repo_root: Path, schema_relpath: str) -> None:
    schema = load_schema_v1(repo_root, schema_relpath)
    validate_instance_against_schema_v1(instance, schema, schema_relpath)
