"""
validate_json_against_schema_v1.py

Constellation 2.0 Phase A
Offline JSON Schema validation boundary.

Requirements:
- Use jsonschema (already proven importable in this repo environment)
- Load schemas from constellation_2/schemas/ via schema_loader_v1
- Return machine-usable pass/fail + error text
- Fail-closed if schema missing or invalid (SchemaLoaderError -> ValidationError boundary)

This module does NOT canonicalize or hash; that is handled by canon_json_v1.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import jsonschema

from constellation_2.phaseA.lib.schema_loader_v1 import SchemaLoaderError, load_schema


class JsonSchemaValidationBoundaryError(Exception):
    """Raised when the validation boundary itself cannot operate (fail-closed)."""


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    schema_name: str
    error: Optional[str]


def validate_obj_against_schema(schema_name: str, obj: Any) -> ValidationResult:
    """
    Validate an in-memory JSON object against a named schema.

    Returns:
      ValidationResult(ok=True, error=None) if valid,
      ValidationResult(ok=False, error=...) if invalid.

    Fail-closed behavior:
      - Unknown/missing schema => raise JsonSchemaValidationBoundaryError
      - Schema parse/loader failure => raise JsonSchemaValidationBoundaryError
      - jsonschema internal error => raise JsonSchemaValidationBoundaryError
    """
    try:
        schema: Dict[str, Any] = load_schema(schema_name)
    except SchemaLoaderError as e:
        raise JsonSchemaValidationBoundaryError(f"Schema load failed for '{schema_name}': {e}") from e

    try:
        # Use the schema-declared draft via validator_for + check_schema.
        ValidatorCls = jsonschema.validators.validator_for(schema)
        ValidatorCls.check_schema(schema)
        v = ValidatorCls(schema)
        errors = sorted(v.iter_errors(obj), key=lambda e: list(e.absolute_path))
    except Exception as e:  # noqa: BLE001
        raise JsonSchemaValidationBoundaryError(f"jsonschema boundary error for '{schema_name}': {e}") from e

    if not errors:
        return ValidationResult(ok=True, schema_name=schema_name, error=None)

    # Provide a deterministic error string: first error only, with stable fields.
    e0 = errors[0]
    path = "/".join([str(p) for p in e0.absolute_path]) if e0.absolute_path else ""
    schema_path = "/".join([str(p) for p in e0.absolute_schema_path]) if e0.absolute_schema_path else ""
    msg = str(e0.message)

    err = f"path='{path}' schema_path='{schema_path}' message='{msg}'"
    return ValidationResult(ok=False, schema_name=schema_name, error=err)


def validate_file_against_schema(schema_name: str, json_obj: Any) -> ValidationResult:
    """
    Alias maintained for clarity in callers.
    (Callers that load JSON from file should pass the parsed object.)
    """
    return validate_obj_against_schema(schema_name, json_obj)
