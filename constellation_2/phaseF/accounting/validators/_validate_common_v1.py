from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Tuple

try:
    import jsonschema
    from jsonschema import Draft202012Validator
except Exception as e:  # pragma: no cover
    jsonschema = None
    Draft202012Validator = None
    _IMPORT_ERR = e


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()

SCHEMA_DIR = REPO_ROOT / "governance" / "04_DATA" / "SCHEMAS" / "C2" / "ACCOUNTING"


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    code: str
    message: str


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _read_json_file(path: Path) -> Tuple[Dict[str, Any], bytes]:
    b = path.read_bytes()
    obj = json.loads(b.decode("utf-8"))
    if not isinstance(obj, dict):
        raise ValueError("Top-level JSON must be an object")
    return obj, b


def _load_schema(schema_filename: str) -> Dict[str, Any]:
    schema_path = (SCHEMA_DIR / schema_filename).resolve()
    if not str(schema_path).startswith(str(SCHEMA_DIR.resolve())):
        raise RuntimeError("Schema path escape detected")

    if not schema_path.is_file():
        raise FileNotFoundError(f"Missing schema file: {schema_path}")

    schema_obj, _ = _read_json_file(schema_path)
    return schema_obj


def validate_instance_against_schema(instance_path: Path, schema_filename: str) -> ValidationResult:
    if jsonschema is None or Draft202012Validator is None:
        return ValidationResult(
            ok=False,
            code="FAIL_VALIDATOR_DEPENDENCY_MISSING",
            message=f"python jsonschema unavailable: {_IMPORT_ERR!r}",
        )

    try:
        inst_obj, _ = _read_json_file(instance_path)
    except Exception as e:
        return ValidationResult(ok=False, code="FAIL_JSON_PARSE", message=str(e))

    try:
        schema_obj = _load_schema(schema_filename)
    except Exception as e:
        return ValidationResult(ok=False, code="FAIL_SCHEMA_LOAD", message=str(e))

    try:
        validator = Draft202012Validator(schema_obj)
        errors = sorted(validator.iter_errors(inst_obj), key=lambda er: (list(er.path), er.message))
        if errors:
            # Produce stable error message (first error only) to keep outputs deterministic.
            er0 = errors[0]
            path_str = "/".join(str(p) for p in er0.path) if er0.path else "<root>"
            msg = f"{path_str}: {er0.message}"
            return ValidationResult(ok=False, code="FAIL_SCHEMA_VIOLATION", message=msg)
    except Exception as e:
        return ValidationResult(ok=False, code="FAIL_SCHEMA_VALIDATION_EXCEPTION", message=str(e))

    return ValidationResult(ok=True, code="OK", message="OK")


def main() -> None:
    ap = argparse.ArgumentParser(description="C2 accounting validator common harness (internal).")
    ap.add_argument("--schema", required=True, help="Schema filename under governance/04_DATA/SCHEMAS/C2/ACCOUNTING/")
    ap.add_argument("--path", required=True, help="Path to JSON instance to validate")
    args = ap.parse_args()

    instance_path = Path(args.path).resolve()
    # Allow validating files outside repo (e.g. staged outputs), but must exist.
    if not instance_path.is_file():
        print(f"FAIL: missing instance file: {instance_path}", file=sys.stderr)
        sys.exit(2)

    res = validate_instance_against_schema(instance_path, args.schema)
    if res.ok:
        print("OK")
        sys.exit(0)

    print(f"{res.code}: {res.message}", file=sys.stderr)
    sys.exit(2)


if __name__ == "__main__":
    main()
