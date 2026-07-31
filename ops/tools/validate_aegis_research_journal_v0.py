from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_JOURNAL_ROOT = REPO_ROOT / "research_journal"
SCHEMA_FILES = (
    "observation.schema.yaml",
    "knowledge.schema.yaml",
    "failure.schema.yaml",
)


@dataclass(frozen=True)
class JournalSchema:
    object_type: str
    directory: str
    required_fields: tuple[str, ...]


def _load_yaml_object(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"YAML object required: {path}")
    return payload


def load_schemas(journal_root: Path) -> list[JournalSchema]:
    schemas: list[JournalSchema] = []
    for filename in SCHEMA_FILES:
        payload = _load_yaml_object(journal_root / filename)
        required_fields = payload.get("required_fields")
        if not isinstance(required_fields, list) or not all(isinstance(item, str) for item in required_fields):
            raise ValueError(f"schema required_fields must be a string list: {filename}")
        schemas.append(
            JournalSchema(
                object_type=str(payload.get("object_type") or filename),
                directory=str(payload.get("directory") or ""),
                required_fields=tuple(required_fields),
            )
        )
    return schemas


def validate_journal(journal_root: Path = DEFAULT_JOURNAL_ROOT) -> list[str]:
    root = Path(journal_root)
    errors: list[str] = []
    seen_ids: dict[str, Path] = {}
    try:
        schemas = load_schemas(root)
    except (OSError, ValueError, yaml.YAMLError) as exc:
        return [str(exc)]

    for schema in schemas:
        directory = root / schema.directory
        if not directory.exists():
            errors.append(f"missing directory: {directory}")
            continue
        for path in sorted(directory.glob("*.yaml")):
            try:
                payload = _load_yaml_object(path)
            except (OSError, ValueError, yaml.YAMLError) as exc:
                errors.append(str(exc))
                continue

            missing_fields = [field for field in schema.required_fields if field not in payload]
            if missing_fields:
                errors.append(f"{path}: missing required fields: {', '.join(missing_fields)}")

            object_id = payload.get("id")
            if object_id is None:
                continue
            object_id_text = str(object_id)
            if object_id_text in seen_ids:
                errors.append(f"duplicate id: {object_id_text}: {seen_ids[object_id_text]} and {path}")
            else:
                seen_ids[object_id_text] = path

    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate the AEGIS research journal v0.")
    parser.add_argument("--journal-root", default=str(DEFAULT_JOURNAL_ROOT))
    args = parser.parse_args()

    errors = validate_journal(Path(args.journal_root))
    if errors:
        for error in errors:
            print(error)
        return 1
    print("AEGIS research journal v0 validation passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
