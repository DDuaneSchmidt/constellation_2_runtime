from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.common.paper_session_admission_constants_v1 import (
    PAPER_SESSION_DEFINITION_SCHEMA_RELPATH_V1,
    REPO_ROOT,
)


@dataclass(frozen=True, slots=True)
class PaperSessionDefinitionV1:
    schema_id: str
    schema_version: str
    session_id: str
    day_utc: str
    mode: str
    operator_entrypoint: str
    execution_entrypoint: str
    required_artifact_families: tuple[str, ...]
    required_dependency_keys: tuple[str, ...]
    dependency_source_refs: tuple[str, ...]
    declared_sleeve_ids: tuple[str, ...]
    recorded_at: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> "PaperSessionDefinitionV1":
        validate_against_repo_schema_v1(obj, REPO_ROOT, PAPER_SESSION_DEFINITION_SCHEMA_RELPATH_V1)
        return cls(
            schema_id=str(obj["schema_id"]),
            schema_version=str(obj["schema_version"]),
            session_id=str(obj["session_id"]),
            day_utc=str(obj["day_utc"]),
            mode=str(obj["mode"]),
            operator_entrypoint=str(obj["operator_entrypoint"]),
            execution_entrypoint=str(obj["execution_entrypoint"]),
            required_artifact_families=tuple(str(item) for item in obj["required_artifact_families"]),
            required_dependency_keys=tuple(str(item) for item in obj["required_dependency_keys"]),
            dependency_source_refs=tuple(str(item) for item in obj["dependency_source_refs"]),
            declared_sleeve_ids=tuple(str(item) for item in obj["declared_sleeve_ids"]),
            recorded_at=str(obj["recorded_at"]),
        )

    def to_dict(self) -> dict[str, Any]:
        obj = {
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "session_id": self.session_id,
            "day_utc": self.day_utc,
            "mode": self.mode,
            "operator_entrypoint": self.operator_entrypoint,
            "execution_entrypoint": self.execution_entrypoint,
            "required_artifact_families": list(self.required_artifact_families),
            "required_dependency_keys": list(self.required_dependency_keys),
            "dependency_source_refs": list(self.dependency_source_refs),
            "declared_sleeve_ids": list(self.declared_sleeve_ids),
            "recorded_at": self.recorded_at,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, PAPER_SESSION_DEFINITION_SCHEMA_RELPATH_V1)
        return obj


def build_paper_session_definition_v1(
    *,
    session_id: str,
    day_utc: str,
    mode: str,
    operator_entrypoint: str,
    execution_entrypoint: str,
    required_artifact_families: list[str] | tuple[str, ...],
    required_dependency_keys: list[str] | tuple[str, ...],
    dependency_source_refs: list[str] | tuple[str, ...],
    declared_sleeve_ids: list[str] | tuple[str, ...],
    recorded_at: str,
) -> PaperSessionDefinitionV1:
    return PaperSessionDefinitionV1.from_dict(
        {
            "schema_id": "paper_session_definition",
            "schema_version": "v1",
            "session_id": str(session_id),
            "day_utc": str(day_utc),
            "mode": str(mode),
            "operator_entrypoint": str(operator_entrypoint),
            "execution_entrypoint": str(execution_entrypoint),
            "required_artifact_families": [str(item) for item in required_artifact_families],
            "required_dependency_keys": [str(item) for item in required_dependency_keys],
            "dependency_source_refs": [str(item) for item in dependency_source_refs],
            "declared_sleeve_ids": [str(item) for item in declared_sleeve_ids],
            "recorded_at": str(recorded_at),
        }
    )
