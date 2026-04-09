from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.common.testing_evidence_plane_constants_v1 import (
    REPO_ROOT,
    SCENARIO_CATALOG_SCHEMA_RELPATH_V1,
)


@dataclass(frozen=True, slots=True)
class ScenarioCatalogV1:
    schema_id: str
    schema_version: str
    catalog_id: str
    scenario_id: str
    domain_scope: str
    scenario_type: str
    purpose: str
    input_artifact_refs: tuple[str, ...]
    policy_refs: tuple[str, ...]
    expected_outputs: tuple[str, ...]
    expected_action_classes: tuple[str, ...]
    expected_precedence_notes: tuple[str, ...]
    created_at: str
    status: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> "ScenarioCatalogV1":
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCENARIO_CATALOG_SCHEMA_RELPATH_V1)
        return cls(
            schema_id=str(obj["schema_id"]),
            schema_version=str(obj["schema_version"]),
            catalog_id=str(obj["catalog_id"]),
            scenario_id=str(obj["scenario_id"]),
            domain_scope=str(obj["domain_scope"]),
            scenario_type=str(obj["scenario_type"]),
            purpose=str(obj["purpose"]),
            input_artifact_refs=tuple(str(item) for item in obj["input_artifact_refs"]),
            policy_refs=tuple(str(item) for item in obj["policy_refs"]),
            expected_outputs=tuple(str(item) for item in obj["expected_outputs"]),
            expected_action_classes=tuple(str(item) for item in obj["expected_action_classes"]),
            expected_precedence_notes=tuple(str(item) for item in obj["expected_precedence_notes"]),
            created_at=str(obj["created_at"]),
            status=str(obj["status"]),
        )

    def to_dict(self) -> dict[str, Any]:
        obj = {
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "catalog_id": self.catalog_id,
            "scenario_id": self.scenario_id,
            "domain_scope": self.domain_scope,
            "scenario_type": self.scenario_type,
            "purpose": self.purpose,
            "input_artifact_refs": list(self.input_artifact_refs),
            "policy_refs": list(self.policy_refs),
            "expected_outputs": list(self.expected_outputs),
            "expected_action_classes": list(self.expected_action_classes),
            "expected_precedence_notes": list(self.expected_precedence_notes),
            "created_at": self.created_at,
            "status": self.status,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCENARIO_CATALOG_SCHEMA_RELPATH_V1)
        return obj


def build_scenario_catalog_v1(
    *,
    catalog_id: str,
    scenario_id: str,
    domain_scope: str,
    scenario_type: str,
    purpose: str,
    input_artifact_refs: list[str] | tuple[str, ...],
    policy_refs: list[str] | tuple[str, ...],
    expected_outputs: list[str] | tuple[str, ...],
    expected_action_classes: list[str] | tuple[str, ...],
    expected_precedence_notes: list[str] | tuple[str, ...],
    created_at: str,
    status: str,
) -> ScenarioCatalogV1:
    return ScenarioCatalogV1.from_dict(
        {
            "schema_id": "scenario_catalog",
            "schema_version": "v1",
            "catalog_id": str(catalog_id),
            "scenario_id": str(scenario_id),
            "domain_scope": str(domain_scope),
            "scenario_type": str(scenario_type),
            "purpose": str(purpose),
            "input_artifact_refs": [str(item) for item in input_artifact_refs],
            "policy_refs": [str(item) for item in policy_refs],
            "expected_outputs": [str(item) for item in expected_outputs],
            "expected_action_classes": [str(item) for item in expected_action_classes],
            "expected_precedence_notes": [str(item) for item in expected_precedence_notes],
            "created_at": str(created_at),
            "status": str(status),
        }
    )
