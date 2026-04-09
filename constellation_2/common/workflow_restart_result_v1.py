from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from constellation_2.common.testing_evidence_plane_constants_v1 import (
    validate_test_result_identity_v1,
    validate_test_result_entry_v1,
)


def _validate_semantics(obj: dict[str, Any]) -> None:
    validate_test_result_identity_v1(obj)
    validate_test_result_entry_v1(obj, result_type="workflow_restart_result")


@dataclass(frozen=True, slots=True)
class WorkflowRestartResultV1:
    schema_id: str
    schema_version: str
    result_id: str
    scenario_id: str | None
    source_test_id: str | None
    subsystem_name: str
    result_type: str
    status: str
    executed_at: str
    input_artifact_refs: tuple[str, ...]
    observed_outputs_summary: str
    expected_outputs_summary: str
    divergence_summary: str
    blocking: bool
    severity: str
    notes: tuple[str, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> "WorkflowRestartResultV1":
        _validate_semantics(obj)
        return cls(
            schema_id=str(obj["schema_id"]),
            schema_version=str(obj["schema_version"]),
            result_id=str(obj["result_id"]),
            scenario_id=None if "scenario_id" not in obj else str(obj["scenario_id"]),
            source_test_id=None if "source_test_id" not in obj else str(obj["source_test_id"]),
            subsystem_name=str(obj["subsystem_name"]),
            result_type=str(obj["result_type"]),
            status=str(obj["status"]),
            executed_at=str(obj["executed_at"]),
            input_artifact_refs=tuple(str(item) for item in obj["input_artifact_refs"]),
            observed_outputs_summary=str(obj["observed_outputs_summary"]),
            expected_outputs_summary=str(obj["expected_outputs_summary"]),
            divergence_summary=str(obj["divergence_summary"]),
            blocking=bool(obj["blocking"]),
            severity=str(obj["severity"]),
            notes=tuple(str(item) for item in obj["notes"]),
        )

    def to_dict(self) -> dict[str, Any]:
        obj = {
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "result_id": self.result_id,
            "subsystem_name": self.subsystem_name,
            "result_type": self.result_type,
            "status": self.status,
            "executed_at": self.executed_at,
            "input_artifact_refs": list(self.input_artifact_refs),
            "observed_outputs_summary": self.observed_outputs_summary,
            "expected_outputs_summary": self.expected_outputs_summary,
            "divergence_summary": self.divergence_summary,
            "blocking": self.blocking,
            "severity": self.severity,
            "notes": list(self.notes),
        }
        if self.scenario_id is not None:
            obj["scenario_id"] = self.scenario_id
        if self.source_test_id is not None:
            obj["source_test_id"] = self.source_test_id
        _validate_semantics(obj)
        return obj


def build_workflow_restart_result_v1(
    *,
    result_id: str,
    subsystem_name: str,
    status: str,
    executed_at: str,
    input_artifact_refs: list[str] | tuple[str, ...],
    observed_outputs_summary: str,
    expected_outputs_summary: str,
    divergence_summary: str,
    blocking: bool,
    severity: str,
    notes: list[str] | tuple[str, ...],
    scenario_id: str | None = None,
    source_test_id: str | None = None,
) -> WorkflowRestartResultV1:
    obj: dict[str, Any] = {
        "schema_id": "workflow_restart_result",
        "schema_version": "v1",
        "result_id": str(result_id),
        "subsystem_name": str(subsystem_name),
        "result_type": "workflow_restart_result",
        "status": str(status),
        "executed_at": str(executed_at),
        "input_artifact_refs": [str(item) for item in input_artifact_refs],
        "observed_outputs_summary": str(observed_outputs_summary),
        "expected_outputs_summary": str(expected_outputs_summary),
        "divergence_summary": str(divergence_summary),
        "blocking": bool(blocking),
        "severity": str(severity),
        "notes": [str(item) for item in notes],
    }
    if scenario_id is not None:
        obj["scenario_id"] = str(scenario_id)
    if source_test_id is not None:
        obj["source_test_id"] = str(source_test_id)
    return WorkflowRestartResultV1.from_dict(obj)
