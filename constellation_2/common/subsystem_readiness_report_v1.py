from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.common.testing_evidence_plane_constants_v1 import (
    READINESS_STATES_V1,
    REPO_ROOT,
    SUBSYSTEM_READINESS_REPORT_SCHEMA_RELPATH_V1,
    TESTING_EVIDENCE_MINIMUM_SUBSYSTEMS_V1,
)


def derive_subsystem_gate_approval_v1(
    *,
    status: str,
    blocking_issues: Iterable[str],
    semantic_gaps: Iterable[str],
) -> bool:
    return str(status) == "READY" and not tuple(blocking_issues) and not tuple(semantic_gaps)


def missing_required_subsystems_v1(subsystem_names: Iterable[str]) -> tuple[str, ...]:
    observed = {str(item).strip() for item in subsystem_names}
    return tuple(name for name in TESTING_EVIDENCE_MINIMUM_SUBSYSTEMS_V1 if name not in observed)


def _validate_row_semantics(row: dict[str, Any]) -> None:
    if row["status"] not in READINESS_STATES_V1 and row["status"] != "SEMANTIC_GAP":
        raise ValueError("UNSUPPORTED_READINESS_STATE")
    derived_gate = derive_subsystem_gate_approval_v1(
        status=str(row["status"]),
        blocking_issues=tuple(str(item) for item in row["blocking_issues"]),
        semantic_gaps=tuple(str(item) for item in row["semantic_gaps"]),
    )
    if bool(row["approved_for_next_gate"]) and not derived_gate:
        raise ValueError("APPROVED_FOR_NEXT_GATE_INCONSISTENT")


def _validate_report_semantics(obj: dict[str, Any]) -> None:
    if obj["status"] not in READINESS_STATES_V1 and obj["status"] != "SEMANTIC_GAP":
        raise ValueError("UNSUPPORTED_READINESS_STATE")
    for row in obj["subsystem_rows"]:
        _validate_row_semantics(row)
    derived_gate = derive_subsystem_gate_approval_v1(
        status=str(obj["status"]),
        blocking_issues=tuple(str(item) for item in obj["blocking_issues"]),
        semantic_gaps=tuple(str(item) for item in obj["semantic_gaps"]),
    )
    if bool(obj["approved_for_next_gate"]) and not derived_gate:
        raise ValueError("APPROVED_FOR_NEXT_GATE_INCONSISTENT")


@dataclass(frozen=True, slots=True)
class SubsystemReadinessReportV1:
    schema_id: str
    schema_version: str
    readiness_id: str
    subsystem_name: str
    status: str
    required_test_refs: tuple[str, ...]
    blocking_issues: tuple[str, ...]
    semantic_gaps: tuple[str, ...]
    approved_for_next_gate: bool
    recorded_at: str
    subsystem_rows: tuple[dict[str, Any], ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> "SubsystemReadinessReportV1":
        validate_against_repo_schema_v1(obj, REPO_ROOT, SUBSYSTEM_READINESS_REPORT_SCHEMA_RELPATH_V1)
        _validate_report_semantics(obj)
        return cls(
            schema_id=str(obj["schema_id"]),
            schema_version=str(obj["schema_version"]),
            readiness_id=str(obj["readiness_id"]),
            subsystem_name=str(obj["subsystem_name"]),
            status=str(obj["status"]),
            required_test_refs=tuple(str(item) for item in obj["required_test_refs"]),
            blocking_issues=tuple(str(item) for item in obj["blocking_issues"]),
            semantic_gaps=tuple(str(item) for item in obj["semantic_gaps"]),
            approved_for_next_gate=bool(obj["approved_for_next_gate"]),
            recorded_at=str(obj["recorded_at"]),
            subsystem_rows=tuple(dict(item) for item in obj["subsystem_rows"]),
        )

    def to_dict(self) -> dict[str, Any]:
        obj = {
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "readiness_id": self.readiness_id,
            "subsystem_name": self.subsystem_name,
            "status": self.status,
            "required_test_refs": list(self.required_test_refs),
            "blocking_issues": list(self.blocking_issues),
            "semantic_gaps": list(self.semantic_gaps),
            "approved_for_next_gate": self.approved_for_next_gate,
            "recorded_at": self.recorded_at,
            "subsystem_rows": [dict(item) for item in self.subsystem_rows],
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SUBSYSTEM_READINESS_REPORT_SCHEMA_RELPATH_V1)
        _validate_report_semantics(obj)
        return obj


def build_subsystem_readiness_report_v1(
    *,
    readiness_id: str,
    subsystem_name: str,
    status: str,
    required_test_refs: list[str] | tuple[str, ...],
    blocking_issues: list[str] | tuple[str, ...],
    semantic_gaps: list[str] | tuple[str, ...],
    recorded_at: str,
    approved_for_next_gate: bool | None = None,
) -> SubsystemReadinessReportV1:
    blocking_issue_list = [str(item) for item in blocking_issues]
    semantic_gap_list = [str(item) for item in semantic_gaps]
    required_ref_list = [str(item) for item in required_test_refs]
    if approved_for_next_gate is None:
        approved_for_next_gate = derive_subsystem_gate_approval_v1(
            status=str(status),
            blocking_issues=blocking_issue_list,
            semantic_gaps=semantic_gap_list,
        )
    row = {
        "subsystem_name": str(subsystem_name),
        "status": str(status),
        "required_test_refs": required_ref_list,
        "blocking_issues": blocking_issue_list,
        "semantic_gaps": semantic_gap_list,
        "approved_for_next_gate": bool(approved_for_next_gate),
    }
    return SubsystemReadinessReportV1.from_dict(
        {
            "schema_id": "subsystem_readiness_report",
            "schema_version": "v1",
            "readiness_id": str(readiness_id),
            "subsystem_name": str(subsystem_name),
            "status": str(status),
            "required_test_refs": required_ref_list,
            "blocking_issues": blocking_issue_list,
            "semantic_gaps": semantic_gap_list,
            "approved_for_next_gate": bool(approved_for_next_gate),
            "recorded_at": str(recorded_at),
            "subsystem_rows": [row],
        }
    )
