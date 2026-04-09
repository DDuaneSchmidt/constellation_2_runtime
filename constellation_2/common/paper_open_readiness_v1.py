from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.common.testing_evidence_plane_constants_v1 import (
    PAPER_OPEN_READINESS_SCHEMA_RELPATH_V1,
    READINESS_STATES_V1,
    REPO_ROOT,
)


def derive_paper_open_readiness_state_v1(
    *,
    subsystem_statuses: Iterable[str],
    unresolved_blockers: Iterable[str],
    unresolved_semantic_gaps: Iterable[str],
) -> tuple[str, bool]:
    blocker_list = tuple(str(item) for item in unresolved_blockers)
    gap_list = tuple(str(item) for item in unresolved_semantic_gaps)
    if blocker_list:
        return ("BLOCKED", False)
    if gap_list:
        return ("NOT_READY", False)
    statuses = tuple(str(item) for item in subsystem_statuses)
    if any(status == "BLOCKED" for status in statuses):
        return ("BLOCKED", False)
    if any(status == "NOT_READY" for status in statuses):
        return ("NOT_READY", False)
    if any(status == "UNKNOWN" for status in statuses):
        return ("UNKNOWN", False)
    return ("READY", True)


def _validate_semantics(obj: dict[str, Any]) -> None:
    if obj["overall_status"] not in READINESS_STATES_V1:
        raise ValueError("UNSUPPORTED_READINESS_STATE")
    authority_scope = str(obj.get("authority_scope") or "").strip()
    ledger_authority_status = str(obj.get("ledger_authority_status") or "").strip()
    if authority_scope and authority_scope != "DERIVED_ONLY_VIEW":
        raise ValueError("UNSUPPORTED_PAPER_OPEN_AUTHORITY_SCOPE")
    if bool(obj["paper_open_allowed"]) and str(obj["overall_status"]) != "READY":
        raise ValueError("PAPER_OPEN_ALLOWED_REQUIRES_READY_STATUS")
    if bool(obj["paper_open_allowed"]) and obj["unresolved_blockers"]:
        raise ValueError("PAPER_OPEN_ALLOWED_REQUIRES_NO_UNRESOLVED_BLOCKERS")
    if bool(obj["paper_open_allowed"]) and obj["unresolved_semantic_gaps"]:
        raise ValueError("PAPER_OPEN_ALLOWED_REQUIRES_NO_UNRESOLVED_SEMANTIC_GAPS")
    if bool(obj["paper_open_allowed"]) and ledger_authority_status and ledger_authority_status != "GRANTED":
        raise ValueError("PAPER_OPEN_ALLOWED_REQUIRES_GRANTED_LEDGER")
    if str(obj["overall_status"]) == "READY" and obj["unresolved_blockers"]:
        raise ValueError("READY_STATUS_CONFLICTS_WITH_UNRESOLVED_BLOCKERS")
    if str(obj["overall_status"]) == "READY" and ledger_authority_status and ledger_authority_status != "GRANTED":
        raise ValueError("READY_STATUS_REQUIRES_GRANTED_LEDGER")


@dataclass(frozen=True, slots=True)
class PaperOpenReadinessV1:
    schema_id: str
    schema_version: str
    readiness_id: str
    environment_name: str
    subsystem_readiness_refs: tuple[str, ...]
    unresolved_blockers: tuple[str, ...]
    unresolved_semantic_gaps: tuple[str, ...]
    overall_status: str
    paper_open_allowed: bool
    authority_scope: str | None
    paper_session_ledger_ref: str | None
    ledger_authority_status: str | None
    rationale: str
    recorded_at: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> "PaperOpenReadinessV1":
        validate_against_repo_schema_v1(obj, REPO_ROOT, PAPER_OPEN_READINESS_SCHEMA_RELPATH_V1)
        _validate_semantics(obj)
        return cls(
            schema_id=str(obj["schema_id"]),
            schema_version=str(obj["schema_version"]),
            readiness_id=str(obj["readiness_id"]),
            environment_name=str(obj["environment_name"]),
            subsystem_readiness_refs=tuple(str(item) for item in obj["subsystem_readiness_refs"]),
            unresolved_blockers=tuple(str(item) for item in obj["unresolved_blockers"]),
            unresolved_semantic_gaps=tuple(str(item) for item in obj["unresolved_semantic_gaps"]),
            overall_status=str(obj["overall_status"]),
            paper_open_allowed=bool(obj["paper_open_allowed"]),
            authority_scope=str(obj["authority_scope"]) if obj.get("authority_scope") is not None else None,
            paper_session_ledger_ref=str(obj["paper_session_ledger_ref"]) if obj.get("paper_session_ledger_ref") is not None else None,
            ledger_authority_status=str(obj["ledger_authority_status"]) if obj.get("ledger_authority_status") is not None else None,
            rationale=str(obj["rationale"]),
            recorded_at=str(obj["recorded_at"]),
        )

    def to_dict(self) -> dict[str, Any]:
        obj = {
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "readiness_id": self.readiness_id,
            "environment_name": self.environment_name,
            "subsystem_readiness_refs": list(self.subsystem_readiness_refs),
            "unresolved_blockers": list(self.unresolved_blockers),
            "unresolved_semantic_gaps": list(self.unresolved_semantic_gaps),
            "overall_status": self.overall_status,
            "paper_open_allowed": self.paper_open_allowed,
            "rationale": self.rationale,
            "recorded_at": self.recorded_at,
        }
        if self.authority_scope is not None:
            obj["authority_scope"] = self.authority_scope
        if self.paper_session_ledger_ref is not None:
            obj["paper_session_ledger_ref"] = self.paper_session_ledger_ref
        if self.ledger_authority_status is not None:
            obj["ledger_authority_status"] = self.ledger_authority_status
        validate_against_repo_schema_v1(obj, REPO_ROOT, PAPER_OPEN_READINESS_SCHEMA_RELPATH_V1)
        _validate_semantics(obj)
        return obj


def build_paper_open_readiness_v1(
    *,
    readiness_id: str,
    environment_name: str,
    subsystem_readiness_refs: list[str] | tuple[str, ...],
    unresolved_blockers: list[str] | tuple[str, ...],
    unresolved_semantic_gaps: list[str] | tuple[str, ...],
    rationale: str,
    recorded_at: str,
    overall_status: str | None = None,
    paper_open_allowed: bool | None = None,
    subsystem_statuses: list[str] | tuple[str, ...] | None = None,
    authority_scope: str | None = None,
    paper_session_ledger_ref: str | None = None,
    ledger_authority_status: str | None = None,
) -> PaperOpenReadinessV1:
    blocker_list = [str(item) for item in unresolved_blockers]
    gap_list = [str(item) for item in unresolved_semantic_gaps]
    if overall_status is None or paper_open_allowed is None:
        if subsystem_statuses is None:
            raise ValueError("SUBSYSTEM_STATUSES_REQUIRED_FOR_DERIVATION")
        derived_status, derived_allowed = derive_paper_open_readiness_state_v1(
            subsystem_statuses=tuple(str(item) for item in subsystem_statuses),
            unresolved_blockers=blocker_list,
            unresolved_semantic_gaps=gap_list,
        )
        if overall_status is None:
            overall_status = derived_status
        if paper_open_allowed is None:
            paper_open_allowed = derived_allowed
    payload = {
        "schema_id": "paper_open_readiness",
        "schema_version": "v1",
        "readiness_id": str(readiness_id),
        "environment_name": str(environment_name),
        "subsystem_readiness_refs": [str(item) for item in subsystem_readiness_refs],
        "unresolved_blockers": blocker_list,
        "unresolved_semantic_gaps": gap_list,
        "overall_status": str(overall_status),
        "paper_open_allowed": bool(paper_open_allowed),
        "rationale": str(rationale),
        "recorded_at": str(recorded_at),
    }
    if authority_scope is not None:
        payload["authority_scope"] = authority_scope
    if paper_session_ledger_ref is not None:
        payload["paper_session_ledger_ref"] = paper_session_ledger_ref
    if ledger_authority_status is not None:
        payload["ledger_authority_status"] = ledger_authority_status
    return PaperOpenReadinessV1.from_dict(payload)
