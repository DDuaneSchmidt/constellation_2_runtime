from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.common.paper_session_admission_constants_v1 import (
    PAPER_SESSION_CLOSURE_SCHEMA_RELPATH_V1,
    REPO_ROOT,
)


@dataclass(frozen=True, slots=True)
class PaperSessionClosureV1:
    schema_id: str
    schema_version: str
    closure_id: str
    session_id: str
    day_utc: str
    graph_ref: str
    blocker_ledger_ref: str
    producer_attempts_ref: str
    status: str
    node_results: tuple[dict[str, Any], ...]
    edge_results: tuple[dict[str, Any], ...]
    unresolved_blockers: tuple[str, ...]
    evaluated_at: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> "PaperSessionClosureV1":
        validate_against_repo_schema_v1(obj, REPO_ROOT, PAPER_SESSION_CLOSURE_SCHEMA_RELPATH_V1)
        return cls(
            schema_id=str(obj["schema_id"]),
            schema_version=str(obj["schema_version"]),
            closure_id=str(obj["closure_id"]),
            session_id=str(obj["session_id"]),
            day_utc=str(obj["day_utc"]),
            graph_ref=str(obj["graph_ref"]),
            blocker_ledger_ref=str(obj["blocker_ledger_ref"]),
            producer_attempts_ref=str(obj["producer_attempts_ref"]),
            status=str(obj["status"]),
            node_results=tuple(dict(item) for item in obj["node_results"]),
            edge_results=tuple(dict(item) for item in obj["edge_results"]),
            unresolved_blockers=tuple(str(item) for item in obj["unresolved_blockers"]),
            evaluated_at=str(obj["evaluated_at"]),
        )

    def to_dict(self) -> dict[str, Any]:
        obj = {
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "closure_id": self.closure_id,
            "session_id": self.session_id,
            "day_utc": self.day_utc,
            "graph_ref": self.graph_ref,
            "blocker_ledger_ref": self.blocker_ledger_ref,
            "producer_attempts_ref": self.producer_attempts_ref,
            "status": self.status,
            "node_results": [dict(item) for item in self.node_results],
            "edge_results": [dict(item) for item in self.edge_results],
            "unresolved_blockers": list(self.unresolved_blockers),
            "evaluated_at": self.evaluated_at,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, PAPER_SESSION_CLOSURE_SCHEMA_RELPATH_V1)
        return obj


def build_paper_session_closure_v1(
    *,
    closure_id: str,
    session_id: str,
    day_utc: str,
    graph_ref: str,
    blocker_ledger_ref: str,
    producer_attempts_ref: str,
    status: str,
    node_results: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    edge_results: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    unresolved_blockers: list[str] | tuple[str, ...],
    evaluated_at: str,
) -> PaperSessionClosureV1:
    return PaperSessionClosureV1.from_dict(
        {
            "schema_id": "paper_session_closure",
            "schema_version": "v1",
            "closure_id": str(closure_id),
            "session_id": str(session_id),
            "day_utc": str(day_utc),
            "graph_ref": str(graph_ref),
            "blocker_ledger_ref": str(blocker_ledger_ref),
            "producer_attempts_ref": str(producer_attempts_ref),
            "status": str(status),
            "node_results": [dict(item) for item in node_results],
            "edge_results": [dict(item) for item in edge_results],
            "unresolved_blockers": [str(item) for item in unresolved_blockers],
            "evaluated_at": str(evaluated_at),
        }
    )
