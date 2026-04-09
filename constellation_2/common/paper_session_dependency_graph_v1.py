from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.common.paper_session_admission_constants_v1 import (
    PAPER_SESSION_DEPENDENCY_GRAPH_SCHEMA_RELPATH_V1,
    REPO_ROOT,
)


@dataclass(frozen=True, slots=True)
class PaperSessionDependencyGraphV1:
    schema_id: str
    schema_version: str
    graph_id: str
    session_id: str
    day_utc: str
    mode: str
    graph_fingerprint: str
    dependency_keys: tuple[str, ...]
    admitted_sleeve_ids: tuple[str, ...]
    nodes: tuple[dict[str, Any], ...]
    edges: tuple[dict[str, Any], ...]
    recorded_at: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> "PaperSessionDependencyGraphV1":
        validate_against_repo_schema_v1(obj, REPO_ROOT, PAPER_SESSION_DEPENDENCY_GRAPH_SCHEMA_RELPATH_V1)
        return cls(
            schema_id=str(obj["schema_id"]),
            schema_version=str(obj["schema_version"]),
            graph_id=str(obj["graph_id"]),
            session_id=str(obj["session_id"]),
            day_utc=str(obj["day_utc"]),
            mode=str(obj["mode"]),
            graph_fingerprint=str(obj["graph_fingerprint"]),
            dependency_keys=tuple(str(item) for item in obj["dependency_keys"]),
            admitted_sleeve_ids=tuple(str(item) for item in obj["admitted_sleeve_ids"]),
            nodes=tuple(dict(item) for item in obj["nodes"]),
            edges=tuple(dict(item) for item in obj["edges"]),
            recorded_at=str(obj["recorded_at"]),
        )

    def to_dict(self) -> dict[str, Any]:
        obj = {
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "graph_id": self.graph_id,
            "session_id": self.session_id,
            "day_utc": self.day_utc,
            "mode": self.mode,
            "graph_fingerprint": self.graph_fingerprint,
            "dependency_keys": list(self.dependency_keys),
            "admitted_sleeve_ids": list(self.admitted_sleeve_ids),
            "nodes": [dict(item) for item in self.nodes],
            "edges": [dict(item) for item in self.edges],
            "recorded_at": self.recorded_at,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, PAPER_SESSION_DEPENDENCY_GRAPH_SCHEMA_RELPATH_V1)
        return obj


def build_paper_session_dependency_graph_v1(
    *,
    graph_id: str,
    session_id: str,
    day_utc: str,
    mode: str,
    graph_fingerprint: str,
    dependency_keys: list[str] | tuple[str, ...],
    admitted_sleeve_ids: list[str] | tuple[str, ...],
    nodes: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    edges: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    recorded_at: str,
) -> PaperSessionDependencyGraphV1:
    return PaperSessionDependencyGraphV1.from_dict(
        {
            "schema_id": "paper_session_dependency_graph",
            "schema_version": "v1",
            "graph_id": str(graph_id),
            "session_id": str(session_id),
            "day_utc": str(day_utc),
            "mode": str(mode),
            "graph_fingerprint": str(graph_fingerprint),
            "dependency_keys": [str(item) for item in dependency_keys],
            "admitted_sleeve_ids": [str(item) for item in admitted_sleeve_ids],
            "nodes": [dict(item) for item in nodes],
            "edges": [dict(item) for item in edges],
            "recorded_at": str(recorded_at),
        }
    )
