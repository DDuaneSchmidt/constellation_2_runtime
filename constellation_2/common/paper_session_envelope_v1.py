from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.common.paper_session_admission_constants_v1 import (
    PAPER_SESSION_ENVELOPE_SCHEMA_RELPATH_V1,
    REPO_ROOT,
)


@dataclass(frozen=True, slots=True)
class PaperSessionEnvelopeV1:
    schema_id: str
    schema_version: str
    envelope_id: str
    session_id: str
    day_utc: str
    mode: str
    graph_fingerprint: str
    dependency_keys: tuple[str, ...]
    admitted_sleeve_ids: tuple[str, ...]
    allowed_truth_roots: tuple[str, ...]
    execution_entrypoint: str
    contract_fingerprints: tuple[dict[str, str], ...]
    config_fingerprint: str
    valid_from: str
    valid_until: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> "PaperSessionEnvelopeV1":
        validate_against_repo_schema_v1(obj, REPO_ROOT, PAPER_SESSION_ENVELOPE_SCHEMA_RELPATH_V1)
        return cls(
            schema_id=str(obj["schema_id"]),
            schema_version=str(obj["schema_version"]),
            envelope_id=str(obj["envelope_id"]),
            session_id=str(obj["session_id"]),
            day_utc=str(obj["day_utc"]),
            mode=str(obj["mode"]),
            graph_fingerprint=str(obj["graph_fingerprint"]),
            dependency_keys=tuple(str(item) for item in obj["dependency_keys"]),
            admitted_sleeve_ids=tuple(str(item) for item in obj["admitted_sleeve_ids"]),
            allowed_truth_roots=tuple(str(item) for item in obj["allowed_truth_roots"]),
            execution_entrypoint=str(obj["execution_entrypoint"]),
            contract_fingerprints=tuple(dict(item) for item in obj["contract_fingerprints"]),
            config_fingerprint=str(obj["config_fingerprint"]),
            valid_from=str(obj["valid_from"]),
            valid_until=str(obj["valid_until"]),
        )

    def to_dict(self) -> dict[str, Any]:
        obj = {
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "envelope_id": self.envelope_id,
            "session_id": self.session_id,
            "day_utc": self.day_utc,
            "mode": self.mode,
            "graph_fingerprint": self.graph_fingerprint,
            "dependency_keys": list(self.dependency_keys),
            "admitted_sleeve_ids": list(self.admitted_sleeve_ids),
            "allowed_truth_roots": list(self.allowed_truth_roots),
            "execution_entrypoint": self.execution_entrypoint,
            "contract_fingerprints": [dict(item) for item in self.contract_fingerprints],
            "config_fingerprint": self.config_fingerprint,
            "valid_from": self.valid_from,
            "valid_until": self.valid_until,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, PAPER_SESSION_ENVELOPE_SCHEMA_RELPATH_V1)
        return obj


def build_paper_session_envelope_v1(
    *,
    envelope_id: str,
    session_id: str,
    day_utc: str,
    mode: str,
    graph_fingerprint: str,
    dependency_keys: list[str] | tuple[str, ...],
    admitted_sleeve_ids: list[str] | tuple[str, ...],
    allowed_truth_roots: list[str] | tuple[str, ...],
    execution_entrypoint: str,
    contract_fingerprints: list[dict[str, str]] | tuple[dict[str, str], ...],
    config_fingerprint: str,
    valid_from: str,
    valid_until: str,
) -> PaperSessionEnvelopeV1:
    return PaperSessionEnvelopeV1.from_dict(
        {
            "schema_id": "paper_session_envelope",
            "schema_version": "v1",
            "envelope_id": str(envelope_id),
            "session_id": str(session_id),
            "day_utc": str(day_utc),
            "mode": str(mode),
            "graph_fingerprint": str(graph_fingerprint),
            "dependency_keys": [str(item) for item in dependency_keys],
            "admitted_sleeve_ids": [str(item) for item in admitted_sleeve_ids],
            "allowed_truth_roots": [str(item) for item in allowed_truth_roots],
            "execution_entrypoint": str(execution_entrypoint),
            "contract_fingerprints": [dict(item) for item in contract_fingerprints],
            "config_fingerprint": str(config_fingerprint),
            "valid_from": str(valid_from),
            "valid_until": str(valid_until),
        }
    )
