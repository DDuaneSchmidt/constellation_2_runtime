from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.common.paper_session_admission_constants_v1 import (
    PAPER_SESSION_PRODUCER_ATTEMPTS_SCHEMA_RELPATH_V1,
    REPO_ROOT,
)


@dataclass(frozen=True, slots=True)
class PaperSessionProducerAttemptsV1:
    schema_id: str
    schema_version: str
    attempt_set_id: str
    session_id: str
    day_utc: str
    status: str
    attempted_producer_ids: tuple[str, ...]
    producer_attempts: tuple[dict[str, Any], ...]
    recorded_at: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> "PaperSessionProducerAttemptsV1":
        validate_against_repo_schema_v1(obj, REPO_ROOT, PAPER_SESSION_PRODUCER_ATTEMPTS_SCHEMA_RELPATH_V1)
        return cls(
            schema_id=str(obj["schema_id"]),
            schema_version=str(obj["schema_version"]),
            attempt_set_id=str(obj["attempt_set_id"]),
            session_id=str(obj["session_id"]),
            day_utc=str(obj["day_utc"]),
            status=str(obj["status"]),
            attempted_producer_ids=tuple(str(item) for item in obj["attempted_producer_ids"]),
            producer_attempts=tuple(dict(item) for item in obj["producer_attempts"]),
            recorded_at=str(obj["recorded_at"]),
        )

    def to_dict(self) -> dict[str, Any]:
        obj = {
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "attempt_set_id": self.attempt_set_id,
            "session_id": self.session_id,
            "day_utc": self.day_utc,
            "status": self.status,
            "attempted_producer_ids": list(self.attempted_producer_ids),
            "producer_attempts": [dict(item) for item in self.producer_attempts],
            "recorded_at": self.recorded_at,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, PAPER_SESSION_PRODUCER_ATTEMPTS_SCHEMA_RELPATH_V1)
        return obj


def build_paper_session_producer_attempts_v1(
    *,
    attempt_set_id: str,
    session_id: str,
    day_utc: str,
    status: str,
    attempted_producer_ids: list[str] | tuple[str, ...],
    producer_attempts: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    recorded_at: str,
) -> PaperSessionProducerAttemptsV1:
    return PaperSessionProducerAttemptsV1.from_dict(
        {
            "schema_id": "paper_session_producer_attempts",
            "schema_version": "v1",
            "attempt_set_id": str(attempt_set_id),
            "session_id": str(session_id),
            "day_utc": str(day_utc),
            "status": str(status),
            "attempted_producer_ids": [str(item) for item in attempted_producer_ids],
            "producer_attempts": [dict(item) for item in producer_attempts],
            "recorded_at": str(recorded_at),
        }
    )
