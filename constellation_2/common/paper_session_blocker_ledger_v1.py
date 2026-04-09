from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.common.paper_session_admission_constants_v1 import (
    PAPER_SESSION_BLOCKER_LEDGER_SCHEMA_RELPATH_V1,
    REPO_ROOT,
)


@dataclass(frozen=True, slots=True)
class PaperSessionBlockerLedgerV1:
    schema_id: str
    schema_version: str
    blocker_ledger_id: str
    session_id: str
    day_utc: str
    blocker_count: int
    blockers: tuple[dict[str, Any], ...]
    recorded_at: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> "PaperSessionBlockerLedgerV1":
        validate_against_repo_schema_v1(obj, REPO_ROOT, PAPER_SESSION_BLOCKER_LEDGER_SCHEMA_RELPATH_V1)
        return cls(
            schema_id=str(obj["schema_id"]),
            schema_version=str(obj["schema_version"]),
            blocker_ledger_id=str(obj["blocker_ledger_id"]),
            session_id=str(obj["session_id"]),
            day_utc=str(obj["day_utc"]),
            blocker_count=int(obj["blocker_count"]),
            blockers=tuple(dict(item) for item in obj["blockers"]),
            recorded_at=str(obj["recorded_at"]),
        )

    def to_dict(self) -> dict[str, Any]:
        obj = {
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "blocker_ledger_id": self.blocker_ledger_id,
            "session_id": self.session_id,
            "day_utc": self.day_utc,
            "blocker_count": self.blocker_count,
            "blockers": [dict(item) for item in self.blockers],
            "recorded_at": self.recorded_at,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, PAPER_SESSION_BLOCKER_LEDGER_SCHEMA_RELPATH_V1)
        return obj


def build_paper_session_blocker_ledger_v1(
    *,
    blocker_ledger_id: str,
    session_id: str,
    day_utc: str,
    blockers: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    recorded_at: str,
) -> PaperSessionBlockerLedgerV1:
    return PaperSessionBlockerLedgerV1.from_dict(
        {
            "schema_id": "paper_session_blocker_ledger",
            "schema_version": "v1",
            "blocker_ledger_id": str(blocker_ledger_id),
            "session_id": str(session_id),
            "day_utc": str(day_utc),
            "blocker_count": len(blockers),
            "blockers": [dict(item) for item in blockers],
            "recorded_at": str(recorded_at),
        }
    )
