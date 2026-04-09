from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.common.paper_session_admission_constants_v1 import (
    PAPER_SESSION_ADMISSION_CERTIFICATE_SCHEMA_RELPATH_V1,
    REPO_ROOT,
)


@dataclass(frozen=True, slots=True)
class PaperSessionAdmissionCertificateV1:
    schema_id: str
    schema_version: str
    certificate_id: str
    session_id: str
    day_utc: str
    status: str
    paper_ready: bool
    graph_fingerprint: str
    envelope_ref: str
    closure_ref: str
    blocker_ledger_ref: str
    issued_at: str
    valid_until: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> "PaperSessionAdmissionCertificateV1":
        validate_against_repo_schema_v1(obj, REPO_ROOT, PAPER_SESSION_ADMISSION_CERTIFICATE_SCHEMA_RELPATH_V1)
        return cls(
            schema_id=str(obj["schema_id"]),
            schema_version=str(obj["schema_version"]),
            certificate_id=str(obj["certificate_id"]),
            session_id=str(obj["session_id"]),
            day_utc=str(obj["day_utc"]),
            status=str(obj["status"]),
            paper_ready=bool(obj["paper_ready"]),
            graph_fingerprint=str(obj["graph_fingerprint"]),
            envelope_ref=str(obj["envelope_ref"]),
            closure_ref=str(obj["closure_ref"]),
            blocker_ledger_ref=str(obj["blocker_ledger_ref"]),
            issued_at=str(obj["issued_at"]),
            valid_until=str(obj["valid_until"]),
        )

    def to_dict(self) -> dict[str, Any]:
        obj = {
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "certificate_id": self.certificate_id,
            "session_id": self.session_id,
            "day_utc": self.day_utc,
            "status": self.status,
            "paper_ready": self.paper_ready,
            "graph_fingerprint": self.graph_fingerprint,
            "envelope_ref": self.envelope_ref,
            "closure_ref": self.closure_ref,
            "blocker_ledger_ref": self.blocker_ledger_ref,
            "issued_at": self.issued_at,
            "valid_until": self.valid_until,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, PAPER_SESSION_ADMISSION_CERTIFICATE_SCHEMA_RELPATH_V1)
        return obj


def build_paper_session_admission_certificate_v1(
    *,
    certificate_id: str,
    session_id: str,
    day_utc: str,
    graph_fingerprint: str,
    envelope_ref: str,
    closure_ref: str,
    blocker_ledger_ref: str,
    issued_at: str,
    valid_until: str,
) -> PaperSessionAdmissionCertificateV1:
    return PaperSessionAdmissionCertificateV1.from_dict(
        {
            "schema_id": "paper_session_admission_certificate",
            "schema_version": "v1",
            "certificate_id": str(certificate_id),
            "session_id": str(session_id),
            "day_utc": str(day_utc),
            "status": "ADMITTED",
            "paper_ready": True,
            "graph_fingerprint": str(graph_fingerprint),
            "envelope_ref": str(envelope_ref),
            "closure_ref": str(closure_ref),
            "blocker_ledger_ref": str(blocker_ledger_ref),
            "issued_at": str(issued_at),
            "valid_until": str(valid_until),
        }
    )
