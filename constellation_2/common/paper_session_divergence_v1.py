from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.common.paper_session_admission_constants_v1 import (
    PAPER_SESSION_DIVERGENCE_SCHEMA_RELPATH_V1,
    REPO_ROOT,
)


@dataclass(frozen=True, slots=True)
class PaperSessionDivergenceV1:
    schema_id: str
    schema_version: str
    divergence_id: str
    session_id: str
    day_utc: str
    certificate_ref: str
    envelope_ref: str
    dependency_key: str
    accessed_path: str
    reason_code: str
    status: str
    detected_at: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> "PaperSessionDivergenceV1":
        validate_against_repo_schema_v1(obj, REPO_ROOT, PAPER_SESSION_DIVERGENCE_SCHEMA_RELPATH_V1)
        return cls(
            schema_id=str(obj["schema_id"]),
            schema_version=str(obj["schema_version"]),
            divergence_id=str(obj["divergence_id"]),
            session_id=str(obj["session_id"]),
            day_utc=str(obj["day_utc"]),
            certificate_ref=str(obj["certificate_ref"]),
            envelope_ref=str(obj["envelope_ref"]),
            dependency_key=str(obj["dependency_key"]),
            accessed_path=str(obj["accessed_path"]),
            reason_code=str(obj["reason_code"]),
            status=str(obj["status"]),
            detected_at=str(obj["detected_at"]),
        )

    def to_dict(self) -> dict[str, Any]:
        obj = {
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "divergence_id": self.divergence_id,
            "session_id": self.session_id,
            "day_utc": self.day_utc,
            "certificate_ref": self.certificate_ref,
            "envelope_ref": self.envelope_ref,
            "dependency_key": self.dependency_key,
            "accessed_path": self.accessed_path,
            "reason_code": self.reason_code,
            "status": self.status,
            "detected_at": self.detected_at,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, PAPER_SESSION_DIVERGENCE_SCHEMA_RELPATH_V1)
        return obj


def build_paper_session_divergence_v1(
    *,
    divergence_id: str,
    session_id: str,
    day_utc: str,
    certificate_ref: str,
    envelope_ref: str,
    dependency_key: str,
    accessed_path: str,
    reason_code: str,
    detected_at: str,
) -> PaperSessionDivergenceV1:
    return PaperSessionDivergenceV1.from_dict(
        {
            "schema_id": "paper_session_divergence",
            "schema_version": "v1",
            "divergence_id": str(divergence_id),
            "session_id": str(session_id),
            "day_utc": str(day_utc),
            "certificate_ref": str(certificate_ref),
            "envelope_ref": str(envelope_ref),
            "dependency_key": str(dependency_key),
            "accessed_path": str(accessed_path),
            "reason_code": str(reason_code),
            "status": "DIVERGED",
            "detected_at": str(detected_at),
        }
    )


def assert_admitted_dependency_v1(
    *,
    truth_root: Path,
    day_utc: str,
    envelope_path: Path | None = None,
    certificate_path: Path | None = None,
    admission_certificate_path: Path | None = None,
    dependency_key: str,
    accessed_path: str | None = None,
    session_id: str | None = None,
    now_utc: str | None = None,
):
    """Compatibility export for deployment/runtime guard probes.

    The canonical implementation lives in paper_session_admission_v1, but
    exposing the guard here keeps the divergence API aligned with the probe
    contract used in deployment validation.
    """

    from constellation_2.common.paper_session_admission_v1 import (
        assert_admitted_dependency_v1 as _assert_admitted_dependency_v1,
    )

    resolved_certificate_path = certificate_path or admission_certificate_path
    if resolved_certificate_path is None:
        raise TypeError("assert_admitted_dependency_v1() missing certificate path")
    resolved_envelope_path = envelope_path
    if resolved_envelope_path is None:
        resolved_envelope_path = (
            truth_root.resolve()
            / "reports"
            / "paper_session_envelope_v1"
            / str(day_utc)
            / "paper_session_envelope.v1.json"
        )
    resolved_accessed_path = str(accessed_path or resolved_certificate_path)

    return _assert_admitted_dependency_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        envelope_path=resolved_envelope_path,
        certificate_path=resolved_certificate_path,
        dependency_key=dependency_key,
        accessed_path=resolved_accessed_path,
        now_utc=now_utc,
    )
