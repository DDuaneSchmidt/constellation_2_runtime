from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class CertificationType(str, Enum):
    FOUNDATION_CERTIFICATION = "FOUNDATION_CERTIFICATION"
    MEMORY_CERTIFICATION = "MEMORY_CERTIFICATION"
    LINEAGE_CERTIFICATION = "LINEAGE_CERTIFICATION"
    GOVERNANCE_CERTIFICATION = "GOVERNANCE_CERTIFICATION"
    FORBIDDEN_ARTIFACT_CERTIFICATION = "FORBIDDEN_ARTIFACT_CERTIFICATION"
    AUTHORITY_BOUNDARY_CERTIFICATION = "AUTHORITY_BOUNDARY_CERTIFICATION"
    BACKLOG_PRIORITY_CERTIFICATION = "BACKLOG_PRIORITY_CERTIFICATION"
    LIFECYCLE_CERTIFICATION = "LIFECYCLE_CERTIFICATION"
    WORKER_CONNECTION_CERTIFICATION = "WORKER_CONNECTION_CERTIFICATION"


class CertificationStatus(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    WARNING = "WARNING"
    SKIPPED_DEPENDENCY_MISSING = "SKIPPED_DEPENDENCY_MISSING"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class CertificationCheckResult:
    certification_type: str
    check_id: str
    status: str
    summary: str
    details: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CertificationReport:
    schema_id: str
    schema_version: str
    day: str
    root: str
    status: str
    checks: list[CertificationCheckResult]
    blockers: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    optional_capabilities: dict[str, Any] = field(default_factory=dict)
    authority_boundary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["checks"] = [check.to_dict() for check in self.checks]
        return payload


def result(
    certification_type: CertificationType,
    check_id: str,
    status: CertificationStatus,
    summary: str,
    *,
    details: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> CertificationCheckResult:
    return CertificationCheckResult(
        certification_type=certification_type.value,
        check_id=check_id,
        status=status.value,
        summary=summary,
        details=list(details or []),
        metadata=dict(metadata or {}),
    )
