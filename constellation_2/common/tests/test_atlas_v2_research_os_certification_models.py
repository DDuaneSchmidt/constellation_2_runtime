from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.certification_models import CertificationReport, CertificationStatus, CertificationType, result


def test_certification_types_include_required_set() -> None:
    assert {item.value for item in CertificationType} == {
        "FOUNDATION_CERTIFICATION",
        "MEMORY_CERTIFICATION",
        "LINEAGE_CERTIFICATION",
        "GOVERNANCE_CERTIFICATION",
        "FORBIDDEN_ARTIFACT_CERTIFICATION",
        "AUTHORITY_BOUNDARY_CERTIFICATION",
        "BACKLOG_PRIORITY_CERTIFICATION",
        "LIFECYCLE_CERTIFICATION",
        "WORKER_CONNECTION_CERTIFICATION",
    }


def test_certification_statuses_include_required_set() -> None:
    assert {item.value for item in CertificationStatus} == {
        "PASS",
        "FAIL",
        "WARNING",
        "SKIPPED_DEPENDENCY_MISSING",
        "BLOCKED",
    }


def test_result_serializes_to_dict() -> None:
    row = result(CertificationType.GOVERNANCE_CERTIFICATION, "check", CertificationStatus.PASS, "ok", details=["detail"], metadata={"k": "v"})
    assert row.to_dict()["certification_type"] == "GOVERNANCE_CERTIFICATION"
    assert row.to_dict()["status"] == "PASS"


def test_report_serializes_nested_checks() -> None:
    check = result(CertificationType.FOUNDATION_CERTIFICATION, "foundation", CertificationStatus.PASS, "ok")
    report = CertificationReport(schema_id="schema", schema_version="v1", day="2026-06-04", root="root", status="PASS", checks=[check])
    payload = report.to_dict()
    assert payload["checks"][0]["check_id"] == "foundation"
