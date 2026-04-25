from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.day_authority_decision_v1 import (
    read_day_authority_decision_v1,
    write_day_authority_decision_from_authority_result_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_session_readiness_refresh_path,
)
from ops.tools import run_day_authority_decision_v1 as tool_module


DAY = "2026-04-14"


def _authority_result(*, validation_state: str, blocking_class: str, reason_code: str | None = None) -> dict:
    first_failure = None
    blocking_evidence: list[str] = []
    missing_refs: list[str] = []
    if reason_code:
        first_failure = {
            "prerequisite_class": blocking_class,
            "reason_code": reason_code,
            "evidence_ref": "results.startup_materialization",
        }
        blocking_evidence = [reason_code]
        missing_refs = ["results.startup_materialization"]
    return {
        "validation_summary": {
            "validation_state": validation_state,
            "blocking_class": blocking_class,
            "blocking_evidence": blocking_evidence,
            "first_failure": first_failure,
            "missing_or_invalid_prerequisite_refs": missing_refs,
            "validation_refs_used": ["results.ib_api_handshake", "results.scope_summary"],
            "compatibility_status": {
                "mapping_status": "OK",
                "schema_status": "OK",
                "reader_compatibility_status": "OK",
                "details": [],
            },
            "diagnostic_warnings": [],
        }
    }


def _write_refresh_report(truth_root: Path, *, authority_result: dict) -> Path:
    report_path = resolve_session_readiness_refresh_path(truth_root=truth_root, day_utc=DAY)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(
            {
                "schema_id": "C2_SESSION_READINESS_REFRESH_V1",
                "schema_version": 1,
                "day_utc": DAY,
                "status": "FAIL",
                "producer": {
                    "git_sha": "a" * 40,
                    "module": "ops/tools/run_session_readiness_refresh_v1.py",
                    "repo": "constellation",
                },
                "results": {
                    "authority_kernel_validation": authority_result,
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return report_path


def test_write_day_authority_from_authority_result_open(tmp_path: Path) -> None:
    upstream_refs = [
        {
            "artifact_id": "session_readiness_refresh_v1",
            "path": str(resolve_session_readiness_refresh_path(truth_root=tmp_path, day_utc=DAY)),
            "sha256": "a" * 64,
            "artifact_class": "admission_result",
            "finality_state": "provisional",
        }
    ]
    result = write_day_authority_decision_from_authority_result_v1(
        day_utc=DAY,
        authority_result=_authority_result(validation_state="PASS", blocking_class="NONE"),
        truth_root=tmp_path,
        producer_module="ops/tools/run_day_authority_decision_v1.py",
        producer_git_sha="b" * 40,
        upstream_artifact_refs=upstream_refs,
    )
    assert result["status"] == "OK"
    ref = read_day_authority_decision_v1(truth_root=tmp_path, trading_day=DAY)
    assert ref.payload["decision_state"] == "OPEN"
    assert ref.payload["blocking_class"] == "NONE"
    assert ref.payload["constitutional_dependency_declaration"]["declared_dependency_artifacts"] == [
        "session_readiness_refresh_v1"
    ]


def test_write_day_authority_from_authority_result_blocked(tmp_path: Path) -> None:
    result = write_day_authority_decision_from_authority_result_v1(
        day_utc=DAY,
        authority_result=_authority_result(
            validation_state="FAIL",
            blocking_class="GOVERNANCE_CONFIG_MISSING",
            reason_code="STARTUP_MATERIALIZATION_FAILED",
        ),
        truth_root=tmp_path,
        producer_module="ops/tools/run_day_authority_decision_v1.py",
        producer_git_sha="c" * 40,
        upstream_artifact_refs=[
            {
                "artifact_id": "session_readiness_refresh_v1",
                "path": str(resolve_session_readiness_refresh_path(truth_root=tmp_path, day_utc=DAY)),
                "sha256": "b" * 64,
                "artifact_class": "admission_result",
                "finality_state": "provisional",
            }
        ],
    )
    assert result["status"] == "OK"
    ref = read_day_authority_decision_v1(truth_root=tmp_path, trading_day=DAY)
    assert ref.payload["decision_state"] == "BLOCKED"
    assert ref.payload["blocking_evidence"] == ["STARTUP_MATERIALIZATION_FAILED"]


def test_run_day_authority_tool_materializes_from_refresh_report(tmp_path: Path) -> None:
    _write_refresh_report(
        tmp_path,
        authority_result=_authority_result(validation_state="PASS", blocking_class="NONE"),
    )
    rc = tool_module.main(["--day_utc", DAY, "--truth_root", str(tmp_path)])
    assert rc == 0
    ref = read_day_authority_decision_v1(truth_root=tmp_path, trading_day=DAY)
    assert ref.payload["decision_state"] == "OPEN"
    assert ref.payload["run_metadata"]["producer_module"] == "ops/tools/run_day_authority_decision_v1.py"
    assert ref.payload["constitutional_lineage"]["artifact_type"] == "day_authority_decision_v1"


def test_canonical_helper_materializes_from_refresh_report_with_canonical_writer_identity(tmp_path: Path) -> None:
    report_path = _write_refresh_report(
        tmp_path,
        authority_result=_authority_result(validation_state="PASS", blocking_class="NONE"),
    )
    result = tool_module.write_day_authority_decision_from_refresh_report_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        report_path=report_path,
        producer_git_sha="d" * 40,
    )
    assert result["status"] == "OK"
    ref = read_day_authority_decision_v1(truth_root=tmp_path, trading_day=DAY)
    assert ref.payload["run_metadata"]["producer_module"] == "ops/tools/run_day_authority_decision_v1.py"
    assert ref.payload["constitutional_dependency_declaration"]["dependency_refs"][0]["artifact_id"] == "session_readiness_refresh_v1"


def test_run_day_authority_tool_requires_refresh_report(tmp_path: Path) -> None:
    try:
        tool_module.main(["--day_utc", DAY, "--truth_root", str(tmp_path)])
    except SystemExit as exc:
        assert str(exc) == (
            f"FAIL: session_readiness_refresh_missing "
            f"path={resolve_session_readiness_refresh_path(truth_root=tmp_path, day_utc=DAY)}"
        )
    else:
        raise AssertionError("expected SystemExit")
