from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.pre_open_materializer_v1 import (
    derive_pre_open_bundle_payload_v1,
    read_pre_open_bundle_ref_v1,
    resolve_pre_open_bundle_path_v1,
    write_pre_open_bundle_v1,
)


DAY = "2026-04-16"


def _row(
    tmp_path: Path,
    artifact_id: str,
    *,
    result_status: str = "PASS",
    blocking_reason_code: str = "",
    role_class: str = "REQUIRED_BINDING_INPUT",
) -> dict:
    return {
        "artifact_id": artifact_id,
        "artifact_name": artifact_id,
        "required": True,
        "role_class": role_class,
        "classification": "PRE_OPEN_PREREQUISITE",
        "canonical_path": str((tmp_path / f"{artifact_id}.json").resolve()),
        "authority_path": str((tmp_path / f"{artifact_id}.json").resolve()),
        "path_family": "CANONICAL_RUNTIME_TRUTH_SUBPATH",
        "observed_status": "OK" if result_status == "PASS" else "MISSING",
        "result_status": result_status,
        "blocker_codes": [blocking_reason_code] if blocking_reason_code else [],
        "blocking_reason_code": blocking_reason_code,
        "schema_status": "VALID" if result_status == "PASS" else "MISSING",
        "schema_ref": "governance/test.schema.json",
        "freshness_rule": "TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        "freshness_status": "CURRENT" if result_status == "PASS" else "STALE",
        "target_day_expected": DAY,
        "target_day_observed": DAY if result_status == "PASS" else "",
        "date_binding_status": "MATCH" if result_status == "PASS" else "MISSING",
        "date_binding_value": DAY if result_status == "PASS" else "",
        "provenance_required": True,
        "provenance_summary": {
            "required": True,
            "present": result_status == "PASS",
            "fields_present": ["producer.module"] if result_status == "PASS" else [],
            "source": "producer" if result_status == "PASS" else "",
        },
        "closure_status": "CLOSED" if result_status == "PASS" else "OPEN",
        "producer": {"module": "test", "git_sha": "abc123"},
        "source_refs": [],
        "observed_dependency_artifacts": [],
    }


def _checks(tmp_path: Path) -> list[dict]:
    return [
        _row(tmp_path, "ib_api_handshake_latest_pointer_v1"),
        _row(tmp_path, "ib_api_handshake_v1"),
        _row(tmp_path, "global_kill_switch_state_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        _row(tmp_path, "primary_scoped_canonical_authority_head_v1"),
    ]


def _producer(script: str, *, return_code: int = 0, stdout: str = "", stderr: str = "") -> dict:
    return {
        "script": script,
        "command": ["python3", script],
        "return_code": return_code,
        "stdout": stdout,
        "stderr": stderr,
        "required_for_completion": True,
    }


def test_pre_open_bundle_round_trip_is_deterministic(tmp_path: Path) -> None:
    with (
        patch("constellation_2.common.pre_open_materializer_v1.collect_pre_open_prerequisite_checks_v1", return_value=_checks(tmp_path)),
        patch("constellation_2.common.pre_open_materializer_v1._current_active_day_observed", return_value="2026-04-14"),
    ):
        payload = derive_pre_open_bundle_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            environment="PAPER",
            ib_account="DUO847203",
            producer_results=[],
            owner_tool="ops/tools/run_pre_open_materializer_v1.py",
        )
    ref = write_pre_open_bundle_v1(truth_root=tmp_path, payload=payload)
    read_ref = read_pre_open_bundle_ref_v1(truth_root=tmp_path, target_day=DAY)
    assert ref.path == resolve_pre_open_bundle_path_v1(truth_root=tmp_path, day_utc=DAY)
    assert read_ref.payload["materialization_state"] == "COMPLETE"
    assert read_ref.payload["active_day_alignment_status"] == "MISMATCH"
    assert read_ref.payload["prerequisite_checks"][0]["artifact_id"] == "ib_api_handshake_latest_pointer_v1"


def test_missing_kill_switch_blocks_pre_open_bundle(tmp_path: Path) -> None:
    checks = _checks(tmp_path)
    checks[2] = _row(tmp_path, "global_kill_switch_state_v1", result_status="FAIL", blocking_reason_code="TARGET_DAY_ARTIFACT_MISSING")
    with patch("constellation_2.common.pre_open_materializer_v1.collect_pre_open_prerequisite_checks_v1", return_value=checks):
        payload = derive_pre_open_bundle_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            environment="PAPER",
            ib_account="DUO847203",
            producer_results=[],
            owner_tool="ops/tools/run_pre_open_materializer_v1.py",
        )
    assert payload["completion_state"] == "INCOMPLETE"
    assert payload["materialization_state"] == "INCOMPLETE"
    assert "TARGET_DAY_ARTIFACT_MISSING" in payload["blocking_reason_codes"]


def test_missing_handshake_blocks_pre_open_bundle(tmp_path: Path) -> None:
    checks = _checks(tmp_path)
    checks[1] = _row(tmp_path, "ib_api_handshake_v1", result_status="FAIL", blocking_reason_code="TARGET_DAY_ARTIFACT_MISSING")
    with patch("constellation_2.common.pre_open_materializer_v1.collect_pre_open_prerequisite_checks_v1", return_value=checks):
        payload = derive_pre_open_bundle_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            environment="PAPER",
            ib_account="DUO847203",
            producer_results=[],
            owner_tool="ops/tools/run_pre_open_materializer_v1.py",
        )
    assert payload["materialization_state"] == "INCOMPLETE"
    assert "TARGET_DAY_ARTIFACT_MISSING" in payload["blocking_reason_codes"]


def test_stale_canonical_authority_head_is_detected(tmp_path: Path) -> None:
    checks = _checks(tmp_path)
    checks[3] = _row(tmp_path, "primary_scoped_canonical_authority_head_v1", result_status="FAIL", blocking_reason_code="TARGET_DAY_DATE_MISMATCH")
    with patch("constellation_2.common.pre_open_materializer_v1.collect_pre_open_prerequisite_checks_v1", return_value=checks):
        payload = derive_pre_open_bundle_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            environment="PAPER",
            ib_account="DUO847203",
            producer_results=[],
            owner_tool="ops/tools/run_pre_open_materializer_v1.py",
        )
    assert "TARGET_DAY_DATE_MISMATCH" in payload["blocking_reason_codes"]


def test_paper_pre_open_uses_active_session_alignment_when_pointer_head_is_stale(tmp_path: Path) -> None:
    checks = _checks(tmp_path)
    checks[3] = _row(
        tmp_path,
        "primary_scoped_canonical_authority_head_v1",
        result_status="FAIL",
        blocking_reason_code="TARGET_DAY_DATE_MISMATCH",
    )
    with (
        patch("constellation_2.common.pre_open_materializer_v1.collect_pre_open_prerequisite_checks_v1", return_value=checks),
        patch("constellation_2.common.pre_open_materializer_v1._current_active_day_observed", return_value=DAY),
    ):
        payload = derive_pre_open_bundle_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            environment="PAPER",
            ib_account="DUO847203",
            producer_results=[],
            owner_tool="ops/tools/run_pre_open_materializer_v1.py",
        )
    assert payload["materialization_state"] == "COMPLETE"
    assert "TARGET_DAY_DATE_MISMATCH" not in payload["blocking_reason_codes"]
    row = next(
        item
        for item in payload["prerequisite_checks"]
        if str(item.get("artifact_id") or "").strip() == "primary_scoped_canonical_authority_head_v1"
    )
    assert row["result_status"] == "PASS"
    assert row["observed_status"] == "ACTIVE_SESSION_PROMOTED"


def test_live_pre_open_still_requires_pointer_head_day_match(tmp_path: Path) -> None:
    checks = _checks(tmp_path)
    checks[3] = _row(
        tmp_path,
        "primary_scoped_canonical_authority_head_v1",
        result_status="FAIL",
        blocking_reason_code="TARGET_DAY_DATE_MISMATCH",
    )
    with (
        patch("constellation_2.common.pre_open_materializer_v1.collect_pre_open_prerequisite_checks_v1", return_value=checks),
        patch("constellation_2.common.pre_open_materializer_v1._current_active_day_observed", return_value=DAY),
    ):
        payload = derive_pre_open_bundle_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            environment="LIVE",
            ib_account="DUO847203",
            producer_results=[],
            owner_tool="ops/tools/run_pre_open_materializer_v1.py",
        )
    assert payload["materialization_state"] == "INCOMPLETE"
    assert "TARGET_DAY_DATE_MISMATCH" in payload["blocking_reason_codes"]


def test_producer_failure_forces_blocked_state(tmp_path: Path) -> None:
    with patch("constellation_2.common.pre_open_materializer_v1.collect_pre_open_prerequisite_checks_v1", return_value=_checks(tmp_path)):
        payload = derive_pre_open_bundle_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            environment="PAPER",
            ib_account="DUO847203",
            producer_results=[
                _producer("ops/tools/run_global_kill_switch_v1.py", return_code=2, stderr="blocked")
            ],
            owner_tool="ops/tools/run_pre_open_materializer_v1.py",
        )
    assert payload["materialization_state"] == "BLOCKED"
    assert "PRE_OPEN_PRODUCER_FAILED:ops/tools/run_global_kill_switch_v1.py" in payload["blocking_reason_codes"]


def test_handshake_producer_result_is_explicitly_unavailable(tmp_path: Path) -> None:
    checks = _checks(tmp_path)
    checks[0] = _row(tmp_path, "ib_api_handshake_latest_pointer_v1", result_status="FAIL", blocking_reason_code="TARGET_DAY_ARTIFACT_MISSING")
    checks[1] = _row(tmp_path, "ib_api_handshake_v1", result_status="FAIL", blocking_reason_code="TARGET_DAY_ARTIFACT_MISSING")
    with patch("constellation_2.common.pre_open_materializer_v1.collect_pre_open_prerequisite_checks_v1", return_value=checks):
        payload = derive_pre_open_bundle_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            environment="PAPER",
            ib_account="DUO847203",
            producer_results=[_producer("ops/tools/run_ib_api_handshake_spine_v1.py", return_code=2)],
            owner_tool="ops/tools/run_pre_open_materializer_v1.py",
        )
    producer_result = payload["producer_results"][0]
    assert producer_result["result_state"] == "UNAVAILABLE"
    assert "TARGET_DAY_ARTIFACT_MISSING" in producer_result["reason_codes"]
    assert producer_result["target_day_expected"] == DAY


def test_pointer_producer_result_is_explicitly_mismatch(tmp_path: Path) -> None:
    checks = _checks(tmp_path)
    checks[3] = _row(
        tmp_path,
        "primary_scoped_canonical_authority_head_v1",
        result_status="FAIL",
        blocking_reason_code="TARGET_DAY_DATE_MISMATCH",
    )
    with patch("constellation_2.common.pre_open_materializer_v1.collect_pre_open_prerequisite_checks_v1", return_value=checks):
        payload = derive_pre_open_bundle_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            environment="PAPER",
            ib_account="DUO847203",
            producer_results=[_producer("ops/tools/run_pointer_heads_materialize_v1.py")],
            owner_tool="ops/tools/run_pre_open_materializer_v1.py",
        )
    producer_result = payload["producer_results"][0]
    assert producer_result["result_state"] == "MISMATCH"
    assert "TARGET_DAY_DATE_MISMATCH" in producer_result["reason_codes"]


def test_kill_switch_missing_vs_blocked_are_distinct(tmp_path: Path) -> None:
    missing_checks = _checks(tmp_path)
    missing_checks[2] = _row(
        tmp_path,
        "global_kill_switch_state_v1",
        result_status="FAIL",
        role_class="REQUIRED_EXECUTION_BOUNDARY",
        blocking_reason_code="TARGET_DAY_ARTIFACT_MISSING",
    )
    blocked_checks = _checks(tmp_path)
    blocked_checks[2] = _row(
        tmp_path,
        "global_kill_switch_state_v1",
        result_status="FAIL",
        role_class="REQUIRED_EXECUTION_BOUNDARY",
        blocking_reason_code="REQUIRED_GATE_FAIL",
    )
    with patch("constellation_2.common.pre_open_materializer_v1.collect_pre_open_prerequisite_checks_v1", return_value=missing_checks):
        missing_payload = derive_pre_open_bundle_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            environment="PAPER",
            ib_account="DUO847203",
            producer_results=[_producer("ops/tools/run_global_kill_switch_v1.py")],
            owner_tool="ops/tools/run_pre_open_materializer_v1.py",
        )
    with patch("constellation_2.common.pre_open_materializer_v1.collect_pre_open_prerequisite_checks_v1", return_value=blocked_checks):
        blocked_payload = derive_pre_open_bundle_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            environment="PAPER",
            ib_account="DUO847203",
            producer_results=[_producer("ops/tools/run_global_kill_switch_v1.py")],
            owner_tool="ops/tools/run_pre_open_materializer_v1.py",
        )
    assert missing_payload["producer_results"][0]["result_state"] == "UNAVAILABLE"
    assert blocked_payload["producer_results"][0]["result_state"] == "BLOCKED"


def test_handshake_external_unavailability_reason_maps_to_unavailable(tmp_path: Path) -> None:
    checks = _checks(tmp_path)
    checks[0] = _row(
        tmp_path,
        "ib_api_handshake_latest_pointer_v1",
        result_status="FAIL",
        blocking_reason_code="REQUIRED_GATE_FAIL",
        role_class="REQUIRED_BINDING_INPUT",
    )
    checks[0]["blocker_codes"] = ["BROKER_EVENTS_MISSING"]
    checks[1] = _row(
        tmp_path,
        "ib_api_handshake_v1",
        result_status="FAIL",
        blocking_reason_code="REQUIRED_GATE_FAIL",
        role_class="REQUIRED_BINDING_INPUT",
    )
    checks[1]["blocker_codes"] = ["BROKER_EVENTS_MISSING", "IB_API_HANDSHAKE_NOT_OK"]
    with patch("constellation_2.common.pre_open_materializer_v1.collect_pre_open_prerequisite_checks_v1", return_value=checks):
        payload = derive_pre_open_bundle_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            environment="PAPER",
            ib_account="DUO847203",
            producer_results=[_producer("ops/tools/run_ib_api_handshake_spine_v1.py", return_code=2)],
            owner_tool="ops/tools/run_pre_open_materializer_v1.py",
        )
    assert payload["producer_results"][0]["result_state"] == "UNAVAILABLE"


def test_handshake_stale_pointer_reason_maps_to_stale(tmp_path: Path) -> None:
    checks = _checks(tmp_path)
    checks[0] = _row(
        tmp_path,
        "ib_api_handshake_latest_pointer_v1",
        result_status="FAIL",
        blocking_reason_code="STALE_ARTIFACT",
        role_class="REQUIRED_BINDING_INPUT",
    )
    checks[0]["blocker_codes"] = ["IB_API_HANDSHAKE_STALE_POINTER"]
    checks[1] = _row(
        tmp_path,
        "ib_api_handshake_v1",
        result_status="FAIL",
        blocking_reason_code="STALE_ARTIFACT",
        role_class="REQUIRED_BINDING_INPUT",
    )
    checks[1]["blocker_codes"] = ["IB_API_HANDSHAKE_STALE_POINTER"]
    with patch("constellation_2.common.pre_open_materializer_v1.collect_pre_open_prerequisite_checks_v1", return_value=checks):
        payload = derive_pre_open_bundle_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            environment="PAPER",
            ib_account="DUO847203",
            producer_results=[_producer("ops/tools/run_ib_api_handshake_spine_v1.py")],
            owner_tool="ops/tools/run_pre_open_materializer_v1.py",
        )
    assert payload["producer_results"][0]["result_state"] == "STALE"
