from __future__ import annotations

from pathlib import Path
import json
from types import SimpleNamespace
from unittest.mock import patch

from constellation_2.common.broker_fact_spine_v1 import (
    DOWNSTREAM_NORMAL,
    TRUST_TRUSTED,
    build_broker_fact_spine_audit_payload_v1,
)
from ops.tools.run_session_authority_v1 import (
    _annotate_source_ref_for_closure,
    _collect_pre_open_bundle_rows,
    _collect_handshake_rows,
    _trade_submit_path,
    _capability_artifact_status,
    _compute_hidden_dependency_check_result,
    _extract_timestamp,
    _freshness_status,
    _normalize_scoped_dependency_type,
    _payload_dependencies,
    _run_final_convergence_tools,
    _run_post_build_alignment_tools,
    _run_target_day_build,
    _refresh_active_session_for_final_convergence,
    _run_primary_sleeve_capability_initialization,
    _session_readiness_refresh_startup_ready,
    _source_ref_blocks_build,
)
from constellation_2.common.session_authority_v1 import (
    CLOSURE_STATUS_CLOSED,
    derive_active_session_payload_v1,
    derive_target_day_admission_payload_v1,
    derive_target_day_build_payload_v1,
    resolve_target_day_admission_attempt_path,
    resolve_target_day_build_attempt_path,
    write_active_session_v1,
    write_target_day_admission_v1,
    write_target_day_build_v1,
)
from constellation_2.common.pre_open_materializer_v1 import write_pre_open_bundle_v1
from constellation_2.common.constitutional_runtime_v1 import validate_governed_artifact_payload_v1
from constellation_2.common.session_promotion_gate_v1 import (
    PROMOTION_STATE_BLOCKED,
    PROMOTION_STATE_PROMOTED,
    derive_session_promotion_decision_payload_v1,
    write_session_promotion_decision_v1,
)


DAY = "2026-04-10"
SOURCE_ROOT = Path(__file__).resolve().parents[3]


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _artifact_row(
    tmp_path: Path,
    artifact_id: str,
    *,
    result_status: str = "PASS",
    role_class: str = "REQUIRED_DERIVED_GATE",
    classification: str = "TEST",
    blocking_reason_code: str = "",
    blocker_codes: list[str] | None = None,
    required: bool = True,
    path_family: str = "CANONICAL_RUNTIME_TRUTH_SUBPATH",
    target_day_expected: str = DAY,
    target_day_observed: str = DAY,
    date_binding_status: str = "MATCH",
    freshness_status: str = "CURRENT",
    provenance_present: bool = True,
    closure_status: str | None = None,
    observed_dependency_artifacts: list[str] | None = None,
) -> dict:
    if closure_status is None:
        closure_status = (
            CLOSURE_STATUS_CLOSED
            if result_status == "PASS"
            and path_family.startswith("CANONICAL_RUNTIME_TRUTH")
            and date_binding_status == "MATCH"
            and freshness_status == "CURRENT"
            and provenance_present
            else "OPEN"
        )
    return {
        "artifact_id": artifact_id,
        "artifact_name": artifact_id,
        "required": required,
        "role_class": role_class,
        "classification": classification,
        "canonical_path": str((tmp_path / f"{artifact_id}.json").resolve()),
        "authority_path": str((tmp_path / f"{artifact_id}.json").resolve()),
        "path_family": path_family,
        "observed_status": result_status,
        "result_status": result_status,
        "blocker_codes": blocker_codes or ([blocking_reason_code] if blocking_reason_code else []),
        "blocking_reason_code": blocking_reason_code,
        "schema_status": "VALID",
        "schema_ref": "governance/test.schema.json",
        "freshness_rule": "TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        "freshness_status": freshness_status,
        "target_day_expected": target_day_expected,
        "target_day_observed": target_day_observed,
        "date_binding_status": date_binding_status,
        "date_binding_value": target_day_observed,
        "provenance_required": True,
        "provenance_summary": {
            "required": True,
            "present": provenance_present,
            "fields_present": ["producer.module", "generated_utc"] if provenance_present else [],
            "source": "test" if provenance_present else "",
        },
        "closure_status": closure_status,
        "producer": {"module": "test", "git_sha": "abc123"},
        "source_refs": [],
        "observed_dependency_artifacts": list(observed_dependency_artifacts or []),
    }


def test_missing_artifact_blocks_admission(tmp_path: Path) -> None:
    build_payload = derive_target_day_build_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        artifact_results=[
            _artifact_row(
                tmp_path,
                "market_calendar_day",
                result_status="FAIL",
                role_class="REQUIRED_BINDING_INPUT",
                blocking_reason_code="TARGET_DAY_ARTIFACT_MISSING",
                blocker_codes=["MARKET_CALENDAR_DAY_MISSING"],
                freshness_status="STALE",
                provenance_present=False,
                closure_status="OPEN",
            ),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
        source_refs=[],
    )
    build_ref = write_target_day_build_v1(truth_root=tmp_path, payload=build_payload)
    admission_ref = write_target_day_admission_v1(
        truth_root=tmp_path,
        payload=derive_target_day_admission_payload_v1(truth_root=tmp_path, target_day=DAY, build_ref=build_ref),
    )
    assert build_ref.payload["closure_status"] == "OPEN"
    assert admission_ref.payload["admission_status"] == "BLOCKED"
    assert admission_ref.payload["blocker_chain"][0]["blocker_code"] == "TARGET_DAY_ARTIFACT_MISSING"


def test_bod_execution_substrate_proof_failure_blocks_build_and_admission(tmp_path: Path) -> None:
    build_payload = derive_target_day_build_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        artifact_results=[
            _artifact_row(
                tmp_path,
                "bod_execution_environment_proof_v1",
                result_status="FAIL",
                role_class="REQUIRED_BINDING_INPUT",
                classification="EXECUTION_SUBSTRATE_PROOF",
                blocking_reason_code="REQUIRED_GATE_FAIL",
                blocker_codes=["BOD_EXECUTION_SUBSTRATE_PROOF_FAIL:BRIDGE_IMPORT_PROBE_NONZERO"],
                closure_status="OPEN",
            ),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
        ],
        source_refs=[],
    )
    build_ref = write_target_day_build_v1(truth_root=tmp_path, payload=build_payload)
    admission_payload = derive_target_day_admission_payload_v1(truth_root=tmp_path, target_day=DAY, build_ref=build_ref)

    assert build_ref.payload["build_status"] == "BLOCKED"
    assert build_ref.payload["blocker_chain"][0]["artifact_id"] == "bod_execution_environment_proof_v1"
    assert admission_payload["admission_status"] == "BLOCKED"


def test_session_control_plane_artifacts_emit_constitutional_metadata(tmp_path: Path) -> None:
    build_payload = derive_target_day_build_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        artifact_results=[
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
        ],
        source_refs=[],
    )
    build_ref = write_target_day_build_v1(truth_root=tmp_path, payload=build_payload)
    admission_ref = write_target_day_admission_v1(
        truth_root=tmp_path,
        payload=derive_target_day_admission_payload_v1(truth_root=tmp_path, target_day=DAY, build_ref=build_ref),
    )
    active_ref = write_active_session_v1(
        truth_root=tmp_path,
        payload=derive_active_session_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            admission_ref=admission_ref,
        ),
    )

    validated_admission = validate_governed_artifact_payload_v1(
        repo_root=SOURCE_ROOT,
        artifact_id="target_day_admission_v1",
        payload=admission_ref.payload,
    )
    validated_active = validate_governed_artifact_payload_v1(
        repo_root=SOURCE_ROOT,
        artifact_id="active_session_v1",
        payload=active_ref.payload,
    )

    assert validated_admission["constitutional_dependency_declaration"]["declared_dependency_artifacts"] == [
        "target_day_build_v1"
    ]
    assert validated_active["constitutional_dependency_declaration"]["declared_dependency_artifacts"] == [
        "target_day_admission_v1",
        "target_day_build_v1",
    ]


def test_wrong_target_day_blocks_admission(tmp_path: Path) -> None:
    build_payload = derive_target_day_build_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        artifact_results=[
            _artifact_row(
                tmp_path,
                "paper_policy_verdict_v1",
                date_binding_status="MISMATCH",
                target_day_observed="2026-04-09",
                freshness_status="STALE",
                blocking_reason_code="TARGET_DAY_DATE_MISMATCH",
                closure_status="OPEN",
            )
        ],
        source_refs=[],
    )
    build_ref = write_target_day_build_v1(truth_root=tmp_path, payload=build_payload)
    admission_payload = derive_target_day_admission_payload_v1(truth_root=tmp_path, target_day=DAY, build_ref=build_ref)
    assert build_ref.payload["blocker_chain"][0]["blocker_code"] == "TARGET_DAY_DATE_MISMATCH"
    assert admission_payload["admission_status"] == "BLOCKED"


def test_stale_artifact_blocks_admission(tmp_path: Path) -> None:
    build_payload = derive_target_day_build_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        artifact_results=[
            _artifact_row(
                tmp_path,
                "trade_submit_readiness_c2_v1",
                freshness_status="STALE",
                blocking_reason_code="STALE_ARTIFACT",
                closure_status="OPEN",
            )
        ],
        source_refs=[],
    )
    assert build_payload["blocker_chain"][0]["blocker_code"] == "STALE_ARTIFACT"
    assert build_payload["closure_status"] == "OPEN"


def test_wrong_authority_path_blocks_admission(tmp_path: Path) -> None:
    build_payload = derive_target_day_build_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        artifact_results=[
            _artifact_row(
                tmp_path,
                "capability_state_v1",
                path_family="AUTHORITATIVE_REPO_TRUTH_SUBPATH",
                blocking_reason_code="WRONG_AUTHORITY_PATH",
                closure_status="OPEN",
            )
        ],
        source_refs=[],
    )
    assert build_payload["blocker_chain"][0]["blocker_code"] == "WRONG_AUTHORITY_PATH"


def test_hidden_dependency_detection_blocks_admission(tmp_path: Path) -> None:
    build_payload = derive_target_day_build_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        artifact_results=[
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
        ],
        source_refs=[],
        hidden_dependency_check_result={
            "status": "FAIL",
            "blocking_reason_code": "HIDDEN_DEPENDENCY_DETECTED",
            "summary": "undeclared_dependency_artifacts=unexpected_gate_v1",
            "declared_inventory_artifacts": ["paper_policy_verdict_v1", "trade_submit_readiness_c2_v1"],
            "observed_dependency_artifacts": ["paper_policy_verdict_v1", "trade_submit_readiness_c2_v1", "unexpected_gate_v1"],
            "undeclared_dependency_artifacts": ["unexpected_gate_v1"],
        },
    )
    build_ref = write_target_day_build_v1(truth_root=tmp_path, payload=build_payload)
    admission_payload = derive_target_day_admission_payload_v1(truth_root=tmp_path, target_day=DAY, build_ref=build_ref)
    assert build_payload["hidden_dependency_check_result"]["status"] == "FAIL"
    assert build_payload["blocker_chain"][-1]["blocker_code"] == "HIDDEN_DEPENDENCY_DETECTED"
    assert admission_payload["admission_status"] == "BLOCKED"
    assert build_payload["hidden_dependency_check_result"]["failing_producers"] == []


def test_paper_bootstrap_can_admit_blocked_build(tmp_path: Path) -> None:
    build_payload = derive_target_day_build_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        artifact_results=[
            _artifact_row(
                tmp_path,
                "trade_submit_readiness_c2_v1",
                result_status="FAIL",
                blocking_reason_code="TARGET_DAY_ARTIFACT_MISSING",
                closure_status="OPEN",
            )
        ],
        source_refs=[],
    )
    build_ref = write_target_day_build_v1(truth_root=tmp_path, payload=build_payload)
    with patch(
        "constellation_2.common.session_authority_v1.evaluate_paper_bootstrap_admission_v1",
        return_value={"eligible": True},
    ):
        admission_payload = derive_target_day_admission_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            build_ref=build_ref,
            environment="PAPER",
            repo_root=tmp_path,
            enforce_consistency_gate=True,
        )

    assert admission_payload["admission_status"] == "ADMIT"
    assert admission_payload["blocker_chain"] == []
    assert admission_payload["blocking_reason_codes"] == []
    assert admission_payload["mode"] == "PAPER_BOOTSTRAP"
    assert admission_payload["reason"] == "PAPER_BOOTSTRAP_SESSION_ADMISSION"


def test_global_kill_switch_declared_dependency_does_not_trigger_hidden_dependency_failure(tmp_path: Path) -> None:
    artifact_results = [
        _artifact_row(tmp_path, "global_kill_switch_state_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        _artifact_row(
            tmp_path,
            "submit_boundary_status_v1",
            role_class="REQUIRED_EXECUTION_BOUNDARY",
            observed_dependency_artifacts=["global_kill_switch_state_v1"],
        ),
    ]
    result = _compute_hidden_dependency_check_result(artifact_results=artifact_results, source_refs=[])
    assert result["status"] == "PASS"
    assert result["undeclared_dependency_artifacts"] == []


def test_pre_open_bundle_rows_surface_bundle_and_prerequisite_checks(tmp_path: Path) -> None:
    bundle_payload = {
        "schema_id": "pre_open_bundle",
        "schema_version": "v1",
        "target_day": DAY,
        "active_day_observed": "2026-04-09",
        "active_day_alignment_status": "MISMATCH",
        "owner_tool": "ops/tools/run_pre_open_materializer_v1.py",
        "materialization_state": "COMPLETE",
        "completion_state": "COMPLETE",
        "blocking_reason_codes": [],
        "prerequisite_checks": [
            _artifact_row(tmp_path, "ib_api_handshake_latest_pointer_v1"),
            _artifact_row(tmp_path, "ib_api_handshake_v1"),
            _artifact_row(tmp_path, "global_kill_switch_state_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
            _artifact_row(tmp_path, "primary_scoped_canonical_authority_head_v1"),
        ],
        "producer_results": [],
        "market_calendar_status": {
            "available": False,
            "artifact_path": "",
            "artifact_sha256": "",
            "severity": "",
            "required_target_day": "",
            "reason_codes": [],
        },
        "built_at_utc": f"{DAY}T00:00:00Z",
        "producer": {"repo": str(tmp_path), "module": "constellation_2/common/pre_open_materializer_v1.py", "git_sha": "abc123"},
    }
    write_pre_open_bundle_v1(truth_root=tmp_path, payload=bundle_payload)

    rows = _collect_pre_open_bundle_rows(truth_root=tmp_path, target_day=DAY)

    assert rows[0]["artifact_id"] == "pre_open_bundle_v1"
    assert rows[0]["result_status"] == "PASS"
    assert rows[1]["artifact_id"] == "ib_api_handshake_latest_pointer_v1"
    assert rows[-1]["artifact_id"] == "primary_scoped_canonical_authority_head_v1"


def test_pre_open_bundle_incomplete_blocks_admission(tmp_path: Path) -> None:
    blocked_row = _artifact_row(
        tmp_path,
        "global_kill_switch_state_v1",
        role_class="REQUIRED_EXECUTION_BOUNDARY",
        result_status="FAIL",
        blocking_reason_code="TARGET_DAY_ARTIFACT_MISSING",
        freshness_status="STALE",
        provenance_present=False,
        closure_status="OPEN",
    )
    bundle_payload = {
        "schema_id": "pre_open_bundle",
        "schema_version": "v1",
        "target_day": DAY,
        "active_day_observed": "2026-04-09",
        "active_day_alignment_status": "MISMATCH",
        "owner_tool": "ops/tools/run_pre_open_materializer_v1.py",
        "materialization_state": "INCOMPLETE",
        "completion_state": "INCOMPLETE",
        "blocking_reason_codes": ["TARGET_DAY_ARTIFACT_MISSING"],
        "prerequisite_checks": [
            _artifact_row(tmp_path, "ib_api_handshake_latest_pointer_v1"),
            _artifact_row(tmp_path, "ib_api_handshake_v1"),
            blocked_row,
            _artifact_row(tmp_path, "primary_scoped_canonical_authority_head_v1"),
        ],
        "producer_results": [],
        "market_calendar_status": {
            "available": False,
            "artifact_path": "",
            "artifact_sha256": "",
            "severity": "",
            "required_target_day": "",
            "reason_codes": [],
        },
        "built_at_utc": f"{DAY}T00:00:00Z",
        "producer": {"repo": str(tmp_path), "module": "constellation_2/common/pre_open_materializer_v1.py", "git_sha": "abc123"},
    }
    write_pre_open_bundle_v1(truth_root=tmp_path, payload=bundle_payload)
    rows = _collect_pre_open_bundle_rows(truth_root=tmp_path, target_day=DAY)
    build_payload = derive_target_day_build_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        artifact_results=[
            rows[0],
            rows[1],
            rows[2],
            rows[3],
            rows[4],
            _artifact_row(tmp_path, "primary_scoped_authorization_gate_verdict_v1"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
        ],
        source_refs=[],
    )
    build_ref = write_target_day_build_v1(truth_root=tmp_path, payload=build_payload)
    admission_payload = derive_target_day_admission_payload_v1(truth_root=tmp_path, target_day=DAY, build_ref=build_ref)
    assert build_payload["build_status"] == "BLOCKED"
    assert admission_payload["admission_status"] == "BLOCKED"
    assert "TARGET_DAY_ARTIFACT_MISSING" in admission_payload["blocking_reason_codes"]


def test_submit_boundary_dependency_extraction_excludes_session_authority_self_refs() -> None:
    deps = _payload_dependencies(
        "submit_boundary_status_v1",
        {
            "required_boundary_checks": [
                {"logical_name": "target_day_build_v1"},
                {"logical_name": "target_day_admission_v1"},
                {"logical_name": "runtime_control_record_v1"},
                {"logical_name": "startup_materialization_v1"},
                {"logical_name": "global_kill_switch_state_v1"},
            ]
        },
    )
    assert "target_day_build_v1" not in deps
    assert "target_day_admission_v1" not in deps
    assert "runtime_control_record_v1" not in deps
    assert "startup_materialization_v1" in deps
    assert "global_kill_switch_state_v1" in deps


def test_valid_complete_artifact_set_admits_successfully(tmp_path: Path) -> None:
    build_payload = derive_target_day_build_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        artifact_results=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
        source_refs=[],
    )
    build_ref = write_target_day_build_v1(truth_root=tmp_path, payload=build_payload)
    admission_ref = write_target_day_admission_v1(
        truth_root=tmp_path,
        payload=derive_target_day_admission_payload_v1(truth_root=tmp_path, target_day=DAY, build_ref=build_ref),
    )
    assert build_ref.payload["closure_status"] == "CLOSED"
    assert admission_ref.payload["admission_status"] == "ADMIT"


def test_handshake_latest_pointer_newer_day_does_not_block_valid_target_day_artifact(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    pointer_path = truth_root / "ib_api_handshake" / "latest_pointer.v1.json"
    handshake_day_path = truth_root / "ib_api_handshake" / DAY / "ib_api_handshake.v1.json"
    handshake_next_path = truth_root / "ib_api_handshake" / "2026-04-11" / "ib_api_handshake.v1.json"
    _write_json(
        pointer_path,
        {
            "schema_id": "C2_IB_API_HANDSHAKE_LATEST_POINTER_V1",
            "schema_version": 1,
            "day_utc": "2026-04-11",
            "produced_utc": "2026-04-11T00:00:00Z",
            "pointers": {
                "handshake_path": str(handshake_next_path),
                "handshake_sha256": "a" * 64,
            },
        },
    )
    _write_json(
        handshake_day_path,
        {
            "schema_id": "C2_IB_API_HANDSHAKE_V1",
            "schema_version": 1,
            "day_utc": DAY,
            "produced_utc": f"{DAY}T00:00:00Z",
            "status": "OK",
            "ok": True,
            "environment": "PAPER",
            "ib_account": "DUO847203",
        },
    )
    _write_json(
        handshake_next_path,
        {
            "schema_id": "C2_IB_API_HANDSHAKE_V1",
            "schema_version": 1,
            "day_utc": "2026-04-11",
            "produced_utc": "2026-04-11T00:00:00Z",
            "status": "OK",
            "ok": True,
            "environment": "PAPER",
            "ib_account": "DUO847203",
        },
    )

    with patch(
        "ops.tools.run_session_authority_v1.classify_runtime_path_v1",
        return_value={"path_class": "CANONICAL_RUNTIME_TRUTH_SUBPATH"},
    ):
        pointer_row, handshake_row = _collect_handshake_rows(
            truth_root=truth_root,
            environment="PAPER",
            ib_account="DUO847203",
            target_day=DAY,
        )

    assert pointer_row["blocking_reason_code"] == ""
    assert pointer_row["date_binding_status"] == "MATCH"
    assert pointer_row["target_day_observed"] == DAY
    assert pointer_row["freshness_status"] == "CURRENT"
    assert handshake_row["blocking_reason_code"] == ""


def test_optional_diagnostic_failure_does_not_block_admission(tmp_path: Path) -> None:
    build_payload = derive_target_day_build_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        artifact_results=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
            _artifact_row(
                tmp_path,
                "deployment_state_machine_v1",
                required=False,
                classification="DIAGNOSTIC_CONTEXT",
                result_status="FAIL",
                blocking_reason_code="REQUIRED_GATE_FAIL",
                closure_status="OPEN",
            ),
        ],
        source_refs=[],
    )
    build_ref = write_target_day_build_v1(truth_root=tmp_path, payload=build_payload)
    admission_ref = write_target_day_admission_v1(
        truth_root=tmp_path,
        payload=derive_target_day_admission_payload_v1(truth_root=tmp_path, target_day=DAY, build_ref=build_ref),
    )
    assert not any(item["artifact_id"] == "deployment_state_machine_v1" for item in build_payload["blocker_chain"])
    assert admission_ref.payload["admission_status"] == "ADMIT"


def test_nonblocking_session_readiness_source_ref_does_not_force_partial_build(tmp_path: Path) -> None:
    build_payload = derive_target_day_build_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        artifact_results=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
        source_refs=[
            {
                "script": "ops/tools/run_session_readiness_refresh_v1.py",
                "return_code": 2,
                "required_for_closure": False,
                "startup_authority_ready": True,
            }
        ],
    )
    assert build_payload["closure_status"] == "CLOSED"
    assert not any(item["blocker_code"] == "PARTIAL_BUILD" for item in build_payload["blocker_chain"])


def test_build_and_admission_do_not_block_on_primary_capital_allocation_when_startup_authority_is_ready(
    tmp_path: Path,
) -> None:
    build_payload = derive_target_day_build_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        artifact_results=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
        source_refs=[
            {
                "script": "ops/tools/run_session_readiness_refresh_v1.py",
                "return_code": 2,
                "required_for_closure": False,
                "startup_authority_ready": True,
                "stdout": (
                    '{"results":{"authority_kernel_validation":{"validation_summary":{"validation_state":"PASS"}},'
                    '"scope_summary":{"primary_ready":true},"day_authority_decision":{"decision_state":"OPEN"}}}'
                ),
            }
        ],
    )
    build_ref = write_target_day_build_v1(truth_root=tmp_path, payload=build_payload)
    admission_ref = write_target_day_admission_v1(
        truth_root=tmp_path,
        payload=derive_target_day_admission_payload_v1(truth_root=tmp_path, target_day=DAY, build_ref=build_ref),
    )
    blocker_codes = {row["blocker_code"] for row in build_payload["blocker_chain"]}
    assert "PRIMARY_CAPITAL_AUTHORITY_ALLOCATION_ARTIFACT_MISSING" not in blocker_codes
    assert build_ref.payload["closure_status"] == "CLOSED"
    assert admission_ref.payload["admission_status"] == "ADMIT"


def test_blocking_source_ref_still_surfaces_partial_build(tmp_path: Path) -> None:
    build_payload = derive_target_day_build_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        artifact_results=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
        source_refs=[
            {
                "script": "ops/tools/run_startup_materialization_v1.py",
                "return_code": 2,
                "required_for_closure": True,
            }
        ],
    )
    assert any(item["blocker_code"] == "PARTIAL_BUILD" for item in build_payload["blocker_chain"])


def test_downstream_build_cycle_source_ref_does_not_block_build_closure() -> None:
    ref = _annotate_source_ref_for_closure(
        {
            "script": "ops/tools/run_submit_boundary_status_v1.py",
            "return_code": 2,
            "stdout": "",
            "stderr": "",
        }
    )
    assert ref["required_for_closure"] is False
    assert ref["nonblocking_reason"] == "DOWNSTREAM_BUILD_CYCLE_CONSUMER"
    assert not _source_ref_blocks_build(ref)


def test_post_build_alignment_reruns_downstream_authorization_tools(monkeypatch) -> None:
    calls: list[tuple[str, tuple[str, ...]]] = []

    def _fake_run_tool(script_relpath: str, *args: str):  # noqa: ANN001
        calls.append((script_relpath, args))
        return {"script": script_relpath, "return_code": 0, "required_for_closure": False}

    monkeypatch.setattr("ops.tools.run_session_authority_v1._run_tool", _fake_run_tool)

    _run_post_build_alignment_tools(truth_root=Path("/tmp/truth"), target_day=DAY)

    assert calls == [
        ("ops/tools/run_submit_boundary_status_v1.py", ("--day_utc", DAY, "--truth_root", "/tmp/truth")),
        ("ops/tools/run_paper_session_ledger_v1.py", ("--day_utc", DAY, "--truth_root", "/tmp/truth")),
        ("ops/tools/run_startup_proof_validation_v1.py", ("--day_utc", DAY, "--truth_root", "/tmp/truth")),
        ("ops/tools/run_trading_day_state_machine_v1.py", ("--day_utc", DAY, "--truth_root", "/tmp/truth")),
    ]


def test_target_day_build_skips_session_readiness_reentry_when_flagged(monkeypatch, tmp_path: Path) -> None:
    calls: list[tuple[str, tuple[str, ...]]] = []

    def _fake_run_tool(script_relpath: str, *args: str):  # noqa: ANN001
        calls.append((script_relpath, args))
        return {"script": script_relpath, "return_code": 0, "stdout": "", "stderr": "", "required_for_closure": True}

    def _fake_surface_row(*, artifact_id: str, required: bool = True, role_class: str = "REQUIRED_DERIVED_GATE", classification: str = "TEST", **_kwargs):  # noqa: ANN001
        return _artifact_row(
            tmp_path,
            artifact_id,
            result_status="PASS",
            required=required,
            role_class=role_class,
            classification=classification,
            closure_status="CLOSED",
        )

    monkeypatch.setattr("ops.tools.run_session_authority_v1._run_tool", _fake_run_tool)
    monkeypatch.setattr("ops.tools.run_session_authority_v1._run_primary_sleeve_capability_initialization", lambda **_kwargs: [])
    monkeypatch.setattr("ops.tools.run_session_authority_v1._collect_market_calendar_row", lambda **_kwargs: _fake_surface_row(artifact_id="market_calendar_day", role_class="REQUIRED_BINDING_INPUT"))
    monkeypatch.setattr("ops.tools.run_session_authority_v1._collect_day_authority_row", lambda **_kwargs: _fake_surface_row(artifact_id="day_authority_decision_v1", role_class="REQUIRED_BINDING_INPUT", classification="SESSION_AUTHORITY_INPUT"))
    monkeypatch.setattr("ops.tools.run_session_authority_v1._collect_pre_open_bundle_rows", lambda **_kwargs: [])
    monkeypatch.setattr("ops.tools.run_session_authority_v1._collect_primary_scoped_authorization_row", lambda **_kwargs: _fake_surface_row(artifact_id="primary_scoped_authorization_gate_verdict_v1", role_class="REQUIRED_DERIVED_GATE"))
    monkeypatch.setattr("ops.tools.run_session_authority_v1._trade_submit_path", lambda **_kwargs: tmp_path / "readiness.json")
    monkeypatch.setattr("ops.tools.run_session_authority_v1._read_surface_row", _fake_surface_row)
    monkeypatch.setattr("ops.tools.run_session_authority_v1._collect_previous_day_economic_rows_from_readiness", lambda **_kwargs: [])
    monkeypatch.setattr(
        "ops.tools.run_session_authority_v1._compute_hidden_dependency_check_result",
        lambda **_kwargs: {
            "status": "PASS",
            "blocking_reason_code": "",
            "summary": "",
            "declared_inventory_artifacts": [],
            "observed_dependency_artifacts": [],
            "undeclared_dependency_artifacts": [],
            "failing_producers": [],
        },
    )
    monkeypatch.setattr(
        "ops.tools.run_session_authority_v1.derive_target_day_build_payload_v1",
        lambda **_kwargs: {
            "schema_id": "target_day_build",
            "schema_version": "v1",
            "target_day": DAY,
            "build_status": "COMPLETE",
            "completeness_result": "COMPLETE",
            "closure_status": "CLOSED",
            "blocker_chain": [],
            "hidden_dependency_check_result": {"status": "PASS", "blocking_reason_code": ""},
            "artifact_results": [],
            "source_refs": [],
        },
    )
    monkeypatch.setattr(
        "ops.tools.run_session_authority_v1.write_target_day_build_v1",
        lambda *, truth_root, payload: SimpleNamespace(path=Path(truth_root) / "target_day_build_v1" / f"{DAY}.json", sha256="a" * 64, payload=payload),
    )

    monkeypatch.setenv("C2_SKIP_SESSION_AUTHORITY_REENTRY", "YES")
    ref = _run_target_day_build(
        truth_root=tmp_path,
        target_day=DAY,
        environment="PAPER",
        ib_account="DUO847203",
    )

    assert ref.payload["build_status"] == "COMPLETE"
    assert not any(script == "ops/tools/run_session_readiness_refresh_v1.py" for script, _args in calls)
    assert any(script == "ops/tools/run_day_authority_decision_v1.py" for script, _args in calls)


def test_final_convergence_reruns_control_plane_then_status(monkeypatch) -> None:
    calls: list[tuple[str, tuple[str, ...]]] = []

    def _fake_run_tool(script_relpath: str, *args: str):  # noqa: ANN001
        calls.append((script_relpath, args))
        return {"script": script_relpath, "return_code": 0, "required_for_closure": False}

    monkeypatch.setattr("ops.tools.run_session_authority_v1._run_tool", _fake_run_tool)
    refreshed = []
    monkeypatch.setattr(
        "ops.tools.run_session_authority_v1._refresh_active_session_for_final_convergence",
        lambda *, truth_root, target_day: refreshed.append((truth_root, target_day)),
    )

    _run_final_convergence_tools(truth_root=Path("/tmp/truth"), target_day=DAY)

    assert refreshed == [(Path("/tmp/truth"), DAY)]
    assert calls == [
        ("ops/tools/run_paper_day_control_plane_v1.py", ("--day_utc", DAY, "--truth_root", "/tmp/truth")),
        (
            "ops/tools/run_session_authority_status_v1.py",
            ("--truth_root", "/tmp/truth", "--environment", "PAPER", "--mode", "WRITE", "--json"),
        ),
        (
            "ops/tools/run_day_open_trigger_v1.py",
            ("--day_utc", DAY, "--truth_root", "/tmp/truth", "--environment", "PAPER"),
        ),
        ("ops/tools/run_operator_day_authority_summary_v1.py", ("--day_utc", DAY, "--truth_root", "/tmp/truth")),
        ("ops/tools/run_day_failure_causality_v1.py", ("--day_utc", DAY, "--truth_root", "/tmp/truth")),
    ]


def test_refresh_active_session_for_final_convergence_updates_build_ref(tmp_path: Path) -> None:
    old_build_ref = write_target_day_build_v1(
        truth_root=tmp_path,
        payload=derive_target_day_build_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            artifact_results=[
                _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
                _artifact_row(tmp_path, "day_authority_decision_v1", role_class="REQUIRED_BINDING_INPUT"),
                _artifact_row(tmp_path, "paper_policy_verdict_v1"),
                _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            ],
            source_refs=[],
        ),
    )
    old_admission_ref = write_target_day_admission_v1(
        truth_root=tmp_path,
        payload=derive_target_day_admission_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            build_ref=old_build_ref,
        ),
    )
    write_active_session_v1(
        truth_root=tmp_path,
        payload=derive_active_session_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            admission_ref=old_admission_ref,
        ),
    )

    newer_build_payload = derive_target_day_build_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        artifact_results=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "day_authority_decision_v1", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
        ],
        source_refs=[],
    )
    newer_build_payload["generated_utc"] = "2099-01-01T00:00:00Z"
    newer_build_ref = write_target_day_build_v1(truth_root=tmp_path, payload=newer_build_payload)
    newer_admission_ref = write_target_day_admission_v1(
        truth_root=tmp_path,
        payload=derive_target_day_admission_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            build_ref=newer_build_ref,
        ),
    )

    refreshed_active_ref = _refresh_active_session_for_final_convergence(truth_root=tmp_path, target_day=DAY)

    assert refreshed_active_ref.payload["target_day_admission_ref"] == str(newer_admission_ref.path)
    assert refreshed_active_ref.payload["target_day_build_ref"]["artifact_path"] == str(newer_build_ref.path)
    assert refreshed_active_ref.payload["target_day_build_ref"]["artifact_sha256"] == newer_build_ref.sha256
    assert refreshed_active_ref.payload["active_day_build_ref"]["artifact_sha256"] == newer_build_ref.sha256


def test_build_and_admission_ignore_downstream_cycle_artifacts_but_track_them(tmp_path: Path) -> None:
    source_refs = [
        {
            "script": "ops/tools/run_submit_boundary_status_v1.py",
            "return_code": 2,
            "required_for_closure": False,
        },
        {
            "script": "ops/tools/run_paper_session_ledger_v1.py",
            "return_code": 2,
            "required_for_closure": False,
        },
        {
            "script": "ops/tools/run_startup_proof_validation_v1.py",
            "return_code": 2,
            "required_for_closure": False,
        },
        {
            "script": "ops/tools/run_trading_day_state_machine_v1.py",
            "return_code": 2,
            "required_for_closure": False,
        },
    ]
    hidden_dependency_result = _compute_hidden_dependency_check_result(
        artifact_results=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "day_authority_decision_v1", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "submit_boundary_status_v1", result_status="FAIL", required=False, closure_status="OPEN"),
            _artifact_row(tmp_path, "paper_session_ledger_v1", result_status="FAIL", required=False, closure_status="OPEN"),
            _artifact_row(tmp_path, "startup_proof_validation_v1", result_status="FAIL", required=False, closure_status="OPEN"),
            _artifact_row(tmp_path, "paper_day_control_plane_v1", result_status="FAIL", required=False, closure_status="OPEN"),
            _artifact_row(tmp_path, "trading_day_control_plane_v1", result_status="FAIL", required=False, closure_status="OPEN"),
            _artifact_row(tmp_path, "trading_day_execution_control_plane_v1", result_status="FAIL", required=False, closure_status="OPEN"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", result_status="FAIL", required=False, closure_status="OPEN"),
        ],
        source_refs=source_refs,
    )
    build_payload = derive_target_day_build_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        artifact_results=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "day_authority_decision_v1", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "submit_boundary_status_v1", result_status="FAIL", required=False, closure_status="OPEN"),
            _artifact_row(tmp_path, "paper_session_ledger_v1", result_status="FAIL", required=False, closure_status="OPEN"),
            _artifact_row(tmp_path, "startup_proof_validation_v1", result_status="FAIL", required=False, closure_status="OPEN"),
            _artifact_row(tmp_path, "paper_day_control_plane_v1", result_status="FAIL", required=False, closure_status="OPEN"),
            _artifact_row(tmp_path, "trading_day_control_plane_v1", result_status="FAIL", required=False, closure_status="OPEN"),
            _artifact_row(tmp_path, "trading_day_execution_control_plane_v1", result_status="FAIL", required=False, closure_status="OPEN"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", result_status="FAIL", required=False, closure_status="OPEN"),
        ],
        source_refs=source_refs,
        hidden_dependency_check_result=hidden_dependency_result,
    )
    build_ref = write_target_day_build_v1(truth_root=tmp_path, payload=build_payload)
    admission_ref = write_target_day_admission_v1(
        truth_root=tmp_path,
        payload=derive_target_day_admission_payload_v1(truth_root=tmp_path, target_day=DAY, build_ref=build_ref),
    )

    assert build_ref.payload["build_status"] == "COMPLETE"
    assert build_ref.payload["completeness_result"] == "COMPLETE"
    assert build_ref.payload["closure_status"] == "CLOSED"
    assert build_ref.payload["hidden_dependency_check_result"]["status"] == "PASS"
    assert build_ref.payload["hidden_dependency_check_result"]["failing_producers"] == [
        "ops/tools/run_submit_boundary_status_v1.py",
        "ops/tools/run_paper_session_ledger_v1.py",
        "ops/tools/run_startup_proof_validation_v1.py",
        "ops/tools/run_trading_day_state_machine_v1.py",
    ]
    required_ids = {row["artifact_id"] for row in build_ref.payload["required_artifacts"]}
    assert "submit_boundary_status_v1" not in required_ids
    assert "paper_session_ledger_v1" not in required_ids
    assert "startup_proof_validation_v1" not in required_ids
    assert "paper_day_control_plane_v1" not in required_ids
    assert "trading_day_control_plane_v1" not in required_ids
    assert "trading_day_execution_control_plane_v1" not in required_ids
    assert "trading_day_state_machine_v1" not in required_ids
    assert not any(
        row["blocker_code"] in {"PARTIAL_BUILD", "REQUIRED_GATE_FAIL"}
        and row["artifact_id"] in {
            "submit_boundary_status_v1",
            "paper_session_ledger_v1",
            "startup_proof_validation_v1",
            "paper_day_control_plane_v1",
            "trading_day_control_plane_v1",
            "trading_day_execution_control_plane_v1",
            "trading_day_state_machine_v1",
        }
        for row in build_ref.payload["blocker_chain"]
    )
    diagnostic_rows = {
        row["artifact_id"]: row
        for row in build_ref.payload["artifact_results"]
        if row["artifact_id"]
        in {
            "paper_day_control_plane_v1",
            "trading_day_control_plane_v1",
            "trading_day_execution_control_plane_v1",
        }
    }
    assert set(diagnostic_rows) == {
        "paper_day_control_plane_v1",
        "trading_day_control_plane_v1",
        "trading_day_execution_control_plane_v1",
    }
    assert all(row["required"] is False for row in diagnostic_rows.values())
    assert all(row["result_status"] == "FAIL" for row in diagnostic_rows.values())
    assert admission_ref.payload["admission_status"] == "ADMIT"


def test_build_can_close_with_missing_control_plane_diagnostics(tmp_path: Path) -> None:
    build_ref = write_target_day_build_v1(
        truth_root=tmp_path,
        payload=derive_target_day_build_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            artifact_results=[
                _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
                _artifact_row(tmp_path, "day_authority_decision_v1", role_class="REQUIRED_BINDING_INPUT"),
                _artifact_row(tmp_path, "paper_policy_verdict_v1"),
                _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
                _artifact_row(
                    tmp_path,
                    "paper_day_control_plane_v1",
                    result_status="FAIL",
                    required=False,
                    closure_status="OPEN",
                    blocking_reason_code="TARGET_DAY_ARTIFACT_MISSING",
                    blocker_codes=["PAPER_DAY_CONTROL_PLANE_V1_MISSING"],
                    freshness_status="STALE",
                    provenance_present=False,
                    target_day_observed="",
                    date_binding_status="MISSING",
                ),
                _artifact_row(
                    tmp_path,
                    "trading_day_control_plane_v1",
                    result_status="FAIL",
                    required=False,
                    closure_status="OPEN",
                    blocking_reason_code="TARGET_DAY_ARTIFACT_MISSING",
                    blocker_codes=["TRADING_DAY_CONTROL_PLANE_V1_MISSING"],
                    freshness_status="STALE",
                    provenance_present=False,
                    target_day_observed="",
                    date_binding_status="MISSING",
                ),
                _artifact_row(
                    tmp_path,
                    "trading_day_execution_control_plane_v1",
                    result_status="FAIL",
                    required=False,
                    closure_status="OPEN",
                    blocking_reason_code="TARGET_DAY_ARTIFACT_MISSING",
                    blocker_codes=["TRADING_DAY_EXECUTION_CONTROL_PLANE_V1_MISSING"],
                    freshness_status="STALE",
                    provenance_present=False,
                    target_day_observed="",
                    date_binding_status="MISSING",
                ),
            ],
            source_refs=[],
        ),
    )

    assert build_ref.payload["build_status"] == "COMPLETE"
    assert build_ref.payload["completeness_result"] == "COMPLETE"
    assert build_ref.payload["closure_status"] == "CLOSED"
    assert not any(
        row["artifact_id"]
        in {
            "paper_day_control_plane_v1",
            "trading_day_control_plane_v1",
            "trading_day_execution_control_plane_v1",
        }
        for row in build_ref.payload["required_artifacts"]
    )


def test_active_session_withheld_rollover_contains_blocked_fields(tmp_path: Path) -> None:
    admitted_build_ref = write_target_day_build_v1(
        truth_root=tmp_path,
        payload=derive_target_day_build_payload_v1(
            truth_root=tmp_path,
            target_day="2026-04-09",
            artifact_results=[
                _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY", target_day_expected="2026-04-09", target_day_observed="2026-04-09")
            ],
            source_refs=[],
        ),
    )
    admitted_admission_ref = write_target_day_admission_v1(
        truth_root=tmp_path,
        payload=derive_target_day_admission_payload_v1(
            truth_root=tmp_path,
            target_day="2026-04-09",
            build_ref=admitted_build_ref,
        ),
    )
    prior_active_ref = write_active_session_v1(
        truth_root=tmp_path,
        payload=derive_active_session_payload_v1(
            truth_root=tmp_path,
            target_day="2026-04-09",
            admission_ref=admitted_admission_ref,
        ),
    )

    blocked_build_ref = write_target_day_build_v1(
        truth_root=tmp_path,
        payload=derive_target_day_build_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            artifact_results=[
                _artifact_row(
                    tmp_path,
                    "market_calendar_day",
                    result_status="FAIL",
                    role_class="REQUIRED_BINDING_INPUT",
                    blocking_reason_code="TARGET_DAY_ARTIFACT_MISSING",
                    blocker_codes=["MARKET_CALENDAR_DAY_MISSING"],
                    freshness_status="STALE",
                    provenance_present=False,
                    closure_status="OPEN",
                )
            ],
            source_refs=[],
        ),
    )
    blocked_admission_ref = write_target_day_admission_v1(
        truth_root=tmp_path,
        payload=derive_target_day_admission_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            build_ref=blocked_build_ref,
        ),
    )
    blocked_active_ref = write_active_session_v1(
        truth_root=tmp_path,
        payload=derive_active_session_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            admission_ref=blocked_admission_ref,
            prior_active_session_ref=prior_active_ref,
        ),
    )
    assert blocked_active_ref.payload["rollover_status"] == "ROLLOVER_WITHHELD"
    assert blocked_active_ref.payload["blocked_target_day"] == DAY
    assert blocked_active_ref.payload["blocked_admission_ref"] == str(blocked_admission_ref.path)
    assert blocked_active_ref.payload["rollover_reason_code"] == "TARGET_DAY_ARTIFACT_MISSING"


def test_active_session_rollover_requires_promoted_decision(tmp_path: Path) -> None:
    pre_open_ref = write_pre_open_bundle_v1(
        truth_root=tmp_path,
        payload={
            "schema_id": "pre_open_bundle",
            "schema_version": "v1",
            "target_day": DAY,
            "active_day_observed": "2026-04-09",
            "active_day_alignment_status": "MISMATCH",
            "owner_tool": "ops/tools/run_pre_open_materializer_v1.py",
            "materialization_state": "COMPLETE",
            "completion_state": "COMPLETE",
            "blocking_reason_codes": [],
            "prerequisite_checks": [],
            "producer_results": [],
            "market_calendar_status": {
                "available": True,
                "artifact_path": str((tmp_path / "market_calendar_coverage_status_v1" / "current.json").resolve()),
                "artifact_sha256": "d" * 64,
                "required_target_day": DAY,
                "severity": "INFO",
                "reason_codes": [],
            },
            "built_at_utc": "2026-04-16T12:00:00Z",
            "producer": {
                "module": "ops/tools/run_pre_open_materializer_v1.py",
                "git_sha": "abc123",
                "repo": "constellation",
            },
        },
    )
    build_ref = write_target_day_build_v1(
        truth_root=tmp_path,
        payload=derive_target_day_build_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            artifact_results=[
                _artifact_row(tmp_path, "pre_open_bundle_v1", role_class="REQUIRED_BINDING_INPUT"),
                _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
            ],
            source_refs=[],
        ),
    )
    admission_ref = write_target_day_admission_v1(
        truth_root=tmp_path,
        payload=derive_target_day_admission_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            build_ref=build_ref,
        ),
    )
    prior_build_ref = write_target_day_build_v1(
        truth_root=tmp_path,
        payload=derive_target_day_build_payload_v1(
            truth_root=tmp_path,
            target_day="2026-04-09",
            artifact_results=[
                _artifact_row(
                    tmp_path,
                    "trading_day_state_machine_v1",
                    role_class="REQUIRED_EXECUTION_BOUNDARY",
                    target_day_expected="2026-04-09",
                    target_day_observed="2026-04-09",
                )
            ],
            source_refs=[],
        ),
    )
    prior_admission_ref = write_target_day_admission_v1(
        truth_root=tmp_path,
        payload=derive_target_day_admission_payload_v1(
            truth_root=tmp_path,
            target_day="2026-04-09",
            build_ref=prior_build_ref,
        ),
    )
    prior_active_ref = write_active_session_v1(
        truth_root=tmp_path,
        payload=derive_active_session_payload_v1(
            truth_root=tmp_path,
            target_day="2026-04-09",
            admission_ref=prior_admission_ref,
        ),
    )
    promoted_ref = write_session_promotion_decision_v1(
        truth_root=tmp_path,
        payload=derive_session_promotion_decision_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            pre_open_bundle_ref=pre_open_ref,
            target_day_admission_ref=admission_ref,
            prior_active_session_ref=prior_active_ref,
            owner_tool="ops/tools/run_session_authority_v1.py",
        ),
    )
    active_payload = derive_active_session_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        admission_ref=admission_ref,
        prior_active_session_ref=prior_active_ref,
        promotion_ref=promoted_ref,
    )
    assert promoted_ref.payload["promotion_state"] == PROMOTION_STATE_PROMOTED
    assert active_payload["active_day"] == DAY
    assert active_payload["promotion_state"] == PROMOTION_STATE_PROMOTED


def test_active_session_withheld_when_promotion_is_blocked(tmp_path: Path) -> None:
    admitted_build_ref = write_target_day_build_v1(
        truth_root=tmp_path,
        payload=derive_target_day_build_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            artifact_results=[
                _artifact_row(tmp_path, "pre_open_bundle_v1", role_class="REQUIRED_BINDING_INPUT"),
                _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
            ],
            source_refs=[],
        ),
    )
    admitted_admission_ref = write_target_day_admission_v1(
        truth_root=tmp_path,
        payload=derive_target_day_admission_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            build_ref=admitted_build_ref,
        ),
    )
    prior_build_ref = write_target_day_build_v1(
        truth_root=tmp_path,
        payload=derive_target_day_build_payload_v1(
            truth_root=tmp_path,
            target_day="2026-04-09",
            artifact_results=[
                _artifact_row(
                    tmp_path,
                    "trading_day_state_machine_v1",
                    role_class="REQUIRED_EXECUTION_BOUNDARY",
                    target_day_expected="2026-04-09",
                    target_day_observed="2026-04-09",
                )
            ],
            source_refs=[],
        ),
    )
    prior_admission_ref = write_target_day_admission_v1(
        truth_root=tmp_path,
        payload=derive_target_day_admission_payload_v1(
            truth_root=tmp_path,
            target_day="2026-04-09",
            build_ref=prior_build_ref,
        ),
    )
    prior_active_ref = write_active_session_v1(
        truth_root=tmp_path,
        payload=derive_active_session_payload_v1(
            truth_root=tmp_path,
            target_day="2026-04-09",
            admission_ref=prior_admission_ref,
        ),
    )
    blocked_promotion_ref = write_session_promotion_decision_v1(
        truth_root=tmp_path,
        payload={
            "schema_id": "session_promotion_decision",
            "schema_version": "v1",
            "decided_at_utc": "2026-04-16T12:00:00Z",
            "target_day": DAY,
            "owner_tool": "ops/tools/run_session_authority_v1.py",
            "rules_version": "session_promotion_gate_v1",
            "pre_open_bundle_ref": {"artifact_path": "bundle", "artifact_sha256": "c" * 64},
            "target_day_admission_ref": {"artifact_path": str(admitted_admission_ref.path), "artifact_sha256": admitted_admission_ref.sha256},
            "prior_current_state": {
                "active_session_ref": {"artifact_path": str(prior_active_ref.path), "artifact_sha256": prior_active_ref.sha256},
                "active_day": "2026-04-09",
                "target_day_admission_status": "ADMIT",
            },
            "pre_open_materialization_state": "BLOCKED",
            "pre_open_completion_state": "INCOMPLETE",
            "target_day_admission_status": "ADMIT",
            "promotion_state": "BLOCKED",
            "candidate_artifacts": [{"artifact_id": "active_session_v1", "artifact_path": str((tmp_path / 'active_session_v1' / 'current.json').resolve())}],
            "promoted_artifacts": [],
            "blocked_reason_codes": ["BROKER_EVENTS_MISSING"],
        },
    )
    blocked_active_payload = derive_active_session_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        admission_ref=admitted_admission_ref,
        prior_active_session_ref=prior_active_ref,
        promotion_ref=blocked_promotion_ref,
    )
    assert blocked_active_payload["promotion_state"] == PROMOTION_STATE_BLOCKED
    assert blocked_active_payload["active_day"] == "2026-04-09"
    assert blocked_active_payload["blocked_target_day"] == DAY
    assert blocked_active_payload["rollover_reason_code"] == "BROKER_EVENTS_MISSING"


def test_wrapper_consumes_session_authority_only() -> None:
    wrapper = Path("/home/node/constellation/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh").read_text(encoding="utf-8")
    assert 'DAY="$(TZ=America/New_York date +%F)"' not in wrapper
    assert "run_session_authority_v1.py" in wrapper
    assert "active_session_v1/current.json" in wrapper
    assert "ROLLOVER_REASON_CODE" in wrapper


def test_critical_path_sources_no_longer_hardcode_runtime_copy_root() -> None:
    critical_paths = [
        Path("/home/node/constellation/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh"),
        Path("/home/node/constellation/ops/tools/run_session_authority_v1.py"),
        Path("/home/node/constellation/ops/tools/run_session_readiness_refresh_v1.py"),
        Path("/home/node/constellation/ops/tools/run_startup_materialization_input_convergence_v1.py"),
        Path("/home/node/constellation/ops/tools/run_paper_day_control_plane_v1.py"),
    ]
    for path in critical_paths:
        text = path.read_text(encoding="utf-8")
        assert "/home/node/constellation_2_runtime" not in text, path
        assert ".venv_c2/bin/python" not in text, path


def test_primary_sleeve_capability_initialization_runs_core_gate_chain(monkeypatch, tmp_path: Path) -> None:
    sleeve_truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    positions_path = sleeve_truth_root / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v2.json"
    positions_path.parent.mkdir(parents=True, exist_ok=True)
    positions_path.write_text("{}", encoding="utf-8")
    calls: list[list[str]] = []

    def _fake_run(cmd, cwd=None, capture_output=None, text=None):  # noqa: ANN001
        calls.append(list(cmd))
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(
        "ops.tools.run_session_authority_v1._resolve_primary_binding",
        lambda environment, ib_account: SimpleNamespace(truth_root=sleeve_truth_root, sleeve_id="PRIMARY"),
    )
    monkeypatch.setattr("ops.tools.run_session_authority_v1._artifact_producer_git_sha", lambda: "a" * 40)
    monkeypatch.setattr("ops.tools.run_session_authority_v1.subprocess.run", _fake_run)

    _run_primary_sleeve_capability_initialization(
        target_day=DAY,
        environment="PAPER",
        ib_account="DUO847203",
    )

    scripts = [Path(cmd[1]).name for cmd in calls]
    assert scripts == [
        "run_accounting_nav_v2_day_v1.py",
        "run_allocation_day_v2.py",
        "run_broker_fact_spine_v1.py",
        "run_reconciliation_report_v3.py",
        "run_exit_reconciliation_day_v1.py",
        "run_c2_capital_risk_envelope_gate_v2.py",
        "run_liquidity_slippage_gate_v1.py",
        "run_operator_daily_gate_v3.py",
        "run_gate_stack_verdict_v1.py",
        "run_gate_authority_plane_v1.py",
    ]
    assert any(str(sleeve_truth_root) in cmd for call in calls for cmd in call)


def test_successful_broker_fact_spine_nested_run_is_not_recorded_as_failing(monkeypatch, tmp_path: Path) -> None:
    sleeve_truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    positions_path = sleeve_truth_root / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v2.json"
    positions_path.parent.mkdir(parents=True, exist_ok=True)
    positions_path.write_text("{}", encoding="utf-8")

    def _write_broker_fact_spine_audit() -> None:
        source_path = sleeve_truth_root / "broker_fact_spine_v1" / "raw_journal" / DAY / "source.jsonl"
        raw_journal_path = sleeve_truth_root / "broker_fact_spine_v1" / "raw_journal" / DAY / "broker_raw_evidence_envelope.v1.jsonl"
        health_path = sleeve_truth_root / "reports" / "broker_observation_health_v1" / DAY / "broker_observation_health.v1.json"
        trust_dependency_path = (
            sleeve_truth_root / "reports" / "broker_observation_trust_dependency_v1" / DAY / "broker_observation_trust_dependency.v1.json"
        )
        fact_root = sleeve_truth_root / "broker_fact_spine_v1" / "fact_ledger" / DAY
        fact_ledger_paths = {
            "observation_session_fact": fact_root / "observation_session_fact.v1.jsonl",
            "observed_order_fact": fact_root / "observed_order_fact.v1.jsonl",
            "observed_order_status_fact": fact_root / "observed_order_status_fact.v1.jsonl",
            "observed_fill_fact": fact_root / "observed_fill_fact.v1.jsonl",
            "observed_position_fact": fact_root / "observed_position_fact.v1.jsonl",
        }
        for path in [source_path, raw_journal_path, health_path, trust_dependency_path, *fact_ledger_paths.values()]:
            path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_text('{"event":"source"}\n', encoding="utf-8")
        raw_journal_path.write_text('{"observed_utc":"2026-04-10T00:00:00Z"}\n', encoding="utf-8")
        health_path.write_text("{}", encoding="utf-8")
        trust_dependency_path.write_text("{}", encoding="utf-8")
        for path in fact_ledger_paths.values():
            path.write_text("", encoding="utf-8")
        audit_payload = build_broker_fact_spine_audit_payload_v1(
            execution_root_path=sleeve_truth_root,
            day_utc=DAY,
            raw_journal_path=raw_journal_path,
            fact_ledger_paths=fact_ledger_paths,
            health_payload={
                "current_state": TRUST_TRUSTED,
                "downstream_consumption_posture": DOWNSTREAM_NORMAL,
                "summary": "ok",
                "counts": {},
            },
            health_path=health_path,
            trust_dependency_path=trust_dependency_path,
            source_path=source_path,
        )
        audit_path = sleeve_truth_root / "reports" / "broker_fact_spine_audit_v1" / DAY / "broker_fact_spine_audit.v1.json"
        _write_json(audit_path, audit_payload)

    def _fake_run(cmd, cwd=None, capture_output=None, text=None):  # noqa: ANN001
        script_name = Path(cmd[1]).name
        if script_name == "run_broker_fact_spine_v1.py":
            _write_broker_fact_spine_audit()
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(
        "ops.tools.run_session_authority_v1._resolve_primary_binding",
        lambda environment, ib_account: SimpleNamespace(truth_root=sleeve_truth_root, sleeve_id="PRIMARY"),
    )
    monkeypatch.setattr("ops.tools.run_session_authority_v1._artifact_producer_git_sha", lambda: "a" * 40)
    monkeypatch.setattr("ops.tools.run_session_authority_v1.subprocess.run", _fake_run)

    nested_refs = _run_primary_sleeve_capability_initialization(
        target_day=DAY,
        environment="PAPER",
        ib_account="DUO847203",
    )

    broker_ref = next(ref for ref in nested_refs if str(ref.get("script") or "").endswith("run_broker_fact_spine_v1.py"))
    assert broker_ref["return_code"] == 0
    assert broker_ref["validation_status"] == "VALID"
    assert broker_ref["produced_artifacts"][0]["artifact_id"] == "broker_fact_spine_audit_v1"
    assert broker_ref["produced_artifacts"][0]["exists"] is True
    assert broker_ref["produced_artifacts"][0]["validation_status"] == "VALID"

    result = _compute_hidden_dependency_check_result(
        artifact_results=[_artifact_row(tmp_path, "paper_policy_verdict_v1")],
        source_refs=nested_refs
        + [
            {
                "script": "ops/tools/run_submit_boundary_status_v1.py",
                "return_code": 2,
                "required_for_closure": True,
            }
        ],
    )
    assert "ops/tools/run_broker_fact_spine_v1.py" not in result["failing_producers"]
    assert result["failing_producers"] == ["ops/tools/run_submit_boundary_status_v1.py"]


def test_nested_producer_failure_surfaces_in_source_refs_and_hidden_dependency_result(
    monkeypatch,
    tmp_path: Path,
) -> None:
    sleeve_truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    positions_path = sleeve_truth_root / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v2.json"
    positions_path.parent.mkdir(parents=True, exist_ok=True)
    positions_path.write_text("{}", encoding="utf-8")

    def _fake_run(cmd, cwd=None, capture_output=None, text=None):  # noqa: ANN001
        script_name = Path(cmd[1]).name
        if script_name == "run_accounting_nav_v2_day_v1.py":
            return SimpleNamespace(returncode=7, stdout="", stderr="NAV_BOOTSTRAP_FAIL")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(
        "ops.tools.run_session_authority_v1._resolve_primary_binding",
        lambda environment, ib_account: SimpleNamespace(truth_root=sleeve_truth_root, sleeve_id="PRIMARY"),
    )
    monkeypatch.setattr("ops.tools.run_session_authority_v1._artifact_producer_git_sha", lambda: "a" * 40)
    monkeypatch.setattr("ops.tools.run_session_authority_v1.subprocess.run", _fake_run)

    nested_refs = _run_primary_sleeve_capability_initialization(
        target_day=DAY,
        environment="PAPER",
        ib_account="DUO847203",
    )

    failing_ref = next(
        ref for ref in nested_refs if str(ref.get("script") or "").endswith("run_accounting_nav_v2_day_v1.py")
    )
    assert failing_ref["return_code"] == 7
    assert failing_ref["required_for_closure"] is False
    assert failing_ref["validation_status"] == "MISSING"
    assert failing_ref["error_detail"] == "NAV_BOOTSTRAP_FAIL"
    assert failing_ref["produced_artifacts"] == [
        {
            "artifact_id": "accounting_nav_v2",
            "artifact_path": str((sleeve_truth_root / "accounting_v2" / "nav" / DAY / "nav.v2.json").resolve()),
            "schema_ref": "governance/04_DATA/SCHEMAS/C2/ACCOUNTING/accounting_nav.v2.schema.json",
            "exists": False,
            "artifact_sha256": "",
            "validation_status": "MISSING",
            "error_detail": "",
        }
    ]

    result = _compute_hidden_dependency_check_result(
        artifact_results=[_artifact_row(tmp_path, "paper_policy_verdict_v1")],
        source_refs=nested_refs
        + [
            {
                "script": "ops/tools/run_submit_boundary_status_v1.py",
                "return_code": 2,
                "required_for_closure": True,
            }
        ],
    )

    assert result["status"] == "PASS"
    assert result["blocking_reason_code"] == ""
    assert result["failing_producers"][0] == "ops/tools/run_accounting_nav_v2_day_v1.py"
    assert result["failing_producers"][-1] == "ops/tools/run_submit_boundary_status_v1.py"
    assert "producer_return_code_nonzero" not in result["summary"]
    assert (
        result["summary"]
        == "failing_producers=ops/tools/run_accounting_nav_v2_day_v1.py,ops/tools/run_submit_boundary_status_v1.py"
    )


def test_previous_day_economic_dependencies_declared_do_not_trigger_hidden_dependency_failure(tmp_path: Path) -> None:
    result = _compute_hidden_dependency_check_result(
        artifact_results=[
            _artifact_row(
                tmp_path,
                "trade_submit_readiness_c2_v1",
                observed_dependency_artifacts=[
                    "capability_state_v1",
                    "economic_state_build_v1",
                    "economic_state_package_v1",
                    "paper_policy_verdict_v1",
                ],
            ),
            _artifact_row(tmp_path, "capability_state_v1"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(
                tmp_path,
                "economic_state_build_v1",
                required=False,
                target_day_expected="2026-04-09",
                target_day_observed="2026-04-09",
            ),
            _artifact_row(
                tmp_path,
                "economic_state_package_v1",
                required=False,
                target_day_expected="2026-04-09",
                target_day_observed="2026-04-09",
            ),
        ],
        source_refs=[],
    )
    assert result["status"] == "PASS"
    assert result["undeclared_dependency_artifacts"] == []
    assert "contract_covered=economic_state_build_v1,economic_state_package_v1" in result["summary"]


def test_active_session_traceability_includes_build_and_admission_refs(tmp_path: Path) -> None:
    build_ref = write_target_day_build_v1(
        truth_root=tmp_path,
        payload=derive_target_day_build_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            artifact_results=[_artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY")],
            source_refs=[],
        ),
    )
    admission_ref = write_target_day_admission_v1(
        truth_root=tmp_path,
        payload=derive_target_day_admission_payload_v1(truth_root=tmp_path, target_day=DAY, build_ref=build_ref),
    )
    active_payload = derive_active_session_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        admission_ref=admission_ref,
    )
    assert active_payload["target_day_admission_ref"] == str(admission_ref.path)
    assert active_payload["target_day_build_ref"]["artifact_path"] == str(build_ref.path)
    assert active_payload["active_day_build_ref"]["artifact_path"] == str(build_ref.path)


def test_capability_artifact_status_accepts_advisory_failures_when_blocking_capabilities_exist() -> None:
    payload = {
        "overall_status": "FAIL",
        "capabilities": [
            {"capability_id": "account_binding_valid", "status": "PASS"},
            {"capability_id": "startup_materialization_ready", "status": "PASS"},
            {"capability_id": "paper_trading_posture_ready", "status": "PASS"},
            {"capability_id": "broker_connectivity_available", "status": "PASS"},
            {"capability_id": "startup_authorization_gate_set_ready", "status": "PASS"},
            {"capability_id": "core_sleeve_gate_set_ready", "status": "FAIL"},
            {"capability_id": "economic_health_gate_set_complete", "status": "FAIL"},
        ],
    }
    observed_status, is_pass, blocker_codes = _capability_artifact_status(payload)
    assert observed_status == "FAIL"
    assert is_pass is True
    assert blocker_codes == []


def test_normalize_scoped_dependency_type_maps_authorization_verdict() -> None:
    assert _normalize_scoped_dependency_type("authorization_gate_verdict_v1_scoped:PRIMARY") == (
        "primary_scoped_authorization_gate_verdict_v1"
    )


def test_extract_timestamp_accepts_generated_at_utc() -> None:
    payload = {"day_utc": DAY, "generated_at_utc": f"{DAY}T00:00:00Z"}
    assert _extract_timestamp(payload) == f"{DAY}T00:00:00Z"
    assert _freshness_status(
        payload=payload,
        target_day=DAY,
        observed_day=DAY,
        freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
    ) == "CURRENT"


def test_session_readiness_refresh_startup_ready_requires_authority_open_primary_scope() -> None:
    assert _session_readiness_refresh_startup_ready(
        {
            "stdout": (
                '{"results":{"authority_kernel_validation":{"validation_summary":{"validation_state":"PASS"}},'
                '"scope_summary":{"primary_ready":true},"day_authority_decision":{"decision_state":"OPEN"}}}'
            )
        }
    )
    assert not _session_readiness_refresh_startup_ready(
        {
            "stdout": (
                '{"results":{"authority_kernel_validation":{"validation_summary":{"validation_state":"PASS"}},'
                '"scope_summary":{"primary_ready":false},"day_authority_decision":{"decision_state":"OPEN"}}}'
            )
        }
    )


def test_build_and_admission_attempt_history_is_immutable_and_day_file_tracks_latest(tmp_path: Path) -> None:
    first_build_payload = derive_target_day_build_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        artifact_results=[
            _artifact_row(
                tmp_path,
                "trade_submit_readiness_c2_v1",
                freshness_status="STALE",
                blocking_reason_code="STALE_ARTIFACT",
                closure_status="OPEN",
            )
        ],
        source_refs=[],
    )
    with patch(
        "constellation_2.common.session_authority_v1.build_attempt_id_v1",
        side_effect=[
            "BUILD_ATTEMPT_BLOCKED",
            "ADMISSION_ATTEMPT_BLOCKED",
            "BUILD_ATTEMPT_READY",
            "ADMISSION_ATTEMPT_READY",
        ],
    ):
        first_build_ref = write_target_day_build_v1(truth_root=tmp_path, payload=first_build_payload)
        first_admission_payload = derive_target_day_admission_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            build_ref=first_build_ref,
        )
        first_admission_ref = write_target_day_admission_v1(truth_root=tmp_path, payload=first_admission_payload)

        second_build_payload = derive_target_day_build_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            artifact_results=[
                _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
                _artifact_row(tmp_path, "paper_policy_verdict_v1"),
                _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
                _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
            ],
            source_refs=[],
        )
        second_build_ref = write_target_day_build_v1(truth_root=tmp_path, payload=second_build_payload)
        second_admission_payload = derive_target_day_admission_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            build_ref=second_build_ref,
        )
        second_admission_ref = write_target_day_admission_v1(truth_root=tmp_path, payload=second_admission_payload)

    build_attempts = sorted((tmp_path / "target_day_build_v1" / DAY).glob("*/target_day_build.v1.json"))
    admission_attempts = sorted((tmp_path / "target_day_admission_v1" / DAY).glob("*/target_day_admission.v1.json"))

    assert len(build_attempts) == 2
    assert len(admission_attempts) == 2
    assert resolve_target_day_build_attempt_path(
        truth_root=tmp_path,
        target_day=DAY,
        attempt_id=build_attempts[0].parent.name,
    ) in build_attempts
    assert resolve_target_day_admission_attempt_path(
        truth_root=tmp_path,
        target_day=DAY,
        attempt_id=admission_attempts[0].parent.name,
    ) in admission_attempts

    assert any('"build_status":"BLOCKED"' in path.read_text(encoding="utf-8") for path in build_attempts)
    assert any('"build_status":"COMPLETE"' in path.read_text(encoding="utf-8") for path in build_attempts)
    assert any('"admission_status":"BLOCKED"' in path.read_text(encoding="utf-8") for path in admission_attempts)
    assert any('"admission_status":"ADMIT"' in path.read_text(encoding="utf-8") for path in admission_attempts)
    assert '"build_status":"COMPLETE"' in second_build_ref.path.read_text(encoding="utf-8")
    assert '"admission_status":"ADMIT"' in second_admission_ref.path.read_text(encoding="utf-8")
    assert second_build_ref.path.read_text(encoding="utf-8") == (
        tmp_path / "target_day_build_v1" / DAY / "BUILD_ATTEMPT_READY" / "target_day_build.v1.json"
    ).read_text(encoding="utf-8")
    assert second_admission_ref.path.read_text(encoding="utf-8") == (
        tmp_path / "target_day_admission_v1" / DAY / "ADMISSION_ATTEMPT_READY" / "target_day_admission.v1.json"
    ).read_text(encoding="utf-8")
    assert any('"build_status":"BLOCKED"' in path.read_text(encoding="utf-8") for path in build_attempts)
    assert any('"admission_status":"BLOCKED"' in path.read_text(encoding="utf-8") for path in admission_attempts)


def test_extract_timestamp_uses_pre_open_built_at_utc() -> None:
    assert _extract_timestamp({"built_at_utc": f"{DAY}T12:34:56Z"}) == f"{DAY}T12:34:56Z"
    assert _freshness_status(
        payload={"built_at_utc": f"{DAY}T12:34:56Z"},
        target_day=DAY,
        observed_day=DAY,
        freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
    ) == "CURRENT"


def test_trade_submit_path_uses_governed_sleeve_truth_root(tmp_path: Path, monkeypatch) -> None:
    canonical_truth = tmp_path / "truth"
    sleeve_truth = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    monkeypatch.setattr(
        "ops.tools.run_session_authority_v1.resolve_governed_sleeve_truth_bindings",
        lambda **kwargs: [SimpleNamespace(truth_root=sleeve_truth)],
    )

    resolved = _trade_submit_path(
        truth_root=canonical_truth,
        target_day=DAY,
        environment="PAPER",
        ib_account="DUO123456",
    )

    assert resolved == (
        sleeve_truth
        / "trade_submit_readiness_c2_v1"
        / "_history"
        / "PAPER"
        / "DUO123456"
        / DAY
        / "status.json"
    ).resolve()
