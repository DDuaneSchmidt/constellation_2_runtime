from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.constitutional_runtime_v1 import (  # noqa: E402
    ConstitutionalRuntimeError,
    FINALITY_CORRECTED,
    FINALITY_FINALIZED,
    FINALITY_SUPERSEDED,
    ARTIFACT_CLASS_ADMISSION_RESULT,
    ARTIFACT_CLASS_COMPILED_STATE,
    ARTIFACT_CLASS_READ_MODEL,
    assert_constitutional_completeness_v1,
    build_governed_artifact_lineage_v1,
    load_constitutional_artifact_authority_registry_v1,
    validate_governed_artifact_payload_v1,
    validate_artifact_contract_rows_v1,
)
from constellation_2.common import execution_journal_v1 as journal  # noqa: E402
from constellation_2.common import performance_projection_v1 as performance_projection  # noqa: E402
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1  # noqa: E402


def _row(artifact_id: str, artifact_class: str, deps: list[str]) -> dict[str, object]:
    return {
        "artifact_id": artifact_id,
        "artifact_class": artifact_class,
        "authoritative_writer": f"writer.{artifact_id}",
        "authoritative_domain": "authority",
        "authoritative_root_type": "canonical_truth_root",
        "schema_relpath": "governance/04_DATA/SCHEMAS/C2/REPORTS/placeholder.schema.json",
        "root_policy": "canonical_only",
        "authoritative_path_pattern": f"/tmp/{artifact_id}.json",
        "legal_consumers": ["consumer"],
        "required_upstream_dependencies": deps,
        "initial_finality_state": "finalized",
        "allowed_finality_states": ["finalized", "corrected", "superseded", "archived"],
        "append_only_audit": True,
        "immutable_policy_snapshot_required": False,
        "frozen_input_bundle_required": False,
        "dependency_law": "declared_downward_only",
        "materialization_policy": {
            "writer_policy": "produce_only",
            "default_consumer_policy": "must_block_if_missing",
            "allowed_materializers": [f"writer.{artifact_id}"],
        },
    }


def test_constitutional_registry_loads_real_active_contracts() -> None:
    registry = load_constitutional_artifact_authority_registry_v1(SOURCE_ROOT)
    artifact_ids = {
        str(row.get("artifact_id") or "").strip()
        for row in registry.get("artifacts") or []
    }
    assert "economic_state_build_v1" in artifact_ids
    assert "execution_build_v1" in artifact_ids
    assert "execution_outcome_v1" in artifact_ids
    assert "operator_daily_gate_v3" in artifact_ids
    assert "session_authority_status_v1" in artifact_ids
    assert "submit_boundary_status_v1" in artifact_ids


def test_illegal_upward_dependency_is_rejected() -> None:
    rows = [
        _row("compiled_a", ARTIFACT_CLASS_COMPILED_STATE, ["admission_a"]),
        _row("admission_a", ARTIFACT_CLASS_ADMISSION_RESULT, []),
    ]
    with pytest.raises(ConstitutionalRuntimeError, match="illegal upward dependency"):
        validate_artifact_contract_rows_v1(rows)


def test_duplicate_artifact_writer_registration_is_rejected() -> None:
    rows = [
        _row("same_id", ARTIFACT_CLASS_COMPILED_STATE, []),
        _row("same_id", ARTIFACT_CLASS_COMPILED_STATE, []),
    ]
    with pytest.raises(ConstitutionalRuntimeError, match="duplicate artifact authority contract"):
        validate_artifact_contract_rows_v1(rows)


def test_read_model_cannot_register_as_canonical_writer() -> None:
    row = _row("ui_projection", ARTIFACT_CLASS_READ_MODEL, [])
    with pytest.raises(ConstitutionalRuntimeError, match="read_model cannot register as canonical writer"):
        validate_artifact_contract_rows_v1([row])


def test_corrected_and_superseded_lineage_requires_explicit_refs() -> None:
    ref = {
        "artifact_id": "positions_snapshot_v5",
        "path": "/tmp/positions_snapshot.v5.json",
        "sha256": "a" * 64,
        "artifact_class": "compiled_state",
        "finality_state": FINALITY_FINALIZED,
    }
    corrected = build_governed_artifact_lineage_v1(
        artifact_type="economic_state_build_v1",
        artifact_version="v1",
        artifact_class="outcome_record",
        authority_id="economic_state_build_v1",
        producer_id="test",
        generated_at_utc="2026-04-17T00:00:00Z",
        effective_at_utc="2026-04-17T00:00:00Z",
        finality_state=FINALITY_CORRECTED,
        input_artifact_refs=[ref],
        policy_snapshot_refs=[],
        code_version="deadbeef",
        run_id="run-1",
        corrected_from_ref=ref,
    )
    assert corrected["corrected_from_ref"]["artifact_id"] == "positions_snapshot_v5"

    superseded = build_governed_artifact_lineage_v1(
        artifact_type="execution_build_v1",
        artifact_version="v1",
        artifact_class="decision_proposal",
        authority_id="execution_build_v1",
        producer_id="test",
        generated_at_utc="2026-04-17T00:00:00Z",
        effective_at_utc="2026-04-17T00:00:00Z",
        finality_state=FINALITY_SUPERSEDED,
        input_artifact_refs=[ref],
        policy_snapshot_refs=[],
        code_version="deadbeef",
        run_id="run-2",
        supersedes_ref=ref,
    )
    assert superseded["supersedes_ref"]["artifact_id"] == "positions_snapshot_v5"

    with pytest.raises(ConstitutionalRuntimeError, match="corrected lineage requires corrected_from_ref"):
        build_governed_artifact_lineage_v1(
            artifact_type="economic_state_build_v1",
            artifact_version="v1",
            artifact_class="outcome_record",
            authority_id="economic_state_build_v1",
            producer_id="test",
            generated_at_utc="2026-04-17T00:00:00Z",
            effective_at_utc="2026-04-17T00:00:00Z",
            finality_state=FINALITY_CORRECTED,
            input_artifact_refs=[ref],
            policy_snapshot_refs=[],
            code_version="deadbeef",
            run_id="run-3",
        )


def test_validate_governed_artifact_payload_accepts_adopted_performance_projection(tmp_path: Path) -> None:
    identity = {
        "day_utc": "2026-04-17",
        "day_attempt_id": "day_attempt:2026-04-17:A001",
        "pipeline_run_id": "pipeline_run:2026-04-17:R001",
        "release_id": "release-001",
        "git_sha": "a" * 40,
    }
    event = journal.build_event_record_v1(
        **identity,
        event_seq=1,
        event_type="STAGE_DURATION_RECORDED",
        event_source="paper_session_startup_flow_v1",
        generated_at_utc="2026-04-17T13:00:02Z",
        status="RECORDED",
        payload={
            "source_artifact_path": "/tmp/ledger.json",
            "source_artifact_sha256": "b" * 64,
            "source_generated_at_utc": "2026-04-17T13:00:02Z",
            "stage_name": "STARTUP_MATERIALIZATION_TO_LEDGER_ELAPSED",
            "started_at_utc": "2026-04-17T13:00:00Z",
            "ended_at_utc": "2026-04-17T13:00:02Z",
            "duration_ms": 2000,
        },
    )
    journal_payload = journal.build_execution_journal_payload_v1(
        **identity,
        generated_at_utc="2026-04-17T13:00:02Z",
        events=[event],
        producer_module="ops/tools/run_execution_journal_v1.py",
    )
    journal_path = tmp_path / "execution_journal.v1.json"
    journal_path.write_text(
        json.dumps(journal_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    journal_sha = canonical_hash_for_c2_artifact_v1(journal_payload)
    payload = performance_projection.build_performance_projection_v1(
        journal_payload=journal_payload,
        journal_ref=str(journal_path),
        journal_sha256=journal_sha,
        journal_generated_at_utc="2026-04-17T13:00:02Z",
        generated_at_utc="2026-04-17T13:01:00Z",
        producer_module="ops/tools/run_performance_projection_v1.py",
    )

    validated = validate_governed_artifact_payload_v1(
        repo_root=SOURCE_ROOT,
        artifact_id="performance_projection_v1",
        payload=payload,
    )

    assert validated["constitutional_lineage"]["artifact_type"] == "performance_projection_v1"
    assert validated["constitutional_dependency_declaration"]["declared_dependency_artifacts"] == [
        "execution_journal_v1"
    ]


def test_constitutional_completeness_rejects_wrong_writer_on_adopted_artifact(tmp_path: Path) -> None:
    payload = {
        "schema_id": "submit_boundary_status",
        "schema_version": "v1",
        "authority_scope": "NON_AUTHORITY_FACT",
        "day_utc": "2026-04-17",
        "session_id": "paper_session:2026-04-17:PAPER",
        "submission_authorized": False,
        "boundary_status": "BLOCKED",
        "required_boundary_checks": [],
        "failed_checks": [],
        "blocking_codes": ["BLOCKED"],
        "closure_state": "BLOCKED",
        "first_blocker_code": "BLOCKED",
        "missing_dependency_artifacts": [],
        "constitutional_dependency_declaration": {
            "schema_id": "artifact_dependency_declaration",
            "schema_version": "v1",
            "artifact_type": "submit_boundary_status_v1",
            "artifact_class": "admission_result",
            "authority_id": "submit_boundary_status_v1",
            "declared_dependency_artifacts": ["target_day_build_v1", "trade_submit_readiness_c2_v1"],
            "dependency_refs": [
                    {
                        "artifact_id": "target_day_build_v1",
                        "path": str((tmp_path / "build.json").resolve()),
                        "sha256": canonical_hash_for_c2_artifact_v1({"artifact": "build.json"}),
                        "artifact_class": "admission_result",
                        "finality_state": "provisional",
                    },
                    {
                        "artifact_id": "trade_submit_readiness_c2_v1",
                        "path": str((tmp_path / "readiness.json").resolve()),
                        "sha256": canonical_hash_for_c2_artifact_v1({"artifact": "readiness.json"}),
                    "artifact_class": "admission_result",
                    "finality_state": "provisional",
                },
            ],
        },
        "constitutional_lineage": {
            "schema_id": "governed_artifact_lineage",
            "schema_version": "v1",
            "artifact_type": "submit_boundary_status_v1",
            "artifact_version": "v1",
            "artifact_class": "admission_result",
            "authority_id": "submit_boundary_status_v1",
            "producer_id": "wrong.writer",
            "generated_at_utc": "2026-04-17T00:00:00Z",
            "effective_at_utc": "2026-04-17T00:00:00Z",
            "finality_state": "provisional",
            "input_artifact_refs": [
                    {
                        "artifact_id": "target_day_build_v1",
                        "path": str((tmp_path / "build.json").resolve()),
                        "sha256": canonical_hash_for_c2_artifact_v1({"artifact": "build.json"}),
                    "artifact_class": "admission_result",
                    "finality_state": "provisional",
                },
                    {
                        "artifact_id": "trade_submit_readiness_c2_v1",
                        "path": str((tmp_path / "readiness.json").resolve()),
                        "sha256": canonical_hash_for_c2_artifact_v1({"artifact": "readiness.json"}),
                    "artifact_class": "admission_result",
                    "finality_state": "provisional",
                },
            ],
            "policy_snapshot_refs": [],
            "code_version": "deadbeef",
            "run_id": "run-1",
            "corrected_from_ref": None,
            "supersedes_ref": None,
        },
        "producer": {"repo": "constellation", "module": "ops/tools/run_submit_boundary_status_v1.py", "git_sha": "a" * 40},
        "produced_at_utc": "2026-04-17T00:00:00Z",
        "freshness_verdict": "CURRENT",
        "linkage_verdict": "LINKED",
        "paper_account": "DUO847203",
    }
    for artifact_name in ("build.json", "readiness.json"):
        path = tmp_path / artifact_name
        path.write_text(
            json.dumps({"artifact": artifact_name}, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConstitutionalRuntimeError, match="CONSTITUTIONAL_ARTIFACT_WRITER_MISMATCH"):
        assert_constitutional_completeness_v1(
            repo_root=SOURCE_ROOT,
            consumer_id="session_authority_v1",
            required_artifacts=[
                {
                    "artifact_id": "submit_boundary_status_v1",
                    "path": str(payload_path.resolve()),
                    "sha256": canonical_hash_for_c2_artifact_v1(payload),
                    "required_finality_states": ["provisional", "finalized", "corrected"],
                }
            ],
        )


def test_validate_governed_payload_allows_blocked_missing_dependencies_when_declared_explicitly(tmp_path: Path) -> None:
    build_path = tmp_path / "build.json"
    build_payload = {"artifact": "build"}
    build_path.write_text(
        json.dumps(build_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    payload = {
        "schema_id": "submit_boundary_status",
        "schema_version": "v1",
        "authority_scope": "NON_AUTHORITY_FACT",
        "day_utc": "2026-04-17",
        "session_id": "paper_session:2026-04-17:PAPER",
        "submission_authorized": False,
        "boundary_status": "BLOCKED",
        "required_boundary_checks": [],
        "failed_checks": [],
        "blocking_codes": ["READINESS_MISSING"],
        "closure_state": "BLOCKED",
        "first_blocker_code": "READINESS_MISSING",
        "missing_dependency_artifacts": ["trade_submit_readiness_c2_v1"],
        "constitutional_dependency_declaration": {
            "schema_id": "artifact_dependency_declaration",
            "schema_version": "v1",
            "artifact_type": "submit_boundary_status_v1",
            "artifact_class": "admission_result",
            "authority_id": "submit_boundary_status_v1",
            "declared_dependency_artifacts": ["target_day_build_v1", "trade_submit_readiness_c2_v1"],
            "dependency_refs": [
                {
                    "artifact_id": "target_day_build_v1",
                    "path": str(build_path.resolve()),
                    "sha256": canonical_hash_for_c2_artifact_v1(build_payload),
                    "artifact_class": "admission_result",
                    "finality_state": "provisional",
                }
            ],
        },
        "constitutional_lineage": {
            "schema_id": "governed_artifact_lineage",
            "schema_version": "v1",
            "artifact_type": "submit_boundary_status_v1",
            "artifact_version": "v1",
            "artifact_class": "admission_result",
            "authority_id": "submit_boundary_status_v1",
            "producer_id": "ops/tools/run_submit_boundary_status_v1.py",
            "generated_at_utc": "2026-04-17T00:00:00Z",
            "effective_at_utc": "2026-04-17T00:00:00Z",
            "finality_state": "provisional",
            "input_artifact_refs": [
                {
                    "artifact_id": "target_day_build_v1",
                    "path": str(build_path.resolve()),
                    "sha256": canonical_hash_for_c2_artifact_v1(build_payload),
                    "artifact_class": "admission_result",
                    "finality_state": "provisional",
                }
            ],
            "policy_snapshot_refs": [],
            "code_version": "abc1234",
            "run_id": "submit-boundary:test",
            "corrected_from_ref": None,
            "supersedes_ref": None,
        },
        "producer": {"repo": "constellation", "module": "test", "git_sha": "abc1234"},
        "produced_at_utc": "2026-04-17T00:00:00Z",
        "freshness_verdict": "CURRENT",
        "linkage_verdict": "LINKED",
        "paper_account": "DUO847203",
    }

    validated = validate_governed_artifact_payload_v1(
        repo_root=SOURCE_ROOT,
        artifact_id="submit_boundary_status_v1",
        payload=payload,
    )

    assert validated["constitutional_dependency_declaration"]["declared_dependency_artifacts"] == [
        "target_day_build_v1",
        "trade_submit_readiness_c2_v1",
    ]
