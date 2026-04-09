from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common import execution_journal_v1 as journal
from constellation_2.common.paper_session_fact_plane_v1 import SurfaceRefV1
import ops.tools.run_execution_journal_v1 as journal_tool


def _base_identity() -> dict[str, str]:
    return {
        "day_utc": "2026-04-08",
        "day_attempt_id": "day_attempt:2026-04-08:A001",
        "pipeline_run_id": "pipeline_run:2026-04-08:R001",
        "release_id": "release-001",
        "git_sha": "a" * 40,
    }


def _source_payload() -> dict[str, object]:
    return {
        "source_artifact_path": "/tmp/source.json",
        "source_artifact_sha256": "b" * 64,
        "source_generated_at_utc": "2026-04-08T13:00:00Z",
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def test_execution_journal_append_success(tmp_path: Path) -> None:
    ref = journal.append_execution_event_v1(
        truth_root=tmp_path,
        producer_module="test.module",
        event_type="DEPLOYMENT_ACTIVATED",
        event_source="deployment_state_machine_v1",
        status="DEPLOY_ACTIVE",
        payload={
            **_source_payload(),
            "final_deployment_decision": "DEPLOY_ACTIVE",
            "blocking_codes": [],
            "first_true_blocker_code": "",
        },
        **_base_identity(),
    )
    assert ref.payload["last_event_seq"] == 1
    assert ref.payload["events"][0]["event_type"] == "DEPLOYMENT_ACTIVATED"


def test_duplicate_sequence_rejection() -> None:
    identity = _base_identity()
    event = journal.build_event_record_v1(
        **identity,
        event_seq=1,
        event_type="DEPLOYMENT_ACTIVATED",
        event_source="deployment_state_machine_v1",
        generated_at_utc="2026-04-08T13:00:00Z",
        status="DEPLOY_ACTIVE",
        payload={
            **_source_payload(),
            "final_deployment_decision": "DEPLOY_ACTIVE",
            "blocking_codes": [],
            "first_true_blocker_code": "",
        },
    )
    payload = {
        "schema_id": "execution_journal",
        "schema_version": "v1",
        "authority_scope": "CANONICAL_EXECUTION_JOURNAL",
        **identity,
        "journal_id": journal.execution_journal_id_v1(**identity),
        "generated_at_utc": "2026-04-08T13:00:00Z",
        "last_event_seq": 2,
        "events": [event, dict(event)],
        "producer": {"repo": "constellation", "module": "test.module", "git_sha": identity["git_sha"]},
    }
    payload["events"][1]["event_seq"] = 1
    with pytest.raises(ValueError, match="DUPLICATE_EVENT_SEQ|SEQ_GAP_OR_DUPLICATE"):
        journal.validate_execution_journal_payload_v1(payload)


def test_cross_identity_contamination_rejection(tmp_path: Path) -> None:
    journal.append_execution_event_v1(
        truth_root=tmp_path,
        producer_module="test.module",
        event_type="DEPLOYMENT_ACTIVATED",
        event_source="deployment_state_machine_v1",
        status="DEPLOY_ACTIVE",
        payload={
            **_source_payload(),
            "final_deployment_decision": "DEPLOY_ACTIVE",
            "blocking_codes": [],
            "first_true_blocker_code": "",
        },
        **_base_identity(),
    )
    with pytest.raises(ValueError, match="CROSS_IDENTITY_CONTAMINATION"):
        journal.append_execution_event_v1(
            truth_root=tmp_path,
            producer_module="test.module",
            event_type="DEPLOYMENT_BLOCKED",
            event_source="deployment_state_machine_v1",
            status="DEPLOY_BLOCKED_VALID",
            payload={
                **_source_payload(),
                "final_deployment_decision": "DEPLOY_BLOCKED_VALID",
                "blocking_codes": ["AUTHORITATIVE_WORKTREE_DIRTY_BUILD_BLOCKED"],
                "first_true_blocker_code": "AUTHORITATIVE_WORKTREE_DIRTY_BUILD_BLOCKED",
            },
            **{**_base_identity(), "day_attempt_id": "day_attempt:2026-04-08:A002"},
        )


def test_invalid_event_type_rejection() -> None:
    with pytest.raises(ValueError, match="INVALID_EVENT_TYPE"):
        journal.build_event_record_v1(
            **_base_identity(),
            event_seq=1,
            event_type="NOT_A_REAL_EVENT",
            event_source="test",
            generated_at_utc="2026-04-08T13:00:00Z",
            status="NOPE",
            payload=_source_payload(),
        )


def test_invalid_source_for_event_type_rejection() -> None:
    with pytest.raises(ValueError, match="INVALID_EVENT_SOURCE"):
        journal.build_event_record_v1(
            **_base_identity(),
            event_seq=1,
            event_type="DEPLOYMENT_ACTIVATED",
            event_source="trading_day_state_machine_v1",
            generated_at_utc="2026-04-08T13:00:00Z",
            status="DEPLOY_ACTIVE",
            payload={
                **_source_payload(),
                "final_deployment_decision": "DEPLOY_ACTIVE",
                "blocking_codes": [],
                "first_true_blocker_code": "",
            },
        )


def test_invalid_payload_family_required_field_rejection() -> None:
    with pytest.raises(ValueError, match="PAYLOAD_FIELD_MISSING"):
        journal.build_event_record_v1(
            **_base_identity(),
            event_seq=1,
            event_type="LEDGER_AUTHORITY_RECORDED",
            event_source="paper_session_ledger_v1",
            generated_at_utc="2026-04-08T13:00:00Z",
            status="GRANTED",
            payload={
                **_source_payload(),
                "ledger_id": "ledger-001",
                "authority_status": "GRANTED",
                "system_ready": True,
                "blocking_codes": [],
            },
        )


def test_transitional_reconciler_backfills_missing_events_without_duplicate_source_events(tmp_path: Path) -> None:
    identity = _base_identity()
    deployment_path = tmp_path / "deployment.json"
    state_machine_path = tmp_path / "state_machine.json"
    startup_path = tmp_path / "startup.json"
    startup_proof_path = tmp_path / "startup_proof.json"
    ledger_path = tmp_path / "ledger.json"

    deployment_payload = {
        "day_utc": identity["day_utc"],
        "deployment_attempt_id": identity["pipeline_run_id"],
        "final_deployment_decision": "DEPLOY_ACTIVE",
        "blocking_codes": [],
        "first_true_blocker_code": "",
        "release_build": {"release_id": identity["release_id"]},
        "evaluated_at_utc": "2026-04-08T13:00:00Z",
    }
    trading_day_payload = {
        "day_utc": identity["day_utc"],
        "day_attempt_id": identity["day_attempt_id"],
        "state_machine_id": "state-001",
        "final_start_decision": "READY_NOW",
        "first_true_blocker": {
            "first_true_blocker_code": "",
            "first_true_blocker_artifact_path": "",
        },
        "evaluated_at_utc": "2026-04-08T13:00:04Z",
    }
    startup_payload = {
        "day_utc": identity["day_utc"],
        "status": "SUCCESS",
        "freshness_verdict": "CURRENT",
        "linkage_verdict": "LINKED",
        "blocking_codes": [],
        "produced_at_utc": "2026-04-08T13:00:01Z",
    }
    startup_proof_payload = {
        "day_utc": identity["day_utc"],
        "status": "STARTUP_READY",
        "ledger_authority_status": "GRANTED",
        "blocking_codes": [],
        "produced_at_utc": "2026-04-08T13:00:03Z",
    }
    ledger_payload = {
        "day_utc": identity["day_utc"],
        "ledger_id": "ledger-001",
        "control_state": {
            "authority_status": "GRANTED",
            "system_ready": True,
            "submission_authorized": True,
            "blocking_codes": [],
        },
        "evaluated_at_utc": "2026-04-08T13:00:02Z",
    }

    for path, payload in (
        (deployment_path, deployment_payload),
        (state_machine_path, trading_day_payload),
        (startup_path, startup_payload),
        (startup_proof_path, startup_proof_payload),
        (ledger_path, ledger_payload),
    ):
        _write_json(path, payload)

    journal.append_deployment_outcome_event_v1(
        truth_root=tmp_path,
        source_path=deployment_path,
        source_payload=deployment_payload,
        producer_module="test.module",
        **identity,
    )
    journal.append_state_machine_decision_event_v1(
        truth_root=tmp_path,
        identity=identity,
        source_path=state_machine_path,
        source_payload=trading_day_payload,
        producer_module="test.module",
    )

    with patch.object(journal_tool, "_load_deployment_payload", return_value=(deployment_path, deployment_payload)), patch.object(
        journal_tool,
        "_resolve_release_git_sha",
        return_value=identity["git_sha"],
    ), patch.object(
        journal_tool,
        "read_startup_materialization_ref_v1",
        return_value=SurfaceRefV1(path=startup_path, payload=startup_payload, sha256="d" * 64),
    ), patch.object(
        journal_tool,
        "read_startup_proof_validation_ref_v1",
        return_value=SurfaceRefV1(path=startup_proof_path, payload=startup_proof_payload, sha256="e" * 64),
    ), patch.object(
        journal_tool,
        "read_paper_session_ledger_ref_v1",
        return_value=SurfaceRefV1(path=ledger_path, payload=ledger_payload, sha256="f" * 64),
    ), patch.object(
        journal_tool,
        "read_trading_day_state_machine_ref_v1",
        return_value=SurfaceRefV1(path=state_machine_path, payload=trading_day_payload, sha256="1" * 64),
    ):
        rc = journal_tool.main(["--day_utc", identity["day_utc"], "--truth_root", str(tmp_path)])

    assert rc == 0
    journal_ref = journal.read_execution_journal_v1(truth_root=tmp_path, day_utc=identity["day_utc"])
    event_types = [row["event_type"] for row in journal_ref.payload["events"]]
    assert event_types.count("DEPLOYMENT_ACTIVATED") == 1
    assert event_types.count("STATE_MACHINE_DECISION_RECORDED") == 1
    assert "STARTUP_MATERIALIZATION_COMPLETED" in event_types
    assert "STARTUP_PROOF_VALIDATION_COMPLETED" in event_types
    assert "LEDGER_AUTHORITY_RECORDED" in event_types
    assert "SUBMISSION_AUTHORIZATION_RECORDED" in event_types


def test_transitional_reconciler_rebuilds_legacy_identity_mismatch_journal(tmp_path: Path) -> None:
    current_identity = _base_identity()
    legacy_identity = {
        **current_identity,
        "pipeline_run_id": "deployment_state_machine_attempt:2026-04-08:legacy",
    }
    deployment_path = tmp_path / "deployment.json"
    state_machine_path = tmp_path / "state_machine.json"
    startup_path = tmp_path / "startup.json"
    startup_proof_path = tmp_path / "startup_proof.json"
    ledger_path = tmp_path / "ledger.json"

    deployment_payload = {
        "day_utc": current_identity["day_utc"],
        "deployment_attempt_id": "deployment_state_machine_attempt:2026-04-08:current",
        "final_deployment_decision": "DEPLOY_ACTIVE",
        "blocking_codes": [],
        "first_true_blocker_code": "",
        "release_build": {"release_id": current_identity["release_id"]},
        "evaluated_at_utc": "2026-04-08T13:00:00Z",
    }
    trading_day_payload = {
        "day_utc": current_identity["day_utc"],
        "day_attempt_id": current_identity["day_attempt_id"],
        "state_machine_id": "state-001",
        "final_start_decision": "READY_NOW",
        "first_true_blocker": {
            "first_true_blocker_code": "",
            "first_true_blocker_artifact_path": "",
        },
        "evaluated_at_utc": "2026-04-08T13:00:04Z",
    }
    startup_payload = {
        "day_utc": current_identity["day_utc"],
        "status": "SUCCESS",
        "freshness_verdict": "CURRENT",
        "linkage_verdict": "LINKED",
        "blocking_codes": [],
        "produced_at_utc": "2026-04-08T13:00:01Z",
    }
    startup_proof_payload = {
        "day_utc": current_identity["day_utc"],
        "status": "STARTUP_READY",
        "ledger_authority_status": "GRANTED",
        "blocking_codes": [],
        "produced_at_utc": "2026-04-08T13:00:03Z",
    }
    ledger_payload = {
        "day_utc": current_identity["day_utc"],
        "ledger_id": "ledger-001",
        "control_state": {
            "authority_status": "GRANTED",
            "system_ready": True,
            "submission_authorized": True,
            "blocking_codes": [],
        },
        "evaluated_at_utc": "2026-04-08T13:00:02Z",
    }

    for path, payload in (
        (deployment_path, deployment_payload),
        (state_machine_path, trading_day_payload),
        (startup_path, startup_payload),
        (startup_proof_path, startup_proof_payload),
        (ledger_path, ledger_payload),
    ):
        _write_json(path, payload)

    journal.append_deployment_outcome_event_v1(
        truth_root=tmp_path,
        source_path=deployment_path,
        source_payload=deployment_payload,
        producer_module="test.module",
        **legacy_identity,
    )

    with patch.object(journal_tool, "_load_deployment_payload", return_value=(deployment_path, deployment_payload)), patch.object(
        journal_tool,
        "_resolve_release_git_sha",
        return_value=current_identity["git_sha"],
    ), patch.object(
        journal_tool,
        "read_startup_materialization_ref_v1",
        return_value=SurfaceRefV1(path=startup_path, payload=startup_payload, sha256="d" * 64),
    ), patch.object(
        journal_tool,
        "read_startup_proof_validation_ref_v1",
        return_value=SurfaceRefV1(path=startup_proof_path, payload=startup_proof_payload, sha256="e" * 64),
    ), patch.object(
        journal_tool,
        "read_paper_session_ledger_ref_v1",
        return_value=SurfaceRefV1(path=ledger_path, payload=ledger_payload, sha256="f" * 64),
    ), patch.object(
        journal_tool,
        "read_trading_day_state_machine_ref_v1",
        return_value=SurfaceRefV1(path=state_machine_path, payload=trading_day_payload, sha256="1" * 64),
    ):
        rc = journal_tool.main(["--day_utc", current_identity["day_utc"], "--truth_root", str(tmp_path)])

    assert rc == 0
    journal_ref = journal.read_execution_journal_v1(truth_root=tmp_path, day_utc=current_identity["day_utc"])
    assert journal_ref.payload["pipeline_run_id"] == deployment_payload["deployment_attempt_id"]
    assert journal_ref.payload["last_event_seq"] >= 6
