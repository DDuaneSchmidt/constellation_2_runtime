from __future__ import annotations

import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common import current_system_projection_v1 as current_projection
from constellation_2.common import execution_journal_v1 as journal


def _identity() -> dict[str, str]:
    return {
        "day_utc": "2026-04-08",
        "day_attempt_id": "day_attempt:2026-04-08:A001",
        "pipeline_run_id": "pipeline_run:2026-04-08:R001",
        "release_id": "release-001",
        "git_sha": "a" * 40,
    }


def _source_payload(path: str = "/tmp/source.json") -> dict[str, object]:
    return {
        "source_artifact_path": path,
        "source_artifact_sha256": "b" * 64,
        "source_generated_at_utc": "2026-04-08T13:00:00Z",
    }


def _journal_payload() -> dict[str, object]:
    identity = _identity()
    events = [
        journal.build_event_record_v1(
            **identity,
            event_seq=1,
            event_type="DEPLOYMENT_ACTIVATED",
            event_source="deployment_state_machine_v1",
            generated_at_utc="2026-04-08T13:00:00Z",
            status="DEPLOY_ACTIVE",
            payload={**_source_payload("/tmp/deploy.json"), "final_deployment_decision": "DEPLOY_ACTIVE", "blocking_codes": [], "first_true_blocker_code": ""},
        ),
        journal.build_event_record_v1(
            **identity,
            event_seq=2,
            event_type="STARTUP_MATERIALIZATION_COMPLETED",
            event_source="startup_materialization_v1",
            generated_at_utc="2026-04-08T13:00:01Z",
            status="SUCCESS",
            payload={**_source_payload("/tmp/startup.json"), "startup_status": "SUCCESS", "freshness_verdict": "CURRENT", "linkage_verdict": "LINKED", "blocking_codes": []},
        ),
        journal.build_event_record_v1(
            **identity,
            event_seq=3,
            event_type="STARTUP_PROOF_VALIDATION_COMPLETED",
            event_source="startup_proof_validation_v1",
            generated_at_utc="2026-04-08T13:00:02Z",
            status="STARTUP_READY",
            payload={**_source_payload("/tmp/startup_proof.json"), "startup_proof_status": "STARTUP_READY", "ledger_authority_status": "GRANTED", "blocking_codes": []},
        ),
        journal.build_event_record_v1(
            **identity,
            event_seq=4,
            event_type="LEDGER_AUTHORITY_RECORDED",
            event_source="paper_session_ledger_v1",
            generated_at_utc="2026-04-08T13:00:03Z",
            status="GRANTED",
            payload={**_source_payload("/tmp/ledger.json"), "ledger_id": "ledger-001", "authority_status": "GRANTED", "system_ready": True, "submission_authorized": True, "blocking_codes": []},
        ),
        journal.build_event_record_v1(
            **identity,
            event_seq=5,
            event_type="SUBMISSION_AUTHORIZATION_RECORDED",
            event_source="paper_session_ledger_v1",
            generated_at_utc="2026-04-08T13:00:03Z",
            status="AUTHORIZED",
            payload={**_source_payload("/tmp/ledger.json"), "ledger_id": "ledger-001", "submission_authorized": True, "authority_status": "GRANTED"},
        ),
        journal.build_event_record_v1(
            **identity,
            event_seq=6,
            event_type="STATE_MACHINE_DECISION_RECORDED",
            event_source="trading_day_state_machine_v1",
            generated_at_utc="2026-04-08T13:00:04Z",
            status="READY_NOW",
            payload={**_source_payload("/tmp/state_machine.json"), "state_machine_id": "state-001", "final_start_decision": "READY_NOW", "first_true_blocker_code": "", "first_true_blocker_artifact_path": ""},
        ),
    ]
    return journal.build_execution_journal_payload_v1(
        **identity,
        generated_at_utc="2026-04-08T13:00:04Z",
        events=events,
        producer_module="test.module",
    )


def test_current_projection_from_valid_journal() -> None:
    payload = current_projection.build_current_system_projection_v1(
        day_utc="2026-04-08",
        journal_payload=_journal_payload(),
        source_artifacts=[
            {"logical_name": "execution_journal_v1", "path": "/tmp/journal.json", "generated_at_utc": "2026-04-08T13:00:04Z", "sha256": "c" * 64, "status": "PRESENT", "producer_git_sha": "a" * 40},
            {"logical_name": "deployment_state_machine_v1", "path": "/tmp/deploy.json", "generated_at_utc": "2026-04-08T13:00:00Z", "sha256": "d" * 64, "status": "PRESENT", "producer_git_sha": "a" * 40},
            {"logical_name": "startup_materialization_v1", "path": "/tmp/startup.json", "generated_at_utc": "2026-04-08T13:00:01Z", "sha256": "e" * 64, "status": "PRESENT", "producer_git_sha": "a" * 40},
            {"logical_name": "startup_proof_validation_v1", "path": "/tmp/startup_proof.json", "generated_at_utc": "2026-04-08T13:00:02Z", "sha256": "f" * 64, "status": "PRESENT", "producer_git_sha": "a" * 40},
            {"logical_name": "paper_session_ledger_v1", "path": "/tmp/ledger.json", "generated_at_utc": "2026-04-08T13:00:03Z", "sha256": "1" * 64, "status": "PRESENT", "producer_git_sha": "a" * 40},
            {"logical_name": "trading_day_state_machine_v1", "path": "/tmp/state_machine.json", "generated_at_utc": "2026-04-08T13:00:04Z", "sha256": "2" * 64, "status": "PRESENT", "producer_git_sha": "a" * 40},
        ],
        deployment_payload={"evaluated_at_utc": "2026-04-08T13:00:00Z"},
        startup_materialization_payload={"produced_at_utc": "2026-04-08T13:00:01Z"},
        startup_proof_payload={"generated_at_utc": "2026-04-08T13:00:02Z"},
        ledger_payload={"evaluated_at_utc": "2026-04-08T13:00:03Z"},
        trading_day_payload={"evaluated_at_utc": "2026-04-08T13:00:04Z"},
        generated_at_utc="2026-04-08T13:00:05Z",
        producer_module="test.module",
    )
    assert payload["authoritative_day_start_decision"] == "READY_NOW"
    assert payload["current_deployment_status"] == "DEPLOY_ACTIVE"
    assert payload["operator_action_required"] is False


def test_current_projection_fails_closed_when_required_event_missing() -> None:
    bad_journal = _journal_payload()
    bad_journal["events"] = [event for event in bad_journal["events"] if event["event_type"] != "LEDGER_AUTHORITY_RECORDED"]
    bad_journal["last_event_seq"] = len(bad_journal["events"])
    with pytest.raises(ValueError, match="REQUIRED_EVENT_MISSING"):
        current_projection.build_current_system_projection_v1(
            day_utc="2026-04-08",
            journal_payload=bad_journal,
            source_artifacts=[],
            deployment_payload={},
            startup_materialization_payload={},
            startup_proof_payload={},
            ledger_payload={},
            trading_day_payload={},
            generated_at_utc="2026-04-08T13:00:05Z",
            producer_module="test.module",
        )

