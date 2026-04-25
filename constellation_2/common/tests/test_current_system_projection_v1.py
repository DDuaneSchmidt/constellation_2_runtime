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


def _source_artifact_row(logical_name: str, path: str, event_type: str, event_key: str) -> dict[str, object]:
    return {
        "logical_name": logical_name,
        "path": path,
        "generated_at_utc": "2026-04-08T13:00:04Z",
        "sha256": "c" * 64 if logical_name == "execution_journal_v1" else "b" * 64,
        "status": "PRESENT",
        "producer_git_sha": "a" * 40,
        "identity_tuple": _identity(),
        "identity_binding_source": "journal_self_identity" if logical_name == "execution_journal_v1" else "execution_journal_v1",
        "identity_binding_event_type": event_type,
        "identity_binding_event_key": event_key,
    }


def _journal_payload(*, submission_authorized: bool = True) -> dict[str, object]:
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
            status="AUTHORIZED" if submission_authorized else "NOT_AUTHORIZED",
            payload={
                **_source_payload("/tmp/ledger.json"),
                "ledger_id": "ledger-001",
                "submission_authorized": submission_authorized,
                "authority_status": "GRANTED",
            },
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
    journal_payload = _journal_payload()
    events_by_type = {event["event_type"]: event for event in journal_payload["events"]}
    payload = current_projection.build_current_system_projection_v1(
        day_utc="2026-04-08",
        journal_payload=journal_payload,
        source_artifacts=[
            _source_artifact_row("execution_journal_v1", "/tmp/journal.json", "", ""),
            _source_artifact_row("deployment_state_machine_v1", "/tmp/deploy.json", "DEPLOYMENT_ACTIVATED", events_by_type["DEPLOYMENT_ACTIVATED"]["event_key"]),
            _source_artifact_row("startup_materialization_v1", "/tmp/startup.json", "STARTUP_MATERIALIZATION_COMPLETED", events_by_type["STARTUP_MATERIALIZATION_COMPLETED"]["event_key"]),
            _source_artifact_row("startup_proof_validation_v1", "/tmp/startup_proof.json", "STARTUP_PROOF_VALIDATION_COMPLETED", events_by_type["STARTUP_PROOF_VALIDATION_COMPLETED"]["event_key"]),
            _source_artifact_row("paper_session_ledger_v1", "/tmp/ledger.json", "LEDGER_AUTHORITY_RECORDED", events_by_type["LEDGER_AUTHORITY_RECORDED"]["event_key"]),
            _source_artifact_row("trading_day_state_machine_v1", "/tmp/state_machine.json", "STATE_MACHINE_DECISION_RECORDED", events_by_type["STATE_MACHINE_DECISION_RECORDED"]["event_key"]),
        ],
        deployment_payload={"day_utc": "2026-04-08", "deployment_attempt_id": _identity()["pipeline_run_id"], "release_build": {"release_id": _identity()["release_id"]}, "evaluated_at_utc": "2026-04-08T13:00:00Z"},
        startup_materialization_payload={"day_utc": "2026-04-08", "produced_at_utc": "2026-04-08T13:00:01Z"},
        startup_proof_payload={"day_utc": "2026-04-08", "generated_at_utc": "2026-04-08T13:00:02Z"},
        ledger_payload={"day_utc": "2026-04-08", "evaluated_at_utc": "2026-04-08T13:00:03Z"},
        trading_day_payload={"day_utc": "2026-04-08", "day_attempt_id": _identity()["day_attempt_id"], "evaluated_at_utc": "2026-04-08T13:00:04Z"},
        generated_at_utc="2026-04-08T13:00:05Z",
        producer_module="test.module",
    )
    assert payload["authoritative_day_start_decision"] == "READY_NOW"
    assert payload["current_deployment_status"] == "DEPLOY_ACTIVE"
    assert payload["operator_action_required"] is False


def test_current_projection_ready_now_with_submit_blocked_is_not_contradiction_for_paper() -> None:
    journal_payload = _journal_payload(submission_authorized=False)
    events_by_type = {event["event_type"]: event for event in journal_payload["events"]}
    payload = current_projection.build_current_system_projection_v1(
        day_utc="2026-04-08",
        journal_payload=journal_payload,
        source_artifacts=[
            _source_artifact_row("execution_journal_v1", "/tmp/journal.json", "", ""),
            _source_artifact_row("deployment_state_machine_v1", "/tmp/deploy.json", "DEPLOYMENT_ACTIVATED", events_by_type["DEPLOYMENT_ACTIVATED"]["event_key"]),
            _source_artifact_row("startup_materialization_v1", "/tmp/startup.json", "STARTUP_MATERIALIZATION_COMPLETED", events_by_type["STARTUP_MATERIALIZATION_COMPLETED"]["event_key"]),
            _source_artifact_row("startup_proof_validation_v1", "/tmp/startup_proof.json", "STARTUP_PROOF_VALIDATION_COMPLETED", events_by_type["STARTUP_PROOF_VALIDATION_COMPLETED"]["event_key"]),
            _source_artifact_row("paper_session_ledger_v1", "/tmp/ledger.json", "LEDGER_AUTHORITY_RECORDED", events_by_type["LEDGER_AUTHORITY_RECORDED"]["event_key"]),
            _source_artifact_row("trading_day_state_machine_v1", "/tmp/state_machine.json", "STATE_MACHINE_DECISION_RECORDED", events_by_type["STATE_MACHINE_DECISION_RECORDED"]["event_key"]),
        ],
        deployment_payload={"day_utc": "2026-04-08", "deployment_attempt_id": _identity()["pipeline_run_id"], "release_build": {"release_id": _identity()["release_id"]}, "evaluated_at_utc": "2026-04-08T13:00:00Z"},
        startup_materialization_payload={"day_utc": "2026-04-08", "produced_at_utc": "2026-04-08T13:00:01Z"},
        startup_proof_payload={"day_utc": "2026-04-08", "generated_at_utc": "2026-04-08T13:00:02Z"},
        ledger_payload={"day_utc": "2026-04-08", "evaluated_at_utc": "2026-04-08T13:00:03Z"},
        trading_day_payload={
            "day_utc": "2026-04-08",
            "day_attempt_id": _identity()["day_attempt_id"],
            "evaluated_at_utc": "2026-04-08T13:00:04Z",
            "open_policy": {"environment": "PAPER"},
        },
        generated_at_utc="2026-04-08T13:00:05Z",
        producer_module="test.module",
    )
    assert payload["authoritative_day_start_decision"] == "READY_NOW"
    assert payload["current_submission_status"] == "NOT_AUTHORIZED"
    assert payload["contradiction_status"] == "NONE"
    assert payload["operator_action_required"] is False


def test_current_projection_ready_now_with_submit_blocked_remains_contradiction_for_non_paper() -> None:
    journal_payload = _journal_payload(submission_authorized=False)
    events_by_type = {event["event_type"]: event for event in journal_payload["events"]}
    payload = current_projection.build_current_system_projection_v1(
        day_utc="2026-04-08",
        journal_payload=journal_payload,
        source_artifacts=[
            _source_artifact_row("execution_journal_v1", "/tmp/journal.json", "", ""),
            _source_artifact_row("deployment_state_machine_v1", "/tmp/deploy.json", "DEPLOYMENT_ACTIVATED", events_by_type["DEPLOYMENT_ACTIVATED"]["event_key"]),
            _source_artifact_row("startup_materialization_v1", "/tmp/startup.json", "STARTUP_MATERIALIZATION_COMPLETED", events_by_type["STARTUP_MATERIALIZATION_COMPLETED"]["event_key"]),
            _source_artifact_row("startup_proof_validation_v1", "/tmp/startup_proof.json", "STARTUP_PROOF_VALIDATION_COMPLETED", events_by_type["STARTUP_PROOF_VALIDATION_COMPLETED"]["event_key"]),
            _source_artifact_row("paper_session_ledger_v1", "/tmp/ledger.json", "LEDGER_AUTHORITY_RECORDED", events_by_type["LEDGER_AUTHORITY_RECORDED"]["event_key"]),
            _source_artifact_row("trading_day_state_machine_v1", "/tmp/state_machine.json", "STATE_MACHINE_DECISION_RECORDED", events_by_type["STATE_MACHINE_DECISION_RECORDED"]["event_key"]),
        ],
        deployment_payload={"day_utc": "2026-04-08", "deployment_attempt_id": _identity()["pipeline_run_id"], "release_build": {"release_id": _identity()["release_id"]}, "evaluated_at_utc": "2026-04-08T13:00:00Z"},
        startup_materialization_payload={"day_utc": "2026-04-08", "produced_at_utc": "2026-04-08T13:00:01Z"},
        startup_proof_payload={"day_utc": "2026-04-08", "generated_at_utc": "2026-04-08T13:00:02Z"},
        ledger_payload={"day_utc": "2026-04-08", "evaluated_at_utc": "2026-04-08T13:00:03Z"},
        trading_day_payload={
            "day_utc": "2026-04-08",
            "day_attempt_id": _identity()["day_attempt_id"],
            "evaluated_at_utc": "2026-04-08T13:00:04Z",
            "open_policy": {"environment": "LIVE"},
        },
        generated_at_utc="2026-04-08T13:00:05Z",
        producer_module="test.module",
    )
    assert payload["authoritative_day_start_decision"] == "READY_NOW"
    assert payload["current_submission_status"] == "NOT_AUTHORIZED"
    assert payload["contradiction_status"] == "PRESENT"
    assert payload["first_true_blocker_code"] == "CURRENT_SYSTEM_PROJECTION_SUBMISSION_NOT_AUTHORIZED"


def test_current_projection_fails_closed_when_required_event_missing() -> None:
    bad_journal = _journal_payload()
    source_events = {event["event_type"]: event for event in bad_journal["events"]}
    bad_journal["events"] = [event for event in bad_journal["events"] if event["event_type"] != "LEDGER_AUTHORITY_RECORDED"]
    renumbered_events = []
    for idx, event in enumerate(bad_journal["events"], start=1):
        renumbered_events.append(
            journal.build_event_record_v1(
                **_identity(),
                event_seq=idx,
                event_type=event["event_type"],
                event_source=event["event_source"],
                generated_at_utc=event["generated_at_utc"],
                status=event["status"],
                payload=event["payload"],
            )
        )
    bad_journal["events"] = renumbered_events
    bad_journal["last_event_seq"] = len(renumbered_events)
    with pytest.raises(ValueError, match="REQUIRED_EVENT_MISSING"):
        current_projection.build_current_system_projection_v1(
            day_utc="2026-04-08",
            journal_payload=bad_journal,
            source_artifacts=[
                _source_artifact_row("execution_journal_v1", "/tmp/journal.json", "", ""),
                _source_artifact_row("deployment_state_machine_v1", "/tmp/deploy.json", "DEPLOYMENT_ACTIVATED", source_events["DEPLOYMENT_ACTIVATED"]["event_key"]),
                _source_artifact_row("startup_materialization_v1", "/tmp/startup.json", "STARTUP_MATERIALIZATION_COMPLETED", source_events["STARTUP_MATERIALIZATION_COMPLETED"]["event_key"]),
                _source_artifact_row("startup_proof_validation_v1", "/tmp/startup_proof.json", "STARTUP_PROOF_VALIDATION_COMPLETED", source_events["STARTUP_PROOF_VALIDATION_COMPLETED"]["event_key"]),
                _source_artifact_row("paper_session_ledger_v1", "/tmp/ledger.json", "LEDGER_AUTHORITY_RECORDED", "missing-ledger-key"),
                _source_artifact_row("trading_day_state_machine_v1", "/tmp/state_machine.json", "STATE_MACHINE_DECISION_RECORDED", source_events["STATE_MACHINE_DECISION_RECORDED"]["event_key"]),
            ],
            deployment_payload={"day_utc": "2026-04-08", "deployment_attempt_id": _identity()["pipeline_run_id"], "release_build": {"release_id": _identity()["release_id"]}, "evaluated_at_utc": "2026-04-08T13:00:00Z"},
            startup_materialization_payload={"day_utc": "2026-04-08", "produced_at_utc": "2026-04-08T13:00:01Z"},
            startup_proof_payload={"day_utc": "2026-04-08", "generated_at_utc": "2026-04-08T13:00:02Z"},
            ledger_payload={"day_utc": "2026-04-08", "evaluated_at_utc": "2026-04-08T13:00:03Z"},
            trading_day_payload={"day_utc": "2026-04-08", "day_attempt_id": _identity()["day_attempt_id"], "evaluated_at_utc": "2026-04-08T13:00:04Z"},
            generated_at_utc="2026-04-08T13:00:05Z",
            producer_module="test.module",
        )


def test_current_projection_rejects_identity_mismatch() -> None:
    journal_payload = _journal_payload()
    events_by_type = {event["event_type"]: event for event in journal_payload["events"]}
    bad_row = _source_artifact_row(
        "trading_day_state_machine_v1",
        "/tmp/state_machine.json",
        "STATE_MACHINE_DECISION_RECORDED",
        events_by_type["STATE_MACHINE_DECISION_RECORDED"]["event_key"],
    )
    bad_row["identity_tuple"] = {**_identity(), "release_id": "release-002"}
    with pytest.raises(ValueError, match="IDENTITY_TUPLE_MISSING|CROSS_IDENTITY_CONTAMINATION"):
        current_projection.build_current_system_projection_v1(
            day_utc="2026-04-08",
            journal_payload=journal_payload,
            source_artifacts=[
                _source_artifact_row("execution_journal_v1", "/tmp/journal.json", "", ""),
                _source_artifact_row("deployment_state_machine_v1", "/tmp/deploy.json", "DEPLOYMENT_ACTIVATED", events_by_type["DEPLOYMENT_ACTIVATED"]["event_key"]),
                _source_artifact_row("startup_materialization_v1", "/tmp/startup.json", "STARTUP_MATERIALIZATION_COMPLETED", events_by_type["STARTUP_MATERIALIZATION_COMPLETED"]["event_key"]),
                _source_artifact_row("startup_proof_validation_v1", "/tmp/startup_proof.json", "STARTUP_PROOF_VALIDATION_COMPLETED", events_by_type["STARTUP_PROOF_VALIDATION_COMPLETED"]["event_key"]),
                _source_artifact_row("paper_session_ledger_v1", "/tmp/ledger.json", "LEDGER_AUTHORITY_RECORDED", events_by_type["LEDGER_AUTHORITY_RECORDED"]["event_key"]),
                bad_row,
            ],
            deployment_payload={"day_utc": "2026-04-08", "deployment_attempt_id": _identity()["pipeline_run_id"], "release_build": {"release_id": _identity()["release_id"]}, "evaluated_at_utc": "2026-04-08T13:00:00Z"},
            startup_materialization_payload={"day_utc": "2026-04-08", "produced_at_utc": "2026-04-08T13:00:01Z"},
            startup_proof_payload={"day_utc": "2026-04-08", "generated_at_utc": "2026-04-08T13:00:02Z"},
            ledger_payload={"day_utc": "2026-04-08", "evaluated_at_utc": "2026-04-08T13:00:03Z"},
            trading_day_payload={"day_utc": "2026-04-08", "day_attempt_id": _identity()["day_attempt_id"], "evaluated_at_utc": "2026-04-08T13:00:04Z"},
            generated_at_utc="2026-04-08T13:00:05Z",
            producer_module="test.module",
        )


def test_current_projection_rejects_missing_identity_field() -> None:
    journal_payload = _journal_payload()
    events_by_type = {event["event_type"]: event for event in journal_payload["events"]}
    bad_row = _source_artifact_row(
        "deployment_state_machine_v1",
        "/tmp/deploy.json",
        "DEPLOYMENT_ACTIVATED",
        events_by_type["DEPLOYMENT_ACTIVATED"]["event_key"],
    )
    del bad_row["identity_tuple"]["pipeline_run_id"]
    with pytest.raises(ValueError, match="REQUIRED_FIELD_MISSING|IDENTITY_TUPLE_MISSING"):
        current_projection.build_current_system_projection_v1(
            day_utc="2026-04-08",
            journal_payload=journal_payload,
            source_artifacts=[
                _source_artifact_row("execution_journal_v1", "/tmp/journal.json", "", ""),
                bad_row,
                _source_artifact_row("startup_materialization_v1", "/tmp/startup.json", "STARTUP_MATERIALIZATION_COMPLETED", events_by_type["STARTUP_MATERIALIZATION_COMPLETED"]["event_key"]),
                _source_artifact_row("startup_proof_validation_v1", "/tmp/startup_proof.json", "STARTUP_PROOF_VALIDATION_COMPLETED", events_by_type["STARTUP_PROOF_VALIDATION_COMPLETED"]["event_key"]),
                _source_artifact_row("paper_session_ledger_v1", "/tmp/ledger.json", "LEDGER_AUTHORITY_RECORDED", events_by_type["LEDGER_AUTHORITY_RECORDED"]["event_key"]),
                _source_artifact_row("trading_day_state_machine_v1", "/tmp/state_machine.json", "STATE_MACHINE_DECISION_RECORDED", events_by_type["STATE_MACHINE_DECISION_RECORDED"]["event_key"]),
            ],
            deployment_payload={"day_utc": "2026-04-08", "deployment_attempt_id": _identity()["pipeline_run_id"], "release_build": {"release_id": _identity()["release_id"]}, "evaluated_at_utc": "2026-04-08T13:00:00Z"},
            startup_materialization_payload={"day_utc": "2026-04-08", "produced_at_utc": "2026-04-08T13:00:01Z"},
            startup_proof_payload={"day_utc": "2026-04-08", "generated_at_utc": "2026-04-08T13:00:02Z"},
            ledger_payload={"day_utc": "2026-04-08", "evaluated_at_utc": "2026-04-08T13:00:03Z"},
            trading_day_payload={"day_utc": "2026-04-08", "day_attempt_id": _identity()["day_attempt_id"], "evaluated_at_utc": "2026-04-08T13:00:04Z"},
            generated_at_utc="2026-04-08T13:00:05Z",
            producer_module="test.module",
        )


def test_current_projection_accepts_stable_event_binding_across_report_sha_churn() -> None:
    journal_payload = _journal_payload()
    events_by_type = {event["event_type"]: event for event in journal_payload["events"]}
    deployment_row = _source_artifact_row(
        "deployment_state_machine_v1",
        "/tmp/deploy.json",
        "DEPLOYMENT_ACTIVATED",
        events_by_type["DEPLOYMENT_ACTIVATED"]["event_key"],
    )
    deployment_row["sha256"] = "d" * 64
    payload = current_projection.build_current_system_projection_v1(
        day_utc="2026-04-08",
        journal_payload=journal_payload,
        source_artifacts=[
            _source_artifact_row("execution_journal_v1", "/tmp/journal.json", "", ""),
            deployment_row,
            _source_artifact_row("startup_materialization_v1", "/tmp/startup.json", "STARTUP_MATERIALIZATION_COMPLETED", events_by_type["STARTUP_MATERIALIZATION_COMPLETED"]["event_key"]),
            _source_artifact_row("startup_proof_validation_v1", "/tmp/startup_proof.json", "STARTUP_PROOF_VALIDATION_COMPLETED", events_by_type["STARTUP_PROOF_VALIDATION_COMPLETED"]["event_key"]),
            _source_artifact_row("paper_session_ledger_v1", "/tmp/ledger.json", "LEDGER_AUTHORITY_RECORDED", events_by_type["LEDGER_AUTHORITY_RECORDED"]["event_key"]),
            _source_artifact_row("trading_day_state_machine_v1", "/tmp/state_machine.json", "STATE_MACHINE_DECISION_RECORDED", events_by_type["STATE_MACHINE_DECISION_RECORDED"]["event_key"]),
        ],
        deployment_payload={"day_utc": "2026-04-08", "deployment_attempt_id": _identity()["pipeline_run_id"], "release_build": {"release_id": _identity()["release_id"]}, "evaluated_at_utc": "2026-04-08T13:00:00Z"},
        startup_materialization_payload={"day_utc": "2026-04-08", "produced_at_utc": "2026-04-08T13:00:01Z"},
        startup_proof_payload={"day_utc": "2026-04-08", "generated_at_utc": "2026-04-08T13:00:02Z"},
        ledger_payload={"day_utc": "2026-04-08", "evaluated_at_utc": "2026-04-08T13:00:03Z"},
        trading_day_payload={"day_utc": "2026-04-08", "day_attempt_id": _identity()["day_attempt_id"], "evaluated_at_utc": "2026-04-08T13:00:04Z"},
        generated_at_utc="2026-04-08T13:00:05Z",
        producer_module="test.module",
    )
    assert payload["current_deployment_status"] == "DEPLOY_ACTIVE"
