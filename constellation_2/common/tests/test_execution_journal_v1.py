from __future__ import annotations

import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common import execution_journal_v1 as journal


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
