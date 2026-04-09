from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common import recurrence_fingerprint_v1 as recurrence_fingerprint
from constellation_2.common import recurrence_registry_v1 as recurrence_registry
from constellation_2.common.paper_session_path_alignment_v1 import resolve_recurrence_registry_path


def _fingerprint_record() -> dict[str, str]:
    return recurrence_fingerprint.build_recurrence_fingerprint_record_v1(
        blocker_family="DEPLOYMENT_BLOCK",
        blocker_code="LIVE_EXECUTION_NOT_ACTIVE_ROOT",
        authority_source="deployment_state_machine_v1",
        stage_id="DEPLOYMENT",
    )


def test_recurrence_registry_insert_and_transitions(tmp_path: Path) -> None:
    before_status, after_status, ref = recurrence_registry.update_recurrence_registry_v1(
        truth_root=tmp_path,
        fingerprint_record=_fingerprint_record(),
        proof_status="INVALID_PROOF",
        observed_at_utc="2026-04-09T14:00:00Z",
        day_utc="2026-04-09",
        day_attempt_id="attempt-001",
        release_id="release-001",
        git_sha="a" * 40,
        terminal_state="DEPLOY_BLOCKED_VALID",
        first_true_blocker_code="LIVE_EXECUTION_NOT_ACTIVE_ROOT",
        producer_module="test.module",
    )
    assert before_status == "UNSEEN"
    assert after_status == "OPEN"
    payload = json.loads(ref.path.read_text(encoding="utf-8"))
    entry = payload["entries_by_fingerprint"][_fingerprint_record()["recurrence_fingerprint"]]
    assert entry["occurrence_count"] == 1
    assert entry["current_status"] == "OPEN"

    before_status, after_status, _ = recurrence_registry.update_recurrence_registry_v1(
        truth_root=tmp_path,
        fingerprint_record=_fingerprint_record(),
        proof_status="MITIGATED_NOT_RECURRENCE_SAFE",
        observed_at_utc="2026-04-09T14:05:00Z",
        day_utc="2026-04-09",
        day_attempt_id="attempt-001",
        release_id="release-001",
        git_sha="a" * 40,
        terminal_state="DEPLOY_ACTIVE",
        first_true_blocker_code="LIVE_EXECUTION_NOT_ACTIVE_ROOT",
        producer_module="test.module",
    )
    assert before_status == "OPEN"
    assert after_status == "CONTAINED"

    before_status, after_status, _ = recurrence_registry.update_recurrence_registry_v1(
        truth_root=tmp_path,
        fingerprint_record=_fingerprint_record(),
        proof_status="RECURRENCE_SAFE",
        observed_at_utc="2026-04-09T14:10:00Z",
        day_utc="2026-04-09",
        day_attempt_id="attempt-001",
        release_id="release-001",
        git_sha="a" * 40,
        terminal_state="DEPLOY_ACTIVE",
        first_true_blocker_code="",
        producer_module="test.module",
    )
    assert before_status == "CONTAINED"
    assert after_status == "ELIMINATED"


def test_recurrence_registry_rejects_malformed_payload(tmp_path: Path) -> None:
    path = resolve_recurrence_registry_path(truth_root=tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_id": "recurrence_registry",
                "schema_version": "v1",
                "authority_scope": "CANONICAL_RECURRENCE_REGISTRY",
                "registry_id": "recurrence_registry_v1",
                "generated_at_utc": "2026-04-09T14:00:00Z",
                "entries_by_fingerprint": {
                    "recurrence:badbadbadbadbadbadbadbad": {
                        "recurrence_fingerprint": "recurrence:aaaaaaaaaaaaaaaaaaaaaaaa",
                        "blocker_family": "DEPLOYMENT_BLOCK",
                        "blocker_code": "LIVE_EXECUTION_NOT_ACTIVE_ROOT",
                        "authority_source": "deployment_state_machine_v1",
                        "stage_id": "DEPLOYMENT",
                        "first_seen_at_utc": "2026-04-09T14:00:00Z",
                        "last_seen_at_utc": "2026-04-09T14:00:00Z",
                        "occurrence_count": 1,
                        "current_status": "OPEN",
                        "last_day_utc": "2026-04-09",
                        "last_day_attempt_id": "attempt-001",
                        "last_release_id": "release-001",
                        "last_git_sha": "a" * 40,
                        "last_terminal_state": "DEPLOY_BLOCKED_VALID",
                        "last_first_true_blocker_code": "LIVE_EXECUTION_NOT_ACTIVE_ROOT"
                    }
                },
                "producer": {"repo": "constellation", "module": "test.module", "git_sha": "a" * 40},
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="FINGERPRINT_MISMATCH"):
        recurrence_registry.load_recurrence_registry_v1(truth_root=tmp_path)
