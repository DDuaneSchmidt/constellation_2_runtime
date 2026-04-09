from __future__ import annotations

import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_recurrence_kill_gate_v1 as gate_runner
from constellation_2.common import recurrence_fingerprint_v1 as recurrence_fingerprint
from constellation_2.common import recurrence_kill_gate_v1 as recurrence_gate


def _identity() -> dict[str, str]:
    return {
        "day_utc": "2026-04-09",
        "day_attempt_id": "trading_day_state_machine_attempt:2026-04-09:T001",
        "pipeline_run_id": "deployment_state_machine_attempt:2026-04-09:D001",
        "release_id": "release-001",
        "git_sha": "a" * 40,
    }


def _sources() -> list[dict[str, str]]:
    return [
        {
            "logical_name": "deployment_state_machine_v1",
            "path": "/tmp/deployment.json",
            "generated_at_utc": "2026-04-09T14:00:00Z",
            "sha256": "b" * 64,
            "status": "DEPLOY_ACTIVE",
        }
    ]


def _deployment_payload() -> dict[str, object]:
    return {
        "day_utc": "2026-04-09",
        "final_deployment_decision": "DEPLOY_ACTIVE",
        "deployment_attempt_id": _identity()["pipeline_run_id"],
        "release_build": {"release_id": _identity()["release_id"]},
    }


def _trading_day_payload() -> dict[str, object]:
    return {
        "day_utc": "2026-04-09",
        "day_attempt_id": _identity()["day_attempt_id"],
        "final_start_decision": "READY_NOW",
        "first_true_blocker": {
            "first_true_blocker_code": "",
            "first_true_blocker_artifact_path": "",
        },
    }


def _live_entrypoint_verification(ok: bool) -> dict[str, object]:
    return {
        "live_entrypoint_verified": ok,
        "proof_failures": [] if ok else ["LIVE_ENTRYPOINT_EXECSTART_INVALID"],
    }


def _rerun_idempotency(ok: bool, reason_code: str = "") -> dict[str, object]:
    return {
        "rerun_idempotent": ok,
        "reason_code": reason_code,
        "before_signature": {"event_count": 7},
        "after_signature": {"event_count": 7 if ok else 8},
        "runner_exit_code": 0 if ok else 1,
        "runner_stdout": "",
        "runner_stderr": "",
    }


def test_recurrence_kill_gate_invalid_proof_for_wrong_entrypoint() -> None:
    fingerprint = recurrence_fingerprint.build_recurrence_fingerprint_record_v1(
        blocker_family="PROOF_INVALID",
        blocker_code="LIVE_ENTRYPOINT_EXECSTART_INVALID",
        authority_source="trading_day_state_machine_v1",
        stage_id="TRADING_DAY_START",
    )
    payload = recurrence_gate.build_recurrence_kill_gate_payload_v1(
        day_utc="2026-04-09",
        identity=_identity(),
        live_entrypoint_verification=_live_entrypoint_verification(False),
        live_day_utc_value="2026-04-09",
        deployment_payload=_deployment_payload(),
        trading_day_payload=_trading_day_payload(),
        rerun_idempotency=_rerun_idempotency(True),
        fingerprint_record=fingerprint,
        recurrence_status_before="UNSEEN",
        recurrence_status_after="OPEN",
        authoritative_sources=_sources(),
        generated_at_utc="2026-04-09T14:10:00Z",
        producer_module="test.module",
    )
    assert payload["proof_status"] == "INVALID_PROOF"


def test_recurrence_kill_gate_invalid_proof_for_wrong_day() -> None:
    fingerprint = recurrence_fingerprint.build_recurrence_fingerprint_record_v1(
        blocker_family="PROOF_INVALID",
        blocker_code="TARGET_DAY_NOT_LIVE_CURRENT_DAY",
        authority_source="trading_day_state_machine_v1",
        stage_id="TRADING_DAY_START",
    )
    payload = recurrence_gate.build_recurrence_kill_gate_payload_v1(
        day_utc="2026-04-09",
        identity=_identity(),
        live_entrypoint_verification=_live_entrypoint_verification(True),
        live_day_utc_value="2026-04-08",
        deployment_payload=_deployment_payload(),
        trading_day_payload=_trading_day_payload(),
        rerun_idempotency=_rerun_idempotency(True),
        fingerprint_record=fingerprint,
        recurrence_status_before="UNSEEN",
        recurrence_status_after="OPEN",
        authoritative_sources=_sources(),
        generated_at_utc="2026-04-09T14:10:00Z",
        producer_module="test.module",
    )
    assert payload["proof_status"] == "INVALID_PROOF"


def test_recurrence_kill_gate_invalid_proof_for_identity_mismatch() -> None:
    fingerprint = recurrence_fingerprint.build_recurrence_fingerprint_record_v1(
        blocker_family="PROOF_INVALID",
        blocker_code="IDENTITY_MISMATCH:execution_journal_v1",
        authority_source="trading_day_state_machine_v1",
        stage_id="TRADING_DAY_START",
    )
    payload = recurrence_gate.build_recurrence_kill_gate_payload_v1(
        day_utc="2026-04-09",
        identity=_identity(),
        live_entrypoint_verification=_live_entrypoint_verification(True),
        live_day_utc_value="2026-04-09",
        deployment_payload=_deployment_payload(),
        trading_day_payload=_trading_day_payload(),
        rerun_idempotency=_rerun_idempotency(True),
        fingerprint_record=fingerprint,
        recurrence_status_before="UNSEEN",
        recurrence_status_after="OPEN",
        additional_proof_failures=["IDENTITY_MISMATCH:execution_journal_v1"],
        authoritative_sources=_sources(),
        generated_at_utc="2026-04-09T14:10:00Z",
        producer_module="test.module",
    )
    assert payload["proof_status"] == "INVALID_PROOF"


def test_recurrence_kill_gate_mitigated_when_success_path_not_idempotent() -> None:
    fingerprint = recurrence_fingerprint.build_recurrence_fingerprint_record_v1(
        blocker_family="DAY_START_BLOCK",
        blocker_code="BLOCKER_FAMILY_REMAINS_CONTAINED",
        authority_source="trading_day_state_machine_v1",
        stage_id="TRADING_DAY_START",
    )
    blocked = {
        **_trading_day_payload(),
        "final_start_decision": "BLOCKED",
        "first_true_blocker": {
            "first_true_blocker_code": "UPSTREAM_INPUT_MISSING",
            "first_true_blocker_artifact_path": "/tmp/missing.json",
        },
    }
    payload = recurrence_gate.build_recurrence_kill_gate_payload_v1(
        day_utc="2026-04-09",
        identity=_identity(),
        live_entrypoint_verification=_live_entrypoint_verification(True),
        live_day_utc_value="2026-04-09",
        deployment_payload=_deployment_payload(),
        trading_day_payload=blocked,
        rerun_idempotency=_rerun_idempotency(True),
        fingerprint_record=fingerprint,
        recurrence_status_before="OPEN",
        recurrence_status_after="CONTAINED",
        authoritative_sources=_sources(),
        generated_at_utc="2026-04-09T14:10:00Z",
        producer_module="test.module",
    )
    assert payload["proof_status"] == "MITIGATED_NOT_RECURRENCE_SAFE"


def test_recurrence_kill_gate_requires_all_criteria_for_safe() -> None:
    fingerprint = recurrence_fingerprint.build_recurrence_fingerprint_record_v1(
        blocker_family="SUCCESS_VERIFICATION",
        blocker_code="NO_BLOCKER",
        authority_source="trading_day_state_machine_v1",
        stage_id="TRADING_DAY_START",
    )
    payload = recurrence_gate.build_recurrence_kill_gate_payload_v1(
        day_utc="2026-04-09",
        identity=_identity(),
        live_entrypoint_verification=_live_entrypoint_verification(True),
        live_day_utc_value="2026-04-09",
        deployment_payload=_deployment_payload(),
        trading_day_payload=_trading_day_payload(),
        rerun_idempotency=_rerun_idempotency(True),
        fingerprint_record=fingerprint,
        recurrence_status_before="CONTAINED",
        recurrence_status_after="ELIMINATED",
        authoritative_sources=_sources(),
        generated_at_utc="2026-04-09T14:10:00Z",
        producer_module="test.module",
    )
    assert payload["proof_status"] == "RECURRENCE_SAFE"
    assert payload["rerun_idempotent"] is True


def test_rerun_idempotency_logic_detects_change() -> None:
    before = {
        "journal_id": "journal-001",
        "last_event_seq": 2,
        "events": [{"event_key": "a"}, {"event_key": "b"}],
    }
    after = {
        "journal_id": "journal-001",
        "last_event_seq": 3,
        "events": [{"event_key": "a"}, {"event_key": "b"}, {"event_key": "c"}],
    }
    proof = recurrence_gate.validate_rerun_idempotency_result_v1(
        before_journal_payload=before,
        after_journal_payload=after,
        runner_exit_code=0,
    )
    assert proof["rerun_idempotent"] is False
    assert proof["reason_code"] == "RERUN_JOURNAL_CHANGED"


def test_live_entrypoint_verification_rejects_contract_mismatch(tmp_path: Path) -> None:
    entrypoint = tmp_path / "entry.sh"
    entrypoint.write_text(
        "#!/usr/bin/env bash\n"
        "DAY=\"$(TZ=America/New_York date +%F)\"\n"
        "echo 'validated paper-day execution requires release manifest'\n",
        encoding="utf-8",
    )
    proof = recurrence_gate.verify_live_entrypoint_v1(
        service_text="WorkingDirectory=/home/node/constellation_active\nExecStart=/usr/bin/bash -lc 'exec /home/node/constellation_active/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh'\n",
        active_symlink_target="/home/node/constellation_releases/release-001",
        active_runtime_contract={
            "release_id": "release-002",
            "git_sha": "b" * 40,
            "release_root": "/home/node/constellation_releases/release-002",
            "status": "ACTIVE",
        },
        active_release_manifest={
            "release_id": "release-001",
            "git_sha": "a" * 40,
            "release_root": "/home/node/constellation_releases/release-001",
        },
        active_entrypoint_path=entrypoint,
    )
    assert proof["live_entrypoint_verified"] is False
    assert "LIVE_ENTRYPOINT_RUNTIME_CONTRACT_RELEASE_ID_MISMATCH" in proof["proof_failures"]


def test_runner_rejects_live_day_wrong_truth_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gate_runner, "live_day_utc_v1", lambda: "2026-04-09")
    monkeypatch.setattr(gate_runner, "resolve_canonical_truth_root", lambda: Path("/tmp/live-truth"))
    monkeypatch.setattr(
        gate_runner,
        "resolve_fact_plane_truth_root_v1",
        lambda value: Path(str(value)),
    )
    with pytest.raises(ValueError, match="LIVE_TRUTH_ROOT_MISMATCH"):
        gate_runner.main(["--day_utc", "2026-04-09", "--truth_root", "/tmp/repo-truth"])


def test_live_entrypoint_verification_requires_entrypoint_file() -> None:
    proof = recurrence_gate.verify_live_entrypoint_v1(
        service_text="WorkingDirectory=/home/node/constellation_active\nExecStart=/usr/bin/bash -lc 'exec /home/node/constellation_active/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh'\n",
        active_symlink_target="/home/node/constellation_releases/release-001",
        active_runtime_contract={
            "release_id": "release-001",
            "git_sha": "a" * 40,
            "release_root": "/home/node/constellation_releases/release-001",
            "status": "ACTIVE",
        },
        active_release_manifest={
            "release_id": "release-001",
            "git_sha": "a" * 40,
            "release_root": "/home/node/constellation_releases/release-001",
        },
        active_entrypoint_path=Path("/tmp/does-not-exist"),
    )
    assert proof["live_entrypoint_verified"] is False
    assert "LIVE_ENTRYPOINT_FILE_MISSING" in proof["proof_failures"]
