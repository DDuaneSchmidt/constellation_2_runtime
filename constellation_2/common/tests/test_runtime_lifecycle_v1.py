from __future__ import annotations

import json
from pathlib import Path

import pytest

from constellation_2.common import runtime_lifecycle_v1


REPO_ROOT = Path("/home/node/constellation")


def _runtime_identity_snapshot(tmp_path: Path) -> dict[str, object]:
    runtime_data_root = tmp_path / "runtime_data"
    runtime_data_root.mkdir(parents=True, exist_ok=True)
    return {
        "contract_path": str(tmp_path / "runtime_contract_v1" / "active_runtime_contract.v1.json"),
        "contract_sha256": "b" * 64,
        "schema_id": "active_runtime_contract.v1",
        "schema_version": "v1",
        "status": "ACTIVE",
        "generated_at_utc": "2026-04-18T12:00:00Z",
        "authoritative_repo_root": str(REPO_ROOT),
        "release_id": "release_20260418",
        "git_sha": "a" * 40,
        "release_root": str(tmp_path / "release_root"),
        "runtime_data_root": str(runtime_data_root),
        "canonical_truth_root": str(runtime_data_root / "truth"),
        "truth_sleeves_root": str(runtime_data_root / "truth_sleeves"),
        "pointer_index_family": "run_pointer_v1",
        "allowed_truth_roots": [
            str(runtime_data_root / "truth"),
            str(runtime_data_root / "truth_sleeves"),
        ],
        "provenance_mode": "release_manifest",
        "runtime_environment": "PAPER",
        "primary_execution_identity_ref": {
            "authority_owner": "execution_identity_binding_v1",
            "sleeve_id": "PRIMARY",
        },
        "resolved_execution_identity": {
            "authority_owner": "execution_identity_binding_v1",
            "environment": "PAPER",
            "sleeve_id": "PRIMARY",
            "account_id": "DUO847203",
            "client_id_orders": 79,
            "client_id_observer": 179,
            "host": "127.0.0.1",
            "port": 4002,
            "sleeve_registry_path": str(REPO_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json"),
            "account_registry_path": str(REPO_ROOT / "governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json"),
        },
        "resolved_execution_roots": {
            "authority_owner": "sleeve_execution_root_v1",
            "environment": "PAPER",
            "ib_account": "DUO847203",
            "sleeve_id": "PRIMARY",
            "mode": "PAPER",
            "truth_partition": "PRIMARY",
            "execution_root_path": str(runtime_data_root / "truth_sleeves" / "PRIMARY" / "PAPER"),
            "truth_sleeves_root": str(runtime_data_root / "truth_sleeves"),
            "sleeve_registry_path": str(REPO_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json"),
        },
        "path_authority_snapshot": {
            "authoritative_repo_root": str(REPO_ROOT),
            "canonical_runtime_truth_root": str(runtime_data_root / "truth"),
            "canonical_runtime_truth_sleeves_root": str(runtime_data_root / "truth_sleeves"),
            "authoritative_repo_truth_root": str(REPO_ROOT / "constellation_2/runtime/truth"),
            "authoritative_repo_truth_sleeves_root": str(REPO_ROOT / "constellation_2/runtime/truth_sleeves"),
            "active_release_root": str(tmp_path / "release_root"),
        },
        "release_provenance": {
            "release_id": "release_20260418",
            "git_sha": "a" * 40,
            "release_root": str(tmp_path / "release_root"),
            "release_manifest_path": str(tmp_path / "release_root" / "release_manifest.v1.json"),
            "provenance_mode": "release_manifest",
            "resolved_at_utc": "2026-04-18T12:00:05Z",
        },
    }


def test_runtime_lifecycle_admission_and_record_start_round_trip(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _runtime_identity_snapshot(tmp_path)
    monkeypatch.setattr(
        runtime_lifecycle_v1,
        "load_active_runtime_identity_snapshot_v1",
        lambda repo_root=None: snapshot,
    )
    monkeypatch.setattr(runtime_lifecycle_v1, "_utc_now_compact", lambda: "20260418T141700Z")
    monkeypatch.setattr(runtime_lifecycle_v1, "_utc_now_iso", lambda: "2026-04-18T14:17:00Z")

    admitted = runtime_lifecycle_v1.request_runtime_lifecycle_admission_v1(
        repo_root=REPO_ROOT,
        entrypoint_name="c2_paper_day_orchestrator_systemd_entry_v1.sh",
        entrypoint_path=REPO_ROOT / "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
        service_name="c2-paper-day-orchestrator.service",
        launcher_pid=321,
    )

    active_state_path = Path(str(admitted["active_state_path"]))
    active_payload = json.loads(active_state_path.read_text(encoding="utf-8"))
    assert admitted["admission_decision"] == "ADMITTED"
    assert admitted["run_id"] == "20260418T141700Z__c2_paper_day_orchestrator_service__pid321"
    assert active_payload["status"] == "ACTIVE"
    assert active_payload["run_id"] == admitted["run_id"]

    startup_receipt = tmp_path / "runtime_startup_identity.v1.json"
    startup_receipt.write_text('{"schema_id":"runtime_startup_identity.v1"}\n', encoding="utf-8")
    receipt_path = runtime_lifecycle_v1.record_runtime_lifecycle_start_v1(
        repo_root=REPO_ROOT,
        entrypoint_name="c2_paper_day_orchestrator_systemd_entry_v1.sh",
        entrypoint_path=REPO_ROOT / "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
        service_name="c2-paper-day-orchestrator.service",
        launcher_pid=321,
        run_id=str(admitted["run_id"]),
        startup_id="20260418T141700Z__c2_paper_day_orchestrator_systemd_entry_v1_sh__pid321",
        startup_receipt_path=startup_receipt,
    )

    receipt_payload = json.loads(receipt_path.read_text(encoding="utf-8"))
    updated_active_payload = json.loads(active_state_path.read_text(encoding="utf-8"))
    assert receipt_payload["status"] == "ADMITTED"
    assert receipt_payload["startup_identity_ref"]["startup_id"].startswith("20260418T141700Z__")
    assert updated_active_payload["startup_id"] == receipt_payload["startup_identity_ref"]["startup_id"]
    assert updated_active_payload["lifecycle_receipt_path"] == str(receipt_path)

    stop_receipt_path = runtime_lifecycle_v1.record_runtime_lifecycle_stop_v1(
        repo_root=REPO_ROOT,
        entrypoint_name="c2_paper_day_orchestrator_systemd_entry_v1.sh",
        entrypoint_path=REPO_ROOT / "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
        service_name="c2-paper-day-orchestrator.service",
        launcher_pid=321,
        run_id=str(admitted["run_id"]),
        launch_phase="RUNTIME",
        wrapper_exit_code=0,
    )
    stop_receipt_payload = json.loads(stop_receipt_path.read_text(encoding="utf-8"))
    assert stop_receipt_payload["exit_disposition"] == "NORMAL_EXIT"
    assert stop_receipt_payload["active_state_release_status"] == "RELEASED"
    assert stop_receipt_payload["startup_identity_ref"]["startup_id"] == updated_active_payload["startup_id"]
    assert stop_receipt_payload["lifecycle_start_ref"]["receipt_path"] == str(receipt_path)
    assert not active_state_path.exists()

    repeated_stop_path = runtime_lifecycle_v1.record_runtime_lifecycle_stop_v1(
        repo_root=REPO_ROOT,
        entrypoint_name="c2_paper_day_orchestrator_systemd_entry_v1.sh",
        entrypoint_path=REPO_ROOT / "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
        service_name="c2-paper-day-orchestrator.service",
        launcher_pid=321,
        run_id=str(admitted["run_id"]),
        launch_phase="RUNTIME",
        wrapper_exit_code=0,
    )
    assert repeated_stop_path == stop_receipt_path


def test_runtime_lifecycle_duplicate_active_run_is_denied(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _runtime_identity_snapshot(tmp_path)
    monkeypatch.setattr(
        runtime_lifecycle_v1,
        "load_active_runtime_identity_snapshot_v1",
        lambda repo_root=None: snapshot,
    )
    monkeypatch.setattr(runtime_lifecycle_v1, "_utc_now_compact", lambda: "20260418T141800Z")
    monkeypatch.setattr(runtime_lifecycle_v1, "_utc_now_iso", lambda: "2026-04-18T14:18:00Z")
    monkeypatch.setattr(runtime_lifecycle_v1, "_pid_is_running", lambda pid: True)

    first = runtime_lifecycle_v1.request_runtime_lifecycle_admission_v1(
        repo_root=REPO_ROOT,
        entrypoint_name="c2_execution_observer_v1.sh",
        entrypoint_path=REPO_ROOT / "ops/run/c2_execution_observer_v1.sh",
        service_name="c2-execution-observer.service",
        launcher_pid=700,
    )
    denied = runtime_lifecycle_v1.request_runtime_lifecycle_admission_v1(
        repo_root=REPO_ROOT,
        entrypoint_name="c2_execution_observer_v1.sh",
        entrypoint_path=REPO_ROOT / "ops/run/c2_execution_observer_v1.sh",
        service_name="c2-execution-observer.service",
        launcher_pid=701,
    )

    denied_receipt = json.loads(Path(str(denied["receipt_path"])).read_text(encoding="utf-8"))
    assert first["admission_decision"] == "ADMITTED"
    assert denied["admission_decision"] == "DENIED"
    assert denied["reason_code"] == runtime_lifecycle_v1.ADMISSION_REASON_ACTIVE_EXISTS
    assert denied_receipt["status"] == "DENIED"
    assert denied_receipt["conflicting_active_run"]["run_id"] == first["run_id"]


def test_runtime_lifecycle_stale_active_run_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _runtime_identity_snapshot(tmp_path)
    monkeypatch.setattr(
        runtime_lifecycle_v1,
        "load_active_runtime_identity_snapshot_v1",
        lambda repo_root=None: snapshot,
    )
    monkeypatch.setattr(runtime_lifecycle_v1, "_utc_now_compact", lambda: "20260418T141900Z")
    monkeypatch.setattr(runtime_lifecycle_v1, "_utc_now_iso", lambda: "2026-04-18T14:19:00Z")
    monkeypatch.setattr(runtime_lifecycle_v1, "_pid_is_running", lambda pid: False)

    admitted = runtime_lifecycle_v1.request_runtime_lifecycle_admission_v1(
        repo_root=REPO_ROOT,
        entrypoint_name="c2_execution_observer_v1.sh",
        entrypoint_path=REPO_ROOT / "ops/run/c2_execution_observer_v1.sh",
        service_name="c2-execution-observer.service",
        launcher_pid=810,
    )
    denied = runtime_lifecycle_v1.request_runtime_lifecycle_admission_v1(
        repo_root=REPO_ROOT,
        entrypoint_name="c2_execution_observer_v1.sh",
        entrypoint_path=REPO_ROOT / "ops/run/c2_execution_observer_v1.sh",
        service_name="c2-execution-observer.service",
        launcher_pid=811,
    )

    denied_receipt = json.loads(Path(str(denied["receipt_path"])).read_text(encoding="utf-8"))
    assert admitted["admission_decision"] == "ADMITTED"
    assert denied["admission_decision"] == "DENIED"
    assert denied["reason_code"] == runtime_lifecycle_v1.ADMISSION_REASON_STALE_ACTIVE
    assert denied_receipt["active_uniqueness_outcome"] == runtime_lifecycle_v1.UNIQUENESS_OUTCOME_DENIED_STALE_ACTIVE


def test_runtime_lifecycle_stop_receipt_captures_prelaunch_signal_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _runtime_identity_snapshot(tmp_path)
    monkeypatch.setattr(
        runtime_lifecycle_v1,
        "load_active_runtime_identity_snapshot_v1",
        lambda repo_root=None: snapshot,
    )
    monkeypatch.setattr(runtime_lifecycle_v1, "_utc_now_compact", lambda: "20260418T142000Z")
    monkeypatch.setattr(runtime_lifecycle_v1, "_utc_now_iso", lambda: "2026-04-18T14:20:00Z")

    admitted = runtime_lifecycle_v1.request_runtime_lifecycle_admission_v1(
        repo_root=REPO_ROOT,
        entrypoint_name="c2_execution_observer_v1.sh",
        entrypoint_path=REPO_ROOT / "ops/run/c2_execution_observer_v1.sh",
        service_name="c2-execution-observer.service",
        launcher_pid=910,
    )

    stop_receipt_path = runtime_lifecycle_v1.record_runtime_lifecycle_stop_v1(
        repo_root=REPO_ROOT,
        entrypoint_name="c2_execution_observer_v1.sh",
        entrypoint_path=REPO_ROOT / "ops/run/c2_execution_observer_v1.sh",
        service_name="c2-execution-observer.service",
        launcher_pid=910,
        run_id=str(admitted["run_id"]),
        launch_phase="PRELAUNCH",
        wrapper_exit_code=143,
        termination_signal="TERM",
    )

    stop_receipt_payload = json.loads(stop_receipt_path.read_text(encoding="utf-8"))
    assert stop_receipt_payload["exit_disposition"] == "PRELAUNCH_FAILURE"
    assert stop_receipt_payload["termination_signal"] == "TERM"
    assert stop_receipt_payload["exit_reason_code"] == runtime_lifecycle_v1.EXIT_REASON_PRELAUNCH
