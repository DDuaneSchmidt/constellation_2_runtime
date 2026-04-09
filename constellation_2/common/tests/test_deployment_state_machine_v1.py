from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common import deployment_state_machine_v1 as deploy_common
import ops.tools.run_deployment_state_machine_v1 as deploy_runner


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def test_dirty_worktree_blocks_immutable_release_build() -> None:
    with patch.object(
        deploy_common,
        "git_status_entries",
        return_value=["## feature/test", " M ops/tools/run_trading_day_state_machine_v1.py"],
    ):
        with pytest.raises(SystemExit, match="dirty worktree"):
            deploy_common.require_clean_git_worktree_or_fail(Path("/tmp/fake_repo"))


def test_bundled_file_hash_summary_is_deterministic() -> None:
    manifest = {
        "included_file_hashes": {
            "ops/a.py": "a" * 64,
            "governance/b.md": "b" * 64,
        }
    }
    summary = deploy_common.bundled_file_hash_summary_from_manifest(manifest)
    assert summary["file_count"] == 2
    assert len(summary["aggregate_sha256"]) == 64


def test_service_resolution_detects_active_root_only(tmp_path: Path) -> None:
    runtime_service = tmp_path / "runtime.service"
    runtime_service.write_text(
        "[Service]\n"
        "ExecStart=/usr/bin/bash -lc 'exec /home/node/constellation_2_runtime/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh'\n",
        encoding="utf-8",
    )
    runtime_eval = deploy_common.evaluate_service_resolution(runtime_service)
    assert runtime_eval["active_root_match"] is False
    assert runtime_eval["resolved_execution_root"] == "/home/node/constellation_2_runtime"

    active_service = tmp_path / "active.service"
    active_service.write_text(
        "[Service]\n"
        "ExecStart=/usr/bin/bash -lc 'exec /home/node/constellation_active/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh'\n",
        encoding="utf-8",
    )
    active_eval = deploy_common.evaluate_service_resolution(active_service)
    assert active_eval["active_root_match"] is True
    assert active_eval["resolved_execution_root"] == "/home/node/constellation_active"


def test_authoritative_service_source_is_active_root_only() -> None:
    service_text = (
        SOURCE_ROOT / "ops/systemd/user/c2-paper-day-orchestrator.service"
    ).read_text(encoding="utf-8")
    assert "WorkingDirectory=/home/node/constellation_active" in service_text
    assert "/home/node/constellation_active/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh" in service_text
    assert "/home/node/constellation_2_runtime/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh" not in service_text


def test_launcher_requires_release_manifest() -> None:
    launcher_text = (
        SOURCE_ROOT / "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh"
    ).read_text(encoding="utf-8")
    assert "validated paper-day execution requires release manifest" in launcher_text
    assert "git rev-parse HEAD" not in launcher_text


def test_post_activation_verification_fails_when_required_files_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    release_root = tmp_path / "release"
    release_root.mkdir(parents=True, exist_ok=True)
    dummy_rel = "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh"
    dummy_path = release_root / dummy_rel
    dummy_path.parent.mkdir(parents=True, exist_ok=True)
    dummy_path.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    _write_json(
        release_root / "release_manifest.v1.json",
        {
            "schema_id": "release_manifest.v1",
            "schema_version": "v1",
            "release_id": "release-001",
            "git_sha": "a" * 40,
            "source_root": "/home/node/constellation",
            "release_root": str(release_root),
            "included_files": [dummy_rel],
            "included_file_hashes": {
                dummy_rel: deploy_common.sha256_file(dummy_path),
            },
            "generated_at_utc": "2026-04-08T00:00:00Z",
        },
    )
    active_pointer = tmp_path / "constellation_active"
    active_pointer.symlink_to(release_root)
    contract_path = tmp_path / "active_runtime_contract.v1.json"
    _write_json(
        contract_path,
        {
            "schema_id": "active_runtime_contract.v1",
            "schema_version": "v1",
            "release_id": "release-001",
            "git_sha": "a" * 40,
            "release_root": str(release_root),
            "runtime_data_root": "/tmp/runtime_data",
            "canonical_truth_root": "/tmp/runtime_data/truth",
            "truth_sleeves_root": "/tmp/runtime_data/truth_sleeves",
            "pointer_index_family": "run_pointer_v1",
            "allowed_truth_roots": ["/tmp/runtime_data/truth", "/tmp/runtime_data/truth_sleeves"],
            "provenance_mode": "release_manifest",
            "generated_at_utc": "2026-04-08T00:00:00Z",
            "status": "ACTIVE",
        },
    )
    service_path = tmp_path / "live.service"
    service_path.write_text(
        "[Service]\n"
        "ExecStart=/usr/bin/bash -lc 'exec /home/node/constellation_active/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh'\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(deploy_common, "ACTIVE_POINTER", active_pointer)
    monkeypatch.setattr(deploy_common, "ACTIVE_RUNTIME_CONTRACT_PATH", contract_path)
    monkeypatch.setattr(
        deploy_common,
        "resolve_live_service_fragment_path",
        lambda: service_path,
    )

    verification = deploy_common.evaluate_post_activation_verification(release_root=release_root)
    assert verification["passed"] is False
    assert any(code.startswith("REQUIRED_STARTUP_STACK_FILE_MISSING:") for code in verification["blocking_codes"])


def test_deploy_active_only_when_checks_pass() -> None:
    decision, blocking_codes, first = deploy_common.classify_deployment_decision(
        authoritative_cleanliness_status="CLEAN",
        release_present=True,
        required_startup_stack_files_present=True,
        active_root_match=True,
        live_execution_root_match=True,
        runtime_copy_direct_exec_detected=False,
    )
    assert decision == "DEPLOY_ACTIVE"
    assert blocking_codes == []
    assert first == ""

    blocked_decision, blocked_codes, blocked_first = deploy_common.classify_deployment_decision(
        authoritative_cleanliness_status="DIRTY",
        release_present=True,
        required_startup_stack_files_present=False,
        active_root_match=True,
        live_execution_root_match=False,
        runtime_copy_direct_exec_detected=True,
    )
    assert blocked_decision == "DEPLOY_BLOCKED_VALID"
    assert blocked_first == "AUTHORITATIVE_WORKTREE_DIRTY_BUILD_BLOCKED"
    assert "LIVE_EXECUTION_NOT_ACTIVE_ROOT" in blocked_codes


def test_deployment_runner_direct_emits_idempotently_with_stable_attempt_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    release_root = tmp_path / "release"
    release_root.mkdir(parents=True, exist_ok=True)
    active_pointer = tmp_path / "constellation_active"
    active_pointer.symlink_to(release_root)
    service_path = tmp_path / "live.service"
    service_path.write_text(
        "[Service]\n"
        "ExecStart=/usr/bin/bash -lc 'exec /home/node/constellation_active/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh'\n",
        encoding="utf-8",
    )

    inspection = deploy_common.ReleaseInspectionV1(
        release_id="release-001",
        release_root=release_root,
        manifest_path=release_root / "release_manifest.v1.json",
        manifest={
            "release_id": "release-001",
            "git_sha": "a" * 40,
        },
        manifest_sha256="b" * 64,
        bundled_file_hash_summary={"file_count": 10, "aggregate_sha256": "c" * 64},
        required_startup_stack_present=True,
        missing_required_files=[],
    )

    monkeypatch.setattr(deploy_runner, "ACTIVE_POINTER", active_pointer)
    monkeypatch.setattr(deploy_runner, "git_sha_or_fail", lambda repo_root: "a" * 40)
    monkeypatch.setattr(deploy_runner, "git_branch_or_fail", lambda repo_root: "feature/test")
    monkeypatch.setattr(deploy_runner, "git_cleanliness_status", lambda repo_root: ("CLEAN", []))
    monkeypatch.setattr(deploy_runner, "inspect_release_root", lambda release_root: inspection)
    monkeypatch.setattr(
        deploy_runner,
        "evaluate_post_activation_verification",
        lambda release_root: {
            "passed": True,
            "release_id": "release-001",
            "release_root": str(release_root),
            "active_symlink_path": str(active_pointer),
            "active_symlink_target": str(release_root),
            "active_pointer_matches_release": True,
            "required_startup_stack_files_present": True,
            "missing_required_startup_stack_files": [],
            "service_unit_path": str(service_path),
            "launcher_path": "/home/node/constellation_active/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
            "resolved_execution_root": "/home/node/constellation_active",
            "active_root_match": True,
            "active_runtime_contract_path": "/tmp/active_runtime_contract.v1.json",
            "active_runtime_contract_match": True,
            "blocking_codes": [],
        },
    )
    monkeypatch.setattr(
        deploy_runner,
        "evaluate_service_resolution",
        lambda service_unit_path: {
            "service_unit_path": str(service_unit_path),
            "launcher_path": "/home/node/constellation_active/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
            "resolved_execution_root": "/home/node/constellation_active",
            "active_root_match": True,
        },
    )
    monkeypatch.setattr(deploy_runner, "resolve_live_service_fragment_path", lambda: service_path)
    monkeypatch.setattr(
        deploy_runner,
        "load_active_runtime_contract_if_present",
        lambda: {
            "release_root": str(release_root),
            "release_id": "release-001",
            "status": "ACTIVE",
        },
    )
    monkeypatch.setattr(
        deploy_runner,
        "read_execution_journal_identity_anchor_v1",
        lambda truth_root, day_utc: {
            "day_utc": day_utc,
            "day_attempt_id": "trading_day_state_machine_attempt:2026-04-08:test",
            "pipeline_run_id": "",
            "release_id": "",
            "git_sha": "a" * 40,
        },
    )

    report_path = truth_root / "reports" / "deployment_state_machine_v1" / day_utc / "deployment_state_machine.v1.json"
    first_rc = deploy_runner.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
    first_payload = json.loads(report_path.read_text(encoding="utf-8"))
    second_rc = deploy_runner.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
    second_payload = json.loads(report_path.read_text(encoding="utf-8"))
    journal_path = truth_root / "reports" / "execution_journal_v1" / day_utc / "execution_journal.v1.json"
    journal_payload = json.loads(journal_path.read_text(encoding="utf-8"))
    event_types = [row["event_type"] for row in journal_payload["events"]]

    assert first_rc == 0
    assert second_rc == 0
    assert second_payload["deployment_attempt_id"] == first_payload["deployment_attempt_id"]
    assert event_types.count("DEPLOYMENT_ACTIVATED") == 1
