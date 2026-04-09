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
