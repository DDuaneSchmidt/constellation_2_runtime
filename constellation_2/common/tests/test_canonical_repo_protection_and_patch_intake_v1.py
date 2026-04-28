from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path(__file__).resolve().parents[3]
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.aegis_chatgpt_packet as packet_tool
import ops.tools.apply_codex_patch_bundle_v1 as apply_tool
import ops.tools.build_constellation_release_v1 as build_tool
import ops.tools.require_canonical_repo_clean_v1 as clean_tool
import ops.tools.run_aegis_paper_preflight_v1 as preflight_tool
import ops.tools.run_trading_prepare_v1 as trading_prepare_tool
import ops.tools.run_trading_preflight_v1 as trading_preflight_tool
import ops.runtime.supervisor as supervisor_tool
import ops.tools.verify_codex_agent_workspace_v1 as verify_tool
from ops.tools.repo_protection_common_v1 import (
    require_runtime_output_outside_repo_runtime_v1,
)


def _run(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True, check=False)


def _manifest_payload(*, repo: Path, task_id: str) -> dict:
    head = _run(["git", "rev-parse", "HEAD"], repo).stdout.strip()
    return {
        "task_id": task_id,
        "base_commit": head,
        "created_utc": "2026-04-26T00:00:00Z",
        "forbidden_paths_touched": [],
        "validation_scope": "READINESS_FREEZE",
        "authoritative_runtime_validation": False,
        "tests": [],
    }


def _init_git_repo(tmp_path: Path) -> Path:
    repo = (tmp_path / "repo").resolve()
    repo.mkdir(parents=True, exist_ok=True)
    assert _run(["git", "init"], repo).returncode == 0
    assert _run(["git", "config", "user.email", "test@example.com"], repo).returncode == 0
    assert _run(["git", "config", "user.name", "test"], repo).returncode == 0
    (repo / "seed.txt").write_text("seed\n", encoding="utf-8")
    assert _run(["git", "add", "seed.txt"], repo).returncode == 0
    assert _run(["git", "commit", "-m", "seed"], repo).returncode == 0
    return repo


def test_clean_guard_passes_on_clean_repo_fixture(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    repo = _init_git_repo(tmp_path)
    monkeypatch.setattr(clean_tool, "require_canonical_repo_root_v1", lambda repo_root: None)
    payload = clean_tool.evaluate_canonical_cleanliness_v1(repo)
    assert payload["status"] == "CLEAN"
    assert payload["dirty_path_count"] == 0


def test_clean_guard_fails_on_dirty_repo_fixture(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    repo = _init_git_repo(tmp_path)
    (repo / "seed.txt").write_text("dirty\n", encoding="utf-8")
    monkeypatch.setattr(clean_tool, "require_canonical_repo_root_v1", lambda repo_root: None)
    payload = clean_tool.evaluate_canonical_cleanliness_v1(repo)
    assert payload["status"] == "DIRTY"
    assert payload["dirty_path_count"] >= 1


def test_workspace_verifier_rejects_canonical_root() -> None:
    canonical = Path("/tmp/canonical").resolve()
    with pytest.raises(SystemExit, match="rejects canonical root"):
        verify_tool.verify_workspace_v1(
            cwd=canonical,
            workspace_root=Path("/tmp/workspaces").resolve(),
            canonical_root=canonical,
        )


def test_workspace_verifier_accepts_agent_workspace(tmp_path: Path) -> None:
    workspace_root = (tmp_path / "workspaces").resolve()
    workspace = (workspace_root / "task1").resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    assert _run(["git", "init"], workspace).returncode == 0
    assert _run(["git", "config", "user.email", "test@example.com"], workspace).returncode == 0
    assert _run(["git", "config", "user.name", "test"], workspace).returncode == 0
    (workspace / "x.txt").write_text("x\n", encoding="utf-8")
    assert _run(["git", "add", "x.txt"], workspace).returncode == 0
    assert _run(["git", "commit", "-m", "x"], workspace).returncode == 0
    payload = verify_tool.verify_workspace_v1(
        cwd=workspace,
        workspace_root=workspace_root,
        canonical_root=(tmp_path / "canonical").resolve(),
    )
    assert payload["status"] == "OK"
    assert payload["dirty_status"] == "CLEAN"


def test_patch_intake_rejects_runtime_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    repo = _init_git_repo(tmp_path)
    inbox = (tmp_path / "inbox").resolve()
    bundle = (inbox / "task").resolve()
    bundle.mkdir(parents=True, exist_ok=True)
    (bundle / "manifest.json").write_text(
        json.dumps(_manifest_payload(repo=repo, task_id="task"), sort_keys=True),
        encoding="utf-8",
    )
    (bundle / "changes.patch").write_text(
        "diff --git a/runtime/exports/bad.txt b/runtime/exports/bad.txt\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(apply_tool, "CANONICAL_REPO_ROOT", repo)
    monkeypatch.setattr(apply_tool, "PATCH_INBOX_ROOT", inbox)
    monkeypatch.setattr(apply_tool, "git_status_porcelain_paths_v1", lambda _repo_root: [])
    with pytest.raises(SystemExit, match="forbidden runtime paths"):
        apply_tool.main(["--task_id", "task"])


def test_patch_intake_rejects_missing_manifest(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    repo = _init_git_repo(tmp_path)
    inbox = (tmp_path / "inbox").resolve()
    bundle = (inbox / "task").resolve()
    bundle.mkdir(parents=True, exist_ok=True)
    (bundle / "changes.patch").write_text("", encoding="utf-8")
    monkeypatch.setattr(apply_tool, "CANONICAL_REPO_ROOT", repo)
    monkeypatch.setattr(apply_tool, "PATCH_INBOX_ROOT", inbox)
    monkeypatch.setattr(apply_tool, "git_status_porcelain_paths_v1", lambda _repo_root: [])
    with pytest.raises(SystemExit, match="manifest missing"):
        apply_tool.main(["--task_id", "task"])


def test_patch_intake_refuses_dirty_canonical_repo(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    repo = _init_git_repo(tmp_path)
    monkeypatch.setattr(apply_tool, "CANONICAL_REPO_ROOT", repo)
    monkeypatch.setattr(apply_tool, "PATCH_INBOX_ROOT", (tmp_path / "inbox").resolve())
    monkeypatch.setattr(apply_tool, "git_status_porcelain_paths_v1", lambda _repo_root: ["dirty.txt"])
    with pytest.raises(SystemExit, match="must be clean"):
        apply_tool.main(["--task_id", "task"])


def test_release_builder_refuses_dirty_source(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(build_tool, "git_status_porcelain_paths_v1", lambda _repo_root: ["dirty.py"])
    with pytest.raises(SystemExit, match="dirty"):
        build_tool._require_release_source_clean_or_fail(build_tool.CANONICAL_REPO_ROOT)


def test_release_builder_scope_excludes_top_level_docs() -> None:
    assert set(build_tool.INCLUDED_ROOTS) == {"ops", "constellation_2", "governance"}
    assert "docs" not in build_tool.INCLUDED_ROOTS


def _patch_ready_packet_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    day = "2026-04-28"
    monkeypatch.setattr(
        packet_tool,
        "_resolve_truth_roots",
        lambda: packet_tool.RootResolution(
            canonical_truth_root=None,
            runtime_truth_root=None,
            truth_sleeves_root=None,
            authority_source="test",
            evidence="test",
            error="",
        ),
    )
    monkeypatch.setattr(
        packet_tool,
        "_build_current_calendar_day_runtime_status",
        lambda _roots: packet_tool.CurrentCalendarDayStatus(
            day_utc=day,
            is_trading_session="true",
            expected_non_trading_day=False,
            paper_session_authority_path="test",
            paper_session_status="GRANTED",
            status="READY",
            canonical_blocker="",
            service_status_summary="test",
            evidence="test",
        ),
    )
    monkeypatch.setattr(
        packet_tool,
        "_build_latest_trading_day_evidence_status",
        lambda _roots: packet_tool.LatestTradingDayEvidenceStatus(
            evidence_day_utc=day,
            status="READY",
            canonical_blocker="",
            submit_boundary_status_path="test",
            closure_authority_path="test",
            trade_lineage_graph_path="test",
            execution_lifecycle_authority_path="test",
            runtime_service_authority_path="test",
            market_data_authority_path="test",
            strategy_decision_authority_path="test",
            portfolio_account_authority_path="test",
            risk_sizing_authority_path="test",
            execution_mode_authority_path="test",
            trading_day_closure_authority_path="test",
            runtime_service_state="OK",
            market_data_state="FRESH",
            market_data_operator_impact="NONE",
            strategy_decision_state="INTENT_CREATED",
            strategy_intent_count="1",
            strategy_zero_intent_reason="<none>",
            portfolio_account_state="OK",
            portfolio_cash_total_cents="10000000",
            portfolio_net_liquidation_cents="10000000",
            risk_sizing_state="ROUNDED",
            risk_final_size="{}",
            execution_mode_state="PAPER_TRANSMIT_ENABLED",
            execution_mode_environment="PAPER",
            broker_transmit_enabled="false",
            trading_day_closure_state="OPEN",
            current_head_path="test",
            submission_index_path="test",
            evidence="test",
        ),
    )


def _packet_status_section(text: str) -> str:
    start = text.index("## Aegis Paper-Trading Status")
    end = text.index("## Latest Paper-Trade Attempt")
    return text[start:end]


def test_packet_reports_dirty_source_not_reproducible(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_ready_packet_runtime(monkeypatch)
    monkeypatch.setattr(
        packet_tool,
        "evaluate_canonical_cleanliness_v1",
        lambda _repo_root: {"status": "DIRTY", "dirty_path_count": 1, "dirty_paths": ["dirty.py"]},
    )
    monkeypatch.setattr(packet_tool, "_git_status_short_lines", lambda: [" M dirty.py"])
    monkeypatch.setattr(packet_tool, "_git_diff_name_only_lines", lambda: ["dirty.py"])
    monkeypatch.setattr(packet_tool, "read_protection_status_v1", lambda: {"status": "UNPROTECTED"})
    _export_id, text = packet_tool._build_packet()
    assert "source_reproducibility_status: NOT_REPRODUCIBLE_DIRTY_WORKTREE" in text
    section = _packet_status_section(text)
    assert "- status: BLOCKED" in section
    assert "- canonical_blocker: SOURCE_REPRODUCIBILITY_BLOCKED" in section
    assert "- owning_subsystem: source_reproducibility_authority" in section
    assert "- owning_gate: pre_submit_source_integrity_gate" in section


def test_packet_tracked_dirty_file_blocks_paper_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_ready_packet_runtime(monkeypatch)
    monkeypatch.setattr(
        packet_tool,
        "evaluate_canonical_cleanliness_v1",
        lambda _repo_root: {"status": "DIRTY", "dirty_path_count": 1, "dirty_paths": ["constellation_2/common/x.py"]},
    )
    monkeypatch.setattr(packet_tool, "_git_status_short_lines", lambda: [" M constellation_2/common/x.py"])
    monkeypatch.setattr(packet_tool, "_git_diff_name_only_lines", lambda: ["constellation_2/common/x.py"])
    monkeypatch.setattr(packet_tool, "read_protection_status_v1", lambda: {"status": "PROTECTED"})
    _export_id, text = packet_tool._build_packet()
    section = _packet_status_section(text)
    assert "- status: BLOCKED" in section
    assert "- canonical_blocker: SOURCE_REPRODUCIBILITY_BLOCKED" in section


def test_packet_untracked_file_blocks_paper_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_ready_packet_runtime(monkeypatch)
    monkeypatch.setattr(
        packet_tool,
        "evaluate_canonical_cleanliness_v1",
        lambda _repo_root: {"status": "DIRTY", "dirty_path_count": 1, "dirty_paths": ["ops/tools/new_tool.py"]},
    )
    monkeypatch.setattr(packet_tool, "_git_status_short_lines", lambda: ["?? ops/tools/new_tool.py"])
    monkeypatch.setattr(packet_tool, "_git_diff_name_only_lines", lambda: [])
    monkeypatch.setattr(packet_tool, "read_protection_status_v1", lambda: {"status": "PROTECTED"})
    _export_id, text = packet_tool._build_packet()
    section = _packet_status_section(text)
    assert "- status: BLOCKED" in section
    assert "- canonical_blocker: SOURCE_REPRODUCIBILITY_BLOCKED" in section


def test_packet_unprotected_repo_blocks_paper_ready_when_tree_clean(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_ready_packet_runtime(monkeypatch)
    monkeypatch.setattr(
        packet_tool,
        "evaluate_canonical_cleanliness_v1",
        lambda _repo_root: {"status": "CLEAN", "dirty_path_count": 0, "dirty_paths": []},
    )
    monkeypatch.setattr(packet_tool, "_git_status_short_lines", lambda: [])
    monkeypatch.setattr(packet_tool, "_git_diff_name_only_lines", lambda: [])
    monkeypatch.setattr(packet_tool, "read_protection_status_v1", lambda: {"status": "UNPROTECTED"})
    _export_id, text = packet_tool._build_packet()
    section = _packet_status_section(text)
    assert "source_reproducibility_status: REPRODUCIBLE" in text
    assert "- status: BLOCKED" in section
    assert "- canonical_blocker: CANONICAL_REPO_PROTECTION_BLOCKED" in section
    assert "- owning_subsystem: canonical_repo_protection_authority" in section
    assert "- owning_gate: pre_submit_source_integrity_gate" in section


def test_packet_ready_cannot_appear_when_source_not_reproducible(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_ready_packet_runtime(monkeypatch)
    monkeypatch.setattr(
        packet_tool,
        "evaluate_canonical_cleanliness_v1",
        lambda _repo_root: {"status": "DIRTY", "dirty_path_count": 1, "dirty_paths": ["dirty.py"]},
    )
    monkeypatch.setattr(packet_tool, "_git_status_short_lines", lambda: [" M dirty.py"])
    monkeypatch.setattr(packet_tool, "_git_diff_name_only_lines", lambda: ["dirty.py"])
    monkeypatch.setattr(packet_tool, "read_protection_status_v1", lambda: {"status": "PROTECTED"})
    _export_id, text = packet_tool._build_packet()
    section = _packet_status_section(text)
    assert "source_reproducibility_status: NOT_REPRODUCIBLE_DIRTY_WORKTREE" in text
    assert "- status: READY" not in section
    assert "- status: DEGRADED_READY" not in section
    assert "- status: BLOCKED" in section


def test_packet_includes_effective_source_integrity_gate_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_ready_packet_runtime(monkeypatch)
    monkeypatch.setattr(
        packet_tool,
        "evaluate_canonical_cleanliness_v1",
        lambda _repo_root: {"status": "DIRTY", "dirty_path_count": 1, "dirty_paths": ["dirty.py"]},
    )
    monkeypatch.setattr(packet_tool, "_git_status_short_lines", lambda: [" M dirty.py"])
    monkeypatch.setattr(packet_tool, "_git_diff_name_only_lines", lambda: ["dirty.py"])
    monkeypatch.setattr(packet_tool, "read_protection_status_v1", lambda: {"status": "UNPROTECTED"})
    _export_id, text = packet_tool._build_packet()
    assert "effective_source_integrity_gate_status: BLOCKED" in text
    assert "effective_source_integrity_gate_blocker: SOURCE_REPRODUCIBILITY_BLOCKED" in text
    assert "effective_source_integrity_gate_owner: source_reproducibility_authority" in text
    assert "raw_git_dirty_status: DIRTY" in text
    assert "raw_dirty_path_count: 1" in text
    assert "raw_source_reproducibility_status: NOT_REPRODUCIBLE_DIRTY_WORKTREE" in text
    assert "raw_canonical_repo_protection_status: UNPROTECTED" in text


def test_packet_reports_clean_protected_source_reproducible(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_ready_packet_runtime(monkeypatch)
    monkeypatch.setattr(
        packet_tool,
        "evaluate_canonical_cleanliness_v1",
        lambda _repo_root: {"status": "CLEAN", "dirty_path_count": 0, "dirty_paths": []},
    )
    monkeypatch.setattr(packet_tool, "_git_status_short_lines", lambda: [])
    monkeypatch.setattr(packet_tool, "_git_diff_name_only_lines", lambda: [])
    monkeypatch.setattr(packet_tool, "read_protection_status_v1", lambda: {"status": "PROTECTED"})
    _export_id, text = packet_tool._build_packet()
    assert "source_reproducibility_status: REPRODUCIBLE" in text
    assert "effective_source_integrity_gate_status: PASSED" in text


def test_runtime_output_path_guard_rejects_repo_runtime() -> None:
    with pytest.raises(SystemExit, match="forbidden"):
        require_runtime_output_outside_repo_runtime_v1(
            Path("/home/node/constellation/runtime/exports/aegis_state/latest")
        )


def test_runtime_generated_files_do_not_default_under_repo_root() -> None:
    source_root = Path("/home/node/constellation").resolve()
    generated_paths = [
        packet_tool.LATEST_PACKET_PATH,
        packet_tool.ARCHIVE_ROOT,
        supervisor_tool.PROCESS_STATE_ROOT,
        supervisor_tool.LOG_ROOT,
        trading_prepare_tool.DECISION_PATH,
        trading_preflight_tool.PREPARE_DECISION_PATH,
        preflight_tool._dry_run_reset_marker_path_v1(
            execution_truth_root=Path("/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER"),
            day_utc="2026-04-28",
        ),
    ]
    for generated_path in generated_paths:
        resolved = Path(generated_path).resolve()
        assert resolved != source_root
        assert not str(resolved).startswith(str(source_root) + "/")
