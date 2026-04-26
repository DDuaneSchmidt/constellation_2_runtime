from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.aegis_chatgpt_packet as packet_tool
import ops.tools.apply_codex_patch_bundle_v1 as apply_tool
import ops.tools.build_constellation_release_v1 as build_tool
import ops.tools.require_canonical_repo_clean_v1 as clean_tool
import ops.tools.verify_codex_agent_workspace_v1 as verify_tool
from ops.tools.repo_protection_common_v1 import (
    require_runtime_output_outside_repo_runtime_v1,
)


def _run(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True, check=False)


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
    (bundle / "manifest.json").write_text("{}", encoding="utf-8")
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


def test_packet_reports_dirty_source_not_reproducible(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(packet_tool, "_git_status_short_lines", lambda: [" M dirty.py"])
    monkeypatch.setattr(packet_tool, "_git_diff_name_only_lines", lambda: ["dirty.py"])
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
        "_build_paper_status",
        lambda roots: packet_tool.PaperStatus(
            section="## Aegis Paper-Trading Status\n\n- Aegis Paper-Trading Status: UNKNOWN\n",
            status="UNKNOWN",
            freshness_status="UNKNOWN",
            day_utc="UNKNOWN",
        ),
    )
    monkeypatch.setattr(packet_tool, "read_protection_status_v1", lambda: {"status": "UNPROTECTED"})
    _export_id, text = packet_tool._build_packet()
    assert "source_reproducibility_status: NOT_REPRODUCIBLE_DIRTY_WORKTREE" in text


def test_packet_reports_clean_protected_source_reproducible(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(packet_tool, "_git_status_short_lines", lambda: [])
    monkeypatch.setattr(packet_tool, "_git_diff_name_only_lines", lambda: [])
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
        "_build_paper_status",
        lambda roots: packet_tool.PaperStatus(
            section="## Aegis Paper-Trading Status\n\n- Aegis Paper-Trading Status: UNKNOWN\n",
            status="UNKNOWN",
            freshness_status="UNKNOWN",
            day_utc="UNKNOWN",
        ),
    )
    monkeypatch.setattr(packet_tool, "read_protection_status_v1", lambda: {"status": "PROTECTED"})
    _export_id, text = packet_tool._build_packet()
    assert "source_reproducibility_status: REPRODUCIBLE_CLEAN_SOURCE" in text


def test_runtime_output_path_guard_rejects_repo_runtime() -> None:
    with pytest.raises(SystemExit, match="forbidden"):
        require_runtime_output_outside_repo_runtime_v1(
            Path("/home/node/constellation/runtime/exports/aegis_state/latest")
        )
