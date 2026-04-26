from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path(__file__).resolve().parents[3]
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.apply_codex_patch_bundle_v1 as apply_tool
import ops.tools.run_readiness_freeze_preflight_v1 as freeze_tool
import ops.tools.verify_patch_bundle_base_commit_v1 as verify_tool


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


def _head(repo: Path) -> str:
    return _run(["git", "rev-parse", "HEAD"], repo).stdout.strip()


def _base_manifest(*, task_id: str, base_commit: str, changed_files: list[str] | None = None) -> dict:
    return {
        "task_id": task_id,
        "base_commit": base_commit,
        "created_utc": "2026-04-26T00:00:00Z",
        "forbidden_paths_touched": [],
        "validation_scope": "READINESS_FREEZE",
        "authoritative_runtime_validation": False,
        "changed_files": list(changed_files or []),
        "tests": [],
    }


def _write_bundle(
    *,
    inbox_root: Path,
    task_id: str,
    manifest: dict,
    patch_text: str = "",
    validation_report: str = "validation_source:\n- workspace\nauthoritative_runtime_evidence:\n- no\n",
) -> Path:
    bundle = (inbox_root / task_id).resolve()
    bundle.mkdir(parents=True, exist_ok=True)
    (bundle / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    (bundle / "changes.patch").write_text(patch_text, encoding="utf-8")
    (bundle / "validation_report.md").write_text(validation_report, encoding="utf-8")
    return bundle


def _prepare_active_release(*, tmp_path: Path) -> tuple[Path, Path, Path]:
    releases_root = (tmp_path / "releases").resolve()
    releases_root.mkdir(parents=True, exist_ok=True)
    latest = (releases_root / "20260426T010000Z__abc").resolve()
    latest.mkdir(parents=True, exist_ok=True)
    active_link = (tmp_path / "constellation_active").resolve()
    if active_link.exists() or active_link.is_symlink():
        active_link.unlink()
    active_link.symlink_to(latest, target_is_directory=True)
    return releases_root, active_link, latest


def test_patch_intake_rejects_missing_base_commit(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    repo = _init_git_repo(tmp_path)
    inbox = (tmp_path / "inbox").resolve()
    manifest = _base_manifest(task_id="task", base_commit=_head(repo))
    manifest.pop("base_commit")
    _write_bundle(inbox_root=inbox, task_id="task", manifest=manifest)

    monkeypatch.setattr(apply_tool, "CANONICAL_REPO_ROOT", repo)
    monkeypatch.setattr(apply_tool, "PATCH_INBOX_ROOT", inbox)
    monkeypatch.setattr(apply_tool, "git_status_porcelain_paths_v1", lambda _repo_root: [])
    with pytest.raises(SystemExit, match="PATCH_BUNDLE_MANIFEST_FIELD_MISSING:base_commit"):
        apply_tool.main(["--task_id", "task"])


def test_patch_intake_rejects_mismatched_base_commit(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    repo = _init_git_repo(tmp_path)
    inbox = (tmp_path / "inbox").resolve()
    stale_manifest = _base_manifest(task_id="task", base_commit="deadbeef")
    _write_bundle(inbox_root=inbox, task_id="task", manifest=stale_manifest)

    monkeypatch.setattr(apply_tool, "CANONICAL_REPO_ROOT", repo)
    monkeypatch.setattr(apply_tool, "PATCH_INBOX_ROOT", inbox)
    monkeypatch.setattr(apply_tool, "git_status_porcelain_paths_v1", lambda _repo_root: [])
    with pytest.raises(SystemExit, match="PATCH_BUNDLE_BASE_COMMIT_MISMATCH"):
        apply_tool.main(["--task_id", "task"])


def test_patch_intake_accepts_matching_base_commit(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    repo = _init_git_repo(tmp_path)
    inbox = (tmp_path / "inbox").resolve()
    start = _head(repo)

    seed = (repo / "seed.txt")
    seed.write_text("seed changed\n", encoding="utf-8")
    patch_text = _run(["git", "diff"], repo).stdout
    seed.write_text("seed\n", encoding="utf-8")
    assert _run(["git", "status", "--short"], repo).stdout.strip() == ""

    manifest = _base_manifest(task_id="task", base_commit=start, changed_files=["seed.txt"])
    _write_bundle(inbox_root=inbox, task_id="task", manifest=manifest, patch_text=patch_text)

    monkeypatch.setattr(apply_tool, "CANONICAL_REPO_ROOT", repo)
    monkeypatch.setattr(apply_tool, "PATCH_INBOX_ROOT", inbox)
    monkeypatch.setattr(apply_tool, "_unprotect_for_intake", lambda _task_id: None)
    monkeypatch.setattr(apply_tool, "_protect_after_intake", lambda: None)

    rc = apply_tool.main(["--task_id", "task", "--commit_message", "apply patch"])
    assert rc == 0
    assert _head(repo) != start
    assert "seed changed" in seed.read_text(encoding="utf-8")


def test_verify_tool_passes_on_matching_head(tmp_path: Path) -> None:
    repo = _init_git_repo(tmp_path)
    inbox = (tmp_path / "inbox").resolve()
    manifest = _base_manifest(task_id="task", base_commit=_head(repo))
    _write_bundle(inbox_root=inbox, task_id="task", manifest=manifest)

    payload = verify_tool.evaluate_patch_bundle_base_commit_v1(
        task_id="task",
        canonical_repo_root=repo,
        patch_inbox_root=inbox,
    )
    assert payload["status"] == "PASS"


def test_verify_tool_fails_on_stale_bundle(tmp_path: Path) -> None:
    repo = _init_git_repo(tmp_path)
    inbox = (tmp_path / "inbox").resolve()
    manifest = _base_manifest(task_id="task", base_commit="deadbeef")
    _write_bundle(inbox_root=inbox, task_id="task", manifest=manifest)

    payload = verify_tool.evaluate_patch_bundle_base_commit_v1(
        task_id="task",
        canonical_repo_root=repo,
        patch_inbox_root=inbox,
    )
    assert payload["status"] == "FAIL"
    assert payload["code"] == "PATCH_BUNDLE_BASE_COMMIT_MISMATCH"


def test_readiness_preflight_fails_dirty_canonical(tmp_path: Path) -> None:
    repo = _init_git_repo(tmp_path)
    (repo / "seed.txt").write_text("dirty\n", encoding="utf-8")
    inbox = (tmp_path / "inbox").resolve()
    inbox.mkdir(parents=True, exist_ok=True)
    releases_root, active_link, _latest = _prepare_active_release(tmp_path=tmp_path)
    payload = freeze_tool.evaluate_readiness_freeze_preflight_v1(
        canonical_repo_root=repo,
        patch_inbox_root=inbox,
        releases_root=releases_root,
        active_release_link=active_link,
    )
    assert payload["status"] == "FAIL"
    assert any(item["code"] == "READINESS_FREEZE_CANONICAL_DIRTY" for item in payload["violations"])


def test_readiness_preflight_fails_multiple_pending_readiness_bundles(tmp_path: Path) -> None:
    repo = _init_git_repo(tmp_path)
    inbox = (tmp_path / "inbox").resolve()
    head = _head(repo)
    manifest_a = _base_manifest(
        task_id="task_a",
        base_commit=head,
        changed_files=["ops/tools/run_submit_boundary_status_v1.py"],
    )
    manifest_b = _base_manifest(
        task_id="task_b",
        base_commit=head,
        changed_files=["ops/tools/run_paper_session_ledger_v1.py"],
    )
    _write_bundle(inbox_root=inbox, task_id="task_a", manifest=manifest_a)
    _write_bundle(inbox_root=inbox, task_id="task_b", manifest=manifest_b)
    releases_root, active_link, _latest = _prepare_active_release(tmp_path=tmp_path)

    payload = freeze_tool.evaluate_readiness_freeze_preflight_v1(
        canonical_repo_root=repo,
        patch_inbox_root=inbox,
        releases_root=releases_root,
        active_release_link=active_link,
    )
    assert payload["status"] == "FAIL"
    assert any(
        item["code"] == "READINESS_FREEZE_MULTIPLE_PENDING_READINESS_BUNDLES"
        for item in payload["violations"]
    )


def test_readiness_preflight_fails_workspace_authoritative_runtime_claim(tmp_path: Path) -> None:
    repo = _init_git_repo(tmp_path)
    inbox = (tmp_path / "inbox").resolve()
    manifest = _base_manifest(
        task_id="task",
        base_commit=_head(repo),
        changed_files=["ops/tools/run_submit_boundary_status_v1.py"],
    )
    validation = (
        "validation_source:\n"
        "- workspace\n"
        "authoritative_runtime_evidence:\n"
        "- yes\n"
        "proof_path: /home/node/constellation_agent_workspace/task/runtime.json\n"
    )
    _write_bundle(inbox_root=inbox, task_id="task", manifest=manifest, validation_report=validation)
    releases_root, active_link, _latest = _prepare_active_release(tmp_path=tmp_path)

    payload = freeze_tool.evaluate_readiness_freeze_preflight_v1(
        canonical_repo_root=repo,
        patch_inbox_root=inbox,
        releases_root=releases_root,
        active_release_link=active_link,
    )
    assert payload["status"] == "FAIL"
    assert any(
        item["code"] == "READINESS_FREEZE_WORKSPACE_RUNTIME_CLAIM_FORBIDDEN"
        for item in payload["violations"]
    )


def test_readiness_preflight_blocks_unrelated_feature_paths(tmp_path: Path) -> None:
    repo = _init_git_repo(tmp_path)
    inbox = (tmp_path / "inbox").resolve()
    manifest = _base_manifest(
        task_id="task",
        base_commit=_head(repo),
        changed_files=[
            "ops/tools/run_submit_boundary_status_v1.py",
            "constellation_2/phaseL/ui/static/index.html",
        ],
    )
    _write_bundle(inbox_root=inbox, task_id="task", manifest=manifest)
    releases_root, active_link, _latest = _prepare_active_release(tmp_path=tmp_path)

    payload = freeze_tool.evaluate_readiness_freeze_preflight_v1(
        canonical_repo_root=repo,
        patch_inbox_root=inbox,
        releases_root=releases_root,
        active_release_link=active_link,
    )
    assert payload["status"] == "FAIL"
    assert any(item["code"] == "READINESS_FREEZE_DISALLOWED_PATH" for item in payload["violations"])


def test_readiness_preflight_requires_validation_source_label(tmp_path: Path) -> None:
    repo = _init_git_repo(tmp_path)
    inbox = (tmp_path / "inbox").resolve()
    manifest = _base_manifest(
        task_id="task",
        base_commit=_head(repo),
        changed_files=["ops/tools/run_submit_boundary_status_v1.py"],
    )
    validation = "authoritative_runtime_evidence:\n- no\n"
    _write_bundle(inbox_root=inbox, task_id="task", manifest=manifest, validation_report=validation)
    releases_root, active_link, _latest = _prepare_active_release(tmp_path=tmp_path)

    payload = freeze_tool.evaluate_readiness_freeze_preflight_v1(
        canonical_repo_root=repo,
        patch_inbox_root=inbox,
        releases_root=releases_root,
        active_release_link=active_link,
    )
    assert payload["status"] == "FAIL"
    assert any(
        item["code"] == "READINESS_FREEZE_VALIDATION_SOURCE_MISSING"
        for item in payload["violations"]
    )
