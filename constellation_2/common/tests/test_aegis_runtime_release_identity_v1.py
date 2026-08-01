from __future__ import annotations

import json
from pathlib import Path

import ops.tools.aegis_submit_enforcement_v1 as enforcement


COMMIT = "a" * 40


def _missing_git(*_args, **_kwargs):  # noqa: ANN002, ANN003
    raise FileNotFoundError("git")


def test_immutable_release_manifest_supplies_source_identity_without_git(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    manifest = tmp_path / "release-manifest.json"
    manifest.write_text(json.dumps({"source_commit": COMMIT}), encoding="utf-8")
    monkeypatch.setenv("AEGIS_RELEASE_MANIFEST", str(manifest))
    monkeypatch.setattr(enforcement.subprocess, "run", _missing_git)

    assert enforcement._git_commit() == COMMIT
    assert enforcement._git_dirty_status() == "CLEAN"


def test_missing_runtime_source_identity_fails_closed_without_git(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.delenv("AEGIS_RELEASE_MANIFEST", raising=False)
    monkeypatch.setattr(enforcement.subprocess, "run", _missing_git)

    assert enforcement._git_commit() == ""
    assert enforcement._git_dirty_status() == "DIRTY"


def test_invalid_manifest_commit_fails_closed(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    manifest = tmp_path / "release-manifest.json"
    manifest.write_text(json.dumps({"source_commit": "not-a-commit"}), encoding="utf-8")
    monkeypatch.setenv("AEGIS_RELEASE_MANIFEST", str(manifest))
    monkeypatch.setattr(enforcement.subprocess, "run", _missing_git)

    assert enforcement._git_commit() == ""
    assert enforcement._git_dirty_status() == "DIRTY"
