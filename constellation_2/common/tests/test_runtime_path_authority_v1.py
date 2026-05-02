from __future__ import annotations

from pathlib import Path

import pytest

import constellation_2.common.runtime_path_authority_v1 as runtime_path_module
from constellation_2.common.runtime_path_authority_v1 import (
    RuntimePathAuthorityV1,
    require_authoritative_repo_runtime_v1,
    resolve_decision_truth_root_v1,
)


def test_decision_truth_root_defaults_to_canonical_runtime_truth(monkeypatch: pytest.MonkeyPatch) -> None:
    authority = RuntimePathAuthorityV1(
        authoritative_repo_root=Path("/tmp/repo"),
        canonical_runtime_truth_root=Path("/tmp/truth"),
        canonical_runtime_truth_sleeves_root=Path("/tmp/truth_sleeves"),
        authoritative_repo_truth_root=Path("/tmp/repo/constellation_2/runtime/truth"),
        authoritative_repo_truth_sleeves_root=Path("/tmp/repo/constellation_2/runtime/truth_sleeves"),
        active_release_root=Path("/tmp/release"),
    )
    monkeypatch.setattr(runtime_path_module, "load_runtime_path_authority_v1", lambda repo_root=None: authority)
    assert resolve_decision_truth_root_v1("", repo_root=Path("/home/node/constellation")) == authority.canonical_runtime_truth_root


def test_phase_controlled_production_truth_root_is_allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    authority = RuntimePathAuthorityV1(
        authoritative_repo_root=Path("/tmp/repo"),
        canonical_runtime_truth_root=Path("/tmp/truth"),
        canonical_runtime_truth_sleeves_root=Path("/tmp/truth_sleeves"),
        authoritative_repo_truth_root=Path("/tmp/repo/constellation_2/runtime/truth"),
        authoritative_repo_truth_sleeves_root=Path("/tmp/repo/constellation_2/runtime/truth_sleeves"),
        active_release_root=Path("/tmp/release"),
    )
    monkeypatch.setattr(runtime_path_module, "load_runtime_path_authority_v1", lambda repo_root=None: authority)

    assert resolve_decision_truth_root_v1(
        "/home/node/constellation_runtime_data/production_truth",
        repo_root=Path("/home/node/constellation"),
    ) == Path("/home/node/constellation_runtime_data/production_truth")


def test_shadow_repo_root_is_rejected_for_authoritative_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runtime_path_module, "resolve_authoritative_repo_root_v1", lambda _repo: Path("/home/node/constellation"))
    monkeypatch.setattr(runtime_path_module, "load_release_current_runtime_authority_v1", lambda caller: {"release_root": "/home/node/constellation_releases/rel-001"})
    with pytest.raises(SystemExit, match="AUTHORITATIVE_REPO_RUNTIME_REQUIRED"):
        require_authoritative_repo_runtime_v1(Path("/home/node/constellation_2_runtime"))


def test_authoritative_repo_root_is_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runtime_path_module, "resolve_authoritative_repo_root_v1", lambda _repo: Path("/home/node/constellation"))
    assert require_authoritative_repo_runtime_v1(Path("/home/node/constellation")) == Path("/home/node/constellation")


def test_active_release_root_is_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    active_release_root = Path("/home/node/constellation_releases/release-123")
    monkeypatch.setattr(runtime_path_module, "resolve_authoritative_repo_root_v1", lambda _repo: Path("/home/node/constellation"))
    monkeypatch.setattr(runtime_path_module, "load_release_current_runtime_authority_v1", lambda caller: {"release_root": str(active_release_root)})
    assert require_authoritative_repo_runtime_v1(active_release_root) == Path("/home/node/constellation")


def test_non_current_release_root_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runtime_path_module, "resolve_authoritative_repo_root_v1", lambda _repo: Path("/home/node/constellation"))
    monkeypatch.setattr(runtime_path_module, "load_release_current_runtime_authority_v1", lambda caller: {"release_root": "/home/node/constellation_releases/release-current"})
    with pytest.raises(SystemExit, match="AUTHORITATIVE_REPO_RUNTIME_REQUIRED"):
        require_authoritative_repo_runtime_v1(Path("/home/node/constellation_releases/release-old"))
