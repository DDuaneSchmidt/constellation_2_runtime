from __future__ import annotations

import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation").resolve()
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

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


def test_shadow_repo_root_is_rejected_for_authoritative_runtime() -> None:
    with pytest.raises(SystemExit, match="AUTHORITATIVE_REPO_RUNTIME_REQUIRED"):
        require_authoritative_repo_runtime_v1(Path("/home/node/constellation_2_runtime"))


def test_authoritative_repo_root_is_accepted() -> None:
    assert require_authoritative_repo_runtime_v1(Path("/home/node/constellation")) == Path("/home/node/constellation")
