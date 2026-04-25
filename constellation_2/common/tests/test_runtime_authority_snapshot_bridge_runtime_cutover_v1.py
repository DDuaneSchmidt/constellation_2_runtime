from __future__ import annotations

import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.runtime_path_authority_v1 import RuntimePathAuthorityV1

import constellation_2.common.runtime_authority_snapshot_bridge_v1 as bridge


MIGRATED_RUNTIME_CRITICAL_SNAPSHOT_SET = {
    "constellation_2/common/fresh_day_admission_v1.py": "constellation_2/common/fresh_day_admission_v1.py",
    "ops/tools/run_paper_session_bootstrap_v1.py": "ops/tools/run_paper_session_bootstrap_v1.py",
}


RUNTIME_IDENTITY_BOOTSTRAP_SYSTEMD_UNCHANGED_SET = {
    "constellation_2/common/runtime_identity_v1.py",
    "ops/tools/run_c2_paper_day_orchestrator_v2.py",
    "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
}


def _read(relpath: str) -> str:
    return (SOURCE_ROOT / relpath).resolve().read_text(encoding="utf-8")


def _legacy_authority(*, tmp_path: Path) -> RuntimePathAuthorityV1:
    authoritative_repo_root = (tmp_path / "repo").resolve()
    canonical_runtime_truth_root = (tmp_path / "old" / "truth").resolve()
    canonical_runtime_truth_sleeves_root = (tmp_path / "old" / "truth_sleeves").resolve()
    active_release_root = (tmp_path / "old" / "release").resolve()
    authoritative_repo_truth_root = (authoritative_repo_root / "constellation_2" / "runtime" / "truth").resolve()
    authoritative_repo_truth_sleeves_root = (
        authoritative_repo_root / "constellation_2" / "runtime" / "truth_sleeves"
    ).resolve()
    for path in (
        authoritative_repo_root,
        canonical_runtime_truth_root,
        canonical_runtime_truth_sleeves_root,
        active_release_root,
        authoritative_repo_truth_root,
        authoritative_repo_truth_sleeves_root,
    ):
        path.mkdir(parents=True, exist_ok=True)
    return RuntimePathAuthorityV1(
        authoritative_repo_root=authoritative_repo_root,
        canonical_runtime_truth_root=canonical_runtime_truth_root,
        canonical_runtime_truth_sleeves_root=canonical_runtime_truth_sleeves_root,
        authoritative_repo_truth_root=authoritative_repo_truth_root,
        authoritative_repo_truth_sleeves_root=authoritative_repo_truth_sleeves_root,
        active_release_root=active_release_root,
    )


def _snapshot_from_authority(authority: RuntimePathAuthorityV1) -> dict[str, str]:
    return {
        "authoritative_repo_root": str(authority.authoritative_repo_root),
        "canonical_runtime_truth_root": str(authority.canonical_runtime_truth_root),
        "canonical_runtime_truth_sleeves_root": str(authority.canonical_runtime_truth_sleeves_root),
        "authoritative_repo_truth_root": str(authority.authoritative_repo_truth_root),
        "authoritative_repo_truth_sleeves_root": str(authority.authoritative_repo_truth_sleeves_root),
        "active_release_root": str(authority.active_release_root),
    }


def test_migrated_runtime_critical_snapshot_set_uses_snapshot_bridge() -> None:
    for relpath, caller_label in MIGRATED_RUNTIME_CRITICAL_SNAPSHOT_SET.items():
        text = _read(relpath)
        assert "runtime_authority_snapshot_bridge_v1" in text, relpath
        assert "load_runtime_path_authority_bridge_v1" in text, relpath
        assert f'caller="{caller_label}"' in text, relpath
        assert (
            "resolve_runtime_path_authority_snapshot_bridge_v1" in text
            or "load_runtime_path_authority_bridge_v1" in text
        ), relpath
        assert "resolve_runtime_path_authority_snapshot_v1(" not in text, relpath
        assert "load_runtime_path_authority_v1(" not in text, relpath


def test_runtime_identity_bootstrap_systemd_sets_remain_unchanged() -> None:
    for relpath in RUNTIME_IDENTITY_BOOTSTRAP_SYSTEMD_UNCHANGED_SET:
        text = _read(relpath)
        assert "runtime_authority_snapshot_bridge_v1" not in text, relpath
        assert "resolve_runtime_path_authority_snapshot_bridge_v1" not in text, relpath
        assert "load_runtime_path_authority_bridge_v1" not in text, relpath


def test_bridge_prefers_release_current_when_valid(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    old_authority = _legacy_authority(tmp_path=tmp_path)
    old_snapshot = _snapshot_from_authority(old_authority)
    new_truth = (tmp_path / "new" / "truth").resolve()
    new_sleeves = (tmp_path / "new" / "truth_sleeves").resolve()
    new_release = (tmp_path / "new" / "release").resolve()
    for path in (new_truth, new_sleeves, new_release):
        path.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(bridge, "load_runtime_path_authority_v1", lambda repo_root=None: old_authority)
    monkeypatch.setattr(bridge, "resolve_runtime_path_authority_snapshot_v1", lambda repo_root=None: dict(old_snapshot))
    monkeypatch.setattr(bridge, "require_truth_root_under_contract", lambda truth_root: Path(truth_root).resolve())
    monkeypatch.setattr(
        bridge,
        "load_release_current_runtime_authority_v1",
        lambda caller="": {
            "source": "release_current_v1",
            "release_current_id": "f" * 64,
            "authoritative_repo_root": str(old_authority.authoritative_repo_root),
            "canonical_truth_root": str(new_truth),
            "truth_sleeves_root": str(new_sleeves),
            "release_root": str(new_release),
        },
    )

    snapshot = bridge.resolve_runtime_path_authority_snapshot_bridge_v1(
        repo_root=SOURCE_ROOT,
        caller="constellation_2/common/fresh_day_admission_v1.py",
    )
    assert snapshot["canonical_runtime_truth_root"] == str(new_truth)
    assert snapshot["canonical_runtime_truth_sleeves_root"] == str(new_sleeves)
    assert snapshot["active_release_root"] == str(new_release)


def test_bridge_falls_back_when_release_current_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    old_authority = _legacy_authority(tmp_path=tmp_path)
    old_snapshot = _snapshot_from_authority(old_authority)
    monkeypatch.setattr(bridge, "load_runtime_path_authority_v1", lambda repo_root=None: old_authority)
    monkeypatch.setattr(bridge, "resolve_runtime_path_authority_snapshot_v1", lambda repo_root=None: dict(old_snapshot))
    monkeypatch.setattr(bridge, "require_truth_root_under_contract", lambda truth_root: Path(truth_root).resolve())
    monkeypatch.setattr(bridge, "load_release_current_runtime_authority_v1", lambda caller="": {"source": "legacy_fallback"})

    snapshot = bridge.resolve_runtime_path_authority_snapshot_bridge_v1(
        repo_root=SOURCE_ROOT,
        caller="constellation_2/common/fresh_day_admission_v1.py",
    )
    assert snapshot == old_snapshot


def test_bridge_fails_closed_when_release_current_invalid(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    old_authority = _legacy_authority(tmp_path=tmp_path)
    old_snapshot = _snapshot_from_authority(old_authority)
    monkeypatch.setattr(bridge, "load_runtime_path_authority_v1", lambda repo_root=None: old_authority)
    monkeypatch.setattr(bridge, "resolve_runtime_path_authority_snapshot_v1", lambda repo_root=None: dict(old_snapshot))
    monkeypatch.setattr(bridge, "require_truth_root_under_contract", lambda truth_root: Path(truth_root).resolve())
    monkeypatch.setattr(
        bridge,
        "load_release_current_runtime_authority_v1",
        lambda caller="": (_ for _ in ()).throw(
            SystemExit("FAIL: runtime_authority_bridge_release_current_invalid path=/tmp/current.json err=ValueError:bad")
        ),
    )

    with pytest.raises(SystemExit, match="runtime_authority_bridge_release_current_invalid"):
        bridge.resolve_runtime_path_authority_snapshot_bridge_v1(
            repo_root=SOURCE_ROOT,
            caller="constellation_2/common/fresh_day_admission_v1.py",
        )


def test_snapshot_shape_unchanged(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    old_authority = _legacy_authority(tmp_path=tmp_path)
    old_snapshot = _snapshot_from_authority(old_authority)
    monkeypatch.setattr(bridge, "load_runtime_path_authority_v1", lambda repo_root=None: old_authority)
    monkeypatch.setattr(bridge, "resolve_runtime_path_authority_snapshot_v1", lambda repo_root=None: dict(old_snapshot))
    monkeypatch.setattr(bridge, "require_truth_root_under_contract", lambda truth_root: Path(truth_root).resolve())
    monkeypatch.setattr(bridge, "load_release_current_runtime_authority_v1", lambda caller="": {"source": "legacy_fallback"})

    snapshot = bridge.resolve_runtime_path_authority_snapshot_bridge_v1(
        repo_root=SOURCE_ROOT,
        caller="constellation_2/common/fresh_day_admission_v1.py",
    )
    assert tuple(snapshot.keys()) == tuple(old_snapshot.keys())
