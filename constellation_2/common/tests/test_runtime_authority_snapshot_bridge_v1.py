from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.runtime_path_authority_v1 import RuntimePathAuthorityV1

import constellation_2.common.runtime_authority_snapshot_bridge_v1 as bridge


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


def _release_current_runtime_authority_payload(
    *,
    tmp_path: Path,
    authoritative_repo_root: Path,
) -> dict[str, str]:
    canonical_runtime_truth_root = (tmp_path / "new" / "truth").resolve()
    canonical_runtime_truth_sleeves_root = (tmp_path / "new" / "truth_sleeves").resolve()
    active_release_root = (tmp_path / "new" / "release").resolve()
    for path in (
        canonical_runtime_truth_root,
        canonical_runtime_truth_sleeves_root,
        active_release_root,
    ):
        path.mkdir(parents=True, exist_ok=True)
    return {
        "source": "release_current_v1",
        "release_current_id": "f" * 64,
        "authoritative_repo_root": str(authoritative_repo_root.resolve()),
        "canonical_truth_root": str(canonical_runtime_truth_root),
        "truth_sleeves_root": str(canonical_runtime_truth_sleeves_root),
        "release_root": str(active_release_root),
    }


def test_bridge_uses_release_current_when_valid(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    old_authority = _legacy_authority(tmp_path=tmp_path)
    old_snapshot = _snapshot_from_authority(old_authority)
    new_payload = _release_current_runtime_authority_payload(
        tmp_path=tmp_path,
        authoritative_repo_root=old_authority.authoritative_repo_root,
    )

    monkeypatch.setattr(bridge, "load_runtime_path_authority_v1", lambda repo_root=None: old_authority)
    monkeypatch.setattr(bridge, "resolve_runtime_path_authority_snapshot_v1", lambda repo_root=None: dict(old_snapshot))
    monkeypatch.setattr(bridge, "load_release_current_runtime_authority_v1", lambda caller="": dict(new_payload))
    monkeypatch.setattr(bridge, "require_truth_root_under_contract", lambda truth_root: Path(truth_root).resolve())

    authority = bridge.load_runtime_path_authority_bridge_v1(repo_root=SOURCE_ROOT, caller="test_snapshot_bridge_valid")
    assert authority.authoritative_repo_root == old_authority.authoritative_repo_root
    assert authority.canonical_runtime_truth_root == Path(new_payload["canonical_truth_root"]).resolve()
    assert authority.canonical_runtime_truth_sleeves_root == Path(new_payload["truth_sleeves_root"]).resolve()
    assert authority.active_release_root == Path(new_payload["release_root"]).resolve()

    snapshot = bridge.resolve_runtime_path_authority_snapshot_bridge_v1(
        repo_root=SOURCE_ROOT,
        caller="test_snapshot_bridge_valid",
    )
    assert snapshot["authoritative_repo_root"] == str(old_authority.authoritative_repo_root)
    assert snapshot["canonical_runtime_truth_root"] == str(Path(new_payload["canonical_truth_root"]).resolve())
    assert snapshot["canonical_runtime_truth_sleeves_root"] == str(Path(new_payload["truth_sleeves_root"]).resolve())
    assert snapshot["active_release_root"] == str(Path(new_payload["release_root"]).resolve())


def test_bridge_falls_back_when_release_current_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    old_authority = _legacy_authority(tmp_path=tmp_path)
    old_snapshot = _snapshot_from_authority(old_authority)

    monkeypatch.setattr(bridge, "load_runtime_path_authority_v1", lambda repo_root=None: old_authority)
    monkeypatch.setattr(bridge, "resolve_runtime_path_authority_snapshot_v1", lambda repo_root=None: dict(old_snapshot))
    monkeypatch.setattr(bridge, "load_release_current_runtime_authority_v1", lambda caller="": {"source": "legacy_fallback"})
    monkeypatch.setattr(bridge, "require_truth_root_under_contract", lambda truth_root: Path(truth_root).resolve())

    authority = bridge.load_runtime_path_authority_bridge_v1(repo_root=SOURCE_ROOT, caller="test_snapshot_bridge_missing")
    assert authority == old_authority
    snapshot = bridge.resolve_runtime_path_authority_snapshot_bridge_v1(
        repo_root=SOURCE_ROOT,
        caller="test_snapshot_bridge_missing",
    )
    assert snapshot == old_snapshot


def test_bridge_fails_closed_when_release_current_invalid(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    old_authority = _legacy_authority(tmp_path=tmp_path)
    old_snapshot = _snapshot_from_authority(old_authority)

    monkeypatch.setattr(bridge, "load_runtime_path_authority_v1", lambda repo_root=None: old_authority)
    monkeypatch.setattr(bridge, "resolve_runtime_path_authority_snapshot_v1", lambda repo_root=None: dict(old_snapshot))
    monkeypatch.setattr(
        bridge,
        "load_release_current_runtime_authority_v1",
        lambda caller="": (_ for _ in ()).throw(
            SystemExit("FAIL: runtime_authority_bridge_release_current_invalid path=/tmp/current.json err=ValueError:bad")
        ),
    )
    monkeypatch.setattr(bridge, "require_truth_root_under_contract", lambda truth_root: Path(truth_root).resolve())

    with pytest.raises(SystemExit, match="runtime_authority_bridge_release_current_invalid"):
        bridge.resolve_runtime_path_authority_snapshot_bridge_v1(
            repo_root=SOURCE_ROOT,
            caller="test_snapshot_bridge_invalid",
        )


def test_bridge_logs_mismatches(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    old_authority = _legacy_authority(tmp_path=tmp_path)
    old_snapshot = _snapshot_from_authority(old_authority)
    new_payload = _release_current_runtime_authority_payload(
        tmp_path=tmp_path,
        authoritative_repo_root=old_authority.authoritative_repo_root,
    )

    monkeypatch.setattr(bridge, "load_runtime_path_authority_v1", lambda repo_root=None: old_authority)
    monkeypatch.setattr(bridge, "resolve_runtime_path_authority_snapshot_v1", lambda repo_root=None: dict(old_snapshot))
    monkeypatch.setattr(bridge, "load_release_current_runtime_authority_v1", lambda caller="": dict(new_payload))
    monkeypatch.setattr(bridge, "require_truth_root_under_contract", lambda truth_root: Path(truth_root).resolve())

    caplog.set_level("WARNING", logger="constellation_2.common.runtime_authority_snapshot_bridge_v1")
    _ = bridge.resolve_runtime_path_authority_snapshot_bridge_v1(
        repo_root=SOURCE_ROOT,
        caller="test_snapshot_bridge_mismatch",
    )
    messages = [record.message for record in caplog.records if "runtime_authority_snapshot_bridge_mismatch" in record.message]
    assert messages
    payload = json.loads(messages[-1].split("runtime_authority_snapshot_bridge_mismatch ", 1)[1])
    fields = {entry["field"] for entry in payload["mismatches"]}
    assert {"canonical_runtime_truth_root", "canonical_runtime_truth_sleeves_root", "active_release_root"}.issubset(fields)
    assert payload["release_current_id"] == "f" * 64


def test_bridge_snapshot_shape_matches_legacy(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    old_authority = _legacy_authority(tmp_path=tmp_path)
    old_snapshot = _snapshot_from_authority(old_authority)
    new_payload = _release_current_runtime_authority_payload(
        tmp_path=tmp_path,
        authoritative_repo_root=old_authority.authoritative_repo_root,
    )

    monkeypatch.setattr(bridge, "load_runtime_path_authority_v1", lambda repo_root=None: old_authority)
    monkeypatch.setattr(bridge, "resolve_runtime_path_authority_snapshot_v1", lambda repo_root=None: dict(old_snapshot))
    monkeypatch.setattr(bridge, "load_release_current_runtime_authority_v1", lambda caller="": dict(new_payload))
    monkeypatch.setattr(bridge, "require_truth_root_under_contract", lambda truth_root: Path(truth_root).resolve())

    snapshot = bridge.resolve_runtime_path_authority_snapshot_bridge_v1(
        repo_root=SOURCE_ROOT,
        caller="test_snapshot_bridge_shape",
    )
    assert tuple(snapshot.keys()) == tuple(old_snapshot.keys())
    assert set(snapshot.keys()) == {
        "authoritative_repo_root",
        "canonical_runtime_truth_root",
        "canonical_runtime_truth_sleeves_root",
        "authoritative_repo_truth_root",
        "authoritative_repo_truth_sleeves_root",
        "active_release_root",
    }
