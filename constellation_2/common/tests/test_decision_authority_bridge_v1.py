from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.runtime_path_authority_v1 import RuntimePathAuthorityV1

import constellation_2.common.decision_authority_bridge_v1 as bridge


def _legacy_authority(tmp_path: Path) -> RuntimePathAuthorityV1:
    authoritative_repo_root = SOURCE_ROOT.resolve()
    canonical_runtime_truth_root = (tmp_path / "old" / "truth").resolve()
    canonical_runtime_truth_sleeves_root = (tmp_path / "old" / "truth_sleeves").resolve()
    active_release_root = (tmp_path / "old" / "release").resolve()
    authoritative_repo_truth_root = (authoritative_repo_root / "constellation_2" / "runtime" / "truth").resolve()
    authoritative_repo_truth_sleeves_root = (
        authoritative_repo_root / "constellation_2" / "runtime" / "truth_sleeves"
    ).resolve()
    canonical_runtime_truth_root.mkdir(parents=True, exist_ok=True)
    canonical_runtime_truth_sleeves_root.mkdir(parents=True, exist_ok=True)
    active_release_root.mkdir(parents=True, exist_ok=True)
    return RuntimePathAuthorityV1(
        authoritative_repo_root=authoritative_repo_root,
        canonical_runtime_truth_root=canonical_runtime_truth_root,
        canonical_runtime_truth_sleeves_root=canonical_runtime_truth_sleeves_root,
        authoritative_repo_truth_root=authoritative_repo_truth_root,
        authoritative_repo_truth_sleeves_root=authoritative_repo_truth_sleeves_root,
        active_release_root=active_release_root,
    )


def _release_current_payload(
    *,
    tmp_path: Path,
    canonical_runtime_truth_root: Path,
    canonical_runtime_truth_sleeves_root: Path,
    active_release_root: Path,
    allowed_truth_roots: list[Path],
) -> dict[str, object]:
    canonical_runtime_truth_root.mkdir(parents=True, exist_ok=True)
    canonical_runtime_truth_sleeves_root.mkdir(parents=True, exist_ok=True)
    active_release_root.mkdir(parents=True, exist_ok=True)
    return {
        "source": "release_current_v1",
        "release_current_path": str((tmp_path / "new" / "truth" / "release_current_v1" / "current.json").resolve()),
        "release_current_id": "f" * 64,
        "authoritative_repo_root": str(SOURCE_ROOT.resolve()),
        "canonical_truth_root": str(canonical_runtime_truth_root.resolve()),
        "truth_sleeves_root": str(canonical_runtime_truth_sleeves_root.resolve()),
        "release_root": str(active_release_root.resolve()),
        "allowed_truth_roots": [str(path.resolve()) for path in allowed_truth_roots],
    }


def test_bridge_uses_release_current_when_valid(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    legacy = _legacy_authority(tmp_path)
    monkeypatch.setattr(bridge, "load_runtime_path_authority_v1", lambda repo_root=None: legacy)
    monkeypatch.setattr(
        bridge,
        "_load_old_allowed_truth_roots",
        lambda: (legacy.canonical_runtime_truth_root, legacy.canonical_runtime_truth_sleeves_root),
    )

    new_truth = (tmp_path / "new" / "truth").resolve()
    new_sleeves = (tmp_path / "new" / "truth_sleeves").resolve()
    new_release = (tmp_path / "new" / "release").resolve()
    payload = _release_current_payload(
        tmp_path=tmp_path,
        canonical_runtime_truth_root=new_truth,
        canonical_runtime_truth_sleeves_root=new_sleeves,
        active_release_root=new_release,
        allowed_truth_roots=[new_truth, new_sleeves],
    )
    monkeypatch.setattr(bridge, "load_release_current_runtime_authority_v1", lambda caller="": dict(payload))

    authority = bridge.load_decision_path_authority_bridge_v1(repo_root=SOURCE_ROOT, caller="test_bridge_valid")
    assert authority.source == "release_current_v1"
    assert authority.canonical_runtime_truth_root == new_truth
    assert authority.canonical_runtime_truth_sleeves_root == new_sleeves
    assert authority.active_release_root == new_release
    assert bridge.resolve_decision_truth_root_bridge_v1("", repo_root=SOURCE_ROOT, caller="test_bridge_valid") == new_truth
    snapshot = bridge.resolve_runtime_path_authority_snapshot_bridge_v1(repo_root=SOURCE_ROOT, caller="test_bridge_valid")
    assert snapshot["canonical_runtime_truth_root"] == str(new_truth)
    assert snapshot["canonical_runtime_truth_sleeves_root"] == str(new_sleeves)
    assert snapshot["active_release_root"] == str(new_release)


def test_bridge_falls_back_when_release_current_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    legacy = _legacy_authority(tmp_path)
    monkeypatch.setattr(bridge, "load_runtime_path_authority_v1", lambda repo_root=None: legacy)
    monkeypatch.setattr(
        bridge,
        "_load_old_allowed_truth_roots",
        lambda: (legacy.canonical_runtime_truth_root, legacy.canonical_runtime_truth_sleeves_root),
    )
    monkeypatch.setattr(
        bridge,
        "load_release_current_runtime_authority_v1",
        lambda caller="": {"source": "legacy_fallback"},
    )

    authority = bridge.load_decision_path_authority_bridge_v1(repo_root=SOURCE_ROOT, caller="test_bridge_missing")
    assert authority.source == "legacy_fallback"
    assert authority.canonical_runtime_truth_root == legacy.canonical_runtime_truth_root
    assert bridge.resolve_decision_truth_root_bridge_v1("", repo_root=SOURCE_ROOT, caller="test_bridge_missing") == (
        legacy.canonical_runtime_truth_root
    )


def test_bridge_fails_closed_when_release_current_invalid(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    legacy = _legacy_authority(tmp_path)
    monkeypatch.setattr(bridge, "load_runtime_path_authority_v1", lambda repo_root=None: legacy)
    monkeypatch.setattr(
        bridge,
        "_load_old_allowed_truth_roots",
        lambda: (legacy.canonical_runtime_truth_root, legacy.canonical_runtime_truth_sleeves_root),
    )
    monkeypatch.setattr(
        bridge,
        "load_release_current_runtime_authority_v1",
        lambda caller="": (_ for _ in ()).throw(
            SystemExit("FAIL: runtime_authority_bridge_release_current_invalid path=/tmp/current.json err=ValueError:bad")
        ),
    )

    with pytest.raises(SystemExit, match="runtime_authority_bridge_release_current_invalid"):
        bridge.load_decision_path_authority_bridge_v1(repo_root=SOURCE_ROOT, caller="test_bridge_invalid")


def test_bridge_logs_required_mismatches(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    legacy = _legacy_authority(tmp_path)
    monkeypatch.setattr(bridge, "load_runtime_path_authority_v1", lambda repo_root=None: legacy)
    monkeypatch.setattr(
        bridge,
        "_load_old_allowed_truth_roots",
        lambda: (legacy.canonical_runtime_truth_root, legacy.canonical_runtime_truth_sleeves_root),
    )

    new_truth = (tmp_path / "new" / "truth").resolve()
    new_sleeves = (tmp_path / "new" / "truth_sleeves").resolve()
    new_release = (tmp_path / "new" / "release").resolve()
    payload = _release_current_payload(
        tmp_path=tmp_path,
        canonical_runtime_truth_root=new_truth,
        canonical_runtime_truth_sleeves_root=new_sleeves,
        active_release_root=new_release,
        allowed_truth_roots=[new_truth, new_sleeves],
    )
    monkeypatch.setattr(bridge, "load_release_current_runtime_authority_v1", lambda caller="": dict(payload))

    caplog.set_level("WARNING", logger="constellation_2.common.decision_authority_bridge_v1")
    _ = bridge.load_decision_path_authority_bridge_v1(repo_root=SOURCE_ROOT, caller="test_bridge_mismatch")

    messages = [record.message for record in caplog.records if "decision_authority_bridge_mismatch" in record.message]
    assert messages
    payload = json.loads(messages[-1].split("decision_authority_bridge_mismatch ", 1)[1])
    fields = {entry["field"] for entry in payload["mismatches"]}
    assert {"canonical_truth_root", "truth_sleeves_root", "release_root", "allowed_truth_roots"}.issubset(fields)


def test_bridge_logs_explicit_path_acceptance_mismatch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    legacy = _legacy_authority(tmp_path)
    monkeypatch.setattr(bridge, "load_runtime_path_authority_v1", lambda repo_root=None: legacy)
    monkeypatch.setattr(
        bridge,
        "_load_old_allowed_truth_roots",
        lambda: (legacy.canonical_runtime_truth_root, legacy.canonical_runtime_truth_sleeves_root),
    )

    new_truth = (tmp_path / "new" / "truth").resolve()
    new_sleeves = (tmp_path / "new" / "truth_sleeves").resolve()
    new_release = (tmp_path / "new" / "release").resolve()
    payload = _release_current_payload(
        tmp_path=tmp_path,
        canonical_runtime_truth_root=new_truth,
        canonical_runtime_truth_sleeves_root=new_sleeves,
        active_release_root=new_release,
        allowed_truth_roots=[new_truth, new_sleeves],
    )
    monkeypatch.setattr(bridge, "load_release_current_runtime_authority_v1", lambda caller="": dict(payload))

    explicit_truth_root = (new_truth / "reports").resolve()
    explicit_truth_root.mkdir(parents=True, exist_ok=True)

    caplog.set_level("WARNING", logger="constellation_2.common.decision_authority_bridge_v1")
    with pytest.raises(ValueError, match="RUNTIME_PATH_AUTHORITY_POLICY_TRUTH_ROOT_FORBIDDEN"):
        bridge.resolve_decision_truth_root_bridge_v1(
            str(explicit_truth_root),
            repo_root=SOURCE_ROOT,
            caller="test_bridge_explicit_mismatch",
        )

    messages = [record.message for record in caplog.records if "decision_authority_bridge_mismatch" in record.message]
    assert messages
    payload = json.loads(messages[-1].split("decision_authority_bridge_mismatch ", 1)[1])
    fields = {entry["field"] for entry in payload["mismatches"]}
    assert "explicit_path_policy_read_allowed" in fields
    assert payload["explicit_truth_root"] == str(explicit_truth_root)
