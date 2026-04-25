from __future__ import annotations

import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import constellation_2.common.decision_authority_bridge_v1 as bridge
from constellation_2.common.runtime_path_authority_v1 import RuntimePathAuthorityV1


MIGRATED_RUNTIME_CRITICAL_DECISION_SET = {
    "ops/tools/run_fresh_day_admission_v1.py": "ops/tools/run_fresh_day_admission_v1.py",
    "ops/tools/run_paper_session_admission_v1.py": "ops/tools/run_paper_session_admission_v1.py",
    "ops/tools/run_paper_session_bootstrap_v1.py": "ops/tools/run_paper_session_bootstrap_v1.py",
    "ops/tools/run_paper_trading_integration_v1.py": "ops/tools/run_paper_trading_integration_v1.py",
}


UNCHANGED_SNAPSHOT_RUNTIME_IDENTITY_BOOTSTRAP_SET = {
    "constellation_2/common/fresh_day_admission_v1.py",
    "constellation_2/common/runtime_identity_v1.py",
    "ops/tools/run_c2_paper_day_orchestrator_v2.py",
    "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
}


def _read(relpath: str) -> str:
    return (SOURCE_ROOT / relpath).resolve().read_text(encoding="utf-8")


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


def test_runtime_critical_decision_callers_use_bridge_helper() -> None:
    for relpath, caller_label in MIGRATED_RUNTIME_CRITICAL_DECISION_SET.items():
        text = _read(relpath)
        assert "decision_authority_bridge_v1" in text, relpath
        assert "resolve_decision_truth_root_bridge_v1" in text, relpath
        assert f'caller="{caller_label}"' in text, relpath
        assert "resolve_decision_truth_root_v1(" not in text, relpath


def test_snapshot_runtime_identity_and_bootstrap_sets_remain_unchanged() -> None:
    for relpath in UNCHANGED_SNAPSHOT_RUNTIME_IDENTITY_BOOTSTRAP_SET:
        text = _read(relpath)
        assert "resolve_decision_truth_root_bridge_v1" not in text, relpath
        assert "decision_authority_bridge_v1" not in text, relpath


def test_bridge_prefers_release_current_when_valid(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    legacy = _legacy_authority(tmp_path)
    monkeypatch.setattr(bridge, "load_runtime_path_authority_v1", lambda repo_root=None: legacy)
    monkeypatch.setattr(
        bridge,
        "_load_old_allowed_truth_roots",
        lambda: (legacy.canonical_runtime_truth_root, legacy.canonical_runtime_truth_sleeves_root),
    )
    new_truth_root = (tmp_path / "new" / "truth").resolve()
    new_truth_sleeves_root = (tmp_path / "new" / "truth_sleeves").resolve()
    new_release_root = (tmp_path / "new" / "release").resolve()
    new_truth_root.mkdir(parents=True, exist_ok=True)
    new_truth_sleeves_root.mkdir(parents=True, exist_ok=True)
    new_release_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(
        bridge,
        "load_release_current_runtime_authority_v1",
        lambda caller="": {
            "source": "release_current_v1",
            "release_current_path": str((tmp_path / "new" / "truth" / "release_current_v1" / "current.json").resolve()),
            "release_current_id": "f" * 64,
            "authoritative_repo_root": str(SOURCE_ROOT.resolve()),
            "canonical_truth_root": str(new_truth_root),
            "truth_sleeves_root": str(new_truth_sleeves_root),
            "release_root": str(new_release_root),
            "allowed_truth_roots": [str(new_truth_root), str(new_truth_sleeves_root)],
        },
    )

    resolved = bridge.resolve_decision_truth_root_bridge_v1(
        "",
        repo_root=SOURCE_ROOT,
        caller="test_decision_authority_bridge_runtime_cutover_v1.py",
    )
    assert resolved == new_truth_root


def test_bridge_falls_back_when_release_current_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    legacy = _legacy_authority(tmp_path)
    monkeypatch.setattr(bridge, "load_runtime_path_authority_v1", lambda repo_root=None: legacy)
    monkeypatch.setattr(
        bridge,
        "_load_old_allowed_truth_roots",
        lambda: (legacy.canonical_runtime_truth_root, legacy.canonical_runtime_truth_sleeves_root),
    )
    monkeypatch.setattr(bridge, "load_release_current_runtime_authority_v1", lambda caller="": {"source": "legacy_fallback"})

    resolved = bridge.resolve_decision_truth_root_bridge_v1(
        "",
        repo_root=SOURCE_ROOT,
        caller="test_decision_authority_bridge_runtime_cutover_v1.py",
    )
    assert resolved == legacy.canonical_runtime_truth_root


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
        bridge.resolve_decision_truth_root_bridge_v1(
            "",
            repo_root=SOURCE_ROOT,
            caller="test_decision_authority_bridge_runtime_cutover_v1.py",
        )
