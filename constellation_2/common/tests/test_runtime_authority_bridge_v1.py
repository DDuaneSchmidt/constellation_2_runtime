from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import constellation_2.common.runtime_authority_bridge_v1 as bridge


def _install_old_runtime_helpers(
    monkeypatch: pytest.MonkeyPatch,
    *,
    old_runtime_data_root: Path,
    old_canonical_truth_root: Path,
    old_truth_sleeves_root: Path,
    old_release_root: Path,
    old_runtime_environment: str = "PAPER",
    old_pointer_index_family: str = "run_pointer_v1",
    old_allowed_truth_roots: list[Path] | None = None,
) -> dict[str, object]:
    old_runtime_data_root.mkdir(parents=True, exist_ok=True)
    old_canonical_truth_root.mkdir(parents=True, exist_ok=True)
    old_truth_sleeves_root.mkdir(parents=True, exist_ok=True)
    (old_truth_sleeves_root / "PRIMARY" / "PAPER").mkdir(parents=True, exist_ok=True)
    old_release_root.mkdir(parents=True, exist_ok=True)
    allowed_roots = old_allowed_truth_roots or [old_canonical_truth_root, old_truth_sleeves_root]

    contract_payload = {
        "authoritative_repo_root": str(SOURCE_ROOT.resolve()),
        "runtime_environment": old_runtime_environment,
        "pointer_index_family": old_pointer_index_family,
        "allowed_truth_roots": [str(item.resolve()) for item in allowed_roots],
    }
    monkeypatch.setattr(bridge, "load_active_runtime_contract_or_fail", lambda: dict(contract_payload))
    monkeypatch.setattr(bridge, "resolve_runtime_data_root", lambda: old_runtime_data_root.resolve())
    monkeypatch.setattr(bridge, "resolve_canonical_truth_root", lambda: old_canonical_truth_root.resolve())
    monkeypatch.setattr(bridge, "resolve_truth_sleeves_root", lambda: old_truth_sleeves_root.resolve())
    monkeypatch.setattr(
        bridge,
        "resolve_release_provenance",
        lambda: {"release_root": str(old_release_root.resolve()), "release_id": "r1", "git_sha": "a" * 40},
    )
    return contract_payload


def _release_current_payload(
    *,
    runtime_data_root: Path,
    canonical_truth_root: Path,
    truth_sleeves_root: Path,
    release_root: Path,
    runtime_environment: str = "PAPER",
    pointer_index_family: str = "run_pointer_v1",
    allowed_truth_roots: list[Path] | None = None,
) -> dict[str, object]:
    runtime_data_root.mkdir(parents=True, exist_ok=True)
    canonical_truth_root.mkdir(parents=True, exist_ok=True)
    truth_sleeves_root.mkdir(parents=True, exist_ok=True)
    (truth_sleeves_root / "PRIMARY" / "PAPER").mkdir(parents=True, exist_ok=True)
    release_root.mkdir(parents=True, exist_ok=True)
    allowed_roots = allowed_truth_roots or [canonical_truth_root, truth_sleeves_root]
    return {
        "release_current_id": "f" * 64,
        "authoritative_repo_root": str(SOURCE_ROOT.resolve()),
        "release_root": str(release_root.resolve()),
        "canonical_truth_root": str(canonical_truth_root.resolve()),
        "truth_sleeves_root": str(truth_sleeves_root.resolve()),
        "runtime_data_root": str(runtime_data_root.resolve()),
        "runtime_environment": runtime_environment,
        "pointer_index_family": pointer_index_family,
        "allowed_truth_roots": [str(item.resolve()) for item in allowed_roots],
    }


def test_bridge_uses_release_current_when_valid(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    old_truth_root = (tmp_path / "old" / "truth").resolve()
    _install_old_runtime_helpers(
        monkeypatch,
        old_runtime_data_root=(tmp_path / "old" / "runtime_data").resolve(),
        old_canonical_truth_root=old_truth_root,
        old_truth_sleeves_root=(tmp_path / "old" / "truth_sleeves").resolve(),
        old_release_root=(tmp_path / "old" / "release").resolve(),
    )
    current_path = (old_truth_root / "release_current_v1" / "current.json").resolve()
    current_path.parent.mkdir(parents=True, exist_ok=True)
    current_path.write_text("{}\n", encoding="utf-8")

    new_payload = _release_current_payload(
        runtime_data_root=(tmp_path / "new" / "runtime_data").resolve(),
        canonical_truth_root=(tmp_path / "new" / "truth").resolve(),
        truth_sleeves_root=(tmp_path / "new" / "truth_sleeves").resolve(),
        release_root=(tmp_path / "new" / "release").resolve(),
    )
    monkeypatch.setattr(
        bridge,
        "read_validated_surface_v1",
        lambda *, path, schema_relpath: SimpleNamespace(path=Path(path), schema_relpath=schema_relpath, payload=dict(new_payload)),
    )

    authority = bridge.load_release_current_runtime_authority_v1(caller="test_bridge_valid")
    assert authority["source"] == "release_current_v1"
    assert authority["canonical_truth_root"] == str(Path(new_payload["canonical_truth_root"]).resolve())
    assert authority["truth_sleeves_root"] == str(Path(new_payload["truth_sleeves_root"]).resolve())
    assert authority["release_root"] == str(Path(new_payload["release_root"]).resolve())
    assert authority["runtime_environment"] == "PAPER"
    assert authority["pointer_index_family"] == "run_pointer_v1"
    assert authority["allowed_truth_roots"] == [
        str(Path(new_payload["allowed_truth_roots"][0]).resolve()),
        str(Path(new_payload["allowed_truth_roots"][1]).resolve()),
    ]
    assert bridge.resolve_canonical_truth_root_bridge_v1(caller="test_bridge_valid") == Path(
        new_payload["canonical_truth_root"]
    ).resolve()
    assert bridge.resolve_truth_sleeves_root_bridge_v1(caller="test_bridge_valid") == Path(
        new_payload["truth_sleeves_root"]
    ).resolve()
    assert bridge.resolve_runtime_data_root_bridge_v1(caller="test_bridge_valid") == Path(
        new_payload["runtime_data_root"]
    ).resolve()
    assert bridge.resolve_release_root_bridge_v1(caller="test_bridge_valid") == Path(new_payload["release_root"]).resolve()
    assert bridge.resolve_truth_root_bridge_v1(repo_root=SOURCE_ROOT, caller="test_bridge_valid") == (
        Path(new_payload["truth_sleeves_root"]).resolve() / "PRIMARY" / "PAPER"
    ).resolve()


def test_bridge_falls_back_when_release_current_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    old_truth_root = (tmp_path / "old" / "truth").resolve()
    _install_old_runtime_helpers(
        monkeypatch,
        old_runtime_data_root=(tmp_path / "old" / "runtime_data").resolve(),
        old_canonical_truth_root=old_truth_root,
        old_truth_sleeves_root=(tmp_path / "old" / "truth_sleeves").resolve(),
        old_release_root=(tmp_path / "old" / "release").resolve(),
    )

    authority = bridge.load_release_current_runtime_authority_v1(caller="test_bridge_missing")
    assert authority["source"] == "legacy_fallback"
    assert authority["canonical_truth_root"] == str(old_truth_root.resolve())
    assert authority["truth_sleeves_root"] == str((tmp_path / "old" / "truth_sleeves").resolve())
    assert authority["release_root"] == str((tmp_path / "old" / "release").resolve())


def test_bridge_fails_closed_when_release_current_invalid(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    old_truth_root = (tmp_path / "old" / "truth").resolve()
    _install_old_runtime_helpers(
        monkeypatch,
        old_runtime_data_root=(tmp_path / "old" / "runtime_data").resolve(),
        old_canonical_truth_root=old_truth_root,
        old_truth_sleeves_root=(tmp_path / "old" / "truth_sleeves").resolve(),
        old_release_root=(tmp_path / "old" / "release").resolve(),
    )
    current_path = (old_truth_root / "release_current_v1" / "current.json").resolve()
    current_path.parent.mkdir(parents=True, exist_ok=True)
    current_path.write_text("{}\n", encoding="utf-8")
    bad_payload = _release_current_payload(
        runtime_data_root=(tmp_path / "new" / "runtime_data").resolve(),
        canonical_truth_root=(tmp_path / "new" / "truth").resolve(),
        truth_sleeves_root=(tmp_path / "new" / "truth_sleeves").resolve(),
        release_root=(tmp_path / "new" / "release").resolve(),
    )
    bad_payload.pop("canonical_truth_root")
    monkeypatch.setattr(
        bridge,
        "read_validated_surface_v1",
        lambda *, path, schema_relpath: SimpleNamespace(path=Path(path), schema_relpath=schema_relpath, payload=dict(bad_payload)),
    )

    with pytest.raises(SystemExit, match="RELEASE_CURRENT_CANONICAL_TRUTH_ROOT_MISSING"):
        bridge.load_release_current_runtime_authority_v1(caller="test_bridge_invalid")


def test_bridge_logs_required_mismatches(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    old_truth_root = (tmp_path / "old" / "truth").resolve()
    old_truth_sleeves_root = (tmp_path / "old" / "truth_sleeves").resolve()
    old_release_root = (tmp_path / "old" / "release").resolve()
    _install_old_runtime_helpers(
        monkeypatch,
        old_runtime_data_root=(tmp_path / "old" / "runtime_data").resolve(),
        old_canonical_truth_root=old_truth_root,
        old_truth_sleeves_root=old_truth_sleeves_root,
        old_release_root=old_release_root,
        old_runtime_environment="PAPER",
        old_pointer_index_family="run_pointer_v1",
    )

    current_path = (old_truth_root / "release_current_v1" / "current.json").resolve()
    current_path.parent.mkdir(parents=True, exist_ok=True)
    current_path.write_text("{}\n", encoding="utf-8")

    new_payload = _release_current_payload(
        runtime_data_root=(tmp_path / "new" / "runtime_data").resolve(),
        canonical_truth_root=(tmp_path / "new" / "truth").resolve(),
        truth_sleeves_root=(tmp_path / "new" / "truth_sleeves").resolve(),
        release_root=(tmp_path / "new" / "release").resolve(),
        runtime_environment="PAPER_SIM",
        pointer_index_family="run_pointer_v2",
        allowed_truth_roots=[
            (tmp_path / "new" / "truth").resolve(),
            (tmp_path / "new" / "truth_sleeves").resolve(),
        ],
    )
    monkeypatch.setattr(
        bridge,
        "read_validated_surface_v1",
        lambda *, path, schema_relpath: SimpleNamespace(path=Path(path), schema_relpath=schema_relpath, payload=dict(new_payload)),
    )

    caplog.set_level("WARNING", logger="constellation_2.common.runtime_authority_bridge_v1")
    authority = bridge.load_release_current_runtime_authority_v1(caller="test_bridge_mismatch")
    assert authority["source"] == "release_current_v1"

    records = [record.message for record in caplog.records if "runtime_authority_bridge_mismatch" in record.message]
    assert records
    payload = json.loads(records[-1].split("runtime_authority_bridge_mismatch ", 1)[1])
    fields = {entry["field"] for entry in payload["mismatches"]}
    assert {"canonical_truth_root", "truth_sleeves_root", "release_root", "runtime_environment", "allowed_truth_roots"}.issubset(
        fields
    )
