from __future__ import annotations

from pathlib import Path
import sys
from types import SimpleNamespace
import json

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.activate_constellation_release_v1 as activate_module


def _activation_kwargs(tmp_path: Path, prior_target: Path | None = None) -> dict:
    return {
        "release_id": "release-test",
        "release_root": (tmp_path / "new_release").resolve(),
        "manifest": {"git_sha": "a" * 40},
        "release_manifest_hash": "b" * 64,
        "activated_at_utc": "2026-05-05T20:00:00Z",
        "approval_id": "activation_receipt:test",
        "prior_target": prior_target,
        "prior_current_release": {"release_id": "prior-release"},
    }


def test_materialize_release_current_runs_reducer_and_validates(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    truth_root.mkdir(parents=True)
    calls: list[list[str]] = []

    def _fake_run(cmd: list[str], **kwargs):  # type: ignore[no-untyped-def]
        calls.append(list(cmd))
        return SimpleNamespace(returncode=0, stdout='{"status":"ok"}\n', stderr="")

    monkeypatch.setattr(activate_module.subprocess, "run", _fake_run)
    monkeypatch.setattr(
        activate_module,
        "read_control_plane_surface_v1",
        lambda **_: SimpleNamespace(payload={"canonical_truth_root": str(truth_root)}),
    )
    monkeypatch.setattr(
        activate_module,
        "load_release_current_shadow_ref_if_present",
        lambda **_: SimpleNamespace(
            path=(truth_root / "release_current_v1" / "current.json").resolve(),
            payload={"release_current_id": "f" * 64},
        ),
    )

    activate_module._materialize_release_current_or_fail()
    assert len(calls) == 1
    assert calls[0][1] == str(activate_module.REDUCE_RELEASE_CURRENT_TOOL)


def test_materialize_release_current_fails_when_publication_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    truth_root = (tmp_path / "truth").resolve()
    truth_root.mkdir(parents=True)
    monkeypatch.setattr(
        activate_module.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout="{}", stderr=""),
    )
    monkeypatch.setattr(
        activate_module,
        "read_control_plane_surface_v1",
        lambda **_: SimpleNamespace(payload={"canonical_truth_root": str(truth_root)}),
    )
    monkeypatch.setattr(
        activate_module,
        "load_release_current_shadow_ref_if_present",
        lambda **_: None,
    )

    with pytest.raises(SystemExit, match="release_current publication missing after activation"):
        activate_module._materialize_release_current_or_fail()


def test_activate_runtime_authority_stack_rolls_back_when_materialization_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    order: list[str] = []
    writes: list[str] = []
    restores: list[Path | None] = []
    current_restores: list[dict | None] = []
    prior_target = (tmp_path / "prior_release").resolve()

    monkeypatch.setattr(
        activate_module,
        "_atomic_activate_symlink",
        lambda _: order.append("activate_pointer"),
    )
    monkeypatch.setattr(
        activate_module,
        "_write_current_release_manifest_v1",
        lambda **_: order.append("write_current_release"),
    )
    monkeypatch.setattr(
        activate_module,
        "_write_active_runtime_contract_or_fail",
        lambda: (order.append("write_contract"), writes.append("write")),
    )
    monkeypatch.setattr(
        activate_module,
        "_materialize_release_current_or_fail",
        lambda: (_ for _ in ()).throw(SystemExit("FAIL: release_current reducer failed")),
    )
    monkeypatch.setattr(
        activate_module,
        "_post_activation_verify_or_fail",
        lambda **_: order.append("post_verify"),
    )
    monkeypatch.setattr(
        activate_module,
        "_restore_prior_active_pointer_or_fail",
        lambda target: (order.append("restore_pointer"), restores.append(target)),
    )
    monkeypatch.setattr(
        activate_module,
        "_restore_current_release_manifest_v1",
        lambda prior: (order.append("restore_current_release"), current_restores.append(prior)),
    )

    with pytest.raises(SystemExit, match="release_current reducer failed"):
        activate_module._activate_runtime_authority_stack_or_fail(**_activation_kwargs(tmp_path, prior_target))

    assert order == [
        "activate_pointer",
        "write_current_release",
        "write_contract",
        "restore_pointer",
        "restore_current_release",
        "write_contract",
    ]
    assert len(writes) == 2
    assert restores == [prior_target]
    assert current_restores == [{"release_id": "prior-release"}]


def test_activate_runtime_authority_stack_success_includes_release_current_materialization(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    order: list[str] = []
    monkeypatch.setattr(
        activate_module,
        "_atomic_activate_symlink",
        lambda _: order.append("activate_pointer"),
    )
    monkeypatch.setattr(
        activate_module,
        "_write_current_release_manifest_v1",
        lambda **_: order.append("write_current_release"),
    )
    monkeypatch.setattr(
        activate_module,
        "_write_active_runtime_contract_or_fail",
        lambda: order.append("write_contract"),
    )
    monkeypatch.setattr(
        activate_module,
        "_materialize_release_current_or_fail",
        lambda: order.append("materialize_release_current"),
    )
    monkeypatch.setattr(
        activate_module,
        "_post_activation_verify_or_fail",
        lambda **_: order.append("post_verify"),
    )

    activate_module._activate_runtime_authority_stack_or_fail(**_activation_kwargs(tmp_path))

    assert order == [
        "activate_pointer",
        "write_current_release",
        "write_contract",
        "materialize_release_current",
        "post_verify",
    ]


def test_activate_runtime_authority_stack_clears_lock_on_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    lock_path = (tmp_path / "activations_v1" / ".activation_in_progress.lock").resolve()
    monkeypatch.setattr(activate_module, "ACTIVATION_IN_PROGRESS_LOCK", lock_path)
    monkeypatch.setattr(activate_module, "_atomic_activate_symlink", lambda _: None)
    monkeypatch.setattr(activate_module, "_write_current_release_manifest_v1", lambda **_: None)
    monkeypatch.setattr(activate_module, "_write_active_runtime_contract_or_fail", lambda: None)
    monkeypatch.setattr(activate_module, "_restore_current_release_manifest_v1", lambda _: None)
    monkeypatch.setattr(
        activate_module,
        "_materialize_release_current_or_fail",
        lambda: (_ for _ in ()).throw(SystemExit("FAIL: reducer")),
    )
    monkeypatch.setattr(activate_module, "_restore_prior_active_pointer_or_fail", lambda _: None)

    with pytest.raises(SystemExit, match="reducer"):
        activate_module._activate_runtime_authority_stack_or_fail(**_activation_kwargs(tmp_path))

    assert not lock_path.exists()


def test_write_current_release_manifest_materializes_pointer_atomically(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    current_path = (tmp_path / "truth" / "releases" / "current_release.v1.json").resolve()
    monkeypatch.setattr(activate_module, "CURRENT_RELEASE_MANIFEST", current_path)
    release_root = (tmp_path / "releases" / "release-new").resolve()
    release_root.mkdir(parents=True)

    prior = {"schema_version": "aegis_current_release.v1", "release_id": "release-old"}
    activate_module._write_current_release_manifest_v1(
        release_id="release-new",
        release_root=release_root,
        manifest={"git_sha": "c" * 40},
        release_manifest_hash="d" * 64,
        activated_at_utc="2026-05-05T20:00:00Z",
        approval_id="activation_receipt:release-new",
        previous_release=prior,
    )

    payload = json.loads(current_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "aegis_current_release.v1"
    assert payload["release_id"] == "release-new"
    assert payload["release_path"] == str(release_root)
    assert payload["commit"] == "c" * 40
    assert payload["release_manifest_hash"] == "d" * 64
    assert payload["previous_release"] == prior


def test_restore_current_release_manifest_restores_prior_payload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    current_path = (tmp_path / "truth" / "releases" / "current_release.v1.json").resolve()
    monkeypatch.setattr(activate_module, "CURRENT_RELEASE_MANIFEST", current_path)
    prior = {"schema_version": "aegis_current_release.v1", "release_id": "release-old"}

    activate_module._restore_current_release_manifest_v1(prior)

    assert json.loads(current_path.read_text(encoding="utf-8")) == prior
