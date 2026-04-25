from __future__ import annotations

import threading
from pathlib import Path

import pytest

from constellation_2.common import configuration_activation_authority_v1 as configuration_activation_module
from constellation_2.common import paper_session_fact_plane_v1 as fact_plane_module
from constellation_2.common.configuration_activation_authority_v1 import (
    CONFIGURATION_STATE_SCHEMA,
    CONFIGURATION_STATE_CURRENT_LOCK_NAME,
    ConfigurationActivationError,
    REPO_ROOT,
    run_configuration_activation_authority_v1,
)
from constellation_2.common.configuration_activation_family_validator_v1 import (
    ConfigurationActivationFamilyValidationError,
    validate_configuration_activation_family_v1,
)
from constellation_2.common.constitutional_runtime_v1 import (
    resolve_constitutional_artifact_path_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    atomic_write_validated_json_v1,
    read_validated_surface_v1,
)
from constellation_2.common.tests.test_configuration_activation_authority_v1 import (
    _write_policy_snapshot,
)


def _current_temp_paths(current_path: Path) -> list[Path]:
    return sorted(current_path.parent.glob(f".{current_path.name}.tmp.*"))


def test_validator_fails_on_corrupted_current_state_and_writer_blocks_superseding_activation(
    tmp_path: Path,
) -> None:
    truth_root = (tmp_path / "truth").resolve()
    first_policy_path = _write_policy_snapshot(
        truth_root=truth_root,
        generated_at_utc="2026-04-18T16:00:00Z",
        effective_at_utc="2026-04-18T16:00:00Z",
        logical_name="corruptible_config",
        document_text='{"mode":"stable"}\n',
    )
    first_result = run_configuration_activation_authority_v1(
        policy_snapshot_path=first_policy_path,
        truth_root=truth_root,
    )
    assert first_result.configuration_state_ref is not None
    assert first_result.compile_result_ref is not None

    corrupted_payload = dict(first_result.configuration_state_ref.payload)
    corrupted_payload["compiled_active_config_ref"] = {
        "artifact_id": "configuration_compile_result_v1",
        "path": str(first_result.compile_result_ref.path),
        "sha256": first_result.compile_result_ref.sha256,
        "artifact_class": "outcome_record",
        "finality_state": "finalized",
    }
    atomic_write_validated_json_v1(
        path=first_result.configuration_state_ref.path,
        payload=corrupted_payload,
        schema_relpath=CONFIGURATION_STATE_SCHEMA,
    )

    report = validate_configuration_activation_family_v1(truth_root=truth_root)
    assert report["ok"] is False
    assert report["errors"]

    second_policy_path = _write_policy_snapshot(
        truth_root=truth_root,
        generated_at_utc="2026-04-18T17:00:00Z",
        effective_at_utc="2026-04-18T17:00:00Z",
        logical_name="corruptible_config",
        document_text='{"mode":"changed"}\n',
    )
    with pytest.raises(ConfigurationActivationFamilyValidationError):
        run_configuration_activation_authority_v1(
            policy_snapshot_path=second_policy_path,
            truth_root=truth_root,
        )


def test_repeated_activation_of_same_candidate_is_idempotent(tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    policy_path = _write_policy_snapshot(
        truth_root=truth_root,
        generated_at_utc="2026-04-18T18:00:00Z",
        effective_at_utc="2026-04-18T18:00:00Z",
        logical_name="idempotent_config",
        document_text='{"mode":"same"}\n',
    )

    first_result = run_configuration_activation_authority_v1(
        policy_snapshot_path=policy_path,
        truth_root=truth_root,
    )
    second_result = run_configuration_activation_authority_v1(
        policy_snapshot_path=policy_path,
        truth_root=truth_root,
    )

    assert first_result.compiled_active_config_ref is not None
    assert first_result.activation_transaction_ref is not None
    assert first_result.configuration_state_ref is not None
    assert first_result.review_diff_ref is not None
    assert second_result.compiled_active_config_ref is not None
    assert second_result.activation_transaction_ref is not None
    assert second_result.configuration_state_ref is not None
    assert second_result.review_diff_ref is not None

    assert second_result.compiled_active_config_ref.path == first_result.compiled_active_config_ref.path
    assert second_result.compiled_active_config_ref.sha256 == first_result.compiled_active_config_ref.sha256
    assert second_result.review_diff_ref.path == first_result.review_diff_ref.path
    assert second_result.activation_transaction_ref.path == first_result.activation_transaction_ref.path
    assert second_result.configuration_state_ref.path == first_result.configuration_state_ref.path
    assert second_result.configuration_state_ref.payload == first_result.configuration_state_ref.payload

    report = validate_configuration_activation_family_v1(truth_root=truth_root)
    assert report["ok"] is True


def test_current_publication_uses_durable_atomic_write_and_repeated_activation_stays_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth_root = (tmp_path / "truth").resolve()
    current_path = resolve_constitutional_artifact_path_v1(
        repo_root=REPO_ROOT,
        artifact_id="configuration_state_v1",
        day_utc="2026-04-18",
        canonical_truth_root=truth_root,
    )
    policy_path = _write_policy_snapshot(
        truth_root=truth_root,
        generated_at_utc="2026-04-18T18:30:00Z",
        effective_at_utc="2026-04-18T18:30:00Z",
        logical_name="durable_idempotent_config",
        document_text='{"mode":"durable"}\n',
    )

    original_writer = fact_plane_module._durable_atomic_write_bytes_v1
    original_fsync = fact_plane_module.os.fsync
    write_stats = {"current_writes": 0, "current_fsync_calls": 0}
    track_current_write = {"active": False}

    def _tracking_fsync(fd: int) -> None:
        if track_current_write["active"]:
            write_stats["current_fsync_calls"] += 1
        original_fsync(fd)

    def _tracking_writer(*, path: Path, data: bytes) -> None:
        if path.resolve() == current_path.resolve():
            write_stats["current_writes"] += 1
            track_current_write["active"] = True
            try:
                original_writer(path=path, data=data)
            finally:
                track_current_write["active"] = False
            return
        original_writer(path=path, data=data)

    monkeypatch.setattr(fact_plane_module.os, "fsync", _tracking_fsync)
    monkeypatch.setattr(fact_plane_module, "_durable_atomic_write_bytes_v1", _tracking_writer)

    first_result = run_configuration_activation_authority_v1(
        policy_snapshot_path=policy_path,
        truth_root=truth_root,
    )
    second_result = run_configuration_activation_authority_v1(
        policy_snapshot_path=policy_path,
        truth_root=truth_root,
    )

    assert first_result.configuration_state_ref is not None
    assert second_result.configuration_state_ref is not None
    assert first_result.configuration_state_ref.path == current_path
    assert second_result.configuration_state_ref.path == current_path
    assert second_result.configuration_state_ref.payload == first_result.configuration_state_ref.payload
    assert write_stats["current_writes"] == 1
    assert write_stats["current_fsync_calls"] >= 2

    current_ref = read_validated_surface_v1(
        path=current_path,
        schema_relpath=CONFIGURATION_STATE_SCHEMA,
    )
    assert current_ref.payload == first_result.configuration_state_ref.payload
    assert _current_temp_paths(current_path) == []

    report = validate_configuration_activation_family_v1(truth_root=truth_root)
    assert report["ok"] is True


def test_current_publication_failure_keeps_visible_current_state_unchanged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth_root = (tmp_path / "truth").resolve()
    first_policy_path = _write_policy_snapshot(
        truth_root=truth_root,
        generated_at_utc="2026-04-18T19:00:00Z",
        effective_at_utc="2026-04-18T19:00:00Z",
        logical_name="durable_config",
        document_text='{"mode":"v1"}\n',
    )
    first_result = run_configuration_activation_authority_v1(
        policy_snapshot_path=first_policy_path,
        truth_root=truth_root,
    )
    assert first_result.configuration_state_ref is not None
    current_path = first_result.configuration_state_ref.path
    current_before = read_validated_surface_v1(
        path=current_path,
        schema_relpath=CONFIGURATION_STATE_SCHEMA,
    )

    second_policy_path = _write_policy_snapshot(
        truth_root=truth_root,
        generated_at_utc="2026-04-18T20:00:00Z",
        effective_at_utc="2026-04-18T20:00:00Z",
        logical_name="durable_config",
        document_text='{"mode":"v2"}\n',
    )

    original_replace = fact_plane_module.os.replace

    def _failing_replace(src: str, dst: str) -> None:
        if Path(dst).resolve() == current_path.resolve():
            raise OSError("SIMULATED_CURRENT_REPLACE_FAILURE")
        original_replace(src, dst)

    monkeypatch.setattr(fact_plane_module.os, "replace", _failing_replace)

    with pytest.raises(OSError, match="SIMULATED_CURRENT_REPLACE_FAILURE"):
        run_configuration_activation_authority_v1(
            policy_snapshot_path=second_policy_path,
            truth_root=truth_root,
        )

    current_after = read_validated_surface_v1(
        path=current_path,
        schema_relpath=CONFIGURATION_STATE_SCHEMA,
    )
    assert current_after.payload == current_before.payload
    assert current_after.sha256 == current_before.sha256
    assert _current_temp_paths(current_path) == []
    assert not (current_path.parent / CONFIGURATION_STATE_CURRENT_LOCK_NAME).exists()

    report = validate_configuration_activation_family_v1(truth_root=truth_root)
    assert report["ok"] is True
    assert report["artifacts"]["configuration_state_v1"]["path"] == str(current_path)


def test_concurrent_current_publication_fails_closed_with_single_visible_advancement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth_root = (tmp_path / "truth").resolve()
    current_path = resolve_constitutional_artifact_path_v1(
        repo_root=REPO_ROOT,
        artifact_id="configuration_state_v1",
        day_utc="2026-04-18",
        canonical_truth_root=truth_root,
    )
    first_policy_path = _write_policy_snapshot(
        truth_root=truth_root,
        generated_at_utc="2026-04-18T21:00:00Z",
        effective_at_utc="2026-04-18T21:00:00Z",
        logical_name="concurrent_config_alpha",
        document_text='{"mode":"alpha"}\n',
    )
    second_policy_path = _write_policy_snapshot(
        truth_root=truth_root,
        generated_at_utc="2026-04-18T21:01:00Z",
        effective_at_utc="2026-04-18T21:01:00Z",
        logical_name="concurrent_config_beta",
        document_text='{"mode":"beta"}\n',
    )

    original_current_writer = configuration_activation_module.atomic_write_idempotent_validated_json_v1
    first_writer_entered = threading.Event()
    release_first_writer = threading.Event()
    outcomes: dict[str, object] = {}

    def _blocking_current_writer(**kwargs):
        path = Path(kwargs["path"]).resolve()
        if path == current_path.resolve() and not first_writer_entered.is_set():
            first_writer_entered.set()
            assert release_first_writer.wait(timeout=5), "Timed out waiting to release first current-state publication"
        return original_current_writer(**kwargs)

    def _run_activation(name: str, policy_path: Path) -> None:
        try:
            outcomes[name] = run_configuration_activation_authority_v1(
                policy_snapshot_path=policy_path,
                truth_root=truth_root,
            )
        except Exception as exc:  # noqa: BLE001
            outcomes[name] = exc

    monkeypatch.setattr(configuration_activation_module, "atomic_write_idempotent_validated_json_v1", _blocking_current_writer)

    first_thread = threading.Thread(target=_run_activation, args=("first", first_policy_path))
    second_thread = threading.Thread(target=_run_activation, args=("second", second_policy_path))
    first_thread.start()
    assert first_writer_entered.wait(timeout=5), "First activation never reached current-state publication"
    second_thread.start()
    second_thread.join(timeout=5)
    assert not second_thread.is_alive(), "Second activation did not fail closed while the publication lock was held"
    release_first_writer.set()
    first_thread.join(timeout=5)
    assert not first_thread.is_alive(), "First activation did not complete after releasing the publication lock"

    assert "first" in outcomes
    assert "second" in outcomes
    assert not isinstance(outcomes["first"], Exception)
    assert isinstance(outcomes["second"], ConfigurationActivationError)
    assert "CONFIGURATION_STATE_CURRENT_LOCK_BUSY" in str(outcomes["second"])
    assert not (current_path.parent / CONFIGURATION_STATE_CURRENT_LOCK_NAME).exists()

    first_result = outcomes["first"]
    assert getattr(first_result, "configuration_state_ref") is not None
    current_ref = read_validated_surface_v1(
        path=current_path,
        schema_relpath=CONFIGURATION_STATE_SCHEMA,
    )
    assert current_ref.payload == first_result.configuration_state_ref.payload
    assert _current_temp_paths(current_path) == []

    report = validate_configuration_activation_family_v1(truth_root=truth_root)
    assert report["ok"] is True
