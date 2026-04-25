from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.control_plane_read_gateway_v1 import ControlPlaneReadRefV1
from constellation_2.common.release_current_v1 import build_release_current_v1, write_release_current_v1
from constellation_2.common.runtime_contract_v1 import (
    load_active_runtime_contract_or_fail,
    resolve_release_provenance,
)
from constellation_2.phaseC.lib.canon_json_v1 import canonical_json_bytes_v1


def _sha(payload: dict[str, object]) -> str:
    raw = canonical_json_bytes_v1(payload) + b"\n"
    return hashlib.sha256(raw).hexdigest()


def _ref(*, domain: str, surface: str, path: str | Path, payload: dict[str, object], schema_relpath: str) -> ControlPlaneReadRefV1:
    return ControlPlaneReadRefV1(
        domain=domain,
        surface=surface,
        read_kind="validated_json",
        path=Path(path).resolve(),
        payload=dict(payload),
        sha256=_sha(payload),
        schema_relpath=schema_relpath,
        metadata={},
    )


def _manifest_payload(*, release_root: Path) -> dict[str, object]:
    return {
        "schema_id": "release_manifest.v1",
        "schema_version": "v1",
        "release_id": "release-20260420-bridge",
        "git_sha": "a" * 40,
        "release_root": str(release_root.resolve()),
    }


def _runtime_payload(*, truth_root: Path, release_root: Path) -> dict[str, object]:
    return {
        "schema_id": "active_runtime_contract.v1",
        "schema_version": "v1",
        "release_id": "release-20260420-bridge",
        "git_sha": "a" * 40,
        "authoritative_repo_root": "/home/node/constellation",
        "release_root": str(release_root.resolve()),
        "runtime_data_root": str((truth_root.parent / "runtime_data").resolve()),
        "canonical_truth_root": str(truth_root.resolve()),
        "truth_sleeves_root": str((truth_root.parent / "truth_sleeves").resolve()),
        "pointer_index_family": "run_pointer_v1",
        "allowed_truth_roots": [
            str(truth_root.resolve()),
            str((truth_root.parent / "truth_sleeves").resolve()),
        ],
        "provenance_mode": "release_manifest",
        "runtime_environment": "PAPER",
        "primary_execution_identity_ref": {
            "authority_owner": "execution_identity_binding_v1",
            "sleeve_id": "PRIMARY",
        },
        "generated_at_utc": "2026-04-20T14:00:00Z",
        "status": "ACTIVE",
    }


def _configuration_state_payload(*, truth_root: Path) -> dict[str, object]:
    return {
        "schema_id": "configuration_state.v1",
        "schema_version": "v1",
        "configuration_state_id": "b" * 64,
        "authority_owner": "configuration_activation_authority_v1",
        "generated_at_utc": "2026-04-20T13:58:00Z",
        "status": "ACTIVE",
        "activation_kind": "FIRST_ACTIVATION",
        "configuration_policy_snapshot_ref": {
            "artifact_id": "configuration_policy_snapshot_v1",
            "path": str((truth_root / "artifacts" / "policy.json").resolve()),
            "sha256": "1" * 64,
            "artifact_class": "policy_snapshot",
            "finality_state": "finalized",
        },
        "configuration_validation_result_ref": {
            "artifact_id": "configuration_validation_result_v1",
            "path": str((truth_root / "artifacts" / "validation.json").resolve()),
            "sha256": "2" * 64,
            "artifact_class": "validation_result",
            "finality_state": "finalized",
        },
        "configuration_compile_result_ref": {
            "artifact_id": "configuration_compile_result_v1",
            "path": str((truth_root / "artifacts" / "compile.json").resolve()),
            "sha256": "3" * 64,
            "artifact_class": "compile_result",
            "finality_state": "finalized",
        },
        "configuration_review_diff_ref": {
            "artifact_id": "configuration_review_diff_v1",
            "path": str((truth_root / "artifacts" / "review.json").resolve()),
            "sha256": "4" * 64,
            "artifact_class": "review_diff",
            "finality_state": "finalized",
        },
        "compiled_active_config_ref": {
            "artifact_id": "compiled_active_config_v1",
            "path": str(
                (truth_root / "compiled_active_config_v1" / ("c" * 64) / "compiled_active_config.v1.json").resolve()
            ),
            "sha256": "5" * 64,
            "artifact_class": "compiled_config",
            "finality_state": "finalized",
        },
        "configuration_activation_transaction_ref": {
            "artifact_id": "configuration_activation_transaction_v1",
            "path": str(
                (
                    truth_root
                    / "reports"
                    / "configuration_activation_transaction_v1"
                    / ("d" * 64)
                    / "configuration_activation_transaction.v1.json"
                ).resolve()
            ),
            "sha256": "6" * 64,
            "artifact_class": "admission_result",
            "finality_state": "finalized",
        },
        "active_compiled_config_sha256": "7" * 64,
        "constitutional_dependency_declaration": {},
        "constitutional_lineage": {},
    }


def _install_runtime_contract_fixture(
    monkeypatch: pytest.MonkeyPatch,
    *,
    tmp_path: Path,
    manifest_payload: dict[str, object],
    runtime_payload: dict[str, object],
    configuration_payload: dict[str, object],
) -> dict[tuple[str, str], ControlPlaneReadRefV1]:
    import constellation_2.common.runtime_contract_v1 as runtime_contract_module

    contract_path = (tmp_path / "runtime_contract_v1" / "active_runtime_contract.v1.json").resolve()
    contract_path.parent.mkdir(parents=True, exist_ok=True)
    contract_path.write_bytes(canonical_json_bytes_v1(runtime_payload) + b"\n")
    monkeypatch.setattr(runtime_contract_module, "ACTIVE_RUNTIME_CONTRACT_PATH", contract_path)

    manifest_path = (tmp_path / "release_manifest.v1.json").resolve()
    manifest_path.write_bytes(canonical_json_bytes_v1(manifest_payload) + b"\n")

    refs = {
        ("release", "active_runtime_contract"): _ref(
            domain="release",
            surface="active_runtime_contract",
            path=contract_path,
            payload=runtime_payload,
            schema_relpath="governance/04_DATA/SCHEMAS/C2/RUNTIME/active_runtime_contract.v1.schema.json",
        ),
        ("release", "release_manifest_active"): _ref(
            domain="release",
            surface="release_manifest_active",
            path=manifest_path,
            payload=manifest_payload,
            schema_relpath="governance/04_DATA/SCHEMAS/C2/RELEASES/release_manifest.v1.schema.json",
        ),
        ("policy", "configuration_state_current"): _ref(
            domain="policy",
            surface="configuration_state_current",
            path=(tmp_path / "truth" / "configuration_state_v1" / "current.json").resolve(),
            payload=configuration_payload,
            schema_relpath="governance/04_DATA/SCHEMAS/C2/RUNTIME/configuration_state.v1.schema.json",
        ),
    }

    def _fake_read_control_plane_surface_v1(*, domain: str, surface: str, **_: object) -> ControlPlaneReadRefV1:
        return refs[(domain, surface)]

    monkeypatch.setattr(runtime_contract_module, "read_control_plane_surface_v1", _fake_read_control_plane_surface_v1)
    return refs


def _write_shadow_current(
    *,
    truth_root: Path,
    manifest_payload: dict[str, object],
    manifest_path: Path,
    runtime_payload: dict[str, object],
    runtime_contract_path: Path,
    configuration_payload: dict[str, object],
    configuration_state_path: Path,
) -> dict[str, object]:
    payload = build_release_current_v1(
        release_manifest_ref=_ref(
            domain="release",
            surface="release_manifest_active",
            path=manifest_path,
            payload=manifest_payload,
            schema_relpath="governance/04_DATA/SCHEMAS/C2/RELEASES/release_manifest.v1.schema.json",
        ),
        active_runtime_contract_ref=_ref(
            domain="release",
            surface="active_runtime_contract",
            path=runtime_contract_path,
            payload=runtime_payload,
            schema_relpath="governance/04_DATA/SCHEMAS/C2/RUNTIME/active_runtime_contract.v1.schema.json",
        ),
        configuration_state_ref=_ref(
            domain="policy",
            surface="configuration_state_current",
            path=configuration_state_path,
            payload=configuration_payload,
            schema_relpath="governance/04_DATA/SCHEMAS/C2/RUNTIME/configuration_state.v1.schema.json",
        ),
        generated_at_utc="2026-04-20T14:05:00Z",
        producer_module="test_release_current_shadow_bridge_v1.py",
    )
    write_release_current_v1(truth_root=truth_root, payload=payload)
    return payload


def test_load_active_runtime_contract_shadow_match_returns_old_without_warning(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    truth_root = (tmp_path / "truth").resolve()
    truth_root.mkdir(parents=True, exist_ok=True)
    release_root = (tmp_path / "release").resolve()
    release_root.mkdir(parents=True, exist_ok=True)
    manifest_payload = _manifest_payload(release_root=release_root)
    runtime_payload = _runtime_payload(truth_root=truth_root, release_root=release_root)
    configuration_payload = _configuration_state_payload(truth_root=truth_root)
    refs = _install_runtime_contract_fixture(
        monkeypatch,
        tmp_path=tmp_path,
        manifest_payload=manifest_payload,
        runtime_payload=runtime_payload,
        configuration_payload=configuration_payload,
    )
    _write_shadow_current(
        truth_root=truth_root,
        manifest_payload=manifest_payload,
        manifest_path=refs[("release", "release_manifest_active")].path,
        runtime_payload=runtime_payload,
        runtime_contract_path=refs[("release", "active_runtime_contract")].path,
        configuration_payload=configuration_payload,
        configuration_state_path=refs[("policy", "configuration_state_current")].path,
    )

    caplog.set_level("WARNING")
    result = load_active_runtime_contract_or_fail()

    assert result == runtime_payload
    assert set(result.keys()) == set(runtime_payload.keys())
    assert "release_current_shadow_mismatch" not in caplog.text


def test_load_active_runtime_contract_fails_closed_when_release_current_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    truth_root = (tmp_path / "truth").resolve()
    truth_root.mkdir(parents=True, exist_ok=True)
    release_root = (tmp_path / "release").resolve()
    release_root.mkdir(parents=True, exist_ok=True)
    manifest_payload = _manifest_payload(release_root=release_root)
    runtime_payload = _runtime_payload(truth_root=truth_root, release_root=release_root)
    configuration_payload = _configuration_state_payload(truth_root=truth_root)
    _install_runtime_contract_fixture(
        monkeypatch,
        tmp_path=tmp_path,
        manifest_payload=manifest_payload,
        runtime_payload=runtime_payload,
        configuration_payload=configuration_payload,
    )

    with pytest.raises(SystemExit, match="release_current_required_missing"):
        load_active_runtime_contract_or_fail()


def test_load_active_runtime_contract_uses_release_current_contract_ref_when_present(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    truth_root = (tmp_path / "truth").resolve()
    truth_root.mkdir(parents=True, exist_ok=True)
    release_root = (tmp_path / "release").resolve()
    release_root.mkdir(parents=True, exist_ok=True)
    manifest_payload = _manifest_payload(release_root=release_root)
    runtime_payload = _runtime_payload(truth_root=truth_root, release_root=release_root)
    configuration_payload = _configuration_state_payload(truth_root=truth_root)
    refs = _install_runtime_contract_fixture(
        monkeypatch,
        tmp_path=tmp_path,
        manifest_payload=manifest_payload,
        runtime_payload=runtime_payload,
        configuration_payload=configuration_payload,
    )

    release_current_runtime_payload = dict(runtime_payload)
    release_current_runtime_payload["generated_at_utc"] = "2026-04-21T00:00:00Z"
    release_current_contract_path = (tmp_path / "release_current_contract" / "active_runtime_contract.v1.json").resolve()
    release_current_contract_path.parent.mkdir(parents=True, exist_ok=True)
    release_current_contract_path.write_bytes(canonical_json_bytes_v1(release_current_runtime_payload) + b"\n")

    _write_shadow_current(
        truth_root=truth_root,
        manifest_payload=manifest_payload,
        manifest_path=refs[("release", "release_manifest_active")].path,
        runtime_payload=release_current_runtime_payload,
        runtime_contract_path=release_current_contract_path,
        configuration_payload=configuration_payload,
        configuration_state_path=refs[("policy", "configuration_state_current")].path,
    )

    caplog.set_level("WARNING")
    result = load_active_runtime_contract_or_fail()

    assert result["generated_at_utc"] == "2026-04-21T00:00:00Z"
    assert result == release_current_runtime_payload
    assert "active_runtime_contract_cutover_mismatch" in caplog.text


def test_resolve_release_provenance_shadow_mismatch_logs_warning_but_returns_old_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    truth_root = (tmp_path / "truth").resolve()
    truth_root.mkdir(parents=True, exist_ok=True)
    release_root = (tmp_path / "release").resolve()
    release_root.mkdir(parents=True, exist_ok=True)
    manifest_payload = _manifest_payload(release_root=release_root)
    runtime_payload = _runtime_payload(truth_root=truth_root, release_root=release_root)
    configuration_payload = _configuration_state_payload(truth_root=truth_root)
    refs = _install_runtime_contract_fixture(
        monkeypatch,
        tmp_path=tmp_path,
        manifest_payload=manifest_payload,
        runtime_payload=runtime_payload,
        configuration_payload=configuration_payload,
    )
    shadow_payload = _write_shadow_current(
        truth_root=truth_root,
        manifest_payload=manifest_payload,
        manifest_path=refs[("release", "release_manifest_active")].path,
        runtime_payload=runtime_payload,
        runtime_contract_path=refs[("release", "active_runtime_contract")].path,
        configuration_payload=configuration_payload,
        configuration_state_path=refs[("policy", "configuration_state_current")].path,
    )
    shadow_payload["configuration_state_id"] = "e" * 64
    write_release_current_v1(truth_root=truth_root, payload=shadow_payload)

    caplog.set_level("WARNING")
    result = resolve_release_provenance()

    assert result["release_id"] == manifest_payload["release_id"]
    assert result["git_sha"] == manifest_payload["git_sha"]
    assert result["release_root"] == str(release_root)
    assert "release_current_shadow_mismatch" in caplog.text
    assert "configuration_state_id" in caplog.text


def test_load_active_runtime_contract_fails_closed_on_malformed_release_current(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    truth_root = (tmp_path / "truth").resolve()
    truth_root.mkdir(parents=True, exist_ok=True)
    release_root = (tmp_path / "release").resolve()
    release_root.mkdir(parents=True, exist_ok=True)
    manifest_payload = _manifest_payload(release_root=release_root)
    runtime_payload = _runtime_payload(truth_root=truth_root, release_root=release_root)
    configuration_payload = _configuration_state_payload(truth_root=truth_root)
    _install_runtime_contract_fixture(
        monkeypatch,
        tmp_path=tmp_path,
        manifest_payload=manifest_payload,
        runtime_payload=runtime_payload,
        configuration_payload=configuration_payload,
    )
    current_path = (truth_root / "release_current_v1" / "current.json").resolve()
    current_path.parent.mkdir(parents=True, exist_ok=True)
    current_path.write_text("{\"schema_id\":\"release_current.v1\"}\n", encoding="utf-8")

    with pytest.raises(SystemExit, match="release_current_shadow_invalid"):
        load_active_runtime_contract_or_fail()
