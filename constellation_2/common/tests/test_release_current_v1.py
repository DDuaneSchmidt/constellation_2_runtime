from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.control_plane_read_gateway_v1 import ControlPlaneReadRefV1
from constellation_2.common.release_current_v1 import (
    SCHEMA_RELPATH_V1,
    build_release_current_v1,
    reduce_release_current_v1,
    write_release_current_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import read_validated_surface_v1
from constellation_2.phaseC.lib.canon_json_v1 import canonical_json_bytes_v1


def _sha(payload: dict[str, object]) -> str:
    raw = canonical_json_bytes_v1(payload) + b"\n"
    return hashlib.sha256(raw).hexdigest()


def _ref(*, domain: str, surface: str, path: str, payload: dict[str, object], schema_relpath: str) -> ControlPlaneReadRefV1:
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


def _manifest_payload() -> dict[str, object]:
    return {
        "schema_id": "release_manifest.v1",
        "schema_version": "v1",
        "release_id": "release-20260420-001",
        "git_sha": "a" * 40,
        "release_root": "/home/node/constellation_releases/release-20260420-001",
    }


def _runtime_payload(*, truth_root: Path) -> dict[str, object]:
    return {
        "schema_id": "active_runtime_contract.v1",
        "schema_version": "v1",
        "release_id": "release-20260420-001",
        "git_sha": "a" * 40,
        "authoritative_repo_root": "/home/node/constellation",
        "release_root": "/home/node/constellation_releases/release-20260420-001",
        "runtime_data_root": str((truth_root.parent).resolve()),
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


def _configuration_state_payload() -> dict[str, object]:
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
            "path": "/tmp/policy.json",
            "sha256": "1" * 64,
            "artifact_class": "policy_snapshot",
            "finality_state": "finalized",
        },
        "configuration_validation_result_ref": {
            "artifact_id": "configuration_validation_result_v1",
            "path": "/tmp/validation.json",
            "sha256": "2" * 64,
            "artifact_class": "validation_result",
            "finality_state": "finalized",
        },
        "configuration_compile_result_ref": {
            "artifact_id": "configuration_compile_result_v1",
            "path": "/tmp/compile.json",
            "sha256": "3" * 64,
            "artifact_class": "compile_result",
            "finality_state": "finalized",
        },
        "configuration_review_diff_ref": {
            "artifact_id": "configuration_review_diff_v1",
            "path": "/tmp/review.json",
            "sha256": "4" * 64,
            "artifact_class": "review_diff",
            "finality_state": "finalized",
        },
        "compiled_active_config_ref": {
            "artifact_id": "compiled_active_config_v1",
            "path": "/tmp/truth/compiled_active_config_v1/" + ("c" * 64) + "/compiled_active_config.v1.json",
            "sha256": "5" * 64,
            "artifact_class": "compiled_config",
            "finality_state": "finalized",
        },
        "configuration_activation_transaction_ref": {
            "artifact_id": "configuration_activation_transaction_v1",
            "path": "/tmp/truth/reports/configuration_activation_transaction_v1/" + ("d" * 64) + "/configuration_activation_transaction.v1.json",
            "sha256": "6" * 64,
            "artifact_class": "admission_result",
            "finality_state": "finalized",
        },
        "active_compiled_config_sha256": "7" * 64,
        "constitutional_dependency_declaration": {},
        "constitutional_lineage": {},
    }


def test_release_current_build_and_write_happy_path(tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    truth_root.mkdir(parents=True, exist_ok=True)
    manifest_ref = _ref(
        domain="release",
        surface="release_manifest_active",
        path="/tmp/release_manifest.v1.json",
        payload=_manifest_payload(),
        schema_relpath="governance/04_DATA/SCHEMAS/C2/RELEASES/release_manifest.v1.schema.json",
    )
    runtime_ref = _ref(
        domain="release",
        surface="active_runtime_contract",
        path="/tmp/active_runtime_contract.v1.json",
        payload=_runtime_payload(truth_root=truth_root),
        schema_relpath="governance/04_DATA/SCHEMAS/C2/RUNTIME/active_runtime_contract.v1.schema.json",
    )
    configuration_ref = _ref(
        domain="policy",
        surface="configuration_state_current",
        path=str((truth_root / "configuration_state_v1" / "current.json").resolve()),
        payload=_configuration_state_payload(),
        schema_relpath="governance/04_DATA/SCHEMAS/C2/RUNTIME/configuration_state.v1.schema.json",
    )

    payload = build_release_current_v1(
        release_manifest_ref=manifest_ref,
        active_runtime_contract_ref=runtime_ref,
        configuration_state_ref=configuration_ref,
        generated_at_utc="2026-04-20T14:01:00Z",
        producer_module="test_release_current_v1.py",
    )

    assert payload["activation_state"] == "SHADOW_ONLY"
    assert payload["status"] == "ACTIVE_SHADOW"
    assert payload["release_id"] == "release-20260420-001"
    assert payload["configuration_state_id"] == "b" * 64
    assert payload["compiled_config_id"] == "c" * 64
    assert payload["activation_transaction_id"] == "d" * 64
    assert payload["allowed_truth_roots"] == [
        str(truth_root.resolve()),
        str((truth_root.parent / "truth_sleeves").resolve()),
    ]
    assert payload["primary_execution_identity_ref"] == {
        "authority_owner": "execution_identity_binding_v1",
        "sleeve_id": "PRIMARY",
    }

    ref = write_release_current_v1(truth_root=truth_root, payload=payload)
    reread = read_validated_surface_v1(path=ref.path, schema_relpath=SCHEMA_RELPATH_V1)
    assert reread.payload == ref.payload
    assert str(ref.path) == str((truth_root / "release_current_v1" / "current.json").resolve())


def test_release_current_fails_closed_on_release_id_mismatch(tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    manifest = _manifest_payload()
    runtime = _runtime_payload(truth_root=truth_root)
    runtime["release_id"] = "release-20260420-002"
    with pytest.raises(ValueError, match="RELEASE_CURRENT_RELEASE_ID_MISMATCH"):
        build_release_current_v1(
            release_manifest_ref=_ref(
                domain="release",
                surface="release_manifest_active",
                path="/tmp/release_manifest.v1.json",
                payload=manifest,
                schema_relpath="governance/04_DATA/SCHEMAS/C2/RELEASES/release_manifest.v1.schema.json",
            ),
            active_runtime_contract_ref=_ref(
                domain="release",
                surface="active_runtime_contract",
                path="/tmp/active_runtime_contract.v1.json",
                payload=runtime,
                schema_relpath="governance/04_DATA/SCHEMAS/C2/RUNTIME/active_runtime_contract.v1.schema.json",
            ),
            configuration_state_ref=_ref(
                domain="policy",
                surface="configuration_state_current",
                path=str((truth_root / "configuration_state_v1" / "current.json").resolve()),
                payload=_configuration_state_payload(),
                schema_relpath="governance/04_DATA/SCHEMAS/C2/RUNTIME/configuration_state.v1.schema.json",
            ),
            generated_at_utc="2026-04-20T14:01:00Z",
            producer_module="test_release_current_v1.py",
        )


def test_release_current_reduce_is_replay_stable(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    truth_root.mkdir(parents=True, exist_ok=True)
    refs = {
        ("release", "release_manifest_active"): _ref(
            domain="release",
            surface="release_manifest_active",
            path="/tmp/release_manifest.v1.json",
            payload=_manifest_payload(),
            schema_relpath="governance/04_DATA/SCHEMAS/C2/RELEASES/release_manifest.v1.schema.json",
        ),
        ("release", "active_runtime_contract"): _ref(
            domain="release",
            surface="active_runtime_contract",
            path="/tmp/active_runtime_contract.v1.json",
            payload=_runtime_payload(truth_root=truth_root),
            schema_relpath="governance/04_DATA/SCHEMAS/C2/RUNTIME/active_runtime_contract.v1.schema.json",
        ),
        ("policy", "configuration_state_current"): _ref(
            domain="policy",
            surface="configuration_state_current",
            path=str((truth_root / "configuration_state_v1" / "current.json").resolve()),
            payload=_configuration_state_payload(),
            schema_relpath="governance/04_DATA/SCHEMAS/C2/RUNTIME/configuration_state.v1.schema.json",
        ),
    }

    def _fake_read_control_plane_surface_v1(*, domain: str, surface: str, **_: object) -> ControlPlaneReadRefV1:
        return refs[(domain, surface)]

    monkeypatch.setattr(
        "constellation_2.common.release_current_v1.read_control_plane_surface_v1",
        _fake_read_control_plane_surface_v1,
    )

    first_ref = reduce_release_current_v1(
        truth_root=truth_root,
        generated_at_utc="2026-04-20T14:01:00Z",
        producer_module="test_release_current_v1.py",
    )
    second_ref = reduce_release_current_v1(
        truth_root=truth_root,
        generated_at_utc="2026-04-20T14:02:00Z",
        producer_module="test_release_current_v1.py",
    )

    assert first_ref.payload["release_current_id"] == second_ref.payload["release_current_id"]
    assert first_ref.path == second_ref.path
    assert second_ref.payload["generated_at_utc"] == first_ref.payload["generated_at_utc"]


def test_release_current_fails_closed_on_missing_primary_execution_identity_ref(tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    runtime = _runtime_payload(truth_root=truth_root)
    runtime.pop("primary_execution_identity_ref")
    with pytest.raises(ValueError, match="RELEASE_CURRENT_PRIMARY_EXECUTION_IDENTITY_REF_INVALID"):
        build_release_current_v1(
            release_manifest_ref=_ref(
                domain="release",
                surface="release_manifest_active",
                path="/tmp/release_manifest.v1.json",
                payload=_manifest_payload(),
                schema_relpath="governance/04_DATA/SCHEMAS/C2/RELEASES/release_manifest.v1.schema.json",
            ),
            active_runtime_contract_ref=_ref(
                domain="release",
                surface="active_runtime_contract",
                path="/tmp/active_runtime_contract.v1.json",
                payload=runtime,
                schema_relpath="governance/04_DATA/SCHEMAS/C2/RUNTIME/active_runtime_contract.v1.schema.json",
            ),
            configuration_state_ref=_ref(
                domain="policy",
                surface="configuration_state_current",
                path=str((truth_root / "configuration_state_v1" / "current.json").resolve()),
                payload=_configuration_state_payload(),
                schema_relpath="governance/04_DATA/SCHEMAS/C2/RUNTIME/configuration_state.v1.schema.json",
            ),
            generated_at_utc="2026-04-20T14:01:00Z",
            producer_module="test_release_current_v1.py",
        )


def test_release_current_fails_closed_on_missing_allowed_truth_roots(tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    runtime = _runtime_payload(truth_root=truth_root)
    runtime.pop("allowed_truth_roots")
    with pytest.raises(ValueError, match="RELEASE_CURRENT_ALLOWED_TRUTH_ROOTS_INVALID"):
        build_release_current_v1(
            release_manifest_ref=_ref(
                domain="release",
                surface="release_manifest_active",
                path="/tmp/release_manifest.v1.json",
                payload=_manifest_payload(),
                schema_relpath="governance/04_DATA/SCHEMAS/C2/RELEASES/release_manifest.v1.schema.json",
            ),
            active_runtime_contract_ref=_ref(
                domain="release",
                surface="active_runtime_contract",
                path="/tmp/active_runtime_contract.v1.json",
                payload=runtime,
                schema_relpath="governance/04_DATA/SCHEMAS/C2/RUNTIME/active_runtime_contract.v1.schema.json",
            ),
            configuration_state_ref=_ref(
                domain="policy",
                surface="configuration_state_current",
                path=str((truth_root / "configuration_state_v1" / "current.json").resolve()),
                payload=_configuration_state_payload(),
                schema_relpath="governance/04_DATA/SCHEMAS/C2/RUNTIME/configuration_state.v1.schema.json",
            ),
            generated_at_utc="2026-04-20T14:01:00Z",
            producer_module="test_release_current_v1.py",
        )


def test_release_current_fails_closed_on_allowed_truth_roots_mismatch(tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    runtime = _runtime_payload(truth_root=truth_root)
    runtime["allowed_truth_roots"] = [str((truth_root.parent / "other").resolve()), str(truth_root.resolve())]
    with pytest.raises(ValueError, match="RELEASE_CURRENT_ALLOWED_TRUTH_ROOTS_MISMATCH"):
        build_release_current_v1(
            release_manifest_ref=_ref(
                domain="release",
                surface="release_manifest_active",
                path="/tmp/release_manifest.v1.json",
                payload=_manifest_payload(),
                schema_relpath="governance/04_DATA/SCHEMAS/C2/RELEASES/release_manifest.v1.schema.json",
            ),
            active_runtime_contract_ref=_ref(
                domain="release",
                surface="active_runtime_contract",
                path="/tmp/active_runtime_contract.v1.json",
                payload=runtime,
                schema_relpath="governance/04_DATA/SCHEMAS/C2/RUNTIME/active_runtime_contract.v1.schema.json",
            ),
            configuration_state_ref=_ref(
                domain="policy",
                surface="configuration_state_current",
                path=str((truth_root / "configuration_state_v1" / "current.json").resolve()),
                payload=_configuration_state_payload(),
                schema_relpath="governance/04_DATA/SCHEMAS/C2/RUNTIME/configuration_state.v1.schema.json",
            ),
            generated_at_utc="2026-04-20T14:01:00Z",
            producer_module="test_release_current_v1.py",
        )


def test_release_current_fails_closed_on_primary_execution_identity_owner_mismatch(tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    runtime = _runtime_payload(truth_root=truth_root)
    runtime["primary_execution_identity_ref"] = {
        "authority_owner": "wrong_owner_v1",
        "sleeve_id": "PRIMARY",
    }
    with pytest.raises(ValueError, match="RELEASE_CURRENT_PRIMARY_EXECUTION_IDENTITY_REF_OWNER_MISMATCH"):
        build_release_current_v1(
            release_manifest_ref=_ref(
                domain="release",
                surface="release_manifest_active",
                path="/tmp/release_manifest.v1.json",
                payload=_manifest_payload(),
                schema_relpath="governance/04_DATA/SCHEMAS/C2/RELEASES/release_manifest.v1.schema.json",
            ),
            active_runtime_contract_ref=_ref(
                domain="release",
                surface="active_runtime_contract",
                path="/tmp/active_runtime_contract.v1.json",
                payload=runtime,
                schema_relpath="governance/04_DATA/SCHEMAS/C2/RUNTIME/active_runtime_contract.v1.schema.json",
            ),
            configuration_state_ref=_ref(
                domain="policy",
                surface="configuration_state_current",
                path=str((truth_root / "configuration_state_v1" / "current.json").resolve()),
                payload=_configuration_state_payload(),
                schema_relpath="governance/04_DATA/SCHEMAS/C2/RUNTIME/configuration_state.v1.schema.json",
            ),
            generated_at_utc="2026-04-20T14:01:00Z",
            producer_module="test_release_current_v1.py",
        )


def test_release_current_reduce_fails_closed_on_truth_root_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    truth_root = (tmp_path / "truth").resolve()
    other_truth_root = (tmp_path / "other_truth").resolve()
    truth_root.mkdir(parents=True, exist_ok=True)
    refs = {
        ("release", "active_runtime_contract"): _ref(
            domain="release",
            surface="active_runtime_contract",
            path="/tmp/active_runtime_contract.v1.json",
            payload=_runtime_payload(truth_root=truth_root),
            schema_relpath="governance/04_DATA/SCHEMAS/C2/RUNTIME/active_runtime_contract.v1.schema.json",
        ),
    }

    def _fake_read_control_plane_surface_v1(*, domain: str, surface: str, **_: object) -> ControlPlaneReadRefV1:
        return refs[(domain, surface)]

    monkeypatch.setattr(
        "constellation_2.common.release_current_v1.read_control_plane_surface_v1",
        _fake_read_control_plane_surface_v1,
    )

    with pytest.raises(ValueError, match="RELEASE_CURRENT_CANONICAL_TRUTH_ROOT_MISMATCH"):
        reduce_release_current_v1(
            truth_root=other_truth_root,
            generated_at_utc="2026-04-20T14:03:00Z",
            producer_module="test_release_current_v1.py",
        )
