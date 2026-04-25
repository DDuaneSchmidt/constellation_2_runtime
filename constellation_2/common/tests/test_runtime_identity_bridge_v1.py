from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import constellation_2.common.runtime_identity_bridge_v1 as bridge
import constellation_2.common.runtime_identity_v1 as runtime_identity_v1


def _old_identity_payload(*, tmp_path: Path) -> dict[str, object]:
    canonical_truth_root = (tmp_path / "old" / "truth").resolve()
    truth_sleeves_root = (tmp_path / "old" / "truth_sleeves").resolve()
    runtime_data_root = (tmp_path / "old" / "runtime_data").resolve()
    release_root = (tmp_path / "old" / "release").resolve()
    for path in (
        canonical_truth_root,
        truth_sleeves_root,
        runtime_data_root,
        release_root,
        truth_sleeves_root / "PRIMARY" / "PAPER",
    ):
        path.mkdir(parents=True, exist_ok=True)
    return {
        "contract_path": str((tmp_path / "old" / "runtime_contract" / "active_runtime_contract.v1.json").resolve()),
        "contract_sha256": "a" * 64,
        "schema_id": "active_runtime_contract.v1",
        "schema_version": "v1",
        "status": "ACTIVE",
        "generated_at_utc": "2026-04-21T12:00:00Z",
        "authoritative_repo_root": str(SOURCE_ROOT.resolve()),
        "release_id": "release_old",
        "git_sha": "a" * 40,
        "release_root": str(release_root),
        "runtime_data_root": str(runtime_data_root),
        "canonical_truth_root": str(canonical_truth_root),
        "truth_sleeves_root": str(truth_sleeves_root),
        "pointer_index_family": "run_pointer_v1",
        "allowed_truth_roots": [str(canonical_truth_root), str(truth_sleeves_root)],
        "provenance_mode": "release_manifest",
        "runtime_environment": "PAPER",
        "primary_execution_identity_ref": {
            "authority_owner": "execution_identity_binding_v1",
            "sleeve_id": "PRIMARY",
        },
        "resolved_execution_identity": {
            "authority_owner": "execution_identity_binding_v1",
            "environment": "PAPER",
            "sleeve_id": "PRIMARY",
            "account_id": "DUO847203",
            "client_id_orders": 79,
            "client_id_observer": 179,
            "host": "127.0.0.1",
            "port": 4002,
            "sleeve_registry_path": str(SOURCE_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json"),
            "account_registry_path": str(SOURCE_ROOT / "governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json"),
        },
        "resolved_execution_roots": {
            "authority_owner": "sleeve_execution_root_v1",
            "environment": "PAPER",
            "ib_account": "DUO847203",
            "sleeve_id": "PRIMARY",
            "mode": "PAPER",
            "truth_partition": "PRIMARY",
            "execution_root_path": str((truth_sleeves_root / "PRIMARY" / "PAPER").resolve()),
            "truth_sleeves_root": str(truth_sleeves_root),
            "sleeve_registry_path": str(SOURCE_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json"),
        },
        "path_authority_snapshot": {
            "authoritative_repo_root": str(SOURCE_ROOT),
            "canonical_runtime_truth_root": str(canonical_truth_root),
            "canonical_runtime_truth_sleeves_root": str(truth_sleeves_root),
            "authoritative_repo_truth_root": str(SOURCE_ROOT / "constellation_2/runtime/truth"),
            "authoritative_repo_truth_sleeves_root": str(SOURCE_ROOT / "constellation_2/runtime/truth_sleeves"),
            "active_release_root": str(release_root),
        },
        "release_provenance": {
            "release_id": "release_old",
            "git_sha": "a" * 40,
            "release_root": str(release_root),
            "release_manifest_path": str((release_root / "release_manifest.v1.json").resolve()),
            "provenance_mode": "release_manifest",
            "resolved_at_utc": "2026-04-21T12:00:05Z",
        },
    }


def _release_current_authority_payload(*, tmp_path: Path) -> dict[str, object]:
    canonical_truth_root = (tmp_path / "new" / "truth").resolve()
    truth_sleeves_root = (tmp_path / "new" / "truth_sleeves").resolve()
    runtime_data_root = (tmp_path / "new" / "runtime_data").resolve()
    release_root = (tmp_path / "new" / "release").resolve()
    for path in (
        canonical_truth_root,
        truth_sleeves_root,
        runtime_data_root,
        release_root,
        truth_sleeves_root / "PRIMARY" / "PAPER",
    ):
        path.mkdir(parents=True, exist_ok=True)
    current_path = (canonical_truth_root / "release_current_v1" / "current.json").resolve()
    current_path.parent.mkdir(parents=True, exist_ok=True)
    current_path.write_text("{}\n", encoding="utf-8")
    return {
        "source": "release_current_v1",
        "release_current_id": "f" * 64,
        "release_current_path": str(current_path),
        "authoritative_repo_root": str(SOURCE_ROOT.resolve()),
        "release_root": str(release_root),
        "canonical_truth_root": str(canonical_truth_root),
        "truth_sleeves_root": str(truth_sleeves_root),
        "runtime_data_root": str(runtime_data_root),
        "runtime_environment": "PAPER",
        "pointer_index_family": "run_pointer_v2",
        "allowed_truth_roots": [str(canonical_truth_root), str(truth_sleeves_root)],
    }


def _release_current_payload(*, authority_payload: dict[str, object], contract_path: Path, contract_sha256: str) -> dict[str, object]:
    return {
        "release_current_id": authority_payload["release_current_id"],
        "release_id": "release_new",
        "git_sha": "b" * 40,
        "authoritative_repo_root": authority_payload["authoritative_repo_root"],
        "primary_execution_identity_ref": {
            "authority_owner": "execution_identity_binding_v1",
            "sleeve_id": "PRIMARY",
        },
        "canonical_truth_root": authority_payload["canonical_truth_root"],
        "active_runtime_contract_ref": {
            "domain": "release",
            "surface": "active_runtime_contract",
            "path": str(contract_path.resolve()),
            "sha256": contract_sha256,
            "schema_relpath": "governance/04_DATA/SCHEMAS/C2/RUNTIME/active_runtime_contract.v1.schema.json",
        },
    }


def _contract_payload(*, authority_payload: dict[str, object]) -> dict[str, object]:
    return {
        "schema_id": "active_runtime_contract.v1",
        "schema_version": "v1",
        "status": "ACTIVE",
        "generated_at_utc": "2026-04-21T12:10:00Z",
        "provenance_mode": "release_manifest",
        "release_id": "release_new",
        "git_sha": "b" * 40,
        "authoritative_repo_root": authority_payload["authoritative_repo_root"],
        "release_root": authority_payload["release_root"],
        "runtime_data_root": authority_payload["runtime_data_root"],
        "canonical_truth_root": authority_payload["canonical_truth_root"],
        "truth_sleeves_root": authority_payload["truth_sleeves_root"],
        "pointer_index_family": authority_payload["pointer_index_family"],
        "allowed_truth_roots": authority_payload["allowed_truth_roots"],
        "runtime_environment": authority_payload["runtime_environment"],
        "primary_execution_identity_ref": {
            "authority_owner": "execution_identity_binding_v1",
            "sleeve_id": "PRIMARY",
        },
    }


def test_bridge_uses_release_current_identity_when_valid(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    old_identity = _old_identity_payload(tmp_path=tmp_path)
    authority_payload = _release_current_authority_payload(tmp_path=tmp_path)
    current_path = (
        Path(str(old_identity["canonical_truth_root"])).resolve() / "release_current_v1" / "current.json"
    ).resolve()
    current_path.parent.mkdir(parents=True, exist_ok=True)
    current_path.write_text("{}\n", encoding="utf-8")
    authority_payload["release_current_path"] = str(current_path)
    contract_path = (tmp_path / "new" / "runtime_contract" / "active_runtime_contract.v1.json").resolve()
    contract_path.parent.mkdir(parents=True, exist_ok=True)
    contract_path.write_text(json.dumps(_contract_payload(authority_payload=authority_payload)), encoding="utf-8")
    contract_sha256 = bridge._sha256_hex(contract_path)
    current_payload = _release_current_payload(
        authority_payload=authority_payload,
        contract_path=contract_path,
        contract_sha256=contract_sha256,
    )

    monkeypatch.setattr(
        bridge.runtime_identity_legacy_v1,
        "_load_active_runtime_identity_snapshot_legacy_v1",
        lambda repo_root=None: dict(old_identity),
    )
    monkeypatch.setattr(bridge, "load_release_current_runtime_authority_v1", lambda caller="": dict(authority_payload))
    monkeypatch.setattr(
        bridge,
        "read_validated_surface_v1",
        lambda *, path, schema_relpath: SimpleNamespace(
            path=Path(path),
            schema_relpath=schema_relpath,
            payload=(
                dict(current_payload)
                if Path(path).resolve() == Path(authority_payload["release_current_path"]).resolve()
                else dict(_contract_payload(authority_payload=authority_payload))
            ),
        ),
    )
    monkeypatch.setattr(bridge, "require_authoritative_repo_runtime_v1", lambda repo_root=None: SOURCE_ROOT.resolve())
    monkeypatch.setattr(
        bridge,
        "resolve_governed_execution_identity_v1",
        lambda **_: SimpleNamespace(
            authority_owner="execution_identity_binding_v1",
            environment="PAPER",
            sleeve_id="PRIMARY",
            account_id="DUO847203",
            client_id_orders=79,
            client_id_observer=179,
            host="127.0.0.1",
            port=4002,
            sleeve_registry_path=SOURCE_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json",
            account_registry_path=SOURCE_ROOT / "governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json",
        ),
    )
    monkeypatch.setattr(
        bridge,
        "resolve_governed_paper_execution_roots",
        lambda **_: SimpleNamespace(
            authority_owner="sleeve_execution_root_v1",
            environment="PAPER",
            ib_account="DUO847203",
            sleeve_id="PRIMARY",
            mode="PAPER",
            truth_partition="PRIMARY",
            execution_root_path=Path(str(authority_payload["truth_sleeves_root"])).resolve() / "PRIMARY" / "PAPER",
            truth_sleeves_root=Path(str(authority_payload["truth_sleeves_root"])).resolve(),
            sleeve_registry_path=SOURCE_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json",
        ),
    )
    monkeypatch.setattr(
        bridge,
        "resolve_runtime_path_authority_snapshot_bridge_v1",
        lambda repo_root=None, caller="": {
            "authoritative_repo_root": str(SOURCE_ROOT.resolve()),
            "canonical_runtime_truth_root": str(Path(str(authority_payload["canonical_truth_root"])).resolve()),
            "canonical_runtime_truth_sleeves_root": str(Path(str(authority_payload["truth_sleeves_root"])).resolve()),
            "authoritative_repo_truth_root": str(SOURCE_ROOT / "constellation_2/runtime/truth"),
            "authoritative_repo_truth_sleeves_root": str(SOURCE_ROOT / "constellation_2/runtime/truth_sleeves"),
            "active_release_root": str(Path(str(authority_payload["release_root"])).resolve()),
        },
    )
    monkeypatch.setattr(
        bridge,
        "resolve_release_provenance",
        lambda: {
            "release_id": "release_new",
            "git_sha": "b" * 40,
            "release_root": str(Path(str(authority_payload["release_root"])).resolve()),
            "release_manifest_path": str((Path(str(authority_payload["release_root"])).resolve() / "release_manifest.v1.json")),
            "provenance_mode": "release_manifest",
            "resolved_at_utc": "2026-04-21T12:10:05Z",
        },
    )

    result = bridge.load_active_runtime_identity_snapshot_bridge_v1(
        repo_root=SOURCE_ROOT,
        caller="test_runtime_identity_bridge_valid",
    )

    assert result["release_id"] == "release_new"
    assert result["git_sha"] == "b" * 40
    assert result["canonical_truth_root"] == str(Path(str(authority_payload["canonical_truth_root"])).resolve())
    assert result["truth_sleeves_root"] == str(Path(str(authority_payload["truth_sleeves_root"])).resolve())
    assert result["release_root"] == str(Path(str(authority_payload["release_root"])).resolve())
    assert result["pointer_index_family"] == "run_pointer_v2"
    assert result["contract_path"] == str(contract_path.resolve())
    assert result["contract_sha256"] == contract_sha256
    assert result["primary_execution_identity_ref"] == {
        "authority_owner": "execution_identity_binding_v1",
        "sleeve_id": "PRIMARY",
    }
    assert set(result.keys()) == set(old_identity.keys())


def test_bridge_falls_back_to_old_identity_when_release_current_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    old_identity = _old_identity_payload(tmp_path=tmp_path)
    canonical_truth_root = Path(str(old_identity["canonical_truth_root"])).resolve()
    missing_current_path = (canonical_truth_root / "release_current_v1" / "current.json").resolve()
    if missing_current_path.exists():
        missing_current_path.unlink()

    monkeypatch.setattr(
        bridge.runtime_identity_legacy_v1,
        "_load_active_runtime_identity_snapshot_legacy_v1",
        lambda repo_root=None: dict(old_identity),
    )
    monkeypatch.setattr(
        bridge,
        "load_release_current_runtime_authority_v1",
        lambda caller="": (_ for _ in ()).throw(AssertionError("should not call runtime authority bridge when current is missing")),
    )

    result = bridge.load_active_runtime_identity_snapshot_bridge_v1(
        repo_root=SOURCE_ROOT,
        caller="test_runtime_identity_bridge_missing",
    )
    assert result == old_identity


def test_bridge_fails_closed_when_release_current_invalid(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    old_identity = _old_identity_payload(tmp_path=tmp_path)
    current_path = (
        Path(str(old_identity["canonical_truth_root"])).resolve() / "release_current_v1" / "current.json"
    ).resolve()
    current_path.parent.mkdir(parents=True, exist_ok=True)
    current_path.write_text("{}\n", encoding="utf-8")

    monkeypatch.setattr(
        bridge.runtime_identity_legacy_v1,
        "_load_active_runtime_identity_snapshot_legacy_v1",
        lambda repo_root=None: dict(old_identity),
    )
    monkeypatch.setattr(
        bridge,
        "load_release_current_runtime_authority_v1",
        lambda caller="": (_ for _ in ()).throw(
            SystemExit("FAIL: runtime_authority_bridge_release_current_invalid path=/tmp/current.json err=ValueError:bad")
        ),
    )

    with pytest.raises(SystemExit, match="runtime_authority_bridge_release_current_invalid"):
        bridge.load_active_runtime_identity_snapshot_bridge_v1(
            repo_root=SOURCE_ROOT,
            caller="test_runtime_identity_bridge_invalid",
        )


def test_bridge_logs_mismatches(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    old_identity = _old_identity_payload(tmp_path=tmp_path)
    authority_payload = _release_current_authority_payload(tmp_path=tmp_path)
    current_path = (
        Path(str(old_identity["canonical_truth_root"])).resolve() / "release_current_v1" / "current.json"
    ).resolve()
    current_path.parent.mkdir(parents=True, exist_ok=True)
    current_path.write_text("{}\n", encoding="utf-8")
    authority_payload["release_current_path"] = str(current_path)
    contract_path = (tmp_path / "new" / "runtime_contract" / "active_runtime_contract.v1.json").resolve()
    contract_path.parent.mkdir(parents=True, exist_ok=True)
    contract_path.write_text(json.dumps(_contract_payload(authority_payload=authority_payload)), encoding="utf-8")
    contract_sha256 = bridge._sha256_hex(contract_path)
    current_payload = _release_current_payload(
        authority_payload=authority_payload,
        contract_path=contract_path,
        contract_sha256=contract_sha256,
    )

    monkeypatch.setattr(
        bridge.runtime_identity_legacy_v1,
        "_load_active_runtime_identity_snapshot_legacy_v1",
        lambda repo_root=None: dict(old_identity),
    )
    monkeypatch.setattr(bridge, "load_release_current_runtime_authority_v1", lambda caller="": dict(authority_payload))
    monkeypatch.setattr(
        bridge,
        "read_validated_surface_v1",
        lambda *, path, schema_relpath: SimpleNamespace(
            path=Path(path),
            schema_relpath=schema_relpath,
            payload=(
                dict(current_payload)
                if Path(path).resolve() == Path(authority_payload["release_current_path"]).resolve()
                else dict(_contract_payload(authority_payload=authority_payload))
            ),
        ),
    )
    monkeypatch.setattr(bridge, "require_authoritative_repo_runtime_v1", lambda repo_root=None: SOURCE_ROOT.resolve())
    monkeypatch.setattr(
        bridge,
        "resolve_governed_execution_identity_v1",
        lambda **_: SimpleNamespace(
            authority_owner="execution_identity_binding_v1",
            environment="PAPER",
            sleeve_id="PRIMARY",
            account_id="DUO847203",
            client_id_orders=79,
            client_id_observer=179,
            host="127.0.0.1",
            port=4002,
            sleeve_registry_path=SOURCE_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json",
            account_registry_path=SOURCE_ROOT / "governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json",
        ),
    )
    monkeypatch.setattr(
        bridge,
        "resolve_governed_paper_execution_roots",
        lambda **_: SimpleNamespace(
            authority_owner="sleeve_execution_root_v1",
            environment="PAPER",
            ib_account="DUO847203",
            sleeve_id="PRIMARY",
            mode="PAPER",
            truth_partition="PRIMARY",
            execution_root_path=Path(str(authority_payload["truth_sleeves_root"])).resolve() / "PRIMARY" / "PAPER",
            truth_sleeves_root=Path(str(authority_payload["truth_sleeves_root"])).resolve(),
            sleeve_registry_path=SOURCE_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json",
        ),
    )
    monkeypatch.setattr(
        bridge,
        "resolve_runtime_path_authority_snapshot_bridge_v1",
        lambda repo_root=None, caller="": {
            "authoritative_repo_root": str(SOURCE_ROOT.resolve()),
            "canonical_runtime_truth_root": str(Path(str(authority_payload["canonical_truth_root"])).resolve()),
            "canonical_runtime_truth_sleeves_root": str(Path(str(authority_payload["truth_sleeves_root"])).resolve()),
            "authoritative_repo_truth_root": str(SOURCE_ROOT / "constellation_2/runtime/truth"),
            "authoritative_repo_truth_sleeves_root": str(SOURCE_ROOT / "constellation_2/runtime/truth_sleeves"),
            "active_release_root": str(Path(str(authority_payload["release_root"])).resolve()),
        },
    )
    monkeypatch.setattr(
        bridge,
        "resolve_release_provenance",
        lambda: {
            "release_id": "release_new",
            "git_sha": "b" * 40,
            "release_root": str(Path(str(authority_payload["release_root"])).resolve()),
            "release_manifest_path": str((Path(str(authority_payload["release_root"])).resolve() / "release_manifest.v1.json")),
            "provenance_mode": "release_manifest",
            "resolved_at_utc": "2026-04-21T12:10:05Z",
        },
    )

    caplog.set_level("WARNING", logger="constellation_2.common.runtime_identity_bridge_v1")
    _ = bridge.load_active_runtime_identity_snapshot_bridge_v1(
        repo_root=SOURCE_ROOT,
        caller="test_runtime_identity_bridge_mismatch",
    )

    messages = [record.message for record in caplog.records if "runtime_identity_bridge_mismatch" in record.message]
    assert messages
    payload = json.loads(messages[-1].split("runtime_identity_bridge_mismatch ", 1)[1])
    fields = {entry["field"] for entry in payload["mismatches"]}
    assert {
        "canonical_truth_root",
        "truth_sleeves_root",
        "release_root",
        "pointer_index_family",
        "allowed_truth_roots",
        "contract_path",
        "contract_sha256",
    }.issubset(fields)


def test_runtime_identity_v1_delegates_to_bridge(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel = {"release_id": "bridge_result"}
    monkeypatch.setattr(
        bridge,
        "load_active_runtime_identity_snapshot_bridge_v1",
        lambda repo_root=None, caller="": dict(sentinel),
    )
    assert runtime_identity_v1.load_active_runtime_identity_snapshot_v1(repo_root=SOURCE_ROOT) == sentinel
