from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from constellation_2.common import runtime_identity_v1


REPO_ROOT = Path("/home/node/constellation")


def _contract_payload(tmp_path: Path) -> dict[str, object]:
    return {
        "schema_id": "active_runtime_contract.v1",
        "schema_version": "v1",
        "release_id": "release_20260418",
        "git_sha": "a" * 40,
        "authoritative_repo_root": str(REPO_ROOT),
        "release_root": str(tmp_path / "release_root"),
        "runtime_data_root": str(tmp_path / "runtime_data"),
        "canonical_truth_root": str(tmp_path / "runtime_data" / "truth"),
        "truth_sleeves_root": str(tmp_path / "runtime_data" / "truth_sleeves"),
        "pointer_index_family": "run_pointer_v1",
        "allowed_truth_roots": [
            str(tmp_path / "runtime_data" / "truth"),
            str(tmp_path / "runtime_data" / "truth_sleeves"),
        ],
        "provenance_mode": "release_manifest",
        "runtime_environment": "PAPER",
        "primary_execution_identity_ref": {
            "authority_owner": "execution_identity_binding_v1",
            "sleeve_id": "PRIMARY",
        },
        "generated_at_utc": "2026-04-18T12:00:00Z",
        "status": "ACTIVE",
    }


def test_load_active_runtime_identity_snapshot_resolves_existing_authorities(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract_path = tmp_path / "active_runtime_contract.v1.json"
    contract = _contract_payload(tmp_path)
    contract_path.write_text(json.dumps(contract), encoding="utf-8")

    monkeypatch.setattr(runtime_identity_v1, "ACTIVE_RUNTIME_CONTRACT_PATH", contract_path)
    monkeypatch.setattr(
        runtime_identity_v1,
        "load_active_runtime_contract_or_fail",
        lambda: contract,
    )
    monkeypatch.setattr(
        runtime_identity_v1,
        "require_authoritative_repo_runtime_v1",
        lambda repo_root=None: REPO_ROOT,
    )
    monkeypatch.setattr(
        runtime_identity_v1,
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
            sleeve_registry_path=REPO_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json",
            account_registry_path=REPO_ROOT / "governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json",
        ),
    )
    monkeypatch.setattr(
        runtime_identity_v1,
        "resolve_governed_paper_execution_roots",
        lambda **_: SimpleNamespace(
            authority_owner="sleeve_execution_root_v1",
            environment="PAPER",
            ib_account="DUO847203",
            sleeve_id="PRIMARY",
            mode="PAPER",
            truth_partition="PRIMARY",
            execution_root_path=tmp_path / "runtime_data" / "truth_sleeves" / "PRIMARY" / "PAPER",
            truth_sleeves_root=tmp_path / "runtime_data" / "truth_sleeves",
            sleeve_registry_path=REPO_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json",
        ),
    )
    monkeypatch.setattr(
        runtime_identity_v1,
        "resolve_runtime_path_authority_snapshot_v1",
        lambda repo_root=None: {
            "authoritative_repo_root": str(REPO_ROOT),
            "canonical_runtime_truth_root": str(tmp_path / "runtime_data" / "truth"),
            "canonical_runtime_truth_sleeves_root": str(tmp_path / "runtime_data" / "truth_sleeves"),
            "authoritative_repo_truth_root": str(REPO_ROOT / "constellation_2/runtime/truth"),
            "authoritative_repo_truth_sleeves_root": str(REPO_ROOT / "constellation_2/runtime/truth_sleeves"),
            "active_release_root": str(tmp_path / "release_root"),
        },
    )
    monkeypatch.setattr(
        runtime_identity_v1,
        "resolve_release_provenance",
        lambda: {
            "release_id": "release_20260418",
            "git_sha": "a" * 40,
            "release_root": str(tmp_path / "release_root"),
            "release_manifest_path": str(tmp_path / "release_root" / "release_manifest.v1.json"),
            "provenance_mode": "release_manifest",
            "resolved_at_utc": "2026-04-18T12:00:05Z",
        },
    )

    snapshot = runtime_identity_v1.load_active_runtime_identity_snapshot_v1(repo_root=REPO_ROOT)

    assert snapshot["authoritative_repo_root"] == str(REPO_ROOT)
    assert snapshot["runtime_environment"] == "PAPER"
    assert snapshot["primary_execution_identity_ref"]["authority_owner"] == "execution_identity_binding_v1"
    assert snapshot["resolved_execution_identity"]["client_id_observer"] == 179
    assert snapshot["resolved_execution_roots"]["execution_root_path"].endswith("/PRIMARY/PAPER")
    assert snapshot["contract_sha256"] == runtime_identity_v1._sha256_hex(contract_path)


def test_load_active_runtime_identity_snapshot_fails_closed_on_repo_root_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract_path = tmp_path / "active_runtime_contract.v1.json"
    contract = _contract_payload(tmp_path)
    contract["authoritative_repo_root"] = "/tmp/not-authoritative"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")

    monkeypatch.setattr(runtime_identity_v1, "ACTIVE_RUNTIME_CONTRACT_PATH", contract_path)
    monkeypatch.setattr(
        runtime_identity_v1,
        "load_active_runtime_contract_or_fail",
        lambda: contract,
    )
    monkeypatch.setattr(
        runtime_identity_v1,
        "require_authoritative_repo_runtime_v1",
        lambda repo_root=None: REPO_ROOT,
    )

    with pytest.raises(SystemExit, match="runtime_identity_authoritative_repo_root_mismatch"):
        runtime_identity_v1.load_active_runtime_identity_snapshot_v1(repo_root=REPO_ROOT)


def test_runtime_startup_identity_receipt_writes_append_only_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_data_root = tmp_path / "runtime_data"
    runtime_data_root.mkdir(parents=True)
    monkeypatch.setattr(
        runtime_identity_v1,
        "load_active_runtime_identity_snapshot_v1",
        lambda repo_root=None: {
            "contract_path": str(tmp_path / "runtime_contract_v1" / "active_runtime_contract.v1.json"),
            "contract_sha256": "b" * 64,
            "schema_id": "active_runtime_contract.v1",
            "schema_version": "v1",
            "status": "ACTIVE",
            "generated_at_utc": "2026-04-18T12:00:00Z",
            "authoritative_repo_root": str(REPO_ROOT),
            "release_id": "release_20260418",
            "git_sha": "a" * 40,
            "release_root": str(tmp_path / "release_root"),
            "runtime_data_root": str(runtime_data_root),
            "canonical_truth_root": str(runtime_data_root / "truth"),
            "truth_sleeves_root": str(runtime_data_root / "truth_sleeves"),
            "pointer_index_family": "run_pointer_v1",
            "allowed_truth_roots": [
                str(runtime_data_root / "truth"),
                str(runtime_data_root / "truth_sleeves"),
            ],
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
                "sleeve_registry_path": str(REPO_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json"),
                "account_registry_path": str(REPO_ROOT / "governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json"),
            },
            "resolved_execution_roots": {
                "authority_owner": "sleeve_execution_root_v1",
                "environment": "PAPER",
                "ib_account": "DUO847203",
                "sleeve_id": "PRIMARY",
                "mode": "PAPER",
                "truth_partition": "PRIMARY",
                "execution_root_path": str(runtime_data_root / "truth_sleeves" / "PRIMARY" / "PAPER"),
                "truth_sleeves_root": str(runtime_data_root / "truth_sleeves"),
                "sleeve_registry_path": str(REPO_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json"),
            },
            "path_authority_snapshot": {
                "authoritative_repo_root": str(REPO_ROOT),
                "canonical_runtime_truth_root": str(runtime_data_root / "truth"),
                "canonical_runtime_truth_sleeves_root": str(runtime_data_root / "truth_sleeves"),
                "authoritative_repo_truth_root": str(REPO_ROOT / "constellation_2/runtime/truth"),
                "authoritative_repo_truth_sleeves_root": str(REPO_ROOT / "constellation_2/runtime/truth_sleeves"),
                "active_release_root": str(tmp_path / "release_root"),
            },
            "release_provenance": {
                "release_id": "release_20260418",
                "git_sha": "a" * 40,
                "release_root": str(tmp_path / "release_root"),
                "release_manifest_path": str(tmp_path / "release_root" / "release_manifest.v1.json"),
                "provenance_mode": "release_manifest",
                "resolved_at_utc": "2026-04-18T12:00:05Z",
            },
        },
    )

    payload = runtime_identity_v1.derive_runtime_startup_identity_payload(
        repo_root=REPO_ROOT,
        entrypoint_name="c2_paper_day_orchestrator_systemd_entry_v1.sh",
        entrypoint_path=REPO_ROOT / "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
        service_name="c2-paper-day-orchestrator.service",
        requested_python="python3",
        resolved_python_executable=sys.executable,
    )

    receipt_path = runtime_identity_v1.write_runtime_startup_identity_receipt_v1(
        repo_root=REPO_ROOT,
        payload=payload,
    )
    written = json.loads(receipt_path.read_text(encoding="utf-8"))

    assert receipt_path.name == "runtime_startup_identity.v1.json"
    assert receipt_path.parent.parent.name == "runtime_startup_identity_v1"
    assert written["schema_id"] == "runtime_startup_identity.v1"
    assert written["status"] == "CAPTURED"
    assert written["runtime_identity"]["runtime_environment"] == "PAPER"
    assert written["resolved_python_executable"] == str(Path(sys.executable).resolve())
