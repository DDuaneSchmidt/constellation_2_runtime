from __future__ import annotations

import json
from pathlib import Path

import pytest

from constellation_2.common.execution_identity_binding_v1 import (
    EXECUTION_IDENTITY_BINDING_OWNER,
    RC_EXECUTION_IDENTITY_ACCOUNT_AMBIGUOUS,
    RC_EXECUTION_IDENTITY_ACCOUNT_MISMATCH,
    RC_EXECUTION_IDENTITY_ACCOUNT_MISSING,
    RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_AMBIGUOUS,
    RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISMATCH,
    RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISSING,
    RC_EXECUTION_IDENTITY_ENVIRONMENT_MISSING,
    RC_EXECUTION_IDENTITY_FORBIDDEN_COMBINATION,
    RC_EXECUTION_IDENTITY_SLEEVE_MISSING,
    enforce_submit_execution_identity_v1,
    resolve_governed_execution_identity_v1,
)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed_repo(
    repo_root: Path,
    *,
    sleeve_rows: list[dict] | None = None,
    account_rows: list[dict] | None = None,
) -> None:
    _write_json(
        repo_root / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json",
        {
            "schema_id": "c2_sleeve_registry",
            "schema_version": "v1",
            "sleeves": sleeve_rows
            or [
                {
                    "sleeve_id": "PRIMARY",
                    "enabled": True,
                    "mode": "PAPER",
                    "execution_mode": "AUTO",
                    "status": "PRODUCTION",
                    "ib_account": "DUO847203",
                    "truth_partition": "truth_sleeves/PRIMARY/PAPER",
                    "ib_gateway_profile": {
                        "host": "127.0.0.1",
                        "port": 4002,
                        "client_id_orders": 7,
                        "client_id_observer": 179,
                    },
                }
            ],
        },
    )
    _write_json(
        repo_root / "governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json",
        {
            "schema_id": "c2_ib_account_registry",
            "schema_version": "v1",
            "accounts": account_rows
            or [
                {
                    "account_id": "DUO847203",
                    "environment": "PAPER",
                    "enabled_for_submission": True,
                    "allowed_engine_ids": ["C2_MEAN_REVERSION_EQ_V1"],
                    "allowed_sleeve_ids": ["PRIMARY"],
                    "notes": [],
                }
            ],
        },
    )


def test_resolve_governed_execution_identity_happy_path(tmp_path: Path) -> None:
    _seed_repo(tmp_path)
    identity = resolve_governed_execution_identity_v1(
        repo_root=tmp_path,
        environment="PAPER",
        sleeve_id="PRIMARY",
    )
    assert identity.authority_owner == EXECUTION_IDENTITY_BINDING_OWNER
    assert identity.account_id == "DUO847203"
    assert identity.client_id_orders == 7
    assert identity.client_id_observer == 179


def test_resolve_governed_execution_identity_rejects_missing_sleeve(tmp_path: Path) -> None:
    _seed_repo(tmp_path)
    with pytest.raises(ValueError, match=RC_EXECUTION_IDENTITY_SLEEVE_MISSING):
        resolve_governed_execution_identity_v1(
            repo_root=tmp_path,
            environment="PAPER",
            sleeve_id="",
        )


def test_resolve_governed_execution_identity_rejects_missing_environment(tmp_path: Path) -> None:
    _seed_repo(tmp_path)
    with pytest.raises(ValueError, match=RC_EXECUTION_IDENTITY_ENVIRONMENT_MISSING):
        resolve_governed_execution_identity_v1(
            repo_root=tmp_path,
            environment="",
            sleeve_id="PRIMARY",
        )


def test_resolve_governed_execution_identity_rejects_missing_account(tmp_path: Path) -> None:
    _seed_repo(
        tmp_path,
        sleeve_rows=[
            {
                "sleeve_id": "PRIMARY",
                "enabled": True,
                "mode": "PAPER",
                "execution_mode": "AUTO",
                "status": "PRODUCTION",
                "ib_account": "",
                "truth_partition": "truth_sleeves/PRIMARY/PAPER",
                "ib_gateway_profile": {"host": "127.0.0.1", "port": 4002, "client_id_orders": 7, "client_id_observer": 179},
            }
        ],
    )
    with pytest.raises(ValueError, match=RC_EXECUTION_IDENTITY_ACCOUNT_MISSING):
        resolve_governed_execution_identity_v1(
            repo_root=tmp_path,
            environment="PAPER",
            sleeve_id="PRIMARY",
        )


def test_resolve_governed_execution_identity_rejects_missing_client_id_orders(tmp_path: Path) -> None:
    _seed_repo(
        tmp_path,
        sleeve_rows=[
            {
                "sleeve_id": "PRIMARY",
                "enabled": True,
                "mode": "PAPER",
                "execution_mode": "AUTO",
                "status": "PRODUCTION",
                "ib_account": "DUO847203",
                "truth_partition": "truth_sleeves/PRIMARY/PAPER",
                "ib_gateway_profile": {"host": "127.0.0.1", "port": 4002, "client_id_orders": "", "client_id_observer": 179},
            }
        ],
    )
    with pytest.raises(ValueError, match=RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISSING):
        resolve_governed_execution_identity_v1(
            repo_root=tmp_path,
            environment="PAPER",
            sleeve_id="PRIMARY",
        )


def test_resolve_governed_execution_identity_rejects_account_ambiguity(tmp_path: Path) -> None:
    _seed_repo(
        tmp_path,
        sleeve_rows=[
            {
                "sleeve_id": "PRIMARY",
                "enabled": True,
                "mode": "PAPER",
                "execution_mode": "AUTO",
                "status": "PRODUCTION",
                "ib_account": "DUO847203",
                "truth_partition": "truth_sleeves/PRIMARY/PAPER",
                "ib_gateway_profile": {"host": "127.0.0.1", "port": 4002, "client_id_orders": 7, "client_id_observer": 179},
            },
            {
                "sleeve_id": "PRIMARY",
                "enabled": True,
                "mode": "PAPER",
                "execution_mode": "AUTO",
                "status": "PRODUCTION",
                "ib_account": "DUO999999",
                "truth_partition": "truth_sleeves/PRIMARY/PAPER",
                "ib_gateway_profile": {"host": "127.0.0.1", "port": 4002, "client_id_orders": 7, "client_id_observer": 179},
            }
        ],
        account_rows=[
            {
                "account_id": "DUO847203",
                "environment": "PAPER",
                "enabled_for_submission": True,
                "allowed_engine_ids": ["C2_MEAN_REVERSION_EQ_V1"],
                "allowed_sleeve_ids": ["PRIMARY"],
                "notes": [],
            },
            {
                "account_id": "DUO999999",
                "environment": "PAPER",
                "enabled_for_submission": True,
                "allowed_engine_ids": ["C2_MEAN_REVERSION_EQ_V1"],
                "allowed_sleeve_ids": ["PRIMARY"],
                "notes": [],
            }
        ],
    )
    with pytest.raises(ValueError, match=RC_EXECUTION_IDENTITY_ACCOUNT_AMBIGUOUS):
        resolve_governed_execution_identity_v1(
            repo_root=tmp_path,
            environment="PAPER",
            sleeve_id="PRIMARY",
        )


def test_resolve_governed_execution_identity_rejects_client_id_orders_ambiguity(tmp_path: Path) -> None:
    _seed_repo(
        tmp_path,
        sleeve_rows=[
            {
                "sleeve_id": "PRIMARY",
                "enabled": True,
                "mode": "PAPER",
                "execution_mode": "AUTO",
                "status": "PRODUCTION",
                "ib_account": "DUO847203",
                "truth_partition": "truth_sleeves/PRIMARY/PAPER",
                "ib_gateway_profile": {"host": "127.0.0.1", "port": 4002, "client_id_orders": 7, "client_id_observer": 179},
            },
            {
                "sleeve_id": "PRIMARY",
                "enabled": True,
                "mode": "PAPER",
                "execution_mode": "AUTO",
                "status": "PRODUCTION",
                "ib_account": "DUO847203",
                "truth_partition": "truth_sleeves/PRIMARY/PAPER",
                "ib_gateway_profile": {"host": "127.0.0.1", "port": 4002, "client_id_orders": 8, "client_id_observer": 179},
            }
        ],
    )
    with pytest.raises(ValueError, match=RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_AMBIGUOUS):
        resolve_governed_execution_identity_v1(
            repo_root=tmp_path,
            environment="PAPER",
            sleeve_id="PRIMARY",
        )


def test_enforce_submit_execution_identity_rejects_runtime_account_mismatch(tmp_path: Path) -> None:
    _seed_repo(tmp_path)
    with pytest.raises(ValueError, match=RC_EXECUTION_IDENTITY_ACCOUNT_MISMATCH):
        enforce_submit_execution_identity_v1(
            repo_root=tmp_path,
            environment="PAPER",
            sleeve_id="PRIMARY",
            runtime_account_id="DUO999999",
            runtime_client_id_orders=7,
        )


def test_enforce_submit_execution_identity_rejects_runtime_client_id_mismatch(tmp_path: Path) -> None:
    _seed_repo(tmp_path)
    with pytest.raises(ValueError, match=RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISMATCH):
        enforce_submit_execution_identity_v1(
            repo_root=tmp_path,
            environment="PAPER",
            sleeve_id="PRIMARY",
            runtime_account_id="DUO847203",
            runtime_client_id_orders=8,
        )


def test_resolve_governed_execution_identity_rejects_forbidden_account_combination(tmp_path: Path) -> None:
    _seed_repo(
        tmp_path,
        account_rows=[
            {
                "account_id": "DUO847203",
                "environment": "PAPER",
                "enabled_for_submission": True,
                "allowed_engine_ids": ["C2_MEAN_REVERSION_EQ_V1"],
                "allowed_sleeve_ids": ["SECONDARY"],
                "notes": [],
            }
        ],
    )
    with pytest.raises(ValueError, match=RC_EXECUTION_IDENTITY_FORBIDDEN_COMBINATION):
        resolve_governed_execution_identity_v1(
            repo_root=tmp_path,
            environment="PAPER",
            sleeve_id="PRIMARY",
        )
