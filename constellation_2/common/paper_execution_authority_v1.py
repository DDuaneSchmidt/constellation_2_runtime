from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.common.sleeve_execution_root_v1 import (
    EXECUTION_ROOT_AUTHORITY_OWNER,
    RC_EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN,
    RC_EXECUTION_ROOT_MODE_MISSING,
    RC_EXECUTION_ROOT_PATH_MISMATCH,
    RC_EXECUTION_ROOT_PATH_UNRESOLVED,
    RC_EXECUTION_ROOT_SLEEVE_ID_MISSING,
    SleeveExecutionRootResolutionV1,
    assert_execution_root_family_path_v1,
    resolve_sleeve_execution_root_v1,
)
from constellation_2.common.trade_submit_readiness_authority_v1 import (
    resolve_governed_account_binding,
)


GovernedPaperExecutionRoots = SleeveExecutionRootResolutionV1


@dataclass(frozen=True)
class GovernedPaperExecutionProfile:
    environment: str
    ib_account: str
    sleeve_id: str
    host: str
    port: int
    client_id_orders: int
    client_id_observer: int
    sleeve_registry_path: Path


def _load_sleeve_registry(repo_root: Path) -> dict[str, Any]:
    path = (Path(repo_root).resolve() / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json").resolve()
    if not path.exists() or not path.is_file():
        raise ValueError(f"SLEEVE_REGISTRY_MISSING:path={path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"SLEEVE_REGISTRY_PARSE_ERROR:path={path}:err={type(exc).__name__}:{exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"SLEEVE_REGISTRY_NOT_OBJECT:path={path}")
    if str(payload.get("schema_id") or "").strip() != "c2_sleeve_registry":
        raise ValueError(f"SLEEVE_REGISTRY_SCHEMA_ID_MISMATCH:path={path}")
    if str(payload.get("schema_version") or "").strip() != "v1":
        raise ValueError(f"SLEEVE_REGISTRY_SCHEMA_VERSION_MISMATCH:path={path}")
    return payload


def resolve_governed_paper_execution_roots(
    *,
    repo_root: Path,
    environment: str,
    ib_account: str,
    sleeve_id: str,
) -> GovernedPaperExecutionRoots:
    return resolve_sleeve_execution_root_v1(
        repo_root=repo_root,
        environment=environment,
        ib_account=ib_account,
        sleeve_id=sleeve_id,
    )


def require_governed_execution_family_path(
    *,
    repo_root: Path,
    environment: str,
    ib_account: str,
    sleeve_id: str,
    family: str,
    actual_path: Path,
) -> GovernedPaperExecutionRoots:
    return assert_execution_root_family_path_v1(
        repo_root=repo_root,
        environment=environment,
        ib_account=ib_account,
        sleeve_id=sleeve_id,
        family=family,
        actual_path=actual_path,
    )


def resolve_governed_paper_execution_profile(
    *,
    repo_root: Path,
    environment: str,
    ib_account: str,
    sleeve_id: str = "PRIMARY",
) -> GovernedPaperExecutionProfile:
    env = str(environment or "").strip().upper()
    if env != "PAPER":
        raise ValueError(f"EXECUTION_PROFILE_ENVIRONMENT_INVALID:environment={environment!r}")
    account_binding = resolve_governed_account_binding(
        repo_root=Path(repo_root).resolve(),
        environment=env,
        requested_ib_account=ib_account,
        sleeve_id=sleeve_id,
    )
    target_sleeve_id = str(sleeve_id or "").strip().upper()
    if not target_sleeve_id:
        raise ValueError(RC_EXECUTION_ROOT_SLEEVE_ID_MISSING)
    registry = _load_sleeve_registry(Path(repo_root))
    sleeves = registry.get("sleeves")
    if not isinstance(sleeves, list):
        raise ValueError("SLEEVE_REGISTRY_SLEEVES_NOT_LIST")

    selected_row: dict[str, Any] | None = None
    for row in sleeves:
        if not isinstance(row, dict):
            continue
        if row.get("enabled") is not True:
            continue
        if str(row.get("mode") or "").strip().upper() != env:
            continue
        if str(row.get("sleeve_id") or "").strip().upper() != target_sleeve_id:
            continue
        if str(row.get("ib_account") or "").strip() != account_binding.ib_account:
            continue
        selected_row = row
        break
    if selected_row is None:
        raise ValueError(
            "EXECUTION_PROFILE_SLEEVE_NOT_FOUND:"
            f"environment={env}:ib_account={account_binding.ib_account}:sleeve_id={target_sleeve_id}"
        )

    gateway_profile = selected_row.get("ib_gateway_profile")
    if not isinstance(gateway_profile, dict):
        raise ValueError(f"EXECUTION_PROFILE_GATEWAY_PROFILE_INVALID:sleeve_id={target_sleeve_id}")

    host = str(gateway_profile.get("host") or "").strip()
    if not host:
        raise ValueError(f"EXECUTION_PROFILE_HOST_MISSING:sleeve_id={target_sleeve_id}")
    try:
        port = int(gateway_profile.get("port"))
    except Exception as exc:
        raise ValueError(f"EXECUTION_PROFILE_PORT_INVALID:sleeve_id={target_sleeve_id}") from exc
    try:
        client_id_orders = int(gateway_profile.get("client_id_orders"))
    except Exception as exc:
        raise ValueError(f"EXECUTION_PROFILE_CLIENT_ID_ORDERS_INVALID:sleeve_id={target_sleeve_id}") from exc
    try:
        client_id_observer = int(gateway_profile.get("client_id_observer"))
    except Exception as exc:
        raise ValueError(f"EXECUTION_PROFILE_CLIENT_ID_OBSERVER_INVALID:sleeve_id={target_sleeve_id}") from exc

    if port <= 0:
        raise ValueError(f"EXECUTION_PROFILE_PORT_NONPOSITIVE:sleeve_id={target_sleeve_id}:port={port}")
    if client_id_orders <= 0:
        raise ValueError(
            f"EXECUTION_PROFILE_CLIENT_ID_ORDERS_NONPOSITIVE:sleeve_id={target_sleeve_id}:client_id_orders={client_id_orders}"
        )
    if client_id_observer <= 0:
        raise ValueError(
            f"EXECUTION_PROFILE_CLIENT_ID_OBSERVER_NONPOSITIVE:sleeve_id={target_sleeve_id}:client_id_observer={client_id_observer}"
        )

    return GovernedPaperExecutionProfile(
        environment=env,
        ib_account=account_binding.ib_account,
        sleeve_id=target_sleeve_id,
        host=host,
        port=port,
        client_id_orders=client_id_orders,
        client_id_observer=client_id_observer,
        sleeve_registry_path=(Path(repo_root).resolve() / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json").resolve(),
    )


__all__ = [
    "EXECUTION_ROOT_AUTHORITY_OWNER",
    "GovernedPaperExecutionProfile",
    "GovernedPaperExecutionRoots",
    "RC_EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN",
    "RC_EXECUTION_ROOT_MODE_MISSING",
    "RC_EXECUTION_ROOT_PATH_MISMATCH",
    "RC_EXECUTION_ROOT_PATH_UNRESOLVED",
    "RC_EXECUTION_ROOT_SLEEVE_ID_MISSING",
    "require_governed_execution_family_path",
    "resolve_governed_paper_execution_profile",
    "resolve_governed_paper_execution_roots",
]
