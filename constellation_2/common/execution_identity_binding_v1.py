from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.common.paper_execution_authority_v1 import (
    GovernedPaperExecutionProfile,
    resolve_governed_paper_execution_profile,
)
from constellation_2.common.trade_submit_readiness_authority_v1 import (
    GovernedAccountBinding,
    resolve_governed_account_binding,
)


EXECUTION_IDENTITY_BINDING_OWNER = "execution_identity_binding_v1"

RC_EXECUTION_IDENTITY_SLEEVE_MISSING = "EXECUTION_IDENTITY_SLEEVE_MISSING"
RC_EXECUTION_IDENTITY_ENVIRONMENT_MISSING = "EXECUTION_IDENTITY_ENVIRONMENT_MISSING"
RC_EXECUTION_IDENTITY_SLEEVE_UNREGISTERED = "EXECUTION_IDENTITY_SLEEVE_UNREGISTERED"
RC_EXECUTION_IDENTITY_ACCOUNT_MISSING = "EXECUTION_IDENTITY_ACCOUNT_MISSING"
RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISSING = "EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISSING"
RC_EXECUTION_IDENTITY_ACCOUNT_AMBIGUOUS = "EXECUTION_IDENTITY_ACCOUNT_AMBIGUOUS"
RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_AMBIGUOUS = "EXECUTION_IDENTITY_CLIENT_ID_ORDERS_AMBIGUOUS"
RC_EXECUTION_IDENTITY_ACCOUNT_MISMATCH = "EXECUTION_IDENTITY_ACCOUNT_MISMATCH"
RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISMATCH = "EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISMATCH"
RC_EXECUTION_IDENTITY_FORBIDDEN_COMBINATION = "EXECUTION_IDENTITY_FORBIDDEN_COMBINATION"


@dataclass(frozen=True)
class GovernedExecutionIdentityV1:
    authority_owner: str
    sleeve_id: str
    environment: str
    account_id: str
    client_id_orders: int
    client_id_observer: int
    host: str
    port: int
    sleeve_registry_path: Path
    account_registry_path: Path


def _load_json_object(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise ValueError(f"MISSING_FILE:path={path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"JSON_PARSE_ERROR:path={path}:err={type(exc).__name__}:{exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:path={path}")
    return payload


def _map_account_binding_error(error_text: str) -> str:
    if error_text.startswith("INVALID_ENVIRONMENT:"):
        return RC_EXECUTION_IDENTITY_ENVIRONMENT_MISSING
    if error_text.startswith("SLEEVE_NOT_ACTIVE_FOR_ENV:"):
        return RC_EXECUTION_IDENTITY_SLEEVE_UNREGISTERED
    if error_text.startswith("ACTIVE_SLEEVE_MISSING_IB_ACCOUNT:"):
        return RC_EXECUTION_IDENTITY_ACCOUNT_MISSING
    if error_text.startswith("GOVERNED_ACTIVE_ACCOUNT_AMBIGUOUS:"):
        return RC_EXECUTION_IDENTITY_ACCOUNT_AMBIGUOUS
    if error_text.startswith("GOVERNED_ACTIVE_ACCOUNT_MISMATCH:"):
        return RC_EXECUTION_IDENTITY_ACCOUNT_MISMATCH
    if error_text.startswith("ACCOUNT_NOT_IN_REGISTRY:"):
        return RC_EXECUTION_IDENTITY_FORBIDDEN_COMBINATION
    if error_text.startswith("ACCOUNT_ENVIRONMENT_MISMATCH:"):
        return RC_EXECUTION_IDENTITY_FORBIDDEN_COMBINATION
    if error_text.startswith("ACCOUNT_DISABLED_FOR_SUBMISSION:"):
        return RC_EXECUTION_IDENTITY_FORBIDDEN_COMBINATION
    if error_text.startswith("ACCOUNT_ALLOWED_SLEEVES_INVALID:"):
        return RC_EXECUTION_IDENTITY_FORBIDDEN_COMBINATION
    if error_text.startswith("SLEEVE_NOT_ALLOWED_FOR_ACCOUNT:"):
        return RC_EXECUTION_IDENTITY_FORBIDDEN_COMBINATION
    return RC_EXECUTION_IDENTITY_FORBIDDEN_COMBINATION


def _map_profile_error(error_text: str) -> str:
    if error_text.startswith("EXECUTION_PROFILE_SLEEVE_NOT_FOUND:"):
        return RC_EXECUTION_IDENTITY_SLEEVE_UNREGISTERED
    if error_text.startswith("EXECUTION_PROFILE_CLIENT_ID_ORDERS_INVALID:"):
        return RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISSING
    if error_text.startswith("EXECUTION_PROFILE_CLIENT_ID_ORDERS_NONPOSITIVE:"):
        return RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISSING
    if error_text.startswith("EXECUTION_PROFILE_GATEWAY_PROFILE_INVALID:"):
        return RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISSING
    return RC_EXECUTION_IDENTITY_FORBIDDEN_COMBINATION


def _matching_sleeve_rows(
    *,
    repo_root: Path,
    environment: str,
    sleeve_id: str,
) -> tuple[dict[str, Any], ...]:
    registry_path = (Path(repo_root).resolve() / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json").resolve()
    registry = _load_json_object(registry_path)
    sleeves = registry.get("sleeves")
    if not isinstance(sleeves, list):
        raise ValueError(f"{RC_EXECUTION_IDENTITY_SLEEVE_UNREGISTERED}:sleeve_registry_invalid:path={registry_path}")
    env = str(environment or "").strip().upper()
    target_sleeve = str(sleeve_id or "").strip().upper()
    matched: list[dict[str, Any]] = []
    for row in sleeves:
        if not isinstance(row, dict):
            continue
        if row.get("enabled") is not True:
            continue
        if str(row.get("mode") or "").strip().upper() != env:
            continue
        if str(row.get("sleeve_id") or "").strip().upper() != target_sleeve:
            continue
        execution_mode = str(row.get("execution_mode") or "").strip().upper()
        status = str(row.get("status") or "").strip().upper()
        if execution_mode and execution_mode != "AUTO":
            continue
        if status and status != "PRODUCTION":
            continue
        matched.append(row)
    return tuple(matched)


def _resolve_account_binding(
    *,
    repo_root: Path,
    environment: str,
    sleeve_id: str,
) -> GovernedAccountBinding:
    try:
        return resolve_governed_account_binding(
            repo_root=Path(repo_root).resolve(),
            environment=environment,
            requested_ib_account="",
            sleeve_id=sleeve_id,
        )
    except ValueError as exc:
        reason_code = _map_account_binding_error(str(exc))
        raise ValueError(f"{reason_code}:{exc}") from exc


def resolve_governed_execution_identity_v1(
    *,
    repo_root: Path,
    environment: str,
    sleeve_id: str,
) -> GovernedExecutionIdentityV1:
    env = str(environment or "").strip().upper()
    if not env:
        raise ValueError(RC_EXECUTION_IDENTITY_ENVIRONMENT_MISSING)
    target_sleeve = str(sleeve_id or "").strip().upper()
    if not target_sleeve:
        raise ValueError(RC_EXECUTION_IDENTITY_SLEEVE_MISSING)

    account_binding = _resolve_account_binding(
        repo_root=repo_root,
        environment=env,
        sleeve_id=target_sleeve,
    )
    sleeve_rows = _matching_sleeve_rows(
        repo_root=repo_root,
        environment=env,
        sleeve_id=target_sleeve,
    )
    if not sleeve_rows:
        raise ValueError(
            f"{RC_EXECUTION_IDENTITY_SLEEVE_UNREGISTERED}:sleeve_id={target_sleeve}:environment={env}"
        )

    candidate_accounts = {
        str(row.get("ib_account") or "").strip()
        for row in sleeve_rows
        if str(row.get("ib_account") or "").strip()
    }
    if not candidate_accounts:
        raise ValueError(
            f"{RC_EXECUTION_IDENTITY_ACCOUNT_MISSING}:sleeve_id={target_sleeve}:environment={env}"
        )
    if len(candidate_accounts) > 1:
        raise ValueError(
            f"{RC_EXECUTION_IDENTITY_ACCOUNT_AMBIGUOUS}:sleeve_id={target_sleeve}:environment={env}:accounts={sorted(candidate_accounts)}"
        )
    governed_account = next(iter(candidate_accounts))
    if governed_account != account_binding.ib_account:
        raise ValueError(
            f"{RC_EXECUTION_IDENTITY_ACCOUNT_MISMATCH}:sleeve_id={target_sleeve}:environment={env}:registry_account={governed_account}:governed_account_binding={account_binding.ib_account}"
        )

    client_id_candidates: set[int] = set()
    for row in sleeve_rows:
        gateway_profile = row.get("ib_gateway_profile")
        if not isinstance(gateway_profile, dict):
            raise ValueError(
                f"{RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISSING}:sleeve_id={target_sleeve}:environment={env}:reason=gateway_profile_missing"
            )
        raw_client_id_orders = gateway_profile.get("client_id_orders")
        if raw_client_id_orders in (None, ""):
            raise ValueError(
                f"{RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISSING}:sleeve_id={target_sleeve}:environment={env}:reason=client_id_orders_missing"
            )
        try:
            client_id_orders = int(raw_client_id_orders)
        except Exception as exc:
            raise ValueError(
                f"{RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISSING}:sleeve_id={target_sleeve}:environment={env}:reason=client_id_orders_invalid"
            ) from exc
        if client_id_orders <= 0:
            raise ValueError(
                f"{RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISSING}:sleeve_id={target_sleeve}:environment={env}:reason=client_id_orders_nonpositive"
            )
        client_id_candidates.add(client_id_orders)
    if len(client_id_candidates) > 1:
        raise ValueError(
            f"{RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_AMBIGUOUS}:sleeve_id={target_sleeve}:environment={env}:client_id_orders={sorted(client_id_candidates)}"
        )

    try:
        execution_profile = resolve_governed_paper_execution_profile(
            repo_root=Path(repo_root).resolve(),
            environment=env,
            ib_account=governed_account,
            sleeve_id=target_sleeve,
        )
    except ValueError as exc:
        reason_code = _map_profile_error(str(exc))
        raise ValueError(f"{reason_code}:{exc}") from exc

    if execution_profile.ib_account != governed_account:
        raise ValueError(
            f"{RC_EXECUTION_IDENTITY_ACCOUNT_MISMATCH}:sleeve_id={target_sleeve}:environment={env}:expected={governed_account}:actual={execution_profile.ib_account}"
        )
    if execution_profile.client_id_orders != next(iter(client_id_candidates)):
        raise ValueError(
            f"{RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISMATCH}:sleeve_id={target_sleeve}:environment={env}:expected={next(iter(client_id_candidates))}:actual={execution_profile.client_id_orders}"
        )

    return GovernedExecutionIdentityV1(
        authority_owner=EXECUTION_IDENTITY_BINDING_OWNER,
        sleeve_id=target_sleeve,
        environment=env,
        account_id=governed_account,
        client_id_orders=execution_profile.client_id_orders,
        client_id_observer=execution_profile.client_id_observer,
        host=execution_profile.host,
        port=execution_profile.port,
        sleeve_registry_path=execution_profile.sleeve_registry_path,
        account_registry_path=account_binding.account_registry_path,
    )


def enforce_submit_execution_identity_v1(
    *,
    repo_root: Path,
    environment: str,
    sleeve_id: str,
    runtime_account_id: str,
    runtime_client_id_orders: int | str,
) -> GovernedExecutionIdentityV1:
    identity = resolve_governed_execution_identity_v1(
        repo_root=Path(repo_root).resolve(),
        environment=environment,
        sleeve_id=sleeve_id,
    )
    account_id = str(runtime_account_id or "").strip()
    if not account_id:
        raise ValueError(
            f"{RC_EXECUTION_IDENTITY_ACCOUNT_MISSING}:sleeve_id={identity.sleeve_id}:environment={identity.environment}"
        )
    if account_id != identity.account_id:
        raise ValueError(
            f"{RC_EXECUTION_IDENTITY_ACCOUNT_MISMATCH}:sleeve_id={identity.sleeve_id}:environment={identity.environment}:expected={identity.account_id}:actual={account_id}"
        )
    if runtime_client_id_orders in ("", None):
        raise ValueError(
            f"{RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISSING}:sleeve_id={identity.sleeve_id}:environment={identity.environment}"
        )
    try:
        client_id_orders = int(runtime_client_id_orders)
    except Exception as exc:
        raise ValueError(
            f"{RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISSING}:sleeve_id={identity.sleeve_id}:environment={identity.environment}:reason=runtime_client_id_invalid"
        ) from exc
    if client_id_orders != identity.client_id_orders:
        raise ValueError(
            f"{RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISMATCH}:sleeve_id={identity.sleeve_id}:environment={identity.environment}:expected={identity.client_id_orders}:actual={client_id_orders}"
        )
    return identity


__all__ = [
    "EXECUTION_IDENTITY_BINDING_OWNER",
    "GovernedExecutionIdentityV1",
    "RC_EXECUTION_IDENTITY_ACCOUNT_AMBIGUOUS",
    "RC_EXECUTION_IDENTITY_ACCOUNT_MISMATCH",
    "RC_EXECUTION_IDENTITY_ACCOUNT_MISSING",
    "RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_AMBIGUOUS",
    "RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISMATCH",
    "RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISSING",
    "RC_EXECUTION_IDENTITY_ENVIRONMENT_MISSING",
    "RC_EXECUTION_IDENTITY_FORBIDDEN_COMBINATION",
    "RC_EXECUTION_IDENTITY_SLEEVE_MISSING",
    "RC_EXECUTION_IDENTITY_SLEEVE_UNREGISTERED",
    "enforce_submit_execution_identity_v1",
    "resolve_governed_execution_identity_v1",
]
