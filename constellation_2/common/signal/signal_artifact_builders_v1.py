from __future__ import annotations

from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from .signal_domain_constants_v1 import (
    AUTHORITY_STATUS_AUTHORITATIVE,
    AUTHORITY_STATUS_CANDIDATE,
    REPO_ROOT,
    SCHEMA_RELPATH_DURATION_TREND_SIGNAL_V1,
    SCHEMA_RELPATH_HY_SPREAD_SIGNAL_V1,
    SCHEMA_RELPATH_HY_VOLATILITY_SIGNAL_V1,
    SCHEMA_RELPATH_SPREAD_DIRECTION_SIGNAL_V1,
)


def candidate_signal_authority_v1() -> dict[str, Any]:
    return {
        "authority_status": AUTHORITY_STATUS_CANDIDATE,
        "execution_authority": False,
    }


def authoritative_signal_authority_v1() -> dict[str, Any]:
    return {
        "authority_status": AUTHORITY_STATUS_AUTHORITATIVE,
        "execution_authority": True,
    }


def validate_signal_authority_v1(*, authority_status: str, execution_authority: bool) -> None:
    status = str(authority_status)
    if status not in {AUTHORITY_STATUS_CANDIDATE, AUTHORITY_STATUS_AUTHORITATIVE}:
        raise ValueError("INVALID_SIGNAL_AUTHORITY_STATUS")
    if status == AUTHORITY_STATUS_CANDIDATE and execution_authority:
        raise ValueError("CANDIDATE_MUST_NOT_HAVE_EXECUTION_AUTHORITY")
    if status == AUTHORITY_STATUS_AUTHORITATIVE and not execution_authority:
        raise ValueError("AUTHORITATIVE_MUST_HAVE_EXECUTION_AUTHORITY")


def _build_signal_artifact_v1(
    *,
    schema_id: str,
    schema_relpath: str,
    signal_id: str,
    engine_version: str,
    policy_version: str,
    authority_status: str,
    execution_authority: bool,
    effective_at: str,
    state_field: str,
    state_value: str | int,
    warnings: list[str] | tuple[str, ...],
    created_at: str,
    extra_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    validate_signal_authority_v1(
        authority_status=str(authority_status),
        execution_authority=bool(execution_authority),
    )
    obj = {
        "schema_id": str(schema_id),
        "schema_version": "v1",
        "signal_id": str(signal_id),
        "engine_version": str(engine_version),
        "policy_version": str(policy_version),
        "authority_status": str(authority_status),
        "execution_authority": bool(execution_authority),
        "effective_at": str(effective_at),
        state_field: state_value,
        "warnings": [str(item) for item in warnings],
        "created_at": str(created_at),
    }
    if extra_fields:
        for key, value in extra_fields.items():
            obj[str(key)] = value
    validate_against_repo_schema_v1(obj, REPO_ROOT, schema_relpath)
    return obj


def build_hy_spread_signal_v1(
    *,
    signal_id: str,
    engine_version: str,
    policy_version: str,
    authority_status: str,
    execution_authority: bool,
    effective_at: str,
    spread_level_bps: int,
    spread_level_state: str,
    warnings: list[str] | tuple[str, ...],
    created_at: str,
) -> dict[str, Any]:
    return _build_signal_artifact_v1(
        schema_id="hy_spread_signal",
        schema_relpath=SCHEMA_RELPATH_HY_SPREAD_SIGNAL_V1,
        signal_id=signal_id,
        engine_version=engine_version,
        policy_version=policy_version,
        authority_status=authority_status,
        execution_authority=execution_authority,
        effective_at=effective_at,
        state_field="spread_level_state",
        state_value=str(spread_level_state),
        warnings=warnings,
        created_at=created_at,
        extra_fields={"spread_level_bps": int(spread_level_bps)},
    )


def build_spread_direction_signal_v1(
    *,
    signal_id: str,
    engine_version: str,
    policy_version: str,
    authority_status: str,
    execution_authority: bool,
    effective_at: str,
    direction_state: str,
    warnings: list[str] | tuple[str, ...],
    created_at: str,
) -> dict[str, Any]:
    return _build_signal_artifact_v1(
        schema_id="spread_direction_signal",
        schema_relpath=SCHEMA_RELPATH_SPREAD_DIRECTION_SIGNAL_V1,
        signal_id=signal_id,
        engine_version=engine_version,
        policy_version=policy_version,
        authority_status=authority_status,
        execution_authority=execution_authority,
        effective_at=effective_at,
        state_field="direction_state",
        state_value=str(direction_state),
        warnings=warnings,
        created_at=created_at,
    )


def build_hy_volatility_signal_v1(
    *,
    signal_id: str,
    engine_version: str,
    policy_version: str,
    authority_status: str,
    execution_authority: bool,
    effective_at: str,
    volatility_state: str,
    warnings: list[str] | tuple[str, ...],
    created_at: str,
) -> dict[str, Any]:
    return _build_signal_artifact_v1(
        schema_id="hy_volatility_signal",
        schema_relpath=SCHEMA_RELPATH_HY_VOLATILITY_SIGNAL_V1,
        signal_id=signal_id,
        engine_version=engine_version,
        policy_version=policy_version,
        authority_status=authority_status,
        execution_authority=execution_authority,
        effective_at=effective_at,
        state_field="volatility_state",
        state_value=str(volatility_state),
        warnings=warnings,
        created_at=created_at,
    )


def build_duration_trend_signal_v1(
    *,
    signal_id: str,
    engine_version: str,
    policy_version: str,
    authority_status: str,
    execution_authority: bool,
    effective_at: str,
    rate_trend_state: str,
    warnings: list[str] | tuple[str, ...],
    created_at: str,
) -> dict[str, Any]:
    return _build_signal_artifact_v1(
        schema_id="duration_trend_signal",
        schema_relpath=SCHEMA_RELPATH_DURATION_TREND_SIGNAL_V1,
        signal_id=signal_id,
        engine_version=engine_version,
        policy_version=policy_version,
        authority_status=authority_status,
        execution_authority=execution_authority,
        effective_at=effective_at,
        state_field="rate_trend_state",
        state_value=str(rate_trend_state),
        warnings=warnings,
        created_at=created_at,
    )
