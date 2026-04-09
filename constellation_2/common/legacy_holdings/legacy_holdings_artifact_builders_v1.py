from __future__ import annotations

from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from .legacy_holdings_domain_constants_v1 import (
    AUTHORITY_STATUS_AUTHORITATIVE,
    AUTHORITY_STATUS_CANDIDATE,
    REPO_ROOT,
    SCHEMA_RELPATH_LEGACY_BOND_RISK_SUMMARY_V1,
    SCHEMA_RELPATH_NORMALIZED_HOLDINGS_ARTIFACT_V1,
)


def candidate_legacy_holdings_authority_v1() -> dict[str, Any]:
    return {
        "authority_status": AUTHORITY_STATUS_CANDIDATE,
        "execution_authority": False,
    }


def authoritative_legacy_holdings_authority_v1() -> dict[str, Any]:
    return {
        "authority_status": AUTHORITY_STATUS_AUTHORITATIVE,
        "execution_authority": True,
    }


def validate_legacy_holdings_authority_v1(*, authority_status: str, execution_authority: bool) -> None:
    status = str(authority_status)
    if status not in {AUTHORITY_STATUS_CANDIDATE, AUTHORITY_STATUS_AUTHORITATIVE}:
        raise ValueError("INVALID_LEGACY_HOLDINGS_AUTHORITY_STATUS")
    if status == AUTHORITY_STATUS_CANDIDATE and execution_authority:
        raise ValueError("CANDIDATE_MUST_NOT_HAVE_EXECUTION_AUTHORITY")
    if status == AUTHORITY_STATUS_AUTHORITATIVE and not execution_authority:
        raise ValueError("AUTHORITATIVE_MUST_HAVE_EXECUTION_AUTHORITY")


def build_normalized_holdings_artifact_v1(
    *,
    artifact_id: str,
    engine_version: str,
    policy_version: str,
    authority_status: str,
    execution_authority: bool,
    source_upload_ref: str,
    account_id: str,
    as_of_date: str,
    holdings: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    input_warnings: list[str] | tuple[str, ...],
    created_at: str,
) -> dict[str, Any]:
    validate_legacy_holdings_authority_v1(
        authority_status=str(authority_status),
        execution_authority=bool(execution_authority),
    )
    obj = {
        "schema_id": "normalized_holdings_artifact",
        "schema_version": "v1",
        "artifact_id": str(artifact_id),
        "engine_version": str(engine_version),
        "policy_version": str(policy_version),
        "authority_status": str(authority_status),
        "execution_authority": bool(execution_authority),
        "source_upload_ref": str(source_upload_ref),
        "account_id": str(account_id),
        "as_of_date": str(as_of_date),
        "holdings": [dict(item) for item in holdings],
        "input_warnings": [str(item) for item in input_warnings],
        "created_at": str(created_at),
    }
    validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH_NORMALIZED_HOLDINGS_ARTIFACT_V1)
    return obj


def build_legacy_bond_risk_summary_v1(
    *,
    summary_id: str,
    engine_version: str,
    policy_version: str,
    authority_status: str,
    execution_authority: bool,
    source_artifact_refs: list[str] | tuple[str, ...],
    corporate_bond_exposure_summary: dict[str, Any],
    maturity_clustering: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    issuer_concentration_flags: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    review_required_items: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    excluded_from_active_sleeve_indicator: bool,
    created_at: str,
) -> dict[str, Any]:
    validate_legacy_holdings_authority_v1(
        authority_status=str(authority_status),
        execution_authority=bool(execution_authority),
    )
    refs = [str(item) for item in source_artifact_refs]
    if not refs:
        raise ValueError("SOURCE_ARTIFACT_REFS_REQUIRED")
    obj = {
        "schema_id": "legacy_bond_risk_summary",
        "schema_version": "v1",
        "summary_id": str(summary_id),
        "engine_version": str(engine_version),
        "policy_version": str(policy_version),
        "authority_status": str(authority_status),
        "execution_authority": bool(execution_authority),
        "source_artifact_refs": refs,
        "corporate_bond_exposure_summary": dict(corporate_bond_exposure_summary),
        "maturity_clustering": [dict(item) for item in maturity_clustering],
        "issuer_concentration_flags": [dict(item) for item in issuer_concentration_flags],
        "review_required_items": [dict(item) for item in review_required_items],
        "excluded_from_active_sleeve_indicator": bool(excluded_from_active_sleeve_indicator),
        "created_at": str(created_at),
    }
    validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH_LEGACY_BOND_RISK_SUMMARY_V1)
    return obj
