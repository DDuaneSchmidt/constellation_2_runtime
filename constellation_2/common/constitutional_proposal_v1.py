from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/constitutional_proposal.v1.schema.json"
ACTION_CLASS_CONSTRUCTIVE = "CONSTRUCTIVE"
ACTION_CLASS_PROTECTIVE = "PROTECTIVE"


def _sorted_strings(values: list[Any] | tuple[Any, ...]) -> list[str]:
    return sorted({str(value).strip() for value in values if str(value).strip()})


def _artifact_hash_rows(
    rows: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...],
) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    for row in rows:
        artifact_ref = str(row.get("artifact_ref") or "").strip()
        sha256 = str(row.get("sha256") or "").strip().lower()
        if not artifact_ref or len(sha256) != 64:
            raise ValueError(f"CONSTITUTIONAL_PROPOSAL_ARTIFACT_HASH_INVALID:{row!r}")
        normalized.append({"artifact_ref": artifact_ref, "sha256": sha256})
    normalized.sort(key=lambda item: (item["artifact_ref"], item["sha256"]))
    return normalized


def normalize_target_scope_v1(scope: Mapping[str, Any] | None) -> dict[str, str]:
    raw = dict(scope or {})
    return {
        "global": str(raw.get("global") or "").strip(),
        "domain": str(raw.get("domain") or "").strip(),
        "account": str(raw.get("account") or "").strip(),
        "sleeve": str(raw.get("sleeve") or "").strip(),
        "action_class": str(raw.get("action_class") or "").strip(),
    }


def build_constitutional_proposal_v1(
    *,
    proposal_id: str,
    proposal_version: str,
    created_at: str,
    source_subsystem: str,
    action_type: str,
    action_class: str,
    target_scope: Mapping[str, Any] | None,
    target_entities: list[Any] | tuple[Any, ...],
    requested_effect: Mapping[str, Any] | None,
    expected_economic_effect: Mapping[str, Any] | None,
    expected_tax_effect: Mapping[str, Any] | None,
    expected_risk_effect: Mapping[str, Any] | None,
    reversibility_class: str,
    urgency_class: str,
    expiration_at: str | None,
    required_fact_types: list[Any] | tuple[Any, ...],
    required_dependency_checks: list[Any] | tuple[Any, ...],
    source_reasoning_reference: str,
    source_policy_bindings: list[Any] | tuple[Any, ...],
    source_artifact_hashes: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...],
) -> dict[str, Any]:
    payload = {
        "schema_id": "constitutional_proposal",
        "schema_version": "v1",
        "proposal_id": str(proposal_id).strip(),
        "proposal_version": str(proposal_version).strip(),
        "created_at": str(created_at).strip(),
        "source_subsystem": str(source_subsystem).strip(),
        "action_type": str(action_type).strip(),
        "action_class": str(action_class).strip().upper(),
        "target_scope": normalize_target_scope_v1(target_scope),
        "target_entities": _sorted_strings(list(target_entities)),
        "requested_effect": dict(requested_effect or {}),
        "expected_economic_effect": dict(expected_economic_effect or {}),
        "expected_tax_effect": dict(expected_tax_effect or {}),
        "expected_risk_effect": dict(expected_risk_effect or {}),
        "reversibility_class": str(reversibility_class).strip(),
        "urgency_class": str(urgency_class).strip(),
        "expiration_at": None if expiration_at is None else str(expiration_at).strip(),
        "required_fact_types": _sorted_strings(list(required_fact_types)),
        "required_dependency_checks": _sorted_strings(list(required_dependency_checks)),
        "source_reasoning_reference": str(source_reasoning_reference).strip(),
        "source_policy_bindings": _sorted_strings(list(source_policy_bindings)),
        "source_artifact_hashes": _artifact_hash_rows(source_artifact_hashes),
    }
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH_V1)
    return payload


def proposal_hash_v1(proposal: Mapping[str, Any]) -> str:
    payload = dict(proposal)
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH_V1)
    return hashlib.sha256(canonical_json_bytes_v1(payload)).hexdigest()


def proposal_hash_bytes_v1(proposal: Mapping[str, Any]) -> bytes:
    payload = dict(proposal)
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH_V1)
    return canonical_json_bytes_v1(payload)


def build_exposure_intent_proposal_v1(
    *,
    day_utc: str,
    intent_obj: Mapping[str, Any],
    intent_path: Path,
    intent_hash: str,
    policy_path: Path,
    policy_hash: str,
) -> dict[str, Any]:
    engine = dict(intent_obj.get("engine") or {})
    underlying = dict(intent_obj.get("underlying") or {})
    constraints = dict(intent_obj.get("constraints") or {})
    symbol = str(underlying.get("symbol") or "").strip()
    engine_id = str(engine.get("engine_id") or "").strip()
    target_notional_pct = str(intent_obj.get("target_notional_pct") or "").strip()
    return build_constitutional_proposal_v1(
        proposal_id=str(intent_obj.get("intent_id") or intent_hash).strip(),
        proposal_version="v1",
        created_at=str(intent_obj.get("created_at_utc") or f"{day_utc}T00:00:00Z").strip(),
        source_subsystem=engine_id or "intent_engine",
        action_type="TARGET_EXPOSURE",
        action_class=ACTION_CLASS_CONSTRUCTIVE,
        target_scope={
            "global": "PAPER",
            "domain": "TRADING",
            "account": "",
            "sleeve": "",
            "action_class": ACTION_CLASS_CONSTRUCTIVE,
        },
        target_entities=[symbol, engine_id],
        requested_effect={
            "symbol": symbol,
            "exposure_type": str(intent_obj.get("exposure_type") or "").strip(),
            "target_notional_pct": target_notional_pct,
        },
        expected_economic_effect={"target_notional_pct": target_notional_pct},
        expected_tax_effect={"status": "UNSPECIFIED"},
        expected_risk_effect={
            "risk_class": str(intent_obj.get("risk_class") or "").strip(),
            "max_risk_pct": str(constraints.get("max_risk_pct") or "").strip(),
        },
        reversibility_class="REVERSIBLE_BY_OFFSETTING_INTENT",
        urgency_class="DAY",
        expiration_at=f"{day_utc}T23:59:59Z",
        required_fact_types=[
            "account_state_fact",
            "execution_capability_fact",
            "policy_binding_fact",
        ],
        required_dependency_checks=[
            "capital_authority_allocation_present",
            "intent_snapshot_present",
            "policy_manifest_present",
        ],
        source_reasoning_reference=str(intent_path.resolve()),
        source_policy_bindings=[str(policy_path.resolve())],
        source_artifact_hashes=[
            {"artifact_ref": str(intent_path.resolve()), "sha256": intent_hash},
            {"artifact_ref": str(policy_path.resolve()), "sha256": policy_hash},
        ],
    )


def build_session_submission_proposal_v1(
    *,
    day_utc: str,
    source_subsystem: str,
    source_reasoning_reference: str,
    source_policy_bindings: list[Any] | tuple[Any, ...],
    source_artifact_hashes: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...],
    target_entities: list[Any] | tuple[Any, ...],
) -> dict[str, Any]:
    return build_constitutional_proposal_v1(
        proposal_id=f"paper-session-submission:{day_utc}",
        proposal_version="v1",
        created_at=f"{day_utc}T00:00:00Z",
        source_subsystem=source_subsystem,
        action_type="ENABLE_SESSION_SUBMISSION",
        action_class=ACTION_CLASS_CONSTRUCTIVE,
        target_scope={
            "global": "PAPER",
            "domain": "TRADING",
            "account": "",
            "sleeve": "",
            "action_class": ACTION_CLASS_CONSTRUCTIVE,
        },
        target_entities=list(target_entities),
        requested_effect={"submission_day_utc": day_utc},
        expected_economic_effect={"status": "ENABLE_SUBMISSION_WINDOW"},
        expected_tax_effect={"status": "UNSPECIFIED"},
        expected_risk_effect={"status": "ENFORCE_EXISTING_BOUNDARIES_ONLY"},
        reversibility_class="REVERSIBLE_BY_BOUNDARY_DENIAL",
        urgency_class="DAY",
        expiration_at=f"{day_utc}T23:59:59Z",
        required_fact_types=[
            "execution_capability_fact",
            "market_state_fact",
            "policy_binding_fact",
            "sleeve_state_fact",
        ],
        required_dependency_checks=[
            "startup_materialization_present",
            "paper_trading_posture_present",
            "submit_boundary_status_present",
        ],
        source_reasoning_reference=source_reasoning_reference,
        source_policy_bindings=list(source_policy_bindings),
        source_artifact_hashes=list(source_artifact_hashes),
    )


def build_post_entry_request_proposal_v1(
    *,
    request: Mapping[str, Any],
    core2_snapshot_ref: Mapping[str, Any] | None,
    core3_snapshot_ref: Mapping[str, Any] | None,
    identity_snapshot_ref: Mapping[str, Any] | None,
) -> dict[str, Any]:
    request_obj = dict(request)
    execution_identity = dict(request_obj.get("execution_identity") or {})
    action_class_raw = str(request_obj.get("action_class") or "").strip().upper()
    action_class = ACTION_CLASS_PROTECTIVE if action_class_raw in {
        "CLOSE_TRADE",
        "REDUCE_TRADE",
        "AMEND_PROTECTION",
        "CANCEL_ORDER",
    } else ACTION_CLASS_CONSTRUCTIVE
    account_id = str(execution_identity.get("account_id") or "").strip()
    sleeve_id = str(execution_identity.get("sleeve_id") or "").strip()
    environment = str(execution_identity.get("environment") or "").strip()
    source_artifact_hashes = []
    for row in (core2_snapshot_ref or {}, core3_snapshot_ref or {}, identity_snapshot_ref or {}):
        artifact_ref = str(dict(row).get("artifact_path") or "").strip()
        sha256 = str(dict(row).get("artifact_sha256") or dict(row).get("snapshot_sha256") or "").strip().lower()
        if artifact_ref and len(sha256) == 64:
            source_artifact_hashes.append({"artifact_ref": artifact_ref, "sha256": sha256})
    source_artifact_hashes.sort(key=lambda item: (item["artifact_ref"], item["sha256"]))
    requested_quantity = request_obj.get("requested_quantity")
    order_parameters = dict(request_obj.get("order_parameters") or {})
    return build_constitutional_proposal_v1(
        proposal_id=str(request_obj.get("request_id") or "").strip(),
        proposal_version="v1",
        created_at=str(request_obj.get("created_at_utc") or "").strip(),
        source_subsystem="post_entry_submit_boundary_v1",
        action_type=action_class_raw or "POST_ENTRY_ACTION",
        action_class=action_class,
        target_scope={
            "global": environment,
            "domain": "POST_ENTRY",
            "account": account_id,
            "sleeve": sleeve_id,
            "action_class": action_class,
        },
        target_entities=[
            str(request_obj.get("trade_identity_id") or "").strip(),
            account_id,
            sleeve_id,
        ],
        requested_effect={
            "request_action_class": action_class_raw,
            "requested_quantity": requested_quantity,
            "order_parameters": order_parameters,
        },
        expected_economic_effect={
            "request_action_class": action_class_raw,
            "requested_quantity": requested_quantity,
        },
        expected_tax_effect={"status": "POST_ENTRY_UNKNOWN"},
        expected_risk_effect={"status": "POST_ENTRY_PROTECTIVE_ACTION"},
        reversibility_class="REVERSIBLE_BY_CANCEL_OR_SUBSEQUENT_ACTION",
        urgency_class="IMMEDIATE",
        expiration_at=None,
        required_fact_types=[
            "position_state_fact",
            "execution_capability_fact",
            "policy_binding_fact",
        ],
        required_dependency_checks=[
            "core2_snapshot_present",
            "core3_projection_present",
            "execution_identity_snapshot_present",
        ],
        source_reasoning_reference=str(request_obj.get("request_id") or "").strip(),
        source_policy_bindings=["post_entry_submit_boundary_v1"],
        source_artifact_hashes=source_artifact_hashes,
    )
