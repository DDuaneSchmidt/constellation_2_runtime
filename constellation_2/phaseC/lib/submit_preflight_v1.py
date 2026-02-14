"""
submit_preflight_v1.py

Constellation 2.0 Phase C
Offline submission preflight evaluator (NO BROKER CALLS).

Fail-closed. Deterministic. No broker.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .canon_json_v1 import CanonicalizationError, canonical_hash_for_c2_artifact_v1
from .validate_against_schema_v1 import SchemaValidationError, validate_against_repo_schema_v1


class SubmitPreflightError(Exception):
    pass


RC_BINDING_MISMATCH = "C2_BINDING_HASH_MISMATCH"
RC_FRESHNESS = "C2_FRESHNESS_CERT_INVALID_OR_EXPIRED"
RC_OPTIONS_ONLY = "C2_OPTIONS_ONLY_VIOLATION"
RC_DEFINED_RISK = "C2_DEFINED_RISK_REQUIRED"
RC_EXIT_POLICY = "C2_EXIT_POLICY_REQUIRED"
RC_SUBMIT_FAIL_CLOSED = "C2_SUBMIT_FAIL_CLOSED_REQUIRED"
RC_DETERMINISM = "C2_DETERMINISM_CANONICALIZATION_FAILED"
RC_PRICE_DET = "C2_PRICE_DETERMINISM_FAILED"


def _parse_utc_z(ts: str) -> datetime:
    if not isinstance(ts, str) or not ts.endswith("Z"):
        raise SubmitPreflightError(f"Timestamp must be Z-suffix UTC ISO-8601: {ts!r}")
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc)


def _hash_or_fail(name: str, obj: Dict[str, Any]) -> str:
    try:
        return canonical_hash_for_c2_artifact_v1(obj)
    except CanonicalizationError as e:
        raise SubmitPreflightError(f"Canonicalization failed for {name}: {e}") from e


def _veto(
    *,
    observed_at_utc: str,
    reason_code: str,
    reason_detail: str,
    intent_hash: Optional[str],
    plan_hash: Optional[str],
    chain_snapshot_hash: Optional[str],
    freshness_cert_hash: Optional[str],
    pointers: List[str],
    upstream_hash: Optional[str],
    repo_root: Path,
) -> Dict[str, Any]:

    veto = {
        "schema_id": "veto_record",
        "schema_version": "v1",
        "observed_at_utc": observed_at_utc,
        "boundary": "SUBMIT",
        "reason_code": reason_code,
        "reason_detail": reason_detail,
        "inputs": {
            "intent_hash": intent_hash,
            "plan_hash": plan_hash,
            "chain_snapshot_hash": chain_snapshot_hash,
            "freshness_cert_hash": freshness_cert_hash,
        },
        "pointers": list(pointers),
        "canonical_json_hash": None,
        "upstream_hash": upstream_hash,
    }

    veto["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(veto)

    validate_against_repo_schema_v1(
        veto,
        repo_root,
        "constellation_2/schemas/veto_record.v1.schema.json",
    )

    return veto


def evaluate_submit_preflight_offline_v1(
    repo_root: Path,
    *,
    intent: Dict[str, Any],
    chain_snapshot: Dict[str, Any],
    freshness_cert: Dict[str, Any],
    order_plan: Dict[str, Any],
    mapping_ledger_record: Dict[str, Any],
    binding_record: Dict[str, Any],
    eval_time_utc: str,
    pointers: List[str],
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:

    intent_hash = None
    plan_hash = None
    chain_hash = None
    cert_hash = None
    binding_hash = None

    try:
        # Schema validation
        validate_against_repo_schema_v1(intent, repo_root, "constellation_2/schemas/options_intent.v2.schema.json")
        validate_against_repo_schema_v1(chain_snapshot, repo_root, "constellation_2/schemas/options_chain_snapshot.v1.schema.json")
        validate_against_repo_schema_v1(freshness_cert, repo_root, "constellation_2/schemas/freshness_certificate.v1.schema.json")
        validate_against_repo_schema_v1(order_plan, repo_root, "constellation_2/schemas/order_plan.v1.schema.json")
        validate_against_repo_schema_v1(mapping_ledger_record, repo_root, "constellation_2/schemas/mapping_ledger_record.v1.schema.json")
        validate_against_repo_schema_v1(binding_record, repo_root, "constellation_2/schemas/binding_record.v1.schema.json")

        t_eval = _parse_utc_z(eval_time_utc)

        intent_hash = _hash_or_fail("intent", intent)
        chain_hash = _hash_or_fail("chain_snapshot", chain_snapshot)
        cert_hash = _hash_or_fail("freshness_cert", freshness_cert)
        plan_hash = _hash_or_fail("order_plan", order_plan)
        map_hash = _hash_or_fail("mapping_ledger_record", mapping_ledger_record)
        binding_hash = _hash_or_fail("binding_record", binding_record)

        # Freshness enforcement
        t_from = _parse_utc_z(freshness_cert["valid_from_utc"])
        t_until = _parse_utc_z(freshness_cert["valid_until_utc"])

        if t_eval < t_from or t_eval > t_until:
            raise SubmitPreflightError("Freshness certificate expired or not yet valid")

        if freshness_cert["snapshot_hash"] != chain_hash:
            raise SubmitPreflightError("Snapshot hash mismatch")

        if freshness_cert["snapshot_as_of_utc"] != chain_snapshot["as_of_utc"]:
            raise SubmitPreflightError("Snapshot as_of mismatch")

        # Binding checks
        if order_plan["intent_hash"] != intent_hash:
            raise SubmitPreflightError("Intent hash mismatch in order_plan")

        if mapping_ledger_record["plan_hash"] != plan_hash:
            raise SubmitPreflightError("Plan hash mismatch in mapping_ledger_record")

        if binding_record["plan_hash"] != plan_hash:
            raise SubmitPreflightError("Plan hash mismatch in binding_record")

        # Structure checks
        if order_plan["structure"] != "VERTICAL_SPREAD":
            raise SubmitPreflightError("Options-only constraint violated")

        legs = order_plan["legs"]
        actions = sorted([legs[0]["action"], legs[1]["action"]])
        if actions != ["BUY", "SELL"]:
            raise SubmitPreflightError("Defined-risk constraint violated")

        if not order_plan.get("exit_policy_ref", {}).get("policy_id"):
            raise SubmitPreflightError("Exit policy missing")

        # Construct decision AFTER all checks
        decision = {
            "schema_id": "submit_preflight_decision",
            "schema_version": "v1",
            "created_at_utc": eval_time_utc,
            "binding_hash": binding_hash,
            "decision": "ALLOW",
            "block_detail": None,
            "upstream_hash": binding_hash,
            "canonical_json_hash": None,
        }

        decision["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(decision)

        validate_against_repo_schema_v1(
            decision,
            repo_root,
            "constellation_2/schemas/submit_preflight_decision.v1.schema.json",
        )

        return decision, None

    except (SchemaValidationError, SubmitPreflightError, CanonicalizationError) as e:

        veto = _veto(
            observed_at_utc=eval_time_utc,
            reason_code=RC_SUBMIT_FAIL_CLOSED,
            reason_detail=str(e),
            intent_hash=intent_hash,
            plan_hash=plan_hash,
            chain_snapshot_hash=chain_hash,
            freshness_cert_hash=cert_hash,
            pointers=pointers,
            upstream_hash=binding_hash,
            repo_root=repo_root,
        )

        return None, veto
