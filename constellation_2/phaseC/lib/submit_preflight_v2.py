"""
submit_preflight_v2.py

Constellation 2.0 Phase C
Offline submission preflight evaluator v2 (NO BROKER CALLS).

Differences vs v1:
- Equity path accepts:
  - equity_order_plan.v1 (schema_version=v1), OR
  - equity_order_plan.v2 (schema_version=v2)
- Options path unchanged (still v1 schemas).

Fail-closed. Deterministic. No broker.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.common.execution_identity_authority_v1 import derive_submission_id_v1

from .canon_json_v1 import CanonicalizationError, canonical_hash_for_c2_artifact_v1
from .validate_against_schema_v1 import SchemaValidationError, validate_against_repo_schema_v1


class SubmitPreflightError(Exception):
    pass


RC_SUBMIT_FAIL_CLOSED = "C2_SUBMIT_FAIL_CLOSED_REQUIRED"
RC_INTENT_PROTECTIVE_STOP_MISSING = "INTENT_PROTECTIVE_STOP_MISSING"


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


def _require_equity_protective_stop_or_fail(order_plan: Dict[str, Any]) -> None:
    stop = order_plan.get("protective_stop")
    if not isinstance(stop, dict):
        raise SubmitPreflightError(f"{RC_INTENT_PROTECTIVE_STOP_MISSING}: equity_order_plan.protective_stop missing")
    order_type = str(stop.get("order_type") or "").strip().upper()
    stop_price = str(stop.get("stop_price") or "").strip()
    stop_loss_bps = stop.get("stop_loss_bps")
    basis = str(stop.get("basis") or "").strip().upper()
    if order_type != "STOP":
        raise SubmitPreflightError(f"{RC_INTENT_PROTECTIVE_STOP_MISSING}: protective_stop.order_type must be STOP")
    if not stop_price:
        raise SubmitPreflightError(f"{RC_INTENT_PROTECTIVE_STOP_MISSING}: protective_stop.stop_price missing")
    if not isinstance(stop_loss_bps, int) or stop_loss_bps <= 0:
        raise SubmitPreflightError(f"{RC_INTENT_PROTECTIVE_STOP_MISSING}: protective_stop.stop_loss_bps missing or non-positive")
    if basis != "ENTRY_REFERENCE_PRICE":
        raise SubmitPreflightError(f"{RC_INTENT_PROTECTIVE_STOP_MISSING}: protective_stop.basis invalid")


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


def evaluate_submit_preflight_offline_v2(
    repo_root: Path,
    *,
    intent: Dict[str, Any],
    chain_snapshot: Optional[Dict[str, Any]],
    freshness_cert: Optional[Dict[str, Any]],
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
    trade_instance_id = None
    submission_id = None

    try:
        _ = _parse_utc_z(eval_time_utc)

        schema_id = str(intent.get("schema_id") or "").strip()

        # OPTIONS PATH (unchanged)
        if schema_id == "options_intent":
            if chain_snapshot is None or freshness_cert is None:
                raise SubmitPreflightError("Options path requires chain_snapshot + freshness_cert")

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

            t_from = _parse_utc_z(freshness_cert["valid_from_utc"])
            t_until = _parse_utc_z(freshness_cert["valid_until_utc"])
            if t_eval < t_from or t_eval > t_until:
                raise SubmitPreflightError("Freshness certificate expired or not yet valid")
            if freshness_cert["snapshot_hash"] != chain_hash:
                raise SubmitPreflightError("Snapshot hash mismatch")
            if freshness_cert["snapshot_as_of_utc"] != chain_snapshot["as_of_utc"]:
                raise SubmitPreflightError("Snapshot as_of mismatch")

            if order_plan["intent_hash"] != intent_hash:
                raise SubmitPreflightError("Intent hash mismatch in order_plan")
            if mapping_ledger_record["plan_hash"] != plan_hash:
                raise SubmitPreflightError("Plan hash mismatch in mapping_ledger_record")
            if binding_record["plan_hash"] != plan_hash:
                raise SubmitPreflightError("Plan hash mismatch in binding_record")
            if binding_record.get("mapping_ledger_hash") != map_hash:
                raise SubmitPreflightError("BindingRecord mapping_ledger_hash mismatch")

            if order_plan["structure"] != "VERTICAL_SPREAD":
                raise SubmitPreflightError("Options-only constraint violated")
            legs = order_plan["legs"]
            actions = sorted([legs[0]["action"], legs[1]["action"]])
            if actions != ["BUY", "SELL"]:
                raise SubmitPreflightError("Defined-risk constraint violated")
            if not order_plan.get("exit_policy_ref", {}).get("policy_id"):
                raise SubmitPreflightError("Exit policy missing")

        # EQUITY PATH (v2 accepts plan v1 or v2)
        elif schema_id == "equity_intent":
            validate_against_repo_schema_v1(intent, repo_root, "constellation_2/schemas/equity_intent.v1.schema.json")

            sv = str(order_plan.get("schema_version") or "").strip()
            if sv == "v2":
                validate_against_repo_schema_v1(order_plan, repo_root, "constellation_2/schemas/equity_order_plan.v2.schema.json")
            else:
                validate_against_repo_schema_v1(order_plan, repo_root, "constellation_2/schemas/equity_order_plan.v1.schema.json")

            validate_against_repo_schema_v1(mapping_ledger_record, repo_root, "constellation_2/schemas/mapping_ledger_record.v2.schema.json")
            validate_against_repo_schema_v1(binding_record, repo_root, "constellation_2/schemas/binding_record.v2.schema.json")

            intent_hash = _hash_or_fail("intent", intent)
            plan_hash = _hash_or_fail("equity_order_plan", order_plan)
            map_hash = _hash_or_fail("mapping_ledger_record", mapping_ledger_record)
            binding_hash = _hash_or_fail("binding_record", binding_record)

            if order_plan["intent_hash"] != intent_hash:
                raise SubmitPreflightError("Intent hash mismatch in equity_order_plan")
            if mapping_ledger_record["plan_hash"] != plan_hash:
                raise SubmitPreflightError("Plan hash mismatch in mapping_ledger_record")
            if binding_record["plan_hash"] != plan_hash:
                raise SubmitPreflightError("Plan hash mismatch in binding_record")
            if binding_record.get("mapping_ledger_hash") != map_hash:
                raise SubmitPreflightError("BindingRecord mapping_ledger_hash mismatch")

            if order_plan["structure"] != "EQUITY_SPOT":
                raise SubmitPreflightError("Equity structure mismatch")
            _require_equity_protective_stop_or_fail(order_plan)

            intent_id = str(intent.get("intent_id") or "").strip()
            if not intent_id:
                raise SubmitPreflightError("Intent id missing in equity_intent")
            if str(order_plan.get("source_intent_id") or "").strip() != intent_id:
                raise SubmitPreflightError("source_intent_id mismatch in equity_order_plan")

            mapping_intent_id = str(mapping_ledger_record.get("intent_id") or "").strip()
            binding_intent_id = str(binding_record.get("intent_id") or "").strip()
            if mapping_intent_id and mapping_intent_id != intent_id:
                raise SubmitPreflightError("intent_id mismatch in mapping_ledger_record")
            if binding_intent_id and binding_intent_id != intent_id:
                raise SubmitPreflightError("intent_id mismatch in binding_record")

            trade_instance_id = str(binding_record.get("trade_instance_id") or mapping_ledger_record.get("trade_instance_id") or "").strip() or None
            submission_id = str(binding_record.get("submission_id") or "").strip() or None
            if trade_instance_id or submission_id:
                if not trade_instance_id or not submission_id:
                    raise SubmitPreflightError("execution identity incomplete in binding_record")
                if str(mapping_ledger_record.get("trade_instance_id") or "").strip() != trade_instance_id:
                    raise SubmitPreflightError("trade_instance_id mismatch in mapping_ledger_record")
                if str(binding_record.get("intent_hash") or "").strip() != intent_hash:
                    raise SubmitPreflightError("intent_hash mismatch in binding_record")
                expected_submission_id = derive_submission_id_v1(
                    intent_id=intent_id,
                    plan_hash=plan_hash,
                    trade_instance_id=trade_instance_id,
                )
                if submission_id != expected_submission_id:
                    raise SubmitPreflightError("submission_id mismatch in binding_record")

        else:
            raise SubmitPreflightError(f"Unsupported intent schema_id: {schema_id!r}")

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
        if trade_instance_id:
            decision["trade_instance_id"] = trade_instance_id
        if submission_id:
            decision["submission_id"] = submission_id
        decision["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(decision)
        validate_against_repo_schema_v1(decision, repo_root, "constellation_2/schemas/submit_preflight_decision.v1.schema.json")
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
