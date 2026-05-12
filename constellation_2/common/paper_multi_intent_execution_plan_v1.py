from __future__ import annotations

import copy
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
GOVERNED_POLICY_RELPATH = "governance/02_REGISTRIES/C2_PAPER_MULTI_INTENT_EXECUTION_POLICY_V1.json"
PLAN_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_multi_intent_execution_plan.v1.schema.json"


class PaperMultiIntentExecutionPlanError(ValueError):
    pass


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().upper() in {"1", "TRUE", "YES", "Y"}
    return False


def _as_int(value: Any, *, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text:
            try:
                return int(text)
            except ValueError:
                return default
    return default


def _decimal_sort_value(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _candidate_identifier(candidate: dict[str, Any]) -> str:
    for key in ("candidate_id", "intent_id", "decision_id"):
        value = str(candidate.get(key) or "").strip()
        if value:
            return value
    engine_id = str(candidate.get("engine_id") or "").strip()
    symbol = str(candidate.get("symbol") or "").strip().upper()
    variant = str(candidate.get("strategy_variant") or candidate.get("strategy_id") or "").strip()
    return ":".join(part for part in (engine_id, symbol, variant) if part)


def _candidate_sort_key(candidate: dict[str, Any]) -> tuple[int, Decimal, str]:
    rank = _as_int(candidate.get("portfolio_score_rank") or candidate.get("rank"), default=999999)
    score = _decimal_sort_value(candidate.get("portfolio_score_total") or candidate.get("score"))
    return (rank, -score, _candidate_identifier(candidate))


def _risk_cents(candidate: dict[str, Any]) -> int | None:
    for key in ("risk_cents", "per_intent_risk_cents", "max_loss_cents"):
        if key in candidate:
            value = _as_int(candidate.get(key), default=-1)
            return value if value >= 0 else None
    risk = candidate.get("risk")
    if isinstance(risk, dict):
        value = _as_int(risk.get("risk_cents") or risk.get("max_loss_cents"), default=-1)
        return value if value >= 0 else None
    return None


def _normalized_candidate(candidate: dict[str, Any], *, risk_cents: int | None) -> dict[str, Any]:
    return {
        "candidate_id": _candidate_identifier(candidate),
        "intent_id": str(candidate.get("intent_id") or "").strip(),
        "engine_id": str(candidate.get("engine_id") or candidate.get("sleeve_id") or "").strip(),
        "symbol": str(candidate.get("symbol") or "").strip().upper(),
        "strategy_variant": str(candidate.get("strategy_variant") or candidate.get("strategy_id") or "").strip(),
        "decision_type": str(candidate.get("decision_type") or "PAPER_INTENT_CANDIDATE").strip(),
        "risk_cents": int(risk_cents or 0),
        "correlation_bucket": str(candidate.get("correlation_bucket") or candidate.get("regime_bucket") or "").strip(),
        "regime_bucket": str(candidate.get("regime_bucket") or "").strip(),
        "rank": _as_int(candidate.get("portfolio_score_rank") or candidate.get("rank"), default=999999),
        "score": str(candidate.get("portfolio_score_total") or candidate.get("score") or "0"),
        "source_intent_path": str(candidate.get("intent_path") or candidate.get("source_intent_path") or "").strip(),
        "source_intent_hash": str(candidate.get("intent_hash") or candidate.get("source_intent_hash") or "").strip(),
    }


def _status_from_payload(payload: dict[str, Any] | None, *, status_keys: Iterable[str]) -> str:
    if not isinstance(payload, dict):
        return ""
    for key in status_keys:
        value = str(payload.get(key) or "").strip().upper()
        if value:
            return value
    return ""


def _authorization_passes(payload: dict[str, Any] | None) -> tuple[bool, dict[str, Any]]:
    if isinstance(payload, dict) and (
        payload.get("submission_authorized") is True
        or payload.get("authorization_passed") is True
        or payload.get("authorized") is True
    ):
        return True, {"status": "PASS", "source_status": "BOOLEAN_TRUE", "reason": ""}
    status = _status_from_payload(payload, status_keys=("authorization_status", "status", "verdict", "authorization_verdict"))
    passes = status in {"PASS", "AUTHORIZED", "GRANTED", "ALLOW", "ALLOWED"}
    return passes, {"status": "PASS" if passes else "BLOCKED", "source_status": status, "reason": "" if passes else "AUTHORIZATION_NOT_PASS"}


def _kill_switch_inactive(payload: dict[str, Any] | None) -> tuple[bool, dict[str, Any]]:
    if isinstance(payload, dict) and payload.get("kill_switch_active") is False:
        return True, {"status": "INACTIVE", "source_status": "KILL_SWITCH_ACTIVE_FALSE", "reason": ""}
    if isinstance(payload, dict) and payload.get("active") is False:
        return True, {"status": "INACTIVE", "source_status": "ACTIVE_FALSE", "reason": ""}
    status = _status_from_payload(payload, status_keys=("kill_switch_status", "status", "state", "verdict"))
    inactive = status in {"INACTIVE", "CLEAR", "CLEARED", "PASS", "OK", "ALLOW", "ALLOWED"}
    return inactive, {"status": "INACTIVE" if inactive else "BLOCKED", "source_status": status, "reason": "" if inactive else "KILL_SWITCH_NOT_INACTIVE"}


def _submit_boundary_ready(payload: dict[str, Any] | None) -> tuple[bool, dict[str, Any]]:
    if isinstance(payload, dict) and (payload.get("submit_allowed") is True or payload.get("submission_authorized") is True):
        return True, {"status": "READY", "source_status": "BOOLEAN_TRUE", "reason": ""}
    status = _status_from_payload(payload, status_keys=("submit_boundary_status", "boundary_status", "status", "verdict"))
    ready = status in {"READY", "AUTHORIZED", "PASS", "OK", "ALLOW", "ALLOWED"}
    return ready, {"status": "READY" if ready else "BLOCKED", "source_status": status, "reason": "" if ready else "SUBMIT_BOUNDARY_NOT_READY"}


def _is_unsupported_paired(candidate: dict[str, Any]) -> bool:
    if candidate.get("paired_execution_supported") is False:
        return True
    status = str(candidate.get("execution_support") or "").strip().upper()
    if status in {"PAIRED_EXECUTION_NOT_SUPPORTED", "UNSUPPORTED_PAIRED_EXECUTION"}:
        return True
    reason_codes = candidate.get("reason_codes") if isinstance(candidate.get("reason_codes"), list) else []
    gate_codes = candidate.get("portfolio_gate_reason_codes") if isinstance(candidate.get("portfolio_gate_reason_codes"), list) else []
    return "PAIRED_EXECUTION_NOT_SUPPORTED" in {str(code).strip().upper() for code in [*reason_codes, *gate_codes]}


def load_paper_multi_intent_execution_policy_v1(policy_path: Path | None = None) -> dict[str, Any]:
    path = Path(policy_path) if policy_path is not None else REPO_ROOT / GOVERNED_POLICY_RELPATH
    if not path.is_file():
        raise PaperMultiIntentExecutionPlanError(f"POLICY_MISSING:{path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise PaperMultiIntentExecutionPlanError(f"POLICY_NOT_OBJECT:{path}")
    validate_paper_multi_intent_execution_policy_v1(payload)
    return payload


def validate_paper_multi_intent_execution_policy_v1(policy: dict[str, Any]) -> None:
    if _as_bool(policy.get("multi_intent_paper_execution_enabled")):
        raise PaperMultiIntentExecutionPlanError("MULTI_INTENT_EXECUTION_MUST_REMAIN_DISABLED_FOR_THIS_STAGE")
    if _as_int(policy.get("max_paper_intents_per_run"), default=0) < 1:
        raise PaperMultiIntentExecutionPlanError("MAX_PAPER_INTENTS_PER_RUN_MUST_BE_AT_LEAST_ONE")
    if _as_int(policy.get("max_total_paper_risk_cents"), default=-1) < 0:
        raise PaperMultiIntentExecutionPlanError("MAX_TOTAL_PAPER_RISK_CENTS_REQUIRED")
    if not _as_bool(policy.get("correlation_regime_suppression_retained")):
        raise PaperMultiIntentExecutionPlanError("CORRELATION_REGIME_SUPPRESSION_MUST_BE_RETAINED")


def build_paper_multi_intent_execution_plan_v1(
    *,
    run_id: str,
    produced_at_utc: str,
    candidates: Iterable[dict[str, Any]],
    policy: dict[str, Any],
    current_selected_intent_id: str = "",
    authorization_status: dict[str, Any] | None = None,
    kill_switch_status: dict[str, Any] | None = None,
    submit_boundary_status: dict[str, Any] | None = None,
    broker_transmit_enabled: bool = False,
) -> dict[str, Any]:
    validate_paper_multi_intent_execution_policy_v1(policy)
    raw_candidates = [copy.deepcopy(candidate) for candidate in candidates]
    sorted_candidates = sorted(raw_candidates, key=_candidate_sort_key)
    multi_enabled = _as_bool(policy.get("multi_intent_paper_execution_enabled"))
    shadow_enabled = _as_bool(policy.get("shadow_multi_intent_plan"))
    max_per_run = _as_int(policy.get("max_paper_intents_per_run"), default=1)
    risk_cap = _as_int(policy.get("max_total_paper_risk_cents"), default=0)
    per_engine_caps = policy.get("per_engine_max_intents") if isinstance(policy.get("per_engine_max_intents"), dict) else {}
    per_symbol_caps = policy.get("per_symbol_max_intents") if isinstance(policy.get("per_symbol_max_intents"), dict) else {}
    retain_bucket_suppression = _as_bool(policy.get("correlation_regime_suppression_retained"))

    auth_pass, auth_summary = _authorization_passes(authorization_status)
    kill_pass, kill_summary = _kill_switch_inactive(kill_switch_status)
    submit_pass, submit_summary = _submit_boundary_ready(submit_boundary_status)
    status_gate_pass = auth_pass and kill_pass and submit_pass

    selected: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []

    if not multi_enabled and not shadow_enabled:
        selected_id = str(current_selected_intent_id or "").strip()
        chosen: dict[str, Any] | None = None
        for candidate in sorted_candidates:
            if selected_id and _candidate_identifier(candidate) == selected_id:
                chosen = candidate
                break
        if chosen is None and sorted_candidates:
            chosen = sorted_candidates[0]
        chosen_id = _candidate_identifier(chosen) if chosen is not None else ""
        for candidate in sorted_candidates:
            risk = _risk_cents(candidate)
            row = _normalized_candidate(candidate, risk_cents=risk)
            if chosen is not None and row["candidate_id"] == chosen_id:
                selected.append(row)
            else:
                rejected.append(
                    {
                        **row,
                        "rejection_reason": "MULTI_INTENT_DISABLED_SELECTED_POINTER_AUTHORITATIVE",
                        "rejection_details": "Default PAPER execution remains single-intent; selected_intent_pointer remains authoritative.",
                    }
                )
    else:
        if not status_gate_pass:
            gate_reasons = [item["reason"] for item in (auth_summary, kill_summary, submit_summary) if item.get("reason")]
            for candidate in sorted_candidates:
                risk = _risk_cents(candidate)
                rejected.append(
                    {
                        **_normalized_candidate(candidate, risk_cents=risk),
                        "rejection_reason": "ENTRY_GATE_NOT_PASS",
                        "rejection_details": ",".join(gate_reasons),
                    }
                )
        elif multi_enabled and not broker_transmit_enabled:
            for candidate in sorted_candidates:
                risk = _risk_cents(candidate)
                rejected.append(
                    {
                        **_normalized_candidate(candidate, risk_cents=risk),
                        "rejection_reason": "BROKER_TRANSMIT_NOT_ENABLED_FOR_FUTURE_MULTI_INTENT_EXECUTION",
                        "rejection_details": "Future execution mode requires explicit broker transmit enablement; this builder never submits.",
                    }
                )
        else:
            total_risk = 0
            engine_counts: dict[str, int] = {}
            symbol_counts: dict[str, int] = {}
            used_buckets: set[str] = set()
            for candidate in sorted_candidates:
                risk = _risk_cents(candidate)
                row = _normalized_candidate(candidate, risk_cents=risk)
                engine_id = str(row["engine_id"])
                symbol = str(row["symbol"])
                bucket = str(row["correlation_bucket"] or row["regime_bucket"] or "")
                if len(selected) >= max_per_run:
                    rejected.append({**row, "rejection_reason": "MAX_PAPER_INTENTS_PER_RUN_EXCEEDED", "rejection_details": f"max={max_per_run}"})
                    continue
                if risk is None:
                    rejected.append({**row, "rejection_reason": "RISK_CENTS_MISSING", "rejection_details": "Candidate omitted explicit per-intent risk."})
                    continue
                if risk_cap <= 0:
                    rejected.append({**row, "rejection_reason": "AGGREGATE_RISK_CAP_MISSING", "rejection_details": "max_total_paper_risk_cents must be positive for shadow/future multi-intent planning."})
                    continue
                if total_risk + risk > risk_cap:
                    rejected.append({**row, "rejection_reason": "AGGREGATE_RISK_CAP_EXCEEDED", "rejection_details": f"candidate_risk={risk};current_total={total_risk};cap={risk_cap}"})
                    continue
                engine_cap = _as_int(per_engine_caps.get(engine_id), default=0)
                if engine_cap > 0 and engine_counts.get(engine_id, 0) >= engine_cap:
                    rejected.append({**row, "rejection_reason": "PER_ENGINE_MAX_INTENTS_EXCEEDED", "rejection_details": f"engine_id={engine_id};cap={engine_cap}"})
                    continue
                symbol_cap = _as_int(per_symbol_caps.get(symbol), default=0)
                if symbol_cap > 0 and symbol_counts.get(symbol, 0) >= symbol_cap:
                    rejected.append({**row, "rejection_reason": "PER_SYMBOL_MAX_INTENTS_EXCEEDED", "rejection_details": f"symbol={symbol};cap={symbol_cap}"})
                    continue
                if retain_bucket_suppression and bucket and bucket in used_buckets:
                    rejected.append({**row, "rejection_reason": "CORRELATION_REGIME_SUPPRESSION_RETAINED", "rejection_details": f"bucket={bucket}"})
                    continue
                if _is_unsupported_paired(candidate):
                    rejected.append({**row, "rejection_reason": "UNSUPPORTED_PAIRED_EXECUTION", "rejection_details": "Paired execution support remains required before this candidate can be executable."})
                    continue
                selected.append(row)
                total_risk += risk
                engine_counts[engine_id] = engine_counts.get(engine_id, 0) + 1
                symbol_counts[symbol] = symbol_counts.get(symbol, 0) + 1
                if bucket:
                    used_buckets.add(bucket)

    selected_risk = sum(int(row["risk_cents"]) for row in selected)
    plan_mode = "DISABLED_ONE_INTENT_COMPAT"
    if shadow_enabled and not multi_enabled:
        plan_mode = "SHADOW_MULTI_INTENT_PLAN"
    elif multi_enabled:
        plan_mode = "FUTURE_GOVERNED_MULTI_INTENT_PLAN"

    payload = {
        "schema_id": "paper_multi_intent_execution_plan",
        "schema_version": "v1",
        "run_id": str(run_id),
        "produced_at_utc": str(produced_at_utc),
        "plan_mode": plan_mode,
        "multi_intent_paper_execution_enabled": bool(multi_enabled),
        "shadow_multi_intent_plan": bool(shadow_enabled),
        "selected_intent_pointer_authoritative": bool(not multi_enabled),
        "current_selected_intent_id": str(current_selected_intent_id or "").strip(),
        "selected_candidates": selected,
        "rejected_candidates": rejected,
        "selected_candidate_count": len(selected),
        "rejected_candidate_count": len(rejected),
        "total_risk": {
            "risk_cents": selected_risk,
            "max_total_paper_risk_cents": risk_cap,
        },
        "per_intent_risk": [{"candidate_id": row["candidate_id"], "risk_cents": int(row["risk_cents"])} for row in selected],
        "correlation_bucket": sorted({str(row.get("correlation_bucket") or "") for row in selected if str(row.get("correlation_bucket") or "")}),
        "authorization_status": auth_summary,
        "kill_switch_status": kill_summary,
        "submit_boundary_status": submit_summary,
        "broker_transmit_enabled": bool(broker_transmit_enabled),
        "order_submission_attempted": False,
        "trading_behavior_changed": False,
        "execution_requirements": {
            "authorization_pass_required_per_intent": True,
            "kill_switch_inactive_required": True,
            "submit_boundary_ready_required": True,
            "broker_transmit_explicitly_enabled_required": True,
            "aggregate_risk_pass_required": True,
            "unsupported_paired_execution_blocked": True,
        },
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    validate_paper_multi_intent_execution_plan_v1(payload)
    return payload


def validate_paper_multi_intent_execution_plan_v1(payload: dict[str, Any]) -> None:
    validate_against_repo_schema_v1(payload, REPO_ROOT, PLAN_SCHEMA_RELPATH)


def write_paper_multi_intent_execution_plan_v1(*, output_path: Path | None, payload: dict[str, Any]) -> Path:
    if output_path is None:
        raise PaperMultiIntentExecutionPlanError("EXPLICIT_OUTPUT_PATH_REQUIRED")
    path = Path(output_path)
    validate_paper_multi_intent_execution_plan_v1(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path
