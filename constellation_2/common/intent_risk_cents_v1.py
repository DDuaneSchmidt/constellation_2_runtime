from __future__ import annotations

import json
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
ORDER_PLAN_SCHEMA_RELPATH = "constellation_2/schemas/equity_order_plan.v2.schema.json"


def _read_json_obj(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _decimal_to_cents(raw: str) -> int:
    normalized = str(raw or "").strip()
    if not normalized:
        raise ValueError("DECIMAL_VALUE_MISSING")
    cents = (Decimal(normalized) * Decimal("100")).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return int(cents)


def _latest_order_plan_path(day_utc: str, intent_hash: str, truth_root: Path) -> Path:
    phasec_dir = (truth_root / "phaseC_preflight_v1" / day_utc).resolve()
    if not phasec_dir.exists() or not phasec_dir.is_dir():
        raise FileNotFoundError(f"PHASEC_PREFLIGHT_DAY_MISSING: {phasec_dir}")
    matches = sorted(phasec_dir.glob(f"attempt_*/{intent_hash}/equity_order_plan.v2.json"))
    if not matches:
        raise FileNotFoundError(f"ORDER_PLAN_NOT_FOUND day_utc={day_utc} intent_hash={intent_hash}")
    return matches[-1].resolve()


def _smoke_test_policy_risk_context(auth_obj: Dict[str, Any], repo_root: Path) -> Dict[str, Any] | None:
    engine_id = str(auth_obj.get("engine_id") or "").strip()
    if engine_id != "C2_INTENT_SIMULATOR_V1":
        return None
    for entry in auth_obj.get("input_manifest") or []:
        if not isinstance(entry, dict):
            continue
        if str(entry.get("type") or "").strip() != "paper_submit_smoke_test_policy_v1":
            continue
        policy_path = Path(str(entry.get("path") or "")).expanduser().resolve()
        if not policy_path.is_file():
            raise FileNotFoundError(f"SMOKE_TEST_POLICY_MISSING: {policy_path}")
        policy_obj = _read_json_obj(policy_path)
        rules = policy_obj.get("smoke_test_rules")
        instrument = rules.get("instrument") if isinstance(rules, dict) else None
        order_terms = rules.get("order_terms") if isinstance(rules, dict) else None
        if not isinstance(rules, dict) or not isinstance(instrument, dict) or not isinstance(order_terms, dict):
            raise ValueError("SMOKE_TEST_POLICY_INVALID")
        quantity_shares = int(rules.get("quantity_shares") or 0)
        if quantity_shares <= 0:
            raise ValueError("SMOKE_TEST_POLICY_QTY_INVALID")
        limit_price = order_terms.get("limit_price")
        if limit_price is None:
            raise ValueError("SMOKE_TEST_POLICY_LIMIT_PRICE_MISSING")
        risk_cents = quantity_shares * _decimal_to_cents(str(limit_price))
        return {
            "path": policy_path,
            "plan_obj": policy_obj,
            "risk_cents": risk_cents,
            "requested_quantity": quantity_shares,
            "risk_model": "SMOKE_TEST_POLICY_LIMIT_NOTIONAL",
        }
    return None


def derive_risk_cents_from_order_plan_v1(plan_obj: Dict[str, Any]) -> Dict[str, Any]:
    schema_id = str(plan_obj.get("schema_id") or "").strip()
    schema_version = str(plan_obj.get("schema_version") or "").strip()
    if schema_id != "equity_order_plan" or schema_version != "v2":
        raise ValueError("ORDER_PLAN_SCHEMA_INVALID")
    qty_shares = int(plan_obj.get("qty_shares") or 0)
    if qty_shares <= 0:
        raise ValueError("ORDER_PLAN_QTY_INVALID")
    risk_proof = plan_obj.get("risk_proof")
    if isinstance(risk_proof, dict) and bool(risk_proof.get("defined_risk_proven") is True):
        total_risk_cents = _decimal_to_cents(str(risk_proof.get("max_loss_usd") or ""))
        if total_risk_cents < 0:
            raise ValueError("ORDER_PLAN_DEFINED_RISK_NEGATIVE")
        return {
            "risk_cents": total_risk_cents,
            "requested_quantity": qty_shares,
            "risk_model": "DEFINED_RISK_MAX_LOSS",
        }
    if str(plan_obj.get("structure") or "").strip() == "EQUITY_SPOT":
        order_terms = plan_obj.get("order_terms")
        if not isinstance(order_terms, dict):
            raise ValueError("ORDER_TERMS_INVALID")
        limit_price = order_terms.get("limit_price")
        if limit_price is None:
            raise ValueError("ORDER_PLAN_LIMIT_PRICE_REQUIRED_FOR_EQUITY_SPOT")
        per_share_cents = _decimal_to_cents(str(limit_price))
        return {
            "risk_cents": qty_shares * per_share_cents,
            "requested_quantity": qty_shares,
            "risk_model": "EQUITY_LIMIT_NOTIONAL",
        }
    raise ValueError("ORDER_PLAN_RISK_DERIVATION_UNSUPPORTED")


def load_intent_risk_context_v1(*, day_utc: str, intent_hash: str, truth_root: Path, repo_root: Path | None = None) -> Dict[str, Any]:
    root = (repo_root or REPO_ROOT).resolve()
    truth_root = truth_root.resolve()
    path = _latest_order_plan_path(day_utc, intent_hash, truth_root)
    plan_obj = _read_json_obj(path)
    validate_against_repo_schema_v1(plan_obj, root, ORDER_PLAN_SCHEMA_RELPATH)
    derived = derive_risk_cents_from_order_plan_v1(plan_obj)
    return {
        "path": path,
        "plan_obj": plan_obj,
        "risk_cents": int(derived["risk_cents"]),
        "requested_quantity": int(derived["requested_quantity"]),
        "risk_model": str(derived["risk_model"]),
    }


def load_authorization_risk_context_v1(
    *,
    day_utc: str,
    intent_hash: str,
    auth_obj: Dict[str, Any],
    truth_root: Path,
    repo_root: Path | None = None,
) -> Dict[str, Any]:
    root = (repo_root or REPO_ROOT).resolve()
    try:
        return load_intent_risk_context_v1(day_utc=day_utc, intent_hash=intent_hash, truth_root=truth_root, repo_root=root)
    except FileNotFoundError:
        fallback = _smoke_test_policy_risk_context(auth_obj, root)
        if fallback is not None:
            return fallback
        raise
