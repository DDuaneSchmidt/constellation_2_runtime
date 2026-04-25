from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import SchemaValidationError, validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_RELPATH = "governance/02_REGISTRIES/C2_RISK_POLICY_REGISTRY_V1.json"
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RISK/c2_risk_policy_registry.v1.schema.json"
REGISTRY_PATH = (REPO_ROOT / REGISTRY_RELPATH).resolve()


class RiskPolicyLoaderError(Exception):
    pass


def _read_registry_or_fail() -> dict[str, Any]:
    if not REGISTRY_PATH.exists():
        raise RiskPolicyLoaderError(f"RISK_POLICY_REGISTRY_MISSING: {REGISTRY_PATH}")
    if not REGISTRY_PATH.is_file():
        raise RiskPolicyLoaderError(f"RISK_POLICY_REGISTRY_NOT_FILE: {REGISTRY_PATH}")
    try:
        obj = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise RiskPolicyLoaderError(f"RISK_POLICY_REGISTRY_JSON_INVALID: {REGISTRY_PATH}") from e
    if not isinstance(obj, dict):
        raise RiskPolicyLoaderError(f"RISK_POLICY_REGISTRY_TOP_LEVEL_NOT_OBJECT: {REGISTRY_PATH}")
    try:
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
    except SchemaValidationError as e:
        raise RiskPolicyLoaderError(f"RISK_POLICY_REGISTRY_SCHEMA_INVALID: {e}") from e
    return obj


def load_risk_policy_for_engine_or_fail(engine_id: str) -> dict:
    engine_key = str(engine_id or "").strip()
    if not engine_key:
        raise RiskPolicyLoaderError("RISK_POLICY_ENGINE_ID_REQUIRED")
    registry = _read_registry_or_fail()
    policies = registry.get("policies")
    if not isinstance(policies, dict):
        raise RiskPolicyLoaderError("RISK_POLICY_REGISTRY_POLICIES_MISSING")
    policy = policies.get(engine_key)
    if not isinstance(policy, dict):
        raise RiskPolicyLoaderError(f"RISK_POLICY_ENGINE_MISSING: {engine_key}")
    if not isinstance(policy.get("target_notional_pct_default"), str) or not str(policy["target_notional_pct_default"]).strip():
        raise RiskPolicyLoaderError(f"RISK_POLICY_TARGET_DEFAULT_MISSING: {engine_key}")
    if not isinstance(policy.get("per_trade_notional_pct_max"), str) or not str(policy["per_trade_notional_pct_max"]).strip():
        raise RiskPolicyLoaderError(f"RISK_POLICY_PER_TRADE_CAP_MISSING: {engine_key}")
    stop_loss_bps = policy.get("stop_loss_bps_default")
    if not isinstance(stop_loss_bps, int) or stop_loss_bps <= 0:
        raise RiskPolicyLoaderError(f"RISK_POLICY_STOP_LOSS_BPS_DEFAULT_MISSING: {engine_key}")
    allow_entry_only = policy.get("allow_entry_only_paper_test")
    if not isinstance(allow_entry_only, bool):
        raise RiskPolicyLoaderError(f"RISK_POLICY_ALLOW_ENTRY_ONLY_PAPER_TEST_MISSING: {engine_key}")
    return dict(policy)


def get_target_notional_pct_default_or_fail(engine_id: str) -> str:
    return str(load_risk_policy_for_engine_or_fail(engine_id)["target_notional_pct_default"])


def get_per_trade_notional_pct_max_or_fail(engine_id: str) -> str:
    return str(load_risk_policy_for_engine_or_fail(engine_id)["per_trade_notional_pct_max"])


def get_stop_loss_bps_default_or_fail(engine_id: str) -> int:
    return int(load_risk_policy_for_engine_or_fail(engine_id)["stop_loss_bps_default"])


def get_allow_entry_only_paper_test_or_fail(engine_id: str) -> bool:
    return bool(load_risk_policy_for_engine_or_fail(engine_id)["allow_entry_only_paper_test"])
