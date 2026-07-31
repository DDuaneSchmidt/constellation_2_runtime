from __future__ import annotations

import hashlib
import json
from typing import Any


SCHEMA_ID = "exit_policy_registry"
SCHEMA_VERSION = "v1"

DEFAULT_POLICY = {
    "sleeve_id": "DEFAULT",
    "setup_type": "GENERIC",
    "default_stop_method": "RECORDED_STOP_OR_REVIEW",
    "stop_loss_pct": 0.05,
    "atr_stop_multiple": None,
    "take_profit_pct": 0.1,
    "target_r_multiple": 2.0,
    "partial_target_r_multiple": 1.0,
    "max_hold_days": 5,
    "max_holding_days": 5,
    "trailing_stop_rule": "REVIEW_AFTER_1R",
    "trailing_stop_pct": 0.04,
    "trailing_stop_policy": "REVIEW_AFTER_1R",
    "invalidation_rule": "SIGNAL_OR_REGIME_REVIEW",
    "required_evidence": ["current_mark", "entry_price", "entry_time", "candidate_lineage"],
    "thesis_decay_policy": "REVIEW_ON_DECAY",
    "regime_invalidation_policy": "REVIEW_ON_INVALIDATION",
    "review_frequency": "DAILY",
    "exit_style": "HYBRID",
}

SLEEVE_POLICIES = {
    "C2_MEAN_REVERSION_EQ_V1": {
        **DEFAULT_POLICY,
        "sleeve_id": "C2_MEAN_REVERSION_EQ_V1",
        "setup_type": "MEAN_REVERSION",
        "stop_loss_pct": 0.035,
        "take_profit_pct": 0.06,
        "target_r_multiple": 2.0,
        "partial_target_r_multiple": 1.0,
        "max_hold_days": 5,
        "max_holding_days": 5,
        "exit_style": "HYBRID",
    },
    "C2_TREND_EQ_PRIMARY_V1": {
        **DEFAULT_POLICY,
        "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
        "setup_type": "TREND",
        "stop_loss_pct": 0.05,
        "take_profit_pct": 0.12,
        "target_r_multiple": 3.0,
        "partial_target_r_multiple": 1.5,
        "max_hold_days": 20,
        "max_holding_days": 20,
        "trailing_stop_rule": "TRAIL_AFTER_1_5R",
        "trailing_stop_pct": 0.05,
        "trailing_stop_policy": "TRAIL_AFTER_1_5R",
    },
    "C2_CROSS_ASSET_TREND_V1": {
        **DEFAULT_POLICY,
        "sleeve_id": "C2_CROSS_ASSET_TREND_V1",
        "setup_type": "CROSS_ASSET_TREND",
        "stop_loss_pct": 0.10,
        "take_profit_pct": 0.20,
        "target_r_multiple": 2.0,
        "partial_target_r_multiple": 1.0,
        "max_hold_days": 60,
        "max_holding_days": 60,
        "trailing_stop_rule": "REVIEW_AFTER_1R",
        "trailing_stop_pct": 0.08,
        "trailing_stop_policy": "REVIEW_AFTER_1R",
        "invalidation_rule": "TREND_OR_REGIME_OR_VOLATILITY_REVIEW",
    },
    "C2_EVENT_DISLOCATION_V1": {
        **DEFAULT_POLICY,
        "sleeve_id": "C2_EVENT_DISLOCATION_V1",
        "setup_type": "EVENT_DISLOCATION",
        "stop_loss_pct": 0.04,
        "take_profit_pct": 0.08,
        "target_r_multiple": 1.5,
        "partial_target_r_multiple": 1.0,
        "max_hold_days": 3,
        "max_holding_days": 3,
        "thesis_decay_policy": "REVIEW_AFTER_EVENT_WINDOW",
    },
}

SAFETY = {
    "broker_submit_transmit_allowed": False,
    "broker_execution_allowed": False,
    "order_routing_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
    "manual_operator_confirmation_required": True,
}


def stable_hash_v1(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")
    ).hexdigest()


def exit_policy_registry_v1() -> dict[str, Any]:
    policies = [SLEEVE_POLICIES[key] for key in sorted(SLEEVE_POLICIES)]
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "default_policy": DEFAULT_POLICY,
        "policies": policies,
        "policy_count": len(policies),
        **SAFETY,
    }
    payload["content_hash"] = stable_hash_v1(payload)
    return payload


def exit_policy_for_sleeve_v1(sleeve_id: str) -> dict[str, Any]:
    return dict(SLEEVE_POLICIES.get(str(sleeve_id or "").strip().upper()) or DEFAULT_POLICY)
