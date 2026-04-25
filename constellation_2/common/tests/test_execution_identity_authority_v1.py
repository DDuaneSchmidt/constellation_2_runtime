from __future__ import annotations

import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.execution_identity_authority_v1 import (  # noqa: E402
    DUPLICATE_CLASSIFICATION_NEW_INSTANCE_NEW_PLAN,
    DUPLICATE_CLASSIFICATION_NEW_INSTANCE_SAME_PLAN,
    DUPLICATE_CLASSIFICATION_SAME_INSTANCE_REPLAY,
    derive_submission_id_v1,
    derive_trade_instance_id_v1,
    resolve_submission_identity_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1  # noqa: E402
import pytest  # noqa: E402


def _plan(intent_hash: str, *, intent_id: str = "intent-abc") -> dict[str, object]:
    return {
        "schema_id": "equity_order_plan",
        "schema_version": "v2",
        "plan_id": "plan-0000000000000001",
        "created_at_utc": "2026-04-14T00:00:00Z",
        "intent_hash": intent_hash,
        "structure": "EQUITY_SPOT",
        "symbol": "SPY",
        "currency": "USD",
        "action": "BUY",
        "qty_shares": 1,
        "order_terms": {"order_type": "LIMIT", "limit_price": "679.91", "time_in_force": "DAY"},
        "risk_proof": None,
        "engine_id": "C2_TREND_EQ_PRIMARY_V1",
        "source_intent_id": intent_id,
        "intent_sha256": "a" * 64,
        "canonical_json_hash": None,
    }


def test_trade_instance_identity_is_attempt_scoped_and_deterministic() -> None:
    t1 = derive_trade_instance_id_v1(
        day_utc="2026-04-14",
        attempt_id="A0010",
        sleeve_id="PRIMARY",
        environment="PAPER",
        intent_id="intent-abc",
        intent_hash="1" * 64,
    )
    t2 = derive_trade_instance_id_v1(
        day_utc="2026-04-14",
        attempt_id="A0010",
        sleeve_id="PRIMARY",
        environment="PAPER",
        intent_id="intent-abc",
        intent_hash="1" * 64,
    )
    t3 = derive_trade_instance_id_v1(
        day_utc="2026-04-14",
        attempt_id="A0011",
        sleeve_id="PRIMARY",
        environment="PAPER",
        intent_id="intent-abc",
        intent_hash="1" * 64,
    )

    assert t1 == t2
    assert t1 != t3


def test_submission_identity_changes_only_when_trade_instance_changes() -> None:
    plan_hash = "2" * 64

    sid_1 = derive_submission_id_v1(intent_id="intent-abc", plan_hash=plan_hash, trade_instance_id="3" * 64)
    sid_1_replay = derive_submission_id_v1(intent_id="intent-abc", plan_hash=plan_hash, trade_instance_id="3" * 64)
    sid_2 = derive_submission_id_v1(intent_id="intent-abc", plan_hash=plan_hash, trade_instance_id="4" * 64)

    assert sid_1 == sid_1_replay
    assert sid_1 != sid_2


def test_duplicate_classification_distinguishes_replay_same_plan_and_new_plan() -> None:
    from constellation_2.common.execution_identity_authority_v1 import classify_duplicate_classification_v1

    assert classify_duplicate_classification_v1(
        prior_trade_instance_id="5" * 64,
        prior_plan_hash="6" * 64,
        current_trade_instance_id="5" * 64,
        current_plan_hash="6" * 64,
    ) == DUPLICATE_CLASSIFICATION_SAME_INSTANCE_REPLAY

    assert classify_duplicate_classification_v1(
        prior_trade_instance_id="5" * 64,
        prior_plan_hash="6" * 64,
        current_trade_instance_id="7" * 64,
        current_plan_hash="6" * 64,
    ) == DUPLICATE_CLASSIFICATION_NEW_INSTANCE_SAME_PLAN

    assert classify_duplicate_classification_v1(
        prior_trade_instance_id="5" * 64,
        prior_plan_hash="6" * 64,
        current_trade_instance_id="7" * 64,
        current_plan_hash="8" * 64,
    ) == DUPLICATE_CLASSIFICATION_NEW_INSTANCE_NEW_PLAN


def test_resolve_submission_identity_supports_trade_instance_and_legacy_modes() -> None:
    plan = _plan("1" * 64)
    plan_hash = canonical_hash_for_c2_artifact_v1(plan)
    submission_id = derive_submission_id_v1(intent_id="intent-abc", plan_hash=plan_hash, trade_instance_id="9" * 64)

    binding = {
        "plan_hash": plan_hash,
        "intent_id": "intent-abc",
        "trade_instance_id": "9" * 64,
        "submission_id": submission_id,
    }
    binding_hash = canonical_hash_for_c2_artifact_v1(binding)
    execution_identity = {
        "intent_id": "intent-abc",
        "plan_hash": plan_hash,
        "binding_hash": binding_hash,
        "trade_instance_id": "9" * 64,
        "submission_id": submission_id,
    }

    resolved = resolve_submission_identity_v1(
        plan_obj=plan,
        binding_obj=binding,
        binding_hash=binding_hash,
        execution_identity_obj=execution_identity,
    )
    assert resolved.identity_mode == "TRADE_INSTANCE_V1"
    assert resolved.submission_id == submission_id

    legacy_binding = {"plan_hash": plan_hash}
    legacy_binding_hash = canonical_hash_for_c2_artifact_v1(legacy_binding)
    legacy_resolved = resolve_submission_identity_v1(
        plan_obj=plan,
        binding_obj=legacy_binding,
        binding_hash=legacy_binding_hash,
        execution_identity_obj=None,
    )
    assert legacy_resolved.identity_mode == "LEGACY_BINDING_HASH_V1"
    assert legacy_resolved.submission_id == legacy_binding_hash


def test_resolve_submission_identity_rejects_record_mismatch() -> None:
    plan = _plan("1" * 64)
    plan_hash = canonical_hash_for_c2_artifact_v1(plan)
    submission_id = derive_submission_id_v1(intent_id="intent-abc", plan_hash=plan_hash, trade_instance_id="9" * 64)
    binding = {
        "plan_hash": plan_hash,
        "intent_id": "intent-abc",
        "trade_instance_id": "9" * 64,
        "submission_id": submission_id,
    }
    binding_hash = canonical_hash_for_c2_artifact_v1(binding)

    with pytest.raises(ValueError, match="EXECUTION_IDENTITY_SUBMISSION_ID_RECORD_MISMATCH"):
        resolve_submission_identity_v1(
            plan_obj=plan,
            binding_obj=binding,
            binding_hash=binding_hash,
            execution_identity_obj={
                "intent_id": "intent-abc",
                "plan_hash": plan_hash,
                "binding_hash": binding_hash,
                "trade_instance_id": "9" * 64,
                "submission_id": "a" * 64,
            },
        )
