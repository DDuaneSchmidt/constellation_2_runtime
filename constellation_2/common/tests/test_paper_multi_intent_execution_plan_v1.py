from __future__ import annotations

import copy
import inspect
import json
import sys
from pathlib import Path

import pytest


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import constellation_2.common.paper_multi_intent_execution_plan_v1 as plan  # noqa: E402
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1  # noqa: E402


PRODUCED_AT = "2026-05-11T16:00:00Z"


def _policy(**overrides: object) -> dict[str, object]:
    payload = {
        "schema_id": "c2_paper_multi_intent_execution_policy",
        "schema_version": "v1",
        "policy_id": "C2_PAPER_MULTI_INTENT_EXECUTION_POLICY_V1",
        "environment": "PAPER",
        "multi_intent_paper_execution_enabled": False,
        "shadow_multi_intent_plan": False,
        "max_paper_intents_per_run": 1,
        "max_total_paper_risk_cents": 0,
        "per_engine_max_intents": {},
        "per_symbol_max_intents": {},
        "correlation_regime_suppression_retained": True,
    }
    payload.update(overrides)
    return payload


def _candidate(
    candidate_id: str,
    *,
    engine_id: str = "C2_VOL_INCOME_DEFINED_RISK_V1",
    symbol: str = "IWM",
    risk_cents: int = 25000,
    rank: int = 1,
    score: str = "34.7",
    bucket: str = "SHORT_VOL",
) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "intent_id": candidate_id,
        "engine_id": engine_id,
        "symbol": symbol,
        "strategy_variant": "paper_v1",
        "risk_cents": risk_cents,
        "portfolio_score_rank": rank,
        "portfolio_score_total": score,
        "correlation_bucket": bucket,
        "regime_bucket": bucket,
        "intent_path": f"/tmp/{candidate_id}.json",
        "intent_hash": "a" * 64,
    }


def _passing_statuses() -> dict[str, dict[str, object]]:
    return {
        "authorization_status": {"status": "PASS"},
        "kill_switch_status": {"kill_switch_active": False},
        "submit_boundary_status": {"submit_allowed": True},
    }


def _build(candidates: list[dict[str, object]], policy: dict[str, object], **kwargs: object) -> dict[str, object]:
    params = {
        "run_id": "paper-run-1",
        "produced_at_utc": PRODUCED_AT,
        "candidates": candidates,
        "policy": policy,
        "current_selected_intent_id": candidates[0]["candidate_id"] if candidates else "",
    }
    params.update(_passing_statuses())
    params.update(kwargs)
    return plan.build_paper_multi_intent_execution_plan_v1(**params)


def test_default_policy_keeps_one_intent_and_selected_pointer_authoritative() -> None:
    candidates = [_candidate("iwm"), _candidate("spy", engine_id="C2_TREND_EQ_PRIMARY_V1", symbol="SPY", rank=2, bucket="TREND")]

    payload = _build(candidates, _policy(), current_selected_intent_id="iwm")

    assert payload["plan_mode"] == "DISABLED_ONE_INTENT_COMPAT"
    assert payload["multi_intent_paper_execution_enabled"] is False
    assert payload["selected_intent_pointer_authoritative"] is True
    assert [row["candidate_id"] for row in payload["selected_candidates"]] == ["iwm"]
    assert payload["rejected_candidates"][0]["rejection_reason"] == "MULTI_INTENT_DISABLED_SELECTED_POINTER_AUTHORITATIVE"
    assert payload["order_submission_attempted"] is False
    assert payload["trading_behavior_changed"] is False


def test_shadow_plan_can_rank_multiple_candidates_without_orders() -> None:
    candidates = [
        _candidate("iwm", risk_cents=25000, rank=1, bucket="SHORT_VOL"),
        _candidate("spy", engine_id="C2_TREND_EQ_PRIMARY_V1", symbol="SPY", risk_cents=10000, rank=2, bucket="TREND"),
    ]

    payload = _build(
        candidates,
        _policy(shadow_multi_intent_plan=True, max_paper_intents_per_run=2, max_total_paper_risk_cents=40000),
    )

    assert payload["plan_mode"] == "SHADOW_MULTI_INTENT_PLAN"
    assert [row["candidate_id"] for row in payload["selected_candidates"]] == ["iwm", "spy"]
    assert payload["total_risk"]["risk_cents"] == 35000
    assert payload["order_submission_attempted"] is False
    assert payload["broker_transmit_enabled"] is False


def test_disabled_config_cannot_authorize_multiple_intent_submission() -> None:
    candidates = [
        _candidate("iwm", risk_cents=25000),
        _candidate("spy", engine_id="C2_TREND_EQ_PRIMARY_V1", symbol="SPY", risk_cents=10000, rank=2, bucket="TREND"),
    ]

    payload = _build(candidates, _policy(max_paper_intents_per_run=10), current_selected_intent_id="iwm")

    assert payload["plan_mode"] == "DISABLED_ONE_INTENT_COMPAT"
    assert payload["selected_candidate_count"] == 1
    assert payload["selected_intent_pointer_authoritative"] is True
    assert payload["order_submission_attempted"] is False


def test_aggregate_risk_cap_blocks_excess_candidates() -> None:
    candidates = [
        _candidate("iwm", risk_cents=25000, bucket="SHORT_VOL"),
        _candidate("spy", engine_id="C2_TREND_EQ_PRIMARY_V1", symbol="SPY", risk_cents=20000, rank=2, bucket="TREND"),
    ]

    payload = _build(candidates, _policy(shadow_multi_intent_plan=True, max_paper_intents_per_run=2, max_total_paper_risk_cents=30000))

    assert [row["candidate_id"] for row in payload["selected_candidates"]] == ["iwm"]
    assert payload["rejected_candidates"][0]["candidate_id"] == "spy"
    assert payload["rejected_candidates"][0]["rejection_reason"] == "AGGREGATE_RISK_CAP_EXCEEDED"


def test_per_symbol_and_engine_caps_work() -> None:
    candidates = [
        _candidate("iwm-1", risk_cents=10000, rank=1, bucket="SHORT_VOL_1"),
        _candidate("iwm-2", risk_cents=10000, rank=2, bucket="SHORT_VOL_2"),
        _candidate("qqq", engine_id="C2_CROSS_ASSET_TREND_V1", symbol="QQQ", risk_cents=10000, rank=3, bucket="TREND"),
    ]

    payload = _build(
        candidates,
        _policy(
            shadow_multi_intent_plan=True,
            max_paper_intents_per_run=3,
            max_total_paper_risk_cents=50000,
            per_engine_max_intents={"C2_VOL_INCOME_DEFINED_RISK_V1": 1},
            per_symbol_max_intents={"IWM": 1},
        ),
    )

    assert [row["candidate_id"] for row in payload["selected_candidates"]] == ["iwm-1", "qqq"]
    rejected = {row["candidate_id"]: row["rejection_reason"] for row in payload["rejected_candidates"]}
    assert rejected["iwm-2"] == "PER_ENGINE_MAX_INTENTS_EXCEEDED"


def test_correlation_regime_suppression_is_retained() -> None:
    candidates = [
        _candidate("iwm", risk_cents=10000, bucket="RISK_ON"),
        _candidate("spy", engine_id="C2_TREND_EQ_PRIMARY_V1", symbol="SPY", risk_cents=10000, rank=2, bucket="RISK_ON"),
    ]

    payload = _build(candidates, _policy(shadow_multi_intent_plan=True, max_paper_intents_per_run=2, max_total_paper_risk_cents=50000))

    assert [row["candidate_id"] for row in payload["selected_candidates"]] == ["iwm"]
    assert payload["rejected_candidates"][0]["rejection_reason"] == "CORRELATION_REGIME_SUPPRESSION_RETAINED"


def test_kill_switch_blocks_all_entries() -> None:
    payload = _build(
        [_candidate("iwm")],
        _policy(shadow_multi_intent_plan=True, max_total_paper_risk_cents=50000),
        kill_switch_status={"kill_switch_active": True, "status": "ACTIVE"},
    )

    assert payload["selected_candidates"] == []
    assert payload["rejected_candidates"][0]["rejection_reason"] == "ENTRY_GATE_NOT_PASS"
    assert payload["kill_switch_status"]["reason"] == "KILL_SWITCH_NOT_INACTIVE"


def test_missing_authorization_blocks_all_entries() -> None:
    payload = _build(
        [_candidate("iwm")],
        _policy(shadow_multi_intent_plan=True, max_total_paper_risk_cents=50000),
        authorization_status={},
    )

    assert payload["selected_candidates"] == []
    assert payload["rejected_candidates"][0]["rejection_reason"] == "ENTRY_GATE_NOT_PASS"
    assert payload["authorization_status"]["reason"] == "AUTHORIZATION_NOT_PASS"


def test_unsupported_paired_execution_remains_blocked() -> None:
    candidate = _candidate("pair", engine_id="C2_MARKET_NEUTRAL_SPREAD_V1", symbol="SPY", risk_cents=10000, bucket="PAIR")
    candidate["portfolio_gate_reason_codes"] = ["PAIRED_EXECUTION_NOT_SUPPORTED"]

    payload = _build([candidate], _policy(shadow_multi_intent_plan=True, max_total_paper_risk_cents=50000))

    assert payload["selected_candidates"] == []
    assert payload["rejected_candidates"][0]["rejection_reason"] == "UNSUPPORTED_PAIRED_EXECUTION"


def test_builder_does_not_mutate_input_candidates() -> None:
    candidates = [_candidate("iwm"), _candidate("spy", engine_id="C2_TREND_EQ_PRIMARY_V1", symbol="SPY", rank=2, bucket="TREND")]
    before = copy.deepcopy(candidates)

    _build(candidates, _policy(shadow_multi_intent_plan=True, max_paper_intents_per_run=2, max_total_paper_risk_cents=50000))

    assert candidates == before


def test_write_requires_explicit_output_path_and_is_stable(tmp_path: Path) -> None:
    payload = _build([_candidate("iwm")], _policy())

    with pytest.raises(plan.PaperMultiIntentExecutionPlanError, match="EXPLICIT_OUTPUT_PATH_REQUIRED"):
        plan.write_paper_multi_intent_execution_plan_v1(output_path=None, payload=payload)

    output = tmp_path / "plans" / "paper_multi_intent_execution_plan.v1.json"
    plan.write_paper_multi_intent_execution_plan_v1(output_path=output, payload=payload)
    assert output.read_bytes() == canonical_json_bytes_v1(payload) + b"\n"
    assert "/home/node/constellation_runtime_data" not in str(output)


def test_governed_default_policy_is_disabled_and_loadable() -> None:
    payload = plan.load_paper_multi_intent_execution_policy_v1()

    assert payload["multi_intent_paper_execution_enabled"] is False
    assert payload["shadow_multi_intent_plan"] is False
    assert payload["max_paper_intents_per_run"] == 1


def test_policy_rejects_enabled_multi_intent_for_this_stage() -> None:
    with pytest.raises(plan.PaperMultiIntentExecutionPlanError, match="MULTI_INTENT_EXECUTION_MUST_REMAIN_DISABLED"):
        plan.validate_paper_multi_intent_execution_policy_v1(_policy(multi_intent_paper_execution_enabled=True))


def test_module_has_no_runtime_trading_imports_or_hooks() -> None:
    source = inspect.getsource(plan)
    forbidden = [
        "ib_insync",
        "placeOrder",
        "submit_order",
        "run_submit_boundary_status_v1",
        "run_aegis_paper_ready_kernel_v1",
        "interactivebrokers",
    ]
    for token in forbidden:
        assert token not in source


def test_plan_round_trips_as_json_schema_valid() -> None:
    payload = _build([_candidate("iwm")], _policy())
    raw = canonical_json_bytes_v1(payload)
    loaded = json.loads(raw.decode("utf-8"))
    plan.validate_paper_multi_intent_execution_plan_v1(loaded)
