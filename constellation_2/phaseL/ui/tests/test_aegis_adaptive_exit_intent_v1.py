from __future__ import annotations

from pathlib import Path

from constellation_2.phaseL.ui.tests.operator_shell_test_sources import pages_source_v1

from ops.aegis.operator_action_command_contracts_v1 import command_registry_v1, execute_aegis_command_v1

ROOT = Path(__file__).resolve().parents[4]
from ops.aegis.trade_lifecycle.daily_exit_review_v1 import evaluate_exit_review_row_v1
from ops.aegis.trade_lifecycle.exit_policy_registry_v1 import exit_policy_for_sleeve_v1, exit_policy_registry_v1
from ops.aegis.trade_lifecycle.trade_intent_ledger_v1 import (
    append_trade_intent_event_v1,
    backfilled_exit_plan_from_trade_v1,
    generate_backfilled_exit_plan_v1,
    read_trade_intent_ledger_v1,
    reduce_trade_intent_state_v1,
)


def _trade(**overrides):
    row = {
        "trade_id": "ticket:02200087bcdf9839aacb8958",
        "position_id": "ticket_02200087bcdf9839aacb8958",
        "symbol": "DOW",
        "side": "BUY",
        "quantity": 10,
        "entry_price": 36.06,
        "current_mark": 36.01,
        "current_stop": 35.50,
        "sleeve_id": "C2_MEAN_REVERSION_EQ_V1",
    }
    row.update(overrides)
    return row


def test_exit_policy_registry_contains_sleeve_defaults() -> None:
    registry = exit_policy_registry_v1()
    policy = exit_policy_for_sleeve_v1("C2_MEAN_REVERSION_EQ_V1")

    assert registry["schema_id"] == "exit_policy_registry"
    assert policy["target_r_multiple"] == 2.0
    assert policy["partial_target_r_multiple"] == 1.0
    assert policy["max_holding_days"] == 5
    assert registry["broker_submit_transmit_allowed"] is False


def test_backfilled_legacy_dow_intent_derives_targets_without_thesis_fabrication(tmp_path: Path) -> None:
    result = generate_backfilled_exit_plan_v1(truth_root=tmp_path, day_utc="2026-05-22", trade=_trade(), operator_or_system="TEST")
    rows = read_trade_intent_ledger_v1(truth_root=tmp_path, day_utc="2026-05-22")
    state = reduce_trade_intent_state_v1(rows)["ticket:02200087bcdf9839aacb8958"]
    plan = state["current_exit_plan"]

    assert result["status"] == "BACKFILLED_INTENT_CREATED"
    assert rows[0]["event_type"] == "BACKFILLED_INTENT"
    assert rows[0]["confidence"] == "PARTIAL"
    assert plan["target_price"] == 37.18
    assert plan["partial_target_price"] == 36.62
    assert "Thesis details were not invented" in plan["inference_warning"]


def test_new_trade_intent_created_is_append_only(tmp_path: Path) -> None:
    first = append_trade_intent_event_v1(
        truth_root=tmp_path,
        day_utc="2026-05-22",
        trade_id="trade:new",
        position_id="position:new",
        symbol="SPY",
        sleeve_id="C2_MEAN_REVERSION_EQ_V1",
        event_type="INTENT_CREATED",
        new_value={"stop_price": 99, "target_price": 104},
        reason="test",
    )
    second = append_trade_intent_event_v1(
        truth_root=tmp_path,
        day_utc="2026-05-22",
        trade_id="trade:new",
        position_id="position:new",
        symbol="SPY",
        sleeve_id="C2_MEAN_REVERSION_EQ_V1",
        event_type="STOP_ADJUSTED",
        prior_value=99,
        new_value=100,
        reason="manual review",
    )
    rows = read_trade_intent_ledger_v1(truth_root=tmp_path, day_utc="2026-05-22")
    state = reduce_trade_intent_state_v1(rows)["trade:new"]

    assert len(rows) == 2
    assert rows[0]["content_hash"] == first["content_hash"]
    assert rows[1]["content_hash"] == second["content_hash"]
    assert state["current_exit_plan"]["stop_price"] == 100
    assert state["changed_since_entry"] == ["stop_price"]


def test_daily_review_decisions_cover_hold_stop_target_time_and_missing_mark() -> None:
    plan = backfilled_exit_plan_from_trade_v1(_trade())
    state = {"current_exit_plan": plan, "original_exit_plan": plan, "intent_confidence": "PARTIAL", "changed_since_entry": []}

    assert evaluate_exit_review_row_v1(trade=_trade(), intent_state=state, day_utc="2026-05-22")["exit_decision"] == "HOLD"
    assert evaluate_exit_review_row_v1(trade=_trade(current_mark=35.49), intent_state=state, day_utc="2026-05-22")["exit_decision"] == "EXIT_FULL"
    assert evaluate_exit_review_row_v1(trade=_trade(current_mark=36.63), intent_state=state, day_utc="2026-05-22")["exit_decision"] == "TAKE_PARTIAL"
    assert evaluate_exit_review_row_v1(trade=_trade(current_mark=None), intent_state=state, day_utc="2026-05-22")["exit_decision"] == "BLOCK"

    timed = {**plan, "time_stop_at": "2026-05-22"}
    assert evaluate_exit_review_row_v1(trade=_trade(), intent_state={**state, "current_exit_plan": timed}, day_utc="2026-05-22")["exit_decision"] == "REVIEW"


def test_thesis_invalidation_emits_exit_full_or_review() -> None:
    plan = {**backfilled_exit_plan_from_trade_v1(_trade()), "thesis_status": "INVALIDATED"}
    state = {"current_exit_plan": plan, "original_exit_plan": plan, "intent_confidence": "PARTIAL", "changed_since_entry": ["thesis_status"]}
    row = evaluate_exit_review_row_v1(trade=_trade(), intent_state=state, day_utc="2026-05-22")

    assert row["exit_decision"] == "EXIT_FULL"
    assert "Thesis invalidation" in row["decision_reason"]


def test_backfill_command_is_manual_safe_and_refreshes_exit_projection(tmp_path: Path, monkeypatch) -> None:
    import ops.aegis.trade_lifecycle.trade_evaluation_projection_v1 as trade_projection_module
    import ops.aegis.trade_lifecycle.daily_exit_review_v1 as daily_module
    import ops.aegis.trade_lifecycle.exit_review_projection_v1 as exit_module

    fake_projection = {
        "content_hash": "fake-trade-projection",
        "trade_lifecycle_ledger": {"content_hash": "fake-ledger"},
        "open_trades": [_trade()],
        "closed_trades": [],
        "all_trades": [_trade()],
    }
    monkeypatch.setattr(trade_projection_module, "build_paper_trade_evaluation_projection_v1", lambda **_: fake_projection)
    monkeypatch.setattr(daily_module, "build_paper_trade_evaluation_projection_v1", lambda **_: fake_projection)
    monkeypatch.setattr(exit_module, "build_paper_trade_evaluation_projection_v1", lambda **_: fake_projection)

    result = execute_aegis_command_v1(
        {
            "command_id": "GENERATE_BACKFILLED_EXIT_PLAN",
            "target_type": "exit_review_position",
            "target_id": "ticket_02200087bcdf9839aacb8958",
            "payload": {"position_id": "ticket_02200087bcdf9839aacb8958"},
        },
        truth_root=tmp_path,
        day_utc="2026-05-22",
    )

    assert result["ok"] is True
    assert result["result_status"] in {"BACKFILLED_INTENT_CREATED", "ALREADY_EXISTS"}
    assert result["command_result"]["status_label"] == "Backfilled exit plan ready"
    assert result["command_result"]["result_status"] == result["result_status"]


def test_exit_intent_commands_registered_and_manual_only() -> None:
    commands = command_registry_v1()["commands_by_id"]
    for command_id in [
        "GENERATE_BACKFILLED_EXIT_PLAN",
        "REVIEW_EXIT_INTENT",
        "UPDATE_STOP_PLAN",
        "UPDATE_TARGET_PLAN",
        "RECORD_PARTIAL_EXIT",
        "RECORD_FULL_EXIT",
        "RECORD_TRADE_OUTCOME",
        "VIEW_EXIT_HISTORY",
    ]:
        command = commands[command_id]
        assert command["safety_classification"]["broker_submit_transmit_allowed"] is False
        assert command["safety_classification"]["autonomous_execution_allowed"] is False
        assert command["safety_classification"]["trade_advice_allowed"] is False


def test_exit_review_ui_shows_original_current_plan_and_history_commands() -> None:
    pages = pages_source_v1(ROOT)
    css = (ROOT / "constellation_2/phaseL/ui/static/aegis.css").read_text(encoding="utf-8")

    assert "Original Exit Plan" in pages
    assert "Current Exit Plan" in pages
    assert "What changed since entry" in pages
    assert "Generate Backfilled Exit Plan" in pages
    assert "VIEW_EXIT_HISTORY" in pages
    assert "UPDATE_TARGET_PLAN" in pages
    assert ".exit-plan-comparison-grid" in css
