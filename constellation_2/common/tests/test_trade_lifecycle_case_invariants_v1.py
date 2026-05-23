from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from constellation_2.common.tests.test_paper_trade_construction_invariants_v1 import DAY, INTENT_ID, _prepare_submit_boundary, _seed, _seed_runtime
from ops.aegis.operator_state.canonical_operator_state_builder_v1 import build_operator_state_snapshot_v1
from ops.aegis.operator_state.manual_capture_record_v1 import append_manual_capture_record_v1
from ops.aegis.trade_lifecycle.paper_trade_construction_v1 import build_paper_trade_construction_v1
from ops.aegis.trade_lifecycle.trade_case_projection_v1 import trade_case_projection_v1
from ops.aegis.trade_lifecycle.trade_lifecycle_case_v1 import build_and_write_trade_lifecycle_case_v1


def test_selected_exposure_opens_trade_lifecycle_case(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root)
    case, _path = build_and_write_trade_lifecycle_case_v1(truth_root=root, day_utc=DAY)

    assert case["schema_id"] == "trade_lifecycle_case"
    assert case["selected_exposure_intent_id"] == INTENT_ID
    assert case["trade_lifecycle_case_id"]
    assert (root / "reports" / "trade_lifecycle_case_v1" / DAY / "trade_lifecycle_case.v1.json").exists()


def test_incomplete_construction_moves_case_to_construction_blocked(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_runtime(root)
    _seed(root, stop_loss_bps=None, engine_id="C2_NO_STOP_POLICY_V1")
    policy_path = root / "config" / "paper_trade_construction_policy.v1.json"
    policy_path.write_text('{"schema_id":"paper_trade_construction_policy","schema_version":"v1","enabled":true,"paper_test_capital_base":"100000","max_notional_per_trade":"5000","max_risk_per_trade":"1000","entry_reference_rule":"latest_close","quantity_rounding_rule":"floor","default_stop_policy":{}}\n', encoding="utf-8")
    case, _path = build_and_write_trade_lifecycle_case_v1(truth_root=root, day_utc=DAY)

    assert case["current_state"] == "CONSTRUCTION_BLOCKED"
    assert "MISSING_STOP_POLICY" in case["blocker_codes"]


def test_complete_construction_moves_case_to_capture_ready(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root)
    case, _path = build_and_write_trade_lifecycle_case_v1(truth_root=root, day_utc=DAY)
    projection = trade_case_projection_v1(case)

    assert case["current_state"] == "CAPTURE_READY"
    assert projection["capture_ready"] is True
    assert projection["entry_reference_price"] == "180.25"
    assert projection["suggested_quantity"] == 3
    assert projection["stop_price"] == "162.23"
    assert projection["max_loss_estimate"] == "54.06"


def test_cannot_capture_manually_unless_case_capture_ready(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, stop_loss_bps=None, engine_id="C2_NO_STOP_POLICY_V1")
    policy_path = root / "config" / "paper_trade_construction_policy.v1.json"
    policy_path.write_text('{"schema_id":"paper_trade_construction_policy","schema_version":"v1","enabled":true,"paper_test_capital_base":"100000","max_notional_per_trade":"5000","max_risk_per_trade":"1000","entry_reference_rule":"latest_close","quantity_rounding_rule":"floor","default_stop_policy":{}}\n', encoding="utf-8")
    case, _path = build_and_write_trade_lifecycle_case_v1(truth_root=root, day_utc=DAY)

    try:
        append_manual_capture_record_v1(
            truth_root=root,
            day_utc=DAY,
            request_payload={
                "trade_lifecycle_case_id": case["trade_lifecycle_case_id"],
                "paper_trade_construction_id": case["paper_trade_construction_id"],
                "selected_exposure_intent_id": INTENT_ID,
                "capture_status": "captured_manually",
                "quantity": "3",
                "fill_price": "180.50",
                "fill_time": f"{DAY}T15:30:00Z",
                "stop_price": "162.23",
                "operator_id": "test",
            },
        )
    except ValueError as exc:
        assert "NOT_CAPTURE_READY" in str(exc)
    else:
        raise AssertionError("captured_manually succeeded for a blocked case")


def test_missing_stop_and_quantity_block_capture_ready(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, authorized_quantity=None, stop_loss_bps=None, enable_defaults=True, engine_id="C2_NO_STOP_POLICY_V1")
    policy_path = root / "config" / "paper_trade_construction_policy.v1.json"
    policy = __import__("json").loads(policy_path.read_text(encoding="utf-8"))
    policy["paper_test_capital_base"] = ""
    policy["default_stop_policy"] = {}
    policy_path.write_text(__import__("json").dumps(policy, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    case, _path = build_and_write_trade_lifecycle_case_v1(truth_root=root, day_utc=DAY)

    assert case["current_state"] == "CONSTRUCTION_BLOCKED"
    assert "MISSING_SUGGESTED_QUANTITY" in case["blocker_codes"]
    assert "MISSING_STOP_POLICY" in case["blocker_codes"]


def test_stale_market_data_blocks_construction_case(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, observed_session="2026-05-19")
    case, _path = build_and_write_trade_lifecycle_case_v1(truth_root=root, day_utc=DAY)

    assert case["current_state"] == "CONSTRUCTION_BLOCKED"
    assert "MISSING_CURRENT_MARKET_DATA" in case["blocker_codes"]


def test_complete_case_allows_manual_capture_record_and_transitions(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_runtime(root)
    _seed(root)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)
    _prepare_submit_boundary(root, construction)
    case, _path = build_and_write_trade_lifecycle_case_v1(truth_root=root, day_utc=DAY)
    record = append_manual_capture_record_v1(
        truth_root=root,
        day_utc=DAY,
        request_payload={
            "trade_lifecycle_case_id": case["trade_lifecycle_case_id"],
            "paper_trade_construction_id": case["paper_trade_construction_id"],
            "selected_exposure_intent_id": INTENT_ID,
            "capture_status": "captured_manually",
            "quantity": "3",
            "fill_price": "180.50",
            "fill_time": f"{DAY}T15:30:00Z",
            "stop_price": "162.23",
            "operator_id": "test",
        },
    )
    updated, _path = build_and_write_trade_lifecycle_case_v1(truth_root=root, day_utc=DAY)

    assert record["trade_lifecycle_case_id"] == case["trade_lifecycle_case_id"]
    assert record["capture_status"] == "captured_manually"
    assert updated["current_state"] == "CAPTURED_HISTORICAL"
    assert updated["manual_capture_record_ids"]


def test_operator_snapshot_and_ui_render_case_projection_only(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root)
    snapshot = build_operator_state_snapshot_v1(truth_root=root, day_utc=DAY)
    pages = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")

    assert snapshot["trade_lifecycle_case_v1"]["current_state"] == "CAPTURE_READY"
    assert snapshot["trade_ticket_projection_v1"]["capture_ready"] is True
    assert "trade_ticket_projection_v1" in pages
    assert "Trade Lifecycle Case / Manual Capture" in pages
    assert 'name="trade_lifecycle_case_id"' in pages
    assert "Entry:" in pages and "Qty:" in pages and "Stop:" in pages and "Risk:" in pages


def test_stale_qqq_cannot_override_current_case(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, stale_qqq_converter=True)
    case, _path = build_and_write_trade_lifecycle_case_v1(truth_root=root, day_utc=DAY)
    construction = build_paper_trade_construction_v1(truth_root=root, day_utc=DAY)

    assert case["symbol"] == "AMT"
    assert construction["symbol"] == "AMT"
    assert case["selected_exposure_intent_id"] == INTENT_ID


def test_api_routes_and_no_broker_order_allocation_side_effects(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root)
    case, _path = build_and_write_trade_lifecycle_case_v1(truth_root=root, day_utc=DAY)
    server = (ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py").read_text(encoding="utf-8")

    assert "/api/aegis/trade-cases/latest" in server
    assert "/api/aegis/trade-cases/" in server
    assert "/api/aegis/trade-case-projection/latest" in server
    assert "/api/aegis/trade-readiness/latest" in server
    assert "/api/aegis/trade-ticket/latest" in server
    assert case["broker_execution_allowed"] is False
    assert case["order_routing_allowed"] is False
    assert case["capital_allocation_allowed"] is False
    assert case["paper_submit_created"] is False
    assert not (root / "reports" / "exposure_intent_paper_submit_v1").exists()
