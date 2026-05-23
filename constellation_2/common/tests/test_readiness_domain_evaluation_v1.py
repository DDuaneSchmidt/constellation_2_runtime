from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from constellation_2.common.tests.test_paper_trade_construction_invariants_v1 import DAY, INTENT_ID, _seed
from ops.aegis.operator_state.canonical_operator_state_builder_v1 import build_operator_state_snapshot_v1
from ops.aegis.operator_state.manual_capture_record_v1 import append_manual_capture_record_v1
from ops.aegis.trade_lifecycle.trade_lifecycle_case_v1 import build_and_write_trade_lifecycle_case_v1
from ops.aegis.trade_lifecycle.trade_ticket_projection_v1 import trade_ticket_projection_v1


def _statuses(case: dict) -> dict[str, str]:
    return case.get("domain_statuses") if isinstance(case.get("domain_statuses"), dict) else {}


def test_qqq_case_produces_separate_domain_evaluations(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, symbol="QQQ", capital=False, engine_id="C2_CROSS_ASSET_TREND_V1", stop_loss_bps=None)
    case, _path = build_and_write_trade_lifecycle_case_v1(truth_root=root, day_utc=DAY)

    assert case["symbol"] == "QQQ"
    assert set(_statuses(case)) == {"market_data", "trade_construction", "capital_authority", "stop_risk", "manual_capture", "paper_submit", "execution"}
    assert len(case["readiness_domain_evaluations"]) == 7


def test_market_data_ready_when_current_price_exists(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, symbol="QQQ", capital=False)
    case, _path = build_and_write_trade_lifecycle_case_v1(truth_root=root, day_utc=DAY)

    assert _statuses(case)["market_data"] == "READY"


def test_capital_blocked_does_not_block_market_data_domain(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, capital=False)
    case, _path = build_and_write_trade_lifecycle_case_v1(truth_root=root, day_utc=DAY)

    statuses = _statuses(case)
    assert statuses["capital_authority"] == "BLOCKED"
    assert statuses["market_data"] == "READY"


def test_paper_submit_blocked_does_not_block_manual_annotation_when_ticket_complete(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root)
    submit_path = root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json"
    submit_path.unlink()
    case, _path = build_and_write_trade_lifecycle_case_v1(truth_root=root, day_utc=DAY)

    statuses = _statuses(case)
    assert statuses["paper_submit"] == "BLOCKED"
    assert statuses["manual_capture"] == "READY"
    assert case["current_state"] == "CAPTURE_READY"
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
                "operator_id": "operator-test",
            },
        )
    except ValueError as exc:
        payload = json.loads(str(exc))
        assert payload["blocker_code"] == "STALE_RUNTIME"
        assert payload["broker_execution_allowed"] is False
        assert payload["autonomous_execution_allowed"] is False
    else:
        raise AssertionError("manual capture save succeeded without active current ticket lineage")


def test_missing_stop_blocks_stop_risk_and_manual_capture(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, stop_loss_bps=None, engine_id="C2_NO_STOP_POLICY_V1")
    policy_path = root / "config" / "paper_trade_construction_policy.v1.json"
    policy_path.write_text('{"schema_id":"paper_trade_construction_policy","schema_version":"v1","enabled":true,"paper_test_capital_base":"100000","max_notional_per_trade":"5000","max_risk_per_trade":"1000","entry_reference_rule":"latest_close","quantity_rounding_rule":"floor","default_stop_policy":{}}\n', encoding="utf-8")
    case, _path = build_and_write_trade_lifecycle_case_v1(truth_root=root, day_utc=DAY)

    statuses = _statuses(case)
    assert statuses["stop_risk"] == "BLOCKED"
    assert statuses["manual_capture"] == "BLOCKED"


def test_cross_asset_trend_manual_capture_ready_even_when_paper_submit_blocked(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, symbol="QQQ", engine_id="C2_CROSS_ASSET_TREND_V1", stop_loss_bps=None, target_notional_pct="0.10", write_policy=False)
    (root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json").unlink()
    case, _path = build_and_write_trade_lifecycle_case_v1(truth_root=root, day_utc=DAY)
    projection = trade_ticket_projection_v1(case)

    statuses = _statuses(case)
    assert statuses["trade_construction"] == "READY"
    assert statuses["stop_risk"] == "READY"
    assert statuses["manual_capture"] == "READY"
    assert statuses["paper_submit"] == "BLOCKED"
    assert statuses["execution"] == "NOT_ENABLED"
    assert case["current_state"] == "CAPTURE_READY"
    assert projection["manual_capture_ready"] is True
    assert projection["paper_submit_ready"] is False
    assert projection["execution_ready"] is False
    assert projection["stop_price"] == "162.23"
    assert projection["max_loss_estimate"] == "54.06"


def test_manual_capture_ready_fixture_enables_capture(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root)
    case, _path = build_and_write_trade_lifecycle_case_v1(truth_root=root, day_utc=DAY)
    projection = trade_ticket_projection_v1(case)

    assert _statuses(case)["manual_capture"] == "READY"
    assert projection["manual_capture_ready"] is True
    assert "captured_manually" in projection["allowed_actions"]


def test_captured_manually_rejected_unless_manual_capture_domain_ready(tmp_path: Path) -> None:
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
                "operator_id": "operator-test",
            },
        )
    except ValueError as exc:
        payload = json.loads(str(exc))
        assert payload["blocker_code"] == "NOT_CAPTURE_READY"
        assert payload["broker_execution_allowed"] is False
    else:
        raise AssertionError("captured_manually succeeded while ManualCaptureDomain was blocked")


def test_ui_renders_all_domain_statuses_and_required_fields_or_blockers() -> None:
    pages = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")

    for text in ["Market Data", "Construction", "Capital", "Stop/Risk", "Manual Capture", "Paper Submit", "Execution"]:
        assert text in pages
    for text in ["Entry:", "Qty:", "Stop:", "Risk:", "Domain blockers"]:
        assert text in pages
    assert "trade_ticket_projection_v1" in pages
    assert "Broker Submit / Paper Broker Sim readiness" in pages
    assert "ib_api_handshake_v1" in pages
    assert "IB handshake NOT REQUIRED FOR MANUAL CAPTURE" in pages
    assert "Broker submit DISABLED" in pages


def test_execution_domain_not_enabled_by_default(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root)
    case, _path = build_and_write_trade_lifecycle_case_v1(truth_root=root, day_utc=DAY)

    assert _statuses(case)["execution"] == "NOT_ENABLED"
    assert case["execution_ready"] is False


def test_snapshot_contains_trade_ticket_projection_and_no_side_effects(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root)
    snapshot = build_operator_state_snapshot_v1(truth_root=root, day_utc=DAY)

    assert snapshot["trade_ticket_projection_v1"]["schema_id"] == "trade_ticket_projection"
    assert snapshot["trade_ticket_projection_v1"]["manual_capture_ready"] is True
    assert snapshot["trade_ticket_projection_v1"]["execution_ready"] is False
    assert not (root / "reports" / "exposure_intent_paper_submit_v1").exists()
    assert snapshot["trade_ticket_projection_v1"]["broker_execution_allowed"] is False
    assert snapshot["trade_ticket_projection_v1"]["order_routing_allowed"] is False
    assert snapshot["trade_ticket_projection_v1"]["capital_allocation_allowed"] is False


def test_legacy_runtime_manual_capture_blocker_is_quarantined_when_trade_ticket_ready(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root)
    runtime_path = root / "reports" / "aegis_runtime_truth_kernel_v1" / DAY / "runtime_truth_kernel.v1.json"
    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_path.write_text(
        '{"schema_id":"aegis_runtime_truth_kernel","manual_trade_capture_allowed":false,"blocked_capabilities":["MANUAL_TRADE_CAPTURE_ALLOWED"]}\n',
        encoding="utf-8",
    )

    snapshot = build_operator_state_snapshot_v1(truth_root=root, day_utc=DAY)
    quarantine = snapshot["legacy_module_runtime_quarantine"]

    assert snapshot["trade_ticket_projection_v1"]["manual_capture_ready"] is True
    assert quarantine["modules"]["bond_sleeve"]["may_block_manual_capture_readiness"] is False
    assert quarantine["modules"]["advisory"]["status"] == "INACTIVE_FOR_TRADE_LIFECYCLE_READINESS"
    assert quarantine["modules"]["tax"]["may_block_paper_cycle"] is False
    assert quarantine["hidden_blocker_risk_detected"] is True
    assert quarantine["hidden_blocker_risk_quarantined"] is True
    assert quarantine["quarantined_items"][0]["status"] == "QUARANTINED_INACTIVE_FOR_TRADE_LIFECYCLE_V1"
    assert quarantine["active_manual_capture_authority"] == "trade_ticket_projection_v1"


def test_dow_mean_reversion_manual_capture_ready_when_submit_boundary_missing(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(
        root,
        symbol="DOW",
        engine_id="C2_MEAN_REVERSION_EQ_V1",
        stop_loss_bps=None,
        target_notional_pct="0.20",
        max_risk_pct="0.02",
        capital=False,
        write_policy=False,
    )
    from constellation_2.common.tests.test_paper_trade_construction_invariants_v1 import _write_paper_envelope

    _write_paper_envelope(root)
    submit_path = root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json"
    submit_path.unlink()

    case, _path = build_and_write_trade_lifecycle_case_v1(truth_root=root, day_utc=DAY)
    projection = trade_ticket_projection_v1(case)
    statuses = _statuses(case)

    assert statuses["capital_authority"] == "READY"
    assert statuses["stop_risk"] == "READY"
    assert statuses["manual_capture"] == "READY"
    assert statuses["paper_submit"] == "BLOCKED"
    assert case["current_state"] == "CAPTURE_READY"
    assert projection["manual_capture_ready"] is True
    assert projection["paper_submit_ready"] is False
    assert projection["symbol"] == "DOW"
    assert projection["suggested_quantity"] == 33
    assert projection["stop_price"] == "171.24"
    assert projection["max_loss_estimate"] == "297.33"

