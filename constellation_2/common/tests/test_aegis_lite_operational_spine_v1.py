from __future__ import annotations

import json
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_lite_operating_status_v1 import build_aegis_lite_operating_status_v1  # noqa: E402
from constellation_2.common.aegis_lite_manual_feedback_v1 import (  # noqa: E402
    build_manual_execution_event_v1,
    build_manual_operator_decision_v1,
    build_portfolio_position_snapshot_v1,
    build_protective_order_snapshot_v1,
    validate_manual_feedback_artifact_v1,
    write_manual_feedback_artifact_v1,
)
from constellation_2.common.aegis_lite_promoted_candidates_v1 import (  # noqa: E402
    build_demo_candidate_input_v1,
    build_demo_promoted_sleeve_library_v1,
)
from constellation_2.phaseL.ui_api.aegis_lite_execution_queue_read_model import (  # noqa: E402
    build_aegis_lite_execution_queue_view,
)
from ops.tools.run_aegis_lite_eod_pipeline_v1 import (  # noqa: E402
    apply_nyse_trading_day_gate_v1,
    build_aegis_lite_eod_pipeline_v1,
    filter_promoted_sleeve_candidates_v1,
    prepare_operational_input_payload_v1,
)


DAY = "2026-05-14"
GENERATED = "2026-05-14T19:35:00Z"


def _candidate(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "candidate_id": "trend-spy",
        "sleeve_id": "C2_TREND_EQ_PRIMARY",
        "source_hypothesis_id": "rh-trend-spy",
        "symbol": "SPY",
        "direction": "LONG",
        "instrument_type": "LONG_EQUITY",
        "entry_reference_price": "520.10",
        "suggested_quantity": 1,
        "sizing_guidance": "Buy 1 share if manual review accepts the stop risk.",
        "stop_price": "514.90",
        "stop_logic": "STOP_BASED:100bps below entry reference",
        "risk_per_trade": "5.20",
        "sleeve_ownership": "C2_TREND_EQ_PRIMARY",
        "confidence": "MEDIUM",
        "conviction": "MEDIUM",
        "reason_codes": ["TREND_SIGNAL_CONFIRMED"],
        "edge_family": "TREND_CONTINUATION",
        "thesis_id": "SPY_TREND_20260514",
        "shared_risk_tags": ["US_EQUITY_BETA"],
        "correlated_symbols": [],
        "regime_dependency": "RISK_ON",
        "macro_sensitivity": "RATES",
        "volatility_liquidity_dependency": "NORMAL_LIQUIDITY",
    }
    base.update(overrides)
    return base


def _input(candidates: list[dict[str, object]]) -> dict[str, object]:
    return {
        "candidates": candidates,
        "data_freshness_status": {"status": "PASS", "reason_codes": []},
        "governance_status": {"status": "PASS", "reason_codes": []},
        "market_regime_state": {"status": "RISK_ON", "reason_codes": []},
        "promoted_sleeve_library": {
            "sleeves": [
                {
                    "sleeve_id": "C2_TREND_EQ_PRIMARY",
                    "source_hypothesis_id": "rh-trend-spy",
                    "research_hypothesis_id": "rh-trend-spy",
                    "promotion_status": "promoted",
                }
            ]
        },
    }


def _run_pipeline(tmp_path: Path, candidates: list[dict[str, object]], *, run_id: str = "test-lite-spine") -> dict[str, object]:
    return build_aegis_lite_eod_pipeline_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        run_id=run_id,
        generated_at_utc=GENERATED,
        input_payload=_input(candidates),
    )


def _force_release_match(tmp_path: Path) -> None:
    status_path = tmp_path / "reports" / "aegis_lite_operating_status_v1" / DAY / "aegis_lite_operating_status.v1.json"
    status = json.loads(status_path.read_text(encoding="utf-8"))
    status["release_repo_match_status"] = "MATCH"
    status["working_repo_changes_inactive"] = False
    status["current_blockers"] = [item for item in status.get("current_blockers", []) if item != "ACTIVE_RELEASE_REPO_MISMATCH"]
    status["warnings"] = [item for item in status.get("warnings", []) if item != "ACTIVE_RELEASE_REPO_MISMATCH"]
    status["readiness_classification"] = "READY_FOR_SUPERVISED_MANUAL_PAPER_TRADING"
    status_path.write_text(json.dumps(status), encoding="utf-8")


def _force_release_mismatch(tmp_path: Path) -> None:
    status_path = tmp_path / "reports" / "aegis_lite_operating_status_v1" / DAY / "aegis_lite_operating_status.v1.json"
    status = json.loads(status_path.read_text(encoding="utf-8"))
    status["release_repo_match_status"] = "MISMATCH"
    status["working_repo_changes_inactive"] = True
    status["current_blockers"] = [*status.get("current_blockers", []), "ACTIVE_RELEASE_REPO_MISMATCH"]
    status["warnings"] = [*status.get("warnings", []), "ACTIVE_RELEASE_REPO_MISMATCH"]
    status["readiness_classification"] = "ADVISORY_ONLY"
    status_path.write_text(json.dumps(status), encoding="utf-8")


def test_current_lite_report_missing_is_not_ready(tmp_path: Path) -> None:
    view = build_aegis_lite_execution_queue_view(DAY, tmp_path)

    assert view["manual_execution_status"] == "NOT_READY"
    assert view["readiness_classification"] == "NOT_READY"
    assert "NO_CURRENT_LITE_REPORT" in view["current_blockers"]
    assert view["executable_trades"] == []


def test_current_lite_queue_missing_is_not_ready(tmp_path: Path) -> None:
    report = _run_pipeline(tmp_path, [_candidate()])
    status_path = Path(str(report["run_receipt"]["aegis_lite_operating_status_path"]))
    status = json.loads(status_path.read_text(encoding="utf-8"))
    Path(status["lite_eod_latest_queue_path"]).unlink()

    view = build_aegis_lite_execution_queue_view(DAY, tmp_path)

    assert view["manual_execution_status"] == "NOT_READY"
    assert "NO_CURRENT_LITE_QUEUE" in view["current_blockers"]
    assert view["executable_trades"] == []


def test_ui_uses_current_lite_queue_not_stale_operator_state(tmp_path: Path) -> None:
    control_plane = tmp_path / "control_plane"
    control_plane.mkdir(parents=True)
    (control_plane / "aegis_operator_state.v1.json").write_text(
        json.dumps({"generated_at_utc": "2026-04-30T20:00:00Z", "selected_trade": {"symbol": "AAPL"}}),
        encoding="utf-8",
    )
    _run_pipeline(tmp_path, [_candidate(symbol="SPY")])
    _force_release_match(tmp_path)

    view = build_aegis_lite_execution_queue_view(DAY, tmp_path)

    assert view["legacy_operator_state"]["diagnostic_only"] is True
    assert view["executable_trades"][0]["symbol"] == "SPY"


def test_blocked_candidate_cannot_appear_in_executable_queue(tmp_path: Path) -> None:
    _run_pipeline(tmp_path, [_candidate(stop_price="", stop_logic="")])

    view = build_aegis_lite_execution_queue_view(DAY, tmp_path)

    assert view["executable_trades"] == []
    assert view["blocked_or_advisory_trades"]
    assert "STOP_RISK_MISSING" in view["blocked_or_advisory_trades"][0]["do_not_trade_blockers"]


def test_lite_timer_exists_at_1550_et_and_legacy_paper_timers_are_deferred() -> None:
    lite_timer = (REPO_ROOT / "ops/systemd/user/aegis-lite-eod-report-v1.timer").read_text(encoding="utf-8")
    lite_service = (REPO_ROOT / "ops/systemd/user/aegis-lite-eod-report-v1.service").read_text(encoding="utf-8")
    assert "OnCalendar=Mon..Fri *-*-* 15:50:00 America/New_York" in lite_timer
    assert "15:35:00 America/New_York" not in lite_timer
    assert "--manual-only" in lite_service
    assert "no broker submit" in lite_service.lower()

    status = build_aegis_lite_operating_status_v1(
        day_utc=DAY,
        generated_at_utc=GENERATED,
        truth_root=Path("/tmp/aegis-lite-timer-test"),
        repo_head_commit="repo",
        active_release_commit="repo",
    )
    assert status["lite_eod_timer_status"]["target_time_et"] == "15:50"
    assert status["lite_eod_timer_status"]["calendar"] == "Mon..Fri *-*-* 15:50:00 America/New_York"
    assert status["lite_eod_timer_status"]["status"] == "CONFIGURED"

    for name in [
        "c2-paper-day-orchestrator",
        "aegis-paper-ready-kernel-v1",
        "aegis-paper-ready-kernel-v1-after-orchestrator",
        "c2-paper-auto-repair-controller",
        "c2-paper-auto-repair-eod-final",
    ]:
        timer_text = (REPO_ROOT / f"ops/systemd/user/{name}.timer").read_text(encoding="utf-8")
        service_text = (REPO_ROOT / f"ops/systemd/user/{name}.service").read_text(encoding="utf-8")
        assert "LEGACY_PAPER_AUTOMATION_DEFERRED" in timer_text
        assert "ConditionEnvironment=AEGIS_ENABLE_LEGACY_PAPER_AUTOMATION=1" in service_text


def test_lite_producer_calendar_gate_blocks_non_nyse_trading_day(tmp_path: Path) -> None:
    calendar = tmp_path / "market_calendar_v1" / "NYSE" / "2026.jsonl"
    calendar.parent.mkdir(parents=True)
    calendar.write_text(json.dumps({"day_utc": DAY, "exchange": "NYSE", "is_trading_session": False}) + "\n", encoding="utf-8")

    payload = apply_nyse_trading_day_gate_v1(input_payload=_input([_candidate()]), truth_root=tmp_path, day_utc=DAY)

    assert payload["candidates"] == []
    assert "NYSE_NON_TRADING_DAY" in payload["data_freshness_status"]["reason_codes"]


def test_lite_producer_does_not_require_ib_state_or_submit_orders(tmp_path: Path) -> None:
    report = _run_pipeline(tmp_path, [_candidate()])

    assert report["run_receipt"]["ib_submit_automation_invoked"] is False
    assert report["run_receipt"]["broker_transmit_control_touched"] is False
    assert report["operating_model"]["broker_submit_required"] is False
    assert report["operating_model"]["autonomous_order_routing_allowed"] is False


def test_eod_pipeline_writes_complete_fail_closed_manual_trade_packet(tmp_path: Path) -> None:
    _run_pipeline(tmp_path, [_candidate()])

    packet_path = next((tmp_path / "reports" / "manual_trade_packet_v1").rglob("manual_trade_packet.v1.json"))
    packet = json.loads(packet_path.read_text(encoding="utf-8"))
    trade = packet["trade_candidates"][0]

    assert packet["manual_execution_only"] is True
    assert packet["broker_submit_required"] is False
    assert trade["run_id"] == "test-lite-spine"
    assert trade["date"] == DAY
    assert trade["sleeve_id"] == "C2_TREND_EQ_PRIMARY"
    assert trade["source_hypothesis_id"] == "rh-trend-spy"
    assert trade["symbol"] == "SPY"
    assert trade["side"] == "BUY"
    assert trade["instrument_type"] == "LONG_EQUITY"
    assert trade["entry_reference_price"] == "520.10"
    assert trade["quantity_or_sizing_guidance"]
    assert trade["stop_price"] == "514.90"
    assert trade["stop_logic"]
    assert trade["risk_per_trade"] == "5.20"
    assert trade["confidence"] == "MEDIUM"
    assert trade["manual_execution_checklist"]
    assert trade["actionable"] is True
    assert trade["do_not_trade_blockers"] == []

    _run_pipeline(tmp_path, [_candidate(stop_price="", stop_logic="")], run_id="blocked-packet")
    blocked_path = next((tmp_path / "reports" / "manual_trade_packet_v1" / DAY / "blocked-packet").rglob("manual_trade_packet.v1.json"))
    blocked_trade = json.loads(blocked_path.read_text(encoding="utf-8"))["trade_candidates"][0]
    assert blocked_trade["actionable"] is False
    assert "MISSING_STOP_PRICE" in blocked_trade["do_not_trade_blockers"]
    assert "MISSING_STOP_LOGIC" in blocked_trade["do_not_trade_blockers"]


def test_unpromoted_research_candidates_cannot_enter_operational_lite_report(tmp_path: Path) -> None:
    candidates_path = tmp_path / "candidates.json"
    library_path = tmp_path / "promoted.json"
    candidates_path.write_text(json.dumps(_input([_candidate(sleeve_id="RESEARCH_ONLY")])), encoding="utf-8")
    library_path.write_text(
        json.dumps(
            {
                "sleeves": [
                    {
                        "sleeve_id": "C2_TREND_EQ_PRIMARY",
                        "promotion_status": "promoted",
                        "human_approval_status": "approved",
                        "implementation_status": "approved",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    payload = prepare_operational_input_payload_v1(
        candidate_input_path=str(candidates_path),
        promoted_sleeve_library_path=str(library_path),
        manual_only=True,
    )
    report = build_aegis_lite_eod_pipeline_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        run_id="test-unpromoted",
        generated_at_utc=GENERATED,
        input_payload=payload,
    )

    assert report["selected_trade_candidates"] == []
    assert report["readiness_classification"] == "ADVISORY_ONLY"
    assert "REPORT_COMPLETENESS:NO_TRADE_CANDIDATES" in report["do_not_trade_blockers"]
    assert "GOVERNANCE_INTEGRITY:NO_PROMOTED_EXECUTABLE_CANDIDATES" in report["do_not_trade_blockers"]


def test_promoted_candidate_can_generate_executable_queue_visible_in_ui(tmp_path: Path) -> None:
    candidates_path = tmp_path / "candidates.json"
    library_path = tmp_path / "promoted.json"
    candidates_path.write_text(json.dumps(_input([_candidate()])), encoding="utf-8")
    library_path.write_text(
        json.dumps(
            {
                "promoted_sleeves": [
                    {
                        "sleeve_id": "C2_TREND_EQ_PRIMARY",
                        "promotion_status": "promoted",
                        "approved_by_human": True,
                        "approved_for_lite_implementation": True,
                        "archived": False,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    payload = prepare_operational_input_payload_v1(
        candidate_input_path=str(candidates_path),
        promoted_sleeve_library_path=str(library_path),
        manual_only=True,
    )
    report = build_aegis_lite_eod_pipeline_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        run_id="test-promoted",
        generated_at_utc=GENERATED,
        input_payload=payload,
    )
    _force_release_match_for_run(tmp_path, "test-promoted")

    view = build_aegis_lite_execution_queue_view(DAY, tmp_path)

    assert report["manual_execution_status"] == "READY_FOR_MANUAL_ENTRY"
    assert view["executable_trades"][0]["symbol"] == "SPY"
    assert view["queue_summary"]["executable_trades_count"] == 1


def test_demo_promoted_candidates_are_visible_as_dry_run_only(tmp_path: Path) -> None:
    library = build_demo_promoted_sleeve_library_v1(generated_at_utc=GENERATED)
    payload = build_demo_candidate_input_v1(generated_at_utc=GENERATED)
    filtered = filter_promoted_sleeve_candidates_v1(payload, library)
    report = build_aegis_lite_eod_pipeline_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        run_id="test-demo",
        generated_at_utc=GENERATED,
        input_payload=filtered,
    )
    _force_release_match_for_run(tmp_path, "test-demo")
    view = build_aegis_lite_execution_queue_view(DAY, tmp_path)

    assert report["selected_trade_candidates"][0]["demo_mode"] is True
    assert "DEMO_ONLY" in view["executable_trades"][0]["execution_confidence_badges"]
    assert "DRY_RUN_ONLY" in view["executable_trades"][0]["execution_confidence_badges"]


def _force_release_match_for_run(tmp_path: Path, run_id: str) -> None:
    status_path = tmp_path / "reports" / "aegis_lite_operating_status_v1" / DAY / "aegis_lite_operating_status.v1.json"
    status = json.loads(status_path.read_text(encoding="utf-8"))
    status["lite_eod_latest_report_path"] = str(tmp_path / "reports" / "aegis_lite_eod_report_v1" / DAY / run_id / "aegis_lite_eod_report.v1.json")
    status["lite_eod_latest_queue_path"] = str(tmp_path / "reports" / "operator_execution_queue_v1" / DAY / run_id / "operator_execution_queue.v1.json")
    status["release_repo_match_status"] = "MATCH"
    status["working_repo_changes_inactive"] = False
    status["readiness_classification"] = "READY_FOR_SUPERVISED_MANUAL_PAPER_TRADING"
    status["current_blockers"] = [item for item in status.get("current_blockers", []) if item != "ACTIVE_RELEASE_REPO_MISMATCH"]
    status["warnings"] = [item for item in status.get("warnings", []) if item != "ACTIVE_RELEASE_REPO_MISMATCH"]
    status_path.write_text(json.dumps(status), encoding="utf-8")


def test_supervised_manual_feedback_artifacts_are_generated(tmp_path: Path) -> None:
    decision = build_manual_operator_decision_v1(
        day_utc=DAY,
        run_id="workflow-proof",
        candidate_id="trend-spy",
        sleeve_id="C2_TREND_EQ_PRIMARY",
        decision="ENTERED",
        decision_time_utc=GENERATED,
    )
    event = build_manual_execution_event_v1(
        day_utc=DAY,
        run_id="workflow-proof",
        candidate_id="trend-spy",
        symbol="SPY",
        direction="LONG",
        instrument_type="LONG_EQUITY",
        side="BUY",
        quantity=1,
        order_type="MKT",
        actual_entry_price="520.20",
        stop_price_entered="514.90",
        stop_order_type="STP",
        execution_time_utc=GENERATED,
    )
    positions = build_portfolio_position_snapshot_v1(
        day_utc=DAY,
        run_id="workflow-proof",
        snapshot_time_utc=GENERATED,
        source="MANUAL",
        open_positions=[
            {
                "position_id": "pos-spy",
                "candidate_id": "trend-spy",
                "sleeve_id": "C2_TREND_EQ_PRIMARY",
                "edge_cluster_id": "EDGE_CLUSTER_001",
                "symbol": "SPY",
                "direction": "LONG",
                "quantity": 1,
                "average_entry_price": "520.20",
                "current_reference_price": "520.20",
                "unrealized_pnl": "0.00",
            }
        ],
        cash="100000",
        net_liquidation="100000",
    )
    protective = build_protective_order_snapshot_v1(
        day_utc=DAY,
        run_id="workflow-proof",
        snapshot_time_utc=GENERATED,
        protective_orders=[
            {
                "snapshot_time_utc": GENERATED,
                "position_id": "pos-spy",
                "candidate_id": "trend-spy",
                "symbol": "SPY",
                "quantity": 1,
                "stop_exists": False,
                "stop_quantity": 0,
            }
        ],
    )
    for artifact in [decision, event, positions, protective]:
        validate_manual_feedback_artifact_v1(artifact)
        path = write_manual_feedback_artifact_v1(truth_root=tmp_path, payload=artifact)
        assert path.exists()
    assert protective["missing_stop_warnings"]


def test_repo_active_release_drift_creates_warning(tmp_path: Path) -> None:
    status = build_aegis_lite_operating_status_v1(
        day_utc=DAY,
        generated_at_utc=GENERATED,
        truth_root=tmp_path,
        repo_head_commit="repo-head",
        active_release_commit="active-release",
    )

    assert status["release_repo_match_status"] == "MISMATCH"
    assert "ACTIVE_RELEASE_REPO_MISMATCH" in status["warnings"]
    assert status["working_repo_changes_inactive"] is True


def test_release_mismatch_downgrades_ui_ready_queue(tmp_path: Path) -> None:
    _run_pipeline(tmp_path, [_candidate()])
    _force_release_mismatch(tmp_path)
    view = build_aegis_lite_execution_queue_view(DAY, tmp_path)

    assert view["readiness_classification"] == "ADVISORY_ONLY"
    assert view["executable_trades"] == []
    assert "ACTIVE_RELEASE_REPO_MISMATCH" in view["blocked_or_advisory_trades"][0]["do_not_trade_blockers"]


def test_valid_current_lite_report_classifies_supervised_manual_not_automation(tmp_path: Path) -> None:
    report = _run_pipeline(tmp_path, [_candidate()])
    _force_release_match(tmp_path)
    view = build_aegis_lite_execution_queue_view(DAY, tmp_path)

    assert report["readiness_classification"] == "READY_FOR_SUPERVISED_MANUAL_PAPER_TRADING"
    assert view["readiness_classification"] == "READY_FOR_SUPERVISED_MANUAL_PAPER_TRADING"
    assert view["ib_automation_status"] == "DEFERRED"
    assert report["readiness_classification"] != "READY_FOR_AUTOMATION"
