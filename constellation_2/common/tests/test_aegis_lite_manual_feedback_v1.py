from __future__ import annotations

import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_lite_eod_v1 import (  # noqa: E402
    build_aegis_lite_eod_report_v1,
    build_sleeve_edge_overlap_review_v1,
    validate_aegis_lite_eod_report_v1,
)
from constellation_2.common.aegis_lite_manual_feedback_v1 import (  # noqa: E402
    build_edge_cluster_v1,
    build_manual_execution_event_v1,
    build_manual_operator_decision_v1,
    build_operator_execution_queue_v1,
    build_portfolio_position_snapshot_v1,
    build_protective_order_snapshot_v1,
    build_trade_outcome_attribution_v1,
    validate_manual_feedback_artifact_v1,
)


DAY = "2026-05-14"
RUN_ID = "lite-feedback-test"
NOW = "2026-05-14T21:15:00Z"


def _candidate(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "candidate_id": "trend-spy",
        "sleeve_id": "C2_TREND_EQ_PRIMARY",
        "symbol": "SPY",
        "direction": "LONG",
        "instrument_type": "LONG_EQUITY",
        "entry_reference_price": "520.10",
        "suggested_quantity": 1,
        "sizing_guidance": "Buy 1 share manually.",
        "stop_price": "514.90",
        "stop_logic": "STOP_BASED",
        "risk_per_trade": "5.20",
        "sleeve_ownership": "C2_TREND_EQ_PRIMARY",
        "confidence": "MEDIUM",
        "conviction": "MEDIUM",
        "reason_codes": ["TREND_SIGNAL_CONFIRMED"],
        "governance_notes": ["Manual execution only."],
        "edge_family": "TREND_CONTINUATION",
        "thesis_id": "SPY_TREND_20260514",
        "shared_risk_tags": ["US_EQUITY_BETA"],
        "correlated_symbols": ["QQQ"],
        "regime_dependency": "RISK_ON",
        "macro_sensitivity": "RATES",
        "volatility_liquidity_dependency": "NORMAL_LIQUIDITY",
        "source_artifact_refs": [],
    }
    base.update(overrides)
    return base


def _base_report(tmp_path: Path, candidates: list[dict[str, object]], **feedback: object) -> dict[str, object]:
    overlap = build_sleeve_edge_overlap_review_v1(
        day_utc=DAY,
        run_id=RUN_ID,
        generated_at_utc=NOW,
        candidates=candidates,
    )
    report = build_aegis_lite_eod_report_v1(
        day_utc=DAY,
        run_id=RUN_ID,
        generated_at_utc=NOW,
        truth_root=tmp_path,
        candidates=candidates,
        overlap_review=overlap,
        data_freshness_status={"status": "PASS", "reason_codes": []},
        governance_status={"status": "PASS", "reason_codes": []},
        market_regime_state={"status": "RISK_ON", "reason_codes": []},
        **feedback,
    )
    validate_aegis_lite_eod_report_v1(report)
    return report


def test_manual_decision_does_not_require_broker_submit() -> None:
    decision = build_manual_operator_decision_v1(
        day_utc=DAY,
        run_id=RUN_ID,
        candidate_id="trend-spy",
        sleeve_id="C2_TREND_EQ_PRIMARY",
        decision="ENTERED",
        decision_time_utc=NOW,
    )

    validate_manual_feedback_artifact_v1(decision)
    assert decision["broker_submit_required"] is False
    assert decision["transmit_automation_required"] is False


def test_manual_execution_event_is_observational_only() -> None:
    event = build_manual_execution_event_v1(
        day_utc=DAY,
        run_id=RUN_ID,
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
        execution_time_utc=NOW,
    )

    validate_manual_feedback_artifact_v1(event)
    assert event["observational_only"] is True
    assert event["broker_submit_required"] is False


def test_missing_stop_creates_next_eod_warning(tmp_path: Path) -> None:
    protective = build_protective_order_snapshot_v1(
        day_utc=DAY,
        run_id=RUN_ID,
        snapshot_time_utc=NOW,
        protective_orders=[
            {
                "snapshot_time_utc": NOW,
                "position_id": "pos-spy",
                "candidate_id": "trend-spy",
                "symbol": "SPY",
                "quantity": 1,
                "stop_exists": False,
                "stop_quantity": 0,
            }
        ],
    )
    validate_manual_feedback_artifact_v1(protective)

    report = _base_report(tmp_path, [_candidate()], protective_order_snapshot=protective)
    assert report["missing_stop_warnings"]
    assert any("MISSING_PROTECTIVE_STOP" in warning for warning in report["warnings"])


def test_open_positions_affect_next_day_concentration_review(tmp_path: Path) -> None:
    positions = build_portfolio_position_snapshot_v1(
        day_utc=DAY,
        run_id=RUN_ID,
        snapshot_time_utc=NOW,
        source="MANUAL",
        cash="100000",
        net_liquidation="100500",
        open_positions=[
            {
                "position_id": "pos-spy",
                "candidate_id": "prior-spy",
                "sleeve_id": "C2_TREND_EQ_PRIMARY",
                "edge_cluster_id": "EDGE_CLUSTER_001",
                "symbol": "SPY",
                "direction": "LONG",
                "quantity": 1,
                "average_entry_price": "519.00",
                "current_reference_price": "520.00",
                "unrealized_pnl": "1.00",
            }
        ],
    )
    validate_manual_feedback_artifact_v1(positions)

    report = _base_report(tmp_path, [_candidate()], portfolio_position_snapshot=positions)
    assert report["open_manual_positions"]
    assert report["current_exposure_by_edge_cluster"]
    assert any("OPEN_POSITION_CONCENTRATION" in warning for warning in report["warnings"])


def test_skipped_trades_receive_forward_outcome_analysis(tmp_path: Path) -> None:
    decision = build_manual_operator_decision_v1(
        day_utc=DAY,
        run_id=RUN_ID,
        candidate_id="trend-spy",
        sleeve_id="C2_TREND_EQ_PRIMARY",
        decision="SKIPPED",
        decision_time_utc=NOW,
    )
    attribution = build_trade_outcome_attribution_v1(
        day_utc=DAY,
        run_id=RUN_ID,
        candidates=[_candidate()],
        decisions=[decision],
        market_outcomes={"trend-spy": {"model_forward_returns": ["1D:+20bps"], "skipped_trade_outcome": "WOULD_HAVE_GAINED"}},
    )
    validate_manual_feedback_artifact_v1(attribution)

    report = _base_report(tmp_path, [_candidate()], manual_operator_decisions=[decision], trade_outcome_attribution=attribution)
    assert report["skipped_candidate_tracking"]
    assert report["performance_summary"]["skipped_candidate_count"] == 1


def test_multiple_trades_from_one_sleeve_are_allowed() -> None:
    clusters = build_edge_cluster_v1(
        day_utc=DAY,
        run_id=RUN_ID,
        candidates=[
            _candidate(candidate_id="spy-trend", symbol="SPY", thesis_id="SPY_TREND"),
            _candidate(candidate_id="qqq-trend", symbol="QQQ", thesis_id="QQQ_TREND"),
        ],
    )
    validate_manual_feedback_artifact_v1(clusters)
    assert len(clusters["edge_clusters"]) == 2


def test_multiple_symbols_in_one_edge_cluster_are_grouped() -> None:
    clusters = build_edge_cluster_v1(
        day_utc=DAY,
        run_id=RUN_ID,
        candidates=[
            _candidate(candidate_id="spy-trend", symbol="SPY", thesis_id="BETA_TREND"),
            _candidate(candidate_id="qqq-trend", symbol="QQQ", thesis_id="BETA_TREND"),
        ],
    )

    assert len(clusters["edge_clusters"]) == 1
    assert set(clusters["edge_clusters"][0]["candidates_in_cluster"]) == {"spy-trend", "qqq-trend"}


def test_distinct_edge_clusters_may_all_pass_governance() -> None:
    clusters = build_edge_cluster_v1(
        day_utc=DAY,
        run_id=RUN_ID,
        candidates=[
            _candidate(candidate_id="spy-trend", symbol="SPY", thesis_id="SPY_TREND"),
            _candidate(
                candidate_id="tlt-duration",
                symbol="TLT",
                thesis_id="TLT_DURATION",
                edge_family="DEFENSIVE_DURATION",
                shared_risk_tags=["US_DURATION"],
            ),
        ],
    )

    assert len(clusters["edge_clusters"]) == 2
    assert all(row["governance_recommendation"] == "approve" for row in clusters["edge_clusters"])


def test_unsupported_trade_class_fails_closed() -> None:
    clusters = build_edge_cluster_v1(day_utc=DAY, run_id=RUN_ID, candidates=[_candidate(instrument_type="MULTI_LEG_SPREAD")])
    queue = build_operator_execution_queue_v1(
        day_utc=DAY,
        run_id=RUN_ID,
        candidates=[_candidate(instrument_type="MULTI_LEG_SPREAD")],
        edge_clusters=clusters,
    )
    validate_manual_feedback_artifact_v1(queue)

    assert queue["execution_queue"][0]["queue_status"] == "UNSUPPORTED_MANUAL_EXECUTION"
    assert queue["unsupported_manual_execution_warnings"]


def test_eod_report_remains_manual_execution_first(tmp_path: Path) -> None:
    clusters = build_edge_cluster_v1(day_utc=DAY, run_id=RUN_ID, candidates=[_candidate()])
    queue = build_operator_execution_queue_v1(day_utc=DAY, run_id=RUN_ID, candidates=[_candidate()], edge_clusters=clusters)
    report = _base_report(tmp_path, [_candidate()], edge_cluster=clusters, operator_execution_queue=queue)

    assert report["operating_model"]["manual_execution_only"] is True
    assert report["operating_model"]["broker_submit_required"] is False
    assert report["manual_execution_queue"]
    assert report["edge_clusters"]


def test_unsupported_trade_class_cannot_be_executable(tmp_path: Path) -> None:
    candidate = _candidate(instrument_type="MULTI_LEG_SPREAD")
    clusters = build_edge_cluster_v1(day_utc=DAY, run_id=RUN_ID, candidates=[candidate])
    queue = build_operator_execution_queue_v1(day_utc=DAY, run_id=RUN_ID, candidates=[candidate], edge_clusters=clusters)
    report = _base_report(tmp_path, [candidate], edge_cluster=clusters, operator_execution_queue=queue)

    assert report["selected_trade_candidates"][0]["executable_status"] == "NON_EXECUTABLE"
    assert "UNSUPPORTED_MANUAL_EXECUTION" in report["selected_trade_candidates"][0]["blockers"]
    assert report["manual_execution_status"] == "NOT_READY"


def test_ready_for_manual_entry_requires_operator_queue(tmp_path: Path) -> None:
    report = _base_report(tmp_path, [_candidate()])

    assert report["manual_execution_status"] == "NOT_READY"
    assert "OPERATOR_QUEUE_MISSING" in report["do_not_trade_blockers"]


def test_missing_protective_stop_blocks_readiness(tmp_path: Path) -> None:
    candidate = _candidate()
    clusters = build_edge_cluster_v1(day_utc=DAY, run_id=RUN_ID, candidates=[candidate])
    queue = build_operator_execution_queue_v1(day_utc=DAY, run_id=RUN_ID, candidates=[candidate], edge_clusters=clusters)
    protective = build_protective_order_snapshot_v1(
        day_utc=DAY,
        run_id=RUN_ID,
        snapshot_time_utc=NOW,
        protective_orders=[
            {
                "snapshot_time_utc": NOW,
                "position_id": "pos-spy",
                "candidate_id": "trend-spy",
                "symbol": "SPY",
                "quantity": 1,
                "stop_exists": False,
                "stop_quantity": 0,
            }
        ],
    )
    report = _base_report(
        tmp_path,
        [candidate],
        edge_cluster=clusters,
        operator_execution_queue=queue,
        protective_order_snapshot=protective,
    )

    assert report["manual_execution_status"] == "NOT_READY"
    assert any(item.startswith("MISSING_PROTECTIVE_STOP") for item in report["do_not_trade_blockers"])


def test_unknown_warn_and_malformed_status_block_readiness(tmp_path: Path) -> None:
    candidate = _candidate()
    clusters = build_edge_cluster_v1(day_utc=DAY, run_id=RUN_ID, candidates=[candidate])
    queue = build_operator_execution_queue_v1(day_utc=DAY, run_id=RUN_ID, candidates=[candidate], edge_clusters=clusters)
    overlap = build_sleeve_edge_overlap_review_v1(day_utc=DAY, run_id=RUN_ID, generated_at_utc=NOW, candidates=[candidate])

    for status in ("UNKNOWN", "WARN", "NOT_A_STATUS"):
        report = build_aegis_lite_eod_report_v1(
            day_utc=DAY,
            run_id=f"{RUN_ID}-{status}",
            generated_at_utc=NOW,
            truth_root=tmp_path,
            candidates=[candidate],
            overlap_review=overlap,
            data_freshness_status={"status": status, "reason_codes": []},
            governance_status={"status": "PASS", "reason_codes": []},
            edge_cluster=clusters,
            operator_execution_queue=queue,
        )
        assert report["manual_execution_status"] == "NOT_READY"
        assert next(gate for gate in report["gates"] if gate["gate_id"] == "DATA_INTEGRITY")["status"] == "BLOCKED"


def test_missing_entry_stop_risk_or_quantity_blocks_queue_readiness() -> None:
    cases = [
        _candidate(candidate_id="missing-entry", entry_reference_price=""),
        _candidate(candidate_id="missing-stop", stop_price=""),
        _candidate(candidate_id="missing-risk", risk_per_trade=""),
        _candidate(candidate_id="missing-quantity", suggested_quantity=0),
    ]
    clusters = build_edge_cluster_v1(day_utc=DAY, run_id=RUN_ID, candidates=cases)
    queue = build_operator_execution_queue_v1(day_utc=DAY, run_id=RUN_ID, candidates=cases, edge_clusters=clusters)

    assert all(row["queue_status"] == "BLOCKED" for row in queue["execution_queue"])
    assert all(row["reason_codes"] for row in queue["execution_queue"])


def test_unprotected_open_position_creates_do_not_trade_blocker(tmp_path: Path) -> None:
    candidate = _candidate()
    clusters = build_edge_cluster_v1(day_utc=DAY, run_id=RUN_ID, candidates=[candidate])
    queue = build_operator_execution_queue_v1(day_utc=DAY, run_id=RUN_ID, candidates=[candidate], edge_clusters=clusters)
    positions = build_portfolio_position_snapshot_v1(
        day_utc=DAY,
        run_id=RUN_ID,
        snapshot_time_utc=NOW,
        source="MANUAL",
        cash="100000",
        net_liquidation="100500",
        open_positions=[
            {
                "position_id": "pos-spy",
                "candidate_id": "trend-spy",
                "sleeve_id": "C2_TREND_EQ_PRIMARY",
                "edge_cluster_id": "EDGE_CLUSTER_001",
                "symbol": "SPY",
                "direction": "LONG",
                "quantity": 1,
                "average_entry_price": "519.00",
                "current_reference_price": "520.00",
                "unrealized_pnl": "1.00",
            }
        ],
    )
    report = _base_report(tmp_path, [candidate], edge_cluster=clusters, operator_execution_queue=queue, portfolio_position_snapshot=positions)

    assert "PROTECTIVE_ORDER_SNAPSHOT_MISSING_FOR_OPEN_POSITIONS" in report["do_not_trade_blockers"]
    assert report["manual_execution_status"] == "NOT_READY"


def test_performance_summary_unavailable_without_forward_return_evidence(tmp_path: Path) -> None:
    decision = build_manual_operator_decision_v1(
        day_utc=DAY,
        run_id=RUN_ID,
        candidate_id="trend-spy",
        sleeve_id="C2_TREND_EQ_PRIMARY",
        decision="SKIPPED",
        decision_time_utc=NOW,
    )
    attribution = build_trade_outcome_attribution_v1(day_utc=DAY, run_id=RUN_ID, candidates=[_candidate()], decisions=[decision])
    report = _base_report(tmp_path, [_candidate()], manual_operator_decisions=[decision], trade_outcome_attribution=attribution)

    assert report["performance_summary"]["status"] == "UNAVAILABLE"


def test_edge_clusters_do_not_merge_opposite_direction_candidates() -> None:
    clusters = build_edge_cluster_v1(
        day_utc=DAY,
        run_id=RUN_ID,
        candidates=[
            _candidate(candidate_id="long-spy", direction="LONG", thesis_id="SPY_PAIR"),
            _candidate(candidate_id="short-spy", direction="SHORT", thesis_id="SPY_PAIR"),
        ],
    )

    assert len(clusters["edge_clusters"]) == 2
