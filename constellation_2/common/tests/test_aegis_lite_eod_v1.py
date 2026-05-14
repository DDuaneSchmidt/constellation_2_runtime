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
    validate_sleeve_edge_overlap_review_v1,
)
from constellation_2.common.aegis_lite_manual_feedback_v1 import (  # noqa: E402
    build_edge_cluster_v1,
    build_operator_execution_queue_v1,
)


DAY = "2026-05-14"
RUN_ID = "test-aegis-lite"
GENERATED = "2026-05-14T19:45:00Z"


def _candidate(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "candidate_id": "trend-spy",
        "sleeve_id": "C2_TREND_EQ_PRIMARY",
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
        "governance_notes": ["Manual IB entry only."],
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


def _freshness(status: str = "PASS") -> dict[str, object]:
    return {
        "status": status,
        "as_of_utc": GENERATED,
        "reason_codes": [] if status == "PASS" else ["MARKET_DATA_STALE"],
    }


def _governance(status: str = "PASS") -> dict[str, object]:
    return {
        "status": status,
        "reason_codes": [] if status == "PASS" else ["GOVERNANCE_BLOCKED"],
    }


def _report(tmp_path: Path, candidates: list[dict[str, object]], *, freshness: str = "PASS") -> dict[str, object]:
    overlap = build_sleeve_edge_overlap_review_v1(
        day_utc=DAY,
        run_id=RUN_ID,
        generated_at_utc=GENERATED,
        candidates=candidates,
        source_artifact_lineage=[],
    )
    validate_sleeve_edge_overlap_review_v1(overlap)
    edge_cluster = build_edge_cluster_v1(day_utc=DAY, run_id=RUN_ID, candidates=candidates)
    operator_queue = build_operator_execution_queue_v1(
        day_utc=DAY,
        run_id=RUN_ID,
        candidates=candidates,
        edge_clusters=edge_cluster,
    )
    report = build_aegis_lite_eod_report_v1(
        day_utc=DAY,
        run_id=RUN_ID,
        generated_at_utc=GENERATED,
        truth_root=tmp_path,
        candidates=candidates,
        overlap_review=overlap,
        data_freshness_status=_freshness(freshness),
        governance_status=_governance(),
        market_regime_state={"status": "RISK_ON", "reason_codes": []},
        source_artifact_lineage=[],
        edge_cluster=edge_cluster,
        operator_execution_queue=operator_queue,
    )
    validate_aegis_lite_eod_report_v1(report)
    return report


def test_multiple_distinct_sleeve_edges_are_all_reported(tmp_path: Path) -> None:
    candidates = [
        _candidate(candidate_id="trend-spy", sleeve_id="C2_TREND_EQ_PRIMARY", symbol="SPY"),
        _candidate(
            candidate_id="defensive-tlt",
            sleeve_id="C2_DEFENSIVE_TAIL",
            symbol="TLT",
            edge_family="DEFENSIVE_DURATION",
            thesis_id="TLT_DURATION_20260514",
            shared_risk_tags=["US_DURATION"],
            correlated_symbols=["IEF"],
        ),
    ]
    report = _report(tmp_path, candidates)

    assert report["edge_overlap_summary"]["distinct_edge_count"] == 2
    assert len(report["selected_trade_candidates"]) == 2
    assert report["manual_execution_status"] == "READY_FOR_MANUAL_ENTRY"
    assert report["operating_model"]["ib_automation_status"] == "DEFERRED"
    assert report["operating_model"]["broker_submit_required"] is False


def test_multiple_sleeves_same_edge_are_not_rejected_by_count_but_are_reviewed(tmp_path: Path) -> None:
    candidates = [
        _candidate(candidate_id="trend-spy", sleeve_id="C2_TREND_EQ_PRIMARY"),
        _candidate(candidate_id="mr-spy", sleeve_id="C2_MEAN_REVERSION_EQ", thesis_id="SPY_TREND_20260514"),
    ]
    report = _report(tmp_path, candidates)

    assert len(report["selected_trade_candidates"]) == 2
    assert report["edge_overlap_summary"]["distinct_edge_count"] == 1
    assert report["edge_overlap_summary"]["duplicate_thesis_groups"]
    recommendations = {row["governance_adjusted_status"] for row in report["selected_trade_candidates"]}
    assert "prefer_best_candidate_in_group" in recommendations
    assert "DUPLICATE_THESIS_DETECTED" in report["warnings"]


def test_duplicate_thesis_detection_sets_candidate_flag(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        [
            _candidate(candidate_id="a"),
            _candidate(candidate_id="b", sleeve_id="C2_EVENT_DISLOCATION", thesis_id="SPY_TREND_20260514"),
        ],
    )

    assert all(candidate["edge_overlap"]["duplicate_thesis_flag"] for candidate in report["selected_trade_candidates"])


def test_concentration_warning_is_candidate_level(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        [
            _candidate(candidate_id="a"),
            _candidate(
                candidate_id="b",
                sleeve_id="C2_EVENT_DISLOCATION",
                thesis_id="SPY_EVENT_20260514",
                edge_family="EVENT_DISLOCATION",
            ),
        ],
    )

    assert report["edge_overlap_summary"]["portfolio_concentration_warnings"]
    assert any("SYMBOL_CONCENTRATION_WARNING" in item for item in report["warnings"])


def test_missing_stop_marks_candidate_non_executable_and_report_not_ready(tmp_path: Path) -> None:
    report = _report(tmp_path, [_candidate(stop_price="", stop_logic="")])

    candidate = report["selected_trade_candidates"][0]
    assert candidate["executable_status"] == "NON_EXECUTABLE"
    assert "STOP_RISK_MISSING" in candidate["blockers"]
    assert report["report_status"] == "BLOCKED"
    assert report["manual_execution_status"] == "NOT_READY"


def test_missing_entry_reference_blocks_executable_report_status(tmp_path: Path) -> None:
    report = _report(tmp_path, [_candidate(entry_reference_price="")])

    candidate = report["selected_trade_candidates"][0]
    assert "ENTRY_REFERENCE_MISSING" in candidate["blockers"]
    assert report["manual_execution_status"] == "NOT_READY"


def test_stale_market_data_blocks_data_integrity_gate(tmp_path: Path) -> None:
    report = _report(tmp_path, [_candidate()], freshness="STALE")

    data_gate = next(gate for gate in report["gates"] if gate["gate_id"] == "DATA_INTEGRITY")
    assert data_gate["status"] == "BLOCKED"
    assert report["report_status"] == "BLOCKED"


def test_report_includes_complete_manual_execution_checklist(tmp_path: Path) -> None:
    report = _report(tmp_path, [_candidate()])

    checklist_ids = {item["check_id"] for item in report["manual_execution_checklist"]}
    assert {"CHECK_SYMBOL", "CHECK_ENTRY", "CHECK_STOP", "CAPTURE_MANUAL_FILL", "NO_AEGIS_TRANSMIT"}.issubset(
        checklist_ids
    )


def test_old_only_one_sleeve_assumption_is_not_enforced(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        [
            _candidate(candidate_id="a", symbol="SPY"),
            _candidate(candidate_id="b", sleeve_id="C2_DEFENSIVE_TAIL", symbol="TLT", thesis_id="TLT_DURATION"),
            _candidate(candidate_id="c", sleeve_id="C2_EVENT_DISLOCATION", symbol="XLE", thesis_id="XLE_EVENT"),
        ],
    )

    assert len(report["selected_trade_candidates"]) == 3
    assert "ONLY_ONE_SLEEVE_ALLOWED" not in str(report)


def test_pipeline_writes_overlap_and_eod_artifacts_without_ib_submit(tmp_path: Path) -> None:
    from ops.tools.run_aegis_lite_eod_pipeline_v1 import build_aegis_lite_eod_pipeline_v1

    report = build_aegis_lite_eod_pipeline_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        run_id=RUN_ID,
        generated_at_utc=GENERATED,
        input_payload={
            "candidates": [_candidate()],
            "data_freshness_status": _freshness(),
            "governance_status": _governance(),
            "market_regime_state": {"status": "RISK_ON", "reason_codes": []},
        },
    )

    assert Path(report["artifact_path"]).exists()
    assert Path(report["edge_overlap_review"]["path"]).exists()
    assert report["run_receipt"]["ib_submit_automation_invoked"] is False
    assert report["run_receipt"]["broker_transmit_control_touched"] is False
