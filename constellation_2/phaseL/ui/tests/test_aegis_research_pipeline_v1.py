from __future__ import annotations

import json
from pathlib import Path

import pytest

from ops.aegis.research_lab.event_earnings_data_v1 import (
    event_earnings_data_status_v1,
    import_earnings_event_calendar_csv_v1,
    import_event_window_ohlcv_csv_v1,
)
from ops.aegis.research_lab.hypothesis_priority_engine_v1 import (
    build_hypothesis_priority_report_v1,
    write_hypothesis_priority_report_v1,
)
from ops.aegis.research_lab.research_pipeline_v1 import (
    append_research_review_decision_v1,
    append_triage_record_v1,
    build_research_pipeline_v1,
    build_research_plan_v1,
    run_research_test_v1,
    write_research_plan_v1,
    write_research_test_result_v1,
)
from constellation_2.phaseL.ui.server.run_ops_dashboard_v1 import (
    EdgeLabWorkflowApiError,
    execute_edge_lab_workflow_action_v1,
)


DAY = "2026-05-17"


def _write_hypothesis(root: Path, hypothesis_id: str, *, status: str = "IDEA") -> None:
    path = root.parent / "research_lab" / "research_hypothesis_v1" / "2026-05-15" / hypothesis_id / "research_hypothesis.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "hypothesis_id": hypothesis_id,
        "title": f"{hypothesis_id} title",
        "status": status,
        "source": "CHATGPT_SEED",
        "edge_family": "MEAN_REVERSION",
        "created_at_utc": "2026-05-15T00:00:00Z",
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _column_members(payload: dict, column: str) -> list[str]:
    return [row["hypothesis_id"] for row in payload["pipeline"][column]]


def test_untriaged_idea_hypotheses_appear_once_in_inbox(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _write_hypothesis(truth, "rh-edge-2026-0101")
    _write_hypothesis(truth, "rh-edge-2026-0103")

    payload = build_research_pipeline_v1(truth_root=truth, day_utc=DAY)
    all_ids = [row["hypothesis_id"] for rows in payload["pipeline"].values() for row in rows]

    assert sorted(all_ids) == ["rh-edge-2026-0101", "rh-edge-2026-0103"]
    assert _column_members(payload, "inbox") == ["rh-edge-2026-0101", "rh-edge-2026-0103"]
    for row in payload["pipeline"]["inbox"]:
        assert row["current_gate"] == "INBOX"
        assert row["next_action"] == "Run triage"
        assert "npm run aegis:triage-hypothesis" in row["next_command"]
        assert row["broker_execution_allowed"] is False
        assert row["autonomous_execution_allowed"] is False


def test_pipeline_gate_progression_and_no_skip_gates(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    hypothesis_id = "rh-edge-2026-0101"
    _write_hypothesis(truth, hypothesis_id)

    draft_plan = build_research_plan_v1(truth_root=truth, day_utc=DAY, hypothesis_id=hypothesis_id)
    assert draft_plan["plan_status"] == "DRAFT_BLOCKED_PENDING_TRIAGE"
    assert draft_plan["blocker"] == "TRIAGE_REQUIRED"

    append_triage_record_v1(
        truth_root=truth,
        day_utc=DAY,
        hypothesis_id=hypothesis_id,
        decision="QUEUE_TEST_PLAN",
        reason="Worth testing",
        operator="David",
    )
    payload = build_research_pipeline_v1(truth_root=truth, day_utc=DAY)
    assert _column_members(payload, "test_plan") == [hypothesis_id]

    plan = build_research_plan_v1(truth_root=truth, day_utc=DAY, hypothesis_id=hypothesis_id)
    assert plan["plan_status"] == "READY_FOR_TESTING"
    write_research_plan_v1(truth_root=truth, day_utc=DAY, payload=plan)
    payload = build_research_pipeline_v1(truth_root=truth, day_utc=DAY)
    assert _column_members(payload, "testing") == [hypothesis_id]

    result = run_research_test_v1(truth_root=truth, day_utc=DAY, hypothesis_id=hypothesis_id)
    assert result["test_status"] in {"DATA_NEEDED", "TEST_NOT_IMPLEMENTED"}
    assert result["fabricated_results"] is False
    write_research_test_result_v1(truth_root=truth, day_utc=DAY, payload=result)
    payload = build_research_pipeline_v1(truth_root=truth, day_utc=DAY)
    assert _column_members(payload, "testing") == [hypothesis_id]
    row = payload["pipeline"]["testing"][0]
    assert row["gate_status"] == "BLOCKED"
    assert row["latest_result_status"] == "DATA_NEEDED"
    assert row["broker_execution_allowed"] is False
    assert row["autonomous_execution_allowed"] is False


def test_rejected_and_archived_hypotheses_move_to_rejected_archived(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _write_hypothesis(truth, "rh-edge-2026-0102", status="REJECTED")
    _write_hypothesis(truth, "rh-edge-2026-0104", status="ARCHIVED")

    payload = build_research_pipeline_v1(truth_root=truth, day_utc=DAY)

    assert sorted(_column_members(payload, "rejected_archived")) == ["rh-edge-2026-0102", "rh-edge-2026-0104"]


def test_current_active_hypotheses_get_concrete_research_plans(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _write_hypothesis(truth, "rh-edge-2026-0101")
    _write_hypothesis(truth, "rh-edge-2026-0103")

    plan_0101 = build_research_plan_v1(truth_root=truth, day_utc=DAY, hypothesis_id="rh-edge-2026-0101")
    plan_0103 = build_research_plan_v1(truth_root=truth, day_utc=DAY, hypothesis_id="rh-edge-2026-0103")

    assert plan_0101["test_type"] == "EVENT_STUDY / REGIME_ANALYSIS"
    assert "1-3 session forward returns" in plan_0101["test_question"]
    assert "average_1d_forward_return" in plan_0101["required_outputs"]
    assert "average_3d_forward_return" in plan_0101["required_outputs"]
    assert plan_0103["test_type"] == "EVENT_STUDY"
    assert "next-session return" in plan_0103["test_question"]
    assert "next_session_average_return" in plan_0103["required_outputs"]


def test_etf_drop_mean_reversion_hypothesis_gets_process_test_plan(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    hypothesis_id = "rh-process-test-etf-drop-mean-reversion-v1"
    path = truth.parent / "research_lab" / "research_hypothesis_v1" / "2026-05-15" / hypothesis_id / "research_hypothesis.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "hypothesis_id": hypothesis_id,
                "title": "ETF mean-reversion after sharp 1-day drop",
                "hypothesis_summary": "Do broad-market ETFs show positive next-day or 3-day mean reversion after unusually large down days when VIX is below extreme-stress levels?",
                "status": "IDEA",
                "source": "MANUAL",
                "edge_family": "MEAN_REVERSION",
                "instruments": ["SPY", "QQQ", "IWM"],
                "created_at_utc": "2026-05-15T00:00:00Z",
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    plan = build_research_plan_v1(truth_root=truth, day_utc=DAY, hypothesis_id=hypothesis_id)

    assert plan["test_type"] == "EVENT_STUDY / REGIME_ANALYSIS"
    assert "VIX" in plan["test_question"]
    assert plan["universe"] == ["SPY", "QQQ", "IWM", "VIX"]
    assert "daily_vix_close_history" in plan["required_inputs"]
    assert "average_3d_forward_return" in plan["required_outputs"]
    assert plan["broker_execution_allowed"] is False
    assert plan["autonomous_execution_allowed"] is False




def _write_nvidia_hypothesis(root: Path) -> str:
    hypothesis_id = "rh-nvidia-earnings-event-dislocation-v1"
    path = root.parent / "research_lab" / "research_hypothesis_v1" / "2026-05-21" / hypothesis_id / "research_hypothesis.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "hypothesis_id": hypothesis_id,
                "title": "NVIDIA Earnings Event Dislocation",
                "hypothesis_summary": "After NVIDIA earnings, do abnormal price gaps, volatility compression/expansion, or follow-through/reversal patterns produce statistically useful short-horizon signals?",
                "status": "IDEA",
                "source": "MANUAL",
                "edge_family": "EVENT_DISLOCATION",
                "event_type": "EARNINGS_RELEASE",
                "instruments": ["NVDA", "SMH", "SOXX", "QQQ", "XLK", "SPY"],
                "missing_datasets": ["NVIDIA historical earnings dates/times", "VIX or implied-volatility proxy"],
                "blocking_items": ["DATA_NEEDED"],
                "created_at_utc": "2026-05-21T00:00:00Z",
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return hypothesis_id


def _write_intraday_market_report(root: Path, day: str = DAY) -> None:
    symbols = ["NVDA", "SMH", "SOXX", "QQQ", "XLK", "SPY", "VIX"]
    report = {
        "schema_id": "aegis_market_data_v1",
        "generated_at_utc": f"{day}T15:00:00Z",
        "day_utc": day,
        "status": "CURRENT",
        "market_data_mode": "INTRADAY_OPERATIONAL",
        "operator_market_data_state": "INTRADAY_OPERATIONAL_READY",
        "provisional_intraday_symbols": symbols,
        "missing_symbols": [],
        "stale_symbols": [],
        "symbols": {
            symbol: {
                "canonical_symbol": symbol,
                "freshness_status": "CURRENT",
                "data_finality": "PROVISIONAL_INTRADAY",
                "market_session_date": day,
                "close": 100.0,
            }
            for symbol in symbols
        },
    }
    path = root / "reports" / "aegis_market_data_v1" / day / "market_data.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")


def _write_etf_drop_market_report(root: Path, day: str = DAY) -> None:
    symbols = ["SPY", "QQQ", "IWM", "VIX"]
    prices = {
        "SPY": {"previous_close": 100.0, "close": 98.0},
        "QQQ": {"previous_close": 100.0, "close": 100.5},
        "IWM": {"previous_close": 100.0, "close": 97.5},
        "VIX": {"previous_close": 18.0, "close": 19.0},
    }
    report = {
        "schema_id": "aegis_market_data_v1",
        "generated_at_utc": f"{day}T15:00:00Z",
        "day_utc": day,
        "status": "CURRENT",
        "market_data_mode": "INTRADAY_OPERATIONAL",
        "operator_market_data_state": "INTRADAY_OPERATIONAL_READY",
        "provisional_intraday_symbols": symbols,
        "missing_symbols": [],
        "stale_symbols": [],
        "symbols": {
            symbol: {
                "canonical_symbol": symbol,
                "freshness_status": "CURRENT",
                "data_finality": "PROVISIONAL_INTRADAY",
                "market_session_date": day,
                "previous_close": values["previous_close"],
                "close": values["close"],
            }
            for symbol, values in prices.items()
        },
    }
    path = root / "reports" / "aegis_market_data_v1" / day / "market_data.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")


def _write_calendar_csv(path: Path) -> None:
    path.write_text(
        "symbol,event_date,event_time,timing,event_type,source,notes\n"
        "NVDA,2026-05-01,16:10,AFTER_HOURS,EARNINGS_RELEASE,unit_test,after hours\n"
        "NVDA,2026-05-08,07:00,PRE_MARKET,EARNINGS_RELEASE,unit_test,pre market\n",
        encoding="utf-8",
    )


def _write_event_ohlcv_csv(path: Path) -> None:
    symbols = ["NVDA", "SMH", "SOXX", "QQQ", "XLK", "SPY", "VIX"]
    dates = [f"2026-05-{day:02d}" for day in range(1, 18)]
    lines = ["symbol,date,open,high,low,close,volume"]
    for symbol_index, symbol in enumerate(symbols):
        base = 100 + symbol_index * 5
        for idx, day in enumerate(dates):
            open_ = base + idx * 1.2
            close = open_ + (1.5 if symbol == "NVDA" and day in {"2026-05-02", "2026-05-08"} else 0.4)
            high = max(open_, close) + 1
            low = min(open_, close) - 1
            volume = 1000000 + idx * 10000 + symbol_index * 1000
            lines.append(f"{symbol},{day},{open_:.2f},{high:.2f},{low:.2f},{close:.2f},{volume}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

def test_nvidia_earnings_dislocation_gets_operator_event_study_plan(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    hypothesis_id = "rh-nvidia-earnings-event-dislocation-v1"
    path = truth.parent / "research_lab" / "research_hypothesis_v1" / "2026-05-21" / hypothesis_id / "research_hypothesis.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "hypothesis_id": hypothesis_id,
                "title": "NVIDIA Earnings Event Dislocation",
                "hypothesis_summary": "After NVIDIA earnings, do abnormal price gaps, volatility compression/expansion, or follow-through/reversal patterns produce statistically useful short-horizon signals?",
                "status": "IDEA",
                "source": "MANUAL",
                "edge_family": "EVENT_DISLOCATION",
                "event_type": "EARNINGS_RELEASE",
                "instruments": ["NVDA", "SMH", "SOXX", "QQQ", "XLK", "SPY"],
                "missing_datasets": ["NVIDIA historical earnings dates/times", "VIX or implied-volatility proxy"],
                "blocking_items": ["DATA_NEEDED"],
                "created_at_utc": "2026-05-21T00:00:00Z",
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    append_triage_record_v1(
        truth_root=truth,
        day_utc=DAY,
        hypothesis_id=hypothesis_id,
        decision="QUEUE_TEST_PLAN",
        reason="Operator-approved research-only event dislocation test.",
        operator="David",
    )
    plan = build_research_plan_v1(truth_root=truth, day_utc=DAY, hypothesis_id=hypothesis_id)
    write_research_plan_v1(truth_root=truth, day_utc=DAY, payload=plan)
    result = run_research_test_v1(truth_root=truth, day_utc=DAY, hypothesis_id=hypothesis_id)
    write_research_test_result_v1(truth_root=truth, day_utc=DAY, payload=result)
    pipeline = build_research_pipeline_v1(truth_root=truth, day_utc=DAY)
    row = next(item for item in pipeline["items"] if item["hypothesis_id"] == hypothesis_id)

    assert plan["test_type"] == "EVENT_STUDY / EVENT_DISLOCATION"
    assert plan["universe"] == ["NVDA", "SMH", "SOXX", "QQQ", "XLK", "SPY", "VIX"]
    assert "nvidia_historical_earnings_dates_times" in plan["required_inputs"]
    assert "forward_return_5d" in plan["required_outputs"]
    assert result["test_status"] == "DATA_NEEDED"
    assert result["fabricated_results"] is False
    assert row["symbols"] == ["NVDA", "SMH", "SOXX", "QQQ", "XLK", "SPY", "VIX"]
    assert row["gate_status"] == "WAITING_FOR_EVENT_DATA"
    assert row["operator_lifecycle_state"] == "RESEARCHING"
    assert row["operator_blocker"]["state"] == "BLOCKED_EXTERNAL_EARNINGS_CALENDAR_REQUIRED"
    assert row["operator_blocker"]["title"] == "External earnings calendar required"
    assert "symbol,event_date,event_time,timing,event_type,source" in row["data_acquisition_plan"]["accepted_format"]
    assert "NVIDIA historical earnings dates/times" in row["data_acquisition_plan"]["data_needed"]
    assert row["broker_execution_allowed"] is False
    assert row["autonomous_execution_allowed"] is False
    priority_rows = [row for rows in pipeline["priority_pipeline"].values() for row in rows]
    priority_row = next(row for row in priority_rows if row["hypothesis_id"] == hypothesis_id)
    assert priority_row["operator_lifecycle_state"] == "RESEARCHING"
    assert "NVIDIA historical earnings dates/times" in priority_row["data_acquisition_plan"]["data_needed"]
    assert priority_row["operator_action_required"] is True



def test_missing_earnings_calendar_writes_explicit_upload_required_artifact(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    hypothesis_id = _write_nvidia_hypothesis(truth)
    append_triage_record_v1(
        truth_root=truth,
        day_utc=DAY,
        hypothesis_id=hypothesis_id,
        decision="QUEUE_TEST_PLAN",
        reason="Worth testing",
        operator="David",
    )

    result = run_research_test_v1(truth_root=truth, day_utc=DAY, hypothesis_id=hypothesis_id)
    status = event_earnings_data_status_v1(truth_root=truth, day_utc=DAY, hypothesis_id=hypothesis_id)

    assert result["test_status"] == "DATA_NEEDED"
    assert result["blocker"] == "EARNINGS_EVENT_CALENDAR_REQUIRED"
    assert result["fabricated_results"] is False
    assert status["event_data_status"] == "MISSING_EARNINGS_EVENT_CALENDAR"
    assert status["operator_status"] == "External earnings calendar required"
    assert "symbol,event_date,event_time,timing,event_type,source" in status["accepted_upload_format"]["accepted_format"]
    assert status["calendar_artifact_path"].endswith("missing_earnings_event_calendar.v1.json")
    assert Path(status["calendar_artifact_path"]).exists()


def test_valid_earnings_calendar_moves_nvidia_to_event_window_data_requirement(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    hypothesis_id = _write_nvidia_hypothesis(truth)
    append_triage_record_v1(
        truth_root=truth,
        day_utc=DAY,
        hypothesis_id=hypothesis_id,
        decision="QUEUE_TEST_PLAN",
        reason="Worth testing",
        operator="David",
    )
    csv_path = tmp_path / "earnings_event_calendar.csv"
    _write_calendar_csv(csv_path)
    imported = import_earnings_event_calendar_csv_v1(
        truth_root=truth,
        day_utc=DAY,
        input_path=csv_path,
        hypothesis_id=hypothesis_id,
        expected_symbols=["NVDA"],
    )

    assert imported["validation_status"] == "VALIDATED"
    result = run_research_test_v1(truth_root=truth, day_utc=DAY, hypothesis_id=hypothesis_id)

    assert result["test_status"] == "DATA_NEEDED"
    assert result["blocker"] == "EVENT_WINDOW_OHLCV_DATA_REQUIRED"
    assert result["event_data_status"] == "MISSING_EVENT_WINDOW_OHLCV"
    assert "symbol,date,open,high,low,close,volume" in result["accepted_upload_format"]["accepted_format"]


def test_nvidia_event_study_computes_forward_returns_from_governed_event_data(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    hypothesis_id = _write_nvidia_hypothesis(truth)
    _write_intraday_market_report(truth)
    calendar_csv = tmp_path / "earnings_event_calendar.csv"
    ohlcv_csv = tmp_path / "event_window_ohlcv.csv"
    _write_calendar_csv(calendar_csv)
    _write_event_ohlcv_csv(ohlcv_csv)
    import_earnings_event_calendar_csv_v1(
        truth_root=truth,
        day_utc=DAY,
        input_path=calendar_csv,
        hypothesis_id=hypothesis_id,
        expected_symbols=["NVDA"],
    )
    window = import_event_window_ohlcv_csv_v1(
        truth_root=truth,
        day_utc=DAY,
        input_path=ohlcv_csv,
        hypothesis_id=hypothesis_id,
    )
    append_triage_record_v1(
        truth_root=truth,
        day_utc=DAY,
        hypothesis_id=hypothesis_id,
        decision="QUEUE_TEST_PLAN",
        reason="Worth testing",
        operator="David",
    )
    plan = build_research_plan_v1(truth_root=truth, day_utc=DAY, hypothesis_id=hypothesis_id)
    write_research_plan_v1(truth_root=truth, day_utc=DAY, payload=plan)

    result = run_research_test_v1(truth_root=truth, day_utc=DAY, hypothesis_id=hypothesis_id)
    write_research_test_result_v1(truth_root=truth, day_utc=DAY, payload=result)
    pipeline = build_research_pipeline_v1(truth_root=truth, day_utc=DAY)
    row = next(item for item in pipeline["items"] if item["hypothesis_id"] == hypothesis_id)

    assert window["validation_status"] == "VALIDATED"
    assert result["test_status"] == "INCONCLUSIVE_SAMPLE_SIZE"
    assert result["sample_size"] == 2
    assert result["metrics"]["forward_return_1d"]["metric_status"] == "COMPUTED"
    assert result["metrics"]["forward_return_3d"]["metric_status"] == "COMPUTED"
    assert result["metrics"]["forward_return_5d"]["metric_status"] == "COMPUTED"
    assert result["fabricated_results"] is False
    assert Path(result["forward_return_event_study_path"]).exists()
    assert row["current_gate"] == "RESULT_REVIEW"
    assert row["operator_lifecycle_state"] == "VALIDATING"
    assert row["broker_execution_allowed"] is False
    assert row["autonomous_execution_allowed"] is False


def test_invalid_earnings_calendar_is_not_accepted_as_governed_event_data(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    hypothesis_id = _write_nvidia_hypothesis(truth)
    csv_path = tmp_path / "bad_earnings_event_calendar.csv"
    csv_path.write_text(
        "symbol,event_date,event_time,timing,event_type,source\n"
        "NVDA,not-a-date,16:10,SOMETIME,EARNINGS_RELEASE,\n",
        encoding="utf-8",
    )

    imported = import_earnings_event_calendar_csv_v1(
        truth_root=truth,
        day_utc=DAY,
        input_path=csv_path,
        hypothesis_id=hypothesis_id,
        expected_symbols=["NVDA"],
    )
    status = event_earnings_data_status_v1(truth_root=truth, day_utc=DAY, hypothesis_id=hypothesis_id)

    assert imported["validation_status"] == "INVALID"
    assert imported["validation_errors"]
    assert status["event_data_status"] == "MISSING_EARNINGS_EVENT_CALENDAR"
    assert status["sample_size"] == 0


def test_research_test_runner_returns_data_needed_or_not_implemented_without_fake_results(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    hypothesis_id = "rh-edge-2026-0103"
    _write_hypothesis(truth, hypothesis_id)

    result = run_research_test_v1(truth_root=truth, day_utc=DAY, hypothesis_id=hypothesis_id)

    assert result["test_status"] in {"DATA_NEEDED", "TEST_NOT_IMPLEMENTED"}
    assert result["test_status"] != "PASS"
    assert result["fabricated_results"] is False
    assert result["sample_size"] == 0
    assert result["minimum_sample_size"] == 20
    assert result["broker_execution_allowed"] is False
    assert result["autonomous_execution_allowed"] is False


def test_etf_drop_mean_reversion_runner_executes_without_missing_runner(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    hypothesis_id = "rh-process-test-etf-drop-mean-reversion-v1"
    _write_hypothesis(
        truth,
        hypothesis_id,
    )
    _write_etf_drop_market_report(truth)
    append_triage_record_v1(
        truth_root=truth,
        day_utc=DAY,
        hypothesis_id=hypothesis_id,
        decision="QUEUE_TEST_PLAN",
        reason="Worth testing",
        operator="David",
    )
    plan = build_research_plan_v1(truth_root=truth, day_utc=DAY, hypothesis_id=hypothesis_id)
    write_research_plan_v1(truth_root=truth, day_utc=DAY, payload=plan)

    result = run_research_test_v1(truth_root=truth, day_utc=DAY, hypothesis_id=hypothesis_id)

    assert result["test_status"] == "INCONCLUSIVE_SAMPLE_SIZE"
    assert result["missing_runner"] == ""
    assert result["fabricated_results"] is False
    assert result["metrics"]["event_count"]["metric_status"] == "COMPUTED"
    assert result["metrics"]["forward_return_1d"]["metric_status"] == "INSUFFICIENT_FORWARD_RETURN_SAMPLE"
    assert result["sample_size"] == 2
    assert result["minimum_sample_size"] == 20
    assert result["broker_execution_allowed"] is False
    assert result["autonomous_execution_allowed"] is False
    assert result["trade_advice_allowed"] is False


def test_data_needed_result_is_promising_not_tier_one(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    hypothesis_id = "rh-process-test-etf-drop-mean-reversion-v1"
    _write_hypothesis(truth, hypothesis_id)
    append_triage_record_v1(
        truth_root=truth,
        day_utc=DAY,
        hypothesis_id=hypothesis_id,
        decision="QUEUE_TEST_PLAN",
        reason="Worth testing",
        operator="David",
    )
    plan = build_research_plan_v1(truth_root=truth, day_utc=DAY, hypothesis_id=hypothesis_id)
    write_research_plan_v1(truth_root=truth, day_utc=DAY, payload=plan)
    result = run_research_test_v1(truth_root=truth, day_utc=DAY, hypothesis_id=hypothesis_id)
    assert result["test_status"] == "DATA_NEEDED"
    write_research_test_result_v1(truth_root=truth, day_utc=DAY, payload=result)

    pipeline = build_research_pipeline_v1(truth_root=truth, day_utc=DAY)
    report = build_hypothesis_priority_report_v1(pipeline_payload=pipeline, day_utc=DAY)
    priority = report["priority_by_hypothesis_id"][hypothesis_id]

    assert priority["tier"] == "TIER_2_PROMISING"
    assert priority["blocker_summary"] == "Waiting for current intraday market data"
    assert "market data" in priority["recommended_action"]


def test_hypothesis_priority_report_tiers_and_sorts_attention(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    active_id = "rh-edge-2026-active"
    blocked_id = "rh-edge-2026-blocked"
    archive_id = "rh-edge-2026-archive"
    _write_hypothesis(truth, active_id)
    _write_hypothesis(truth, blocked_id)
    _write_hypothesis(truth, archive_id, status="ARCHIVED")

    for hypothesis_id in [active_id, blocked_id]:
        append_triage_record_v1(
            truth_root=truth,
            day_utc=DAY,
            hypothesis_id=hypothesis_id,
            decision="QUEUE_TEST_PLAN",
            reason="Worth testing",
            operator="David",
        )
        plan = build_research_plan_v1(truth_root=truth, day_utc=DAY, hypothesis_id=hypothesis_id)
        write_research_plan_v1(truth_root=truth, day_utc=DAY, payload=plan)

    result = run_research_test_v1(truth_root=truth, day_utc=DAY, hypothesis_id=active_id)
    result["test_status"] = "TEST_NOT_IMPLEMENTED"
    result["latest_result"] = "TEST_NOT_IMPLEMENTED"
    result["blocker"] = "MISSING_EVENT_STUDY_RUNNER"
    write_research_test_result_v1(truth_root=truth, day_utc=DAY, payload=result)
    append_research_review_decision_v1(
        truth_root=truth,
        day_utc=DAY,
        hypothesis_id=active_id,
        decision="PAPER_TEST_CANDIDATE",
        reason="Review accepted for paper trial",
        operator="David",
    )

    pipeline = build_research_pipeline_v1(truth_root=truth, day_utc=DAY)
    report = build_hypothesis_priority_report_v1(pipeline_payload=pipeline, day_utc=DAY)
    ranked_ids = [row["hypothesis_id"] for row in report["ranked_hypotheses"]]

    assert ranked_ids.index(active_id) < ranked_ids.index(blocked_id) < ranked_ids.index(archive_id)
    assert report["priority_by_hypothesis_id"][active_id]["tier"] == "TIER_1_ACTIVE"
    assert report["priority_by_hypothesis_id"][blocked_id]["tier"] in {"TIER_2_PROMISING", "TIER_4_WATCHLIST"}
    assert report["priority_by_hypothesis_id"][archive_id]["tier"] == "TIER_5_ARCHIVE"
    assert report["recommended_focus_today"][0]["hypothesis_id"] == active_id
    assert pipeline["priority_pipeline"]["active"][0]["hypothesis_id"] == active_id
    assert pipeline["priority_pipeline"]["archived"][0]["hypothesis_id"] == archive_id

    paths = write_hypothesis_priority_report_v1(truth_root=truth, day_utc=DAY, payload=report)
    assert Path(paths["json"]).exists()
    assert Path(paths["txt"]).exists()
    assert "no broker submit" in Path(paths["txt"]).read_text(encoding="utf-8")


def test_edge_lab_ui_triage_action_moves_hypothesis_and_writes_audit(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    hypothesis_id = "rh-edge-2026-0101"
    _write_hypothesis(truth, hypothesis_id)

    result = execute_edge_lab_workflow_action_v1(
        truth_root=truth,
        day_utc=DAY,
        endpoint="triage",
        request_payload={
            "hypothesis_id": hypothesis_id,
            "decision": "QUEUE_TEST_PLAN",
            "reason": "Worth testing",
            "operator": "David",
        },
    )

    assert result["ok"] is True
    assert result["to_gate"] == "TEST_PLAN"
    assert result["broker_execution_allowed"] is False
    assert result["autonomous_execution_allowed"] is False
    assert result["automatic_sleeve_mutation_allowed"] is False
    audit_path = Path(result["edge_lab_ui_action_log"])
    assert audit_path.exists()
    audit_rows = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines()]
    assert audit_rows[-1]["action_type"] == "TRIAGE"
    assert audit_rows[-1]["from_gate"] == "INBOX"
    assert audit_rows[-1]["to_gate"] == "TEST_PLAN"


def test_edge_lab_ui_invalid_transition_is_rejected(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    hypothesis_id = "rh-edge-2026-0101"
    _write_hypothesis(truth, hypothesis_id)

    with pytest.raises(EdgeLabWorkflowApiError):
        execute_edge_lab_workflow_action_v1(
            truth_root=truth,
            day_utc=DAY,
            endpoint="run-test",
            request_payload={"hypothesis_id": hypothesis_id, "operator": "David"},
        )


def test_edge_lab_ui_build_plan_and_run_test_are_safe_and_non_fabricating(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    hypothesis_id = "rh-edge-2026-0103"
    _write_hypothesis(truth, hypothesis_id)

    execute_edge_lab_workflow_action_v1(
        truth_root=truth,
        day_utc=DAY,
        endpoint="triage",
        request_payload={"hypothesis_id": hypothesis_id, "decision": "QUEUE_TEST_PLAN", "reason": "Worth testing", "operator": "David"},
    )
    plan_result = execute_edge_lab_workflow_action_v1(
        truth_root=truth,
        day_utc=DAY,
        endpoint="build-plan",
        request_payload={"hypothesis_id": hypothesis_id, "operator": "David"},
    )
    assert plan_result["to_gate"] == "TESTING"
    assert Path(plan_result["generated_artifacts"]["json"]).exists()

    test_result = execute_edge_lab_workflow_action_v1(
        truth_root=truth,
        day_utc=DAY,
        endpoint="run-test",
        request_payload={"hypothesis_id": hypothesis_id, "operator": "David"},
    )
    assert test_result["to_gate"] == "TESTING"
    assert test_result["result"]["test_status"] == "DATA_NEEDED"
    assert test_result["result"]["fabricated_results"] is False
    assert test_result["broker_execution_allowed"] is False
    assert test_result["autonomous_execution_allowed"] is False


def test_edge_lab_ui_review_is_blocked_while_research_data_is_missing(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    hypothesis_id = "rh-edge-2026-0101"
    _write_hypothesis(truth, hypothesis_id)
    execute_edge_lab_workflow_action_v1(
        truth_root=truth,
        day_utc=DAY,
        endpoint="triage",
        request_payload={"hypothesis_id": hypothesis_id, "decision": "QUEUE_TEST_PLAN", "reason": "Worth testing", "operator": "David"},
    )
    execute_edge_lab_workflow_action_v1(
        truth_root=truth,
        day_utc=DAY,
        endpoint="build-plan",
        request_payload={"hypothesis_id": hypothesis_id, "operator": "David"},
    )
    execute_edge_lab_workflow_action_v1(
        truth_root=truth,
        day_utc=DAY,
        endpoint="run-test",
        request_payload={"hypothesis_id": hypothesis_id, "operator": "David"},
    )

    with pytest.raises(EdgeLabWorkflowApiError):
        execute_edge_lab_workflow_action_v1(
            truth_root=truth,
            day_utc=DAY,
            endpoint="review",
            request_payload={"hypothesis_id": hypothesis_id, "decision": "REJECTED", "reason": "Not strong enough", "operator": "David"},
        )
