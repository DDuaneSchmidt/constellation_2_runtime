from __future__ import annotations

import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_ai_feedback_engine_v1 import (  # noqa: E402
    build_ai_feedback_review_v1,
    build_evidence_gate_v1,
    validate_ai_feedback_review_v1,
    validate_evidence_gate_v1,
)
from constellation_2.common.aegis_chatgpt_control_packet_v1 import build_aegis_chatgpt_control_packet_v1  # noqa: E402
from constellation_2.common.aegis_event_monitoring_v1 import run_event_monitor_v1  # noqa: E402
from constellation_2.common.aegis_market_context_v1 import (  # noqa: E402
    build_event_market_snapshot_v1,
    classify_breadth_v1,
    classify_macro_event_v1,
    classify_regime_v1,
    classify_volatility_v1,
    validate_event_market_snapshot_v1,
    write_event_market_snapshot_v1,
)
from constellation_2.common.aegis_sleeve_review_feedback_v1 import build_eod_sleeve_review_v1, validate_eod_sleeve_review_v1  # noqa: E402
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1  # noqa: E402
from ops.tools.build_event_market_snapshot_v1 import main as snapshot_cli_main  # noqa: E402


DAY = "2026-05-15"
NOW = "2026-05-15T19:00:00Z"


def _market_data(**overrides: object) -> dict[str, object]:
    data: dict[str, object] = {
        "source_timestamp_utc": "2026-05-15T18:55:00Z",
        "market_open_status": "OPEN",
        "trading_day_type": "TRADING_DAY",
        "spy_price": "505",
        "spy_prev_close": "500",
        "qqq_price": "430",
        "qqq_prev_close": "426",
        "vix_price": "18",
        "vix_prev_close": "17",
        "spy_realized_vol": "16",
        "spy_realized_vol_prev": "15",
        "volatility_expansion_ratio": "1.05",
        "advancing_issues": "2200",
        "declining_issues": "900",
        "unchanged_issues": "100",
        "spy_20d_return_pct": "4.5",
        "spy_50d_return_pct": "6.0",
        "spy_above_20dma": True,
        "spy_above_50dma": True,
        "qqq_above_20dma": True,
        "qqq_above_50dma": True,
    }
    data.update(overrides)
    return data


def _snapshot(**overrides: object) -> dict[str, object]:
    return build_event_market_snapshot_v1(
        day_utc=DAY,
        generated_at_utc=NOW,
        market_data=_market_data(**overrides),
        macro_calendar=[{"date": DAY, "event_type": "CPI", "risk_level": "HIGH", "event_time_utc": "2026-05-15T12:30:00Z"}],
        source_lineage=[{"artifact_type": "fixture", "path": "/tmp/context"}],
    )


def test_market_snapshot_generation_and_deterministic_ordering(tmp_path: Path) -> None:
    first = _snapshot()
    second = _snapshot()
    validate_event_market_snapshot_v1(first)
    path = write_event_market_snapshot_v1(truth_root=tmp_path, payload=first)

    assert path == tmp_path / "reports" / "event_market_snapshot_v1" / DAY / "event_market_snapshot.v1.json"
    assert first["volatility_classification"] == "NORMAL_VOL"
    assert first["breadth_classification"] == "STRONG_BREADTH"
    assert first["regime_label"] == "TRENDING_UP"
    assert first["macro_event_today"] is True
    assert first["macro_event_type"] == "CPI"
    assert first["macro_event_risk_level"] == "HIGH"
    assert canonical_json_bytes_v1(first) == canonical_json_bytes_v1(second)


def test_volatility_breadth_macro_and_regime_classification() -> None:
    assert classify_volatility_v1(vix_level="12", spy_realized_vol="10", volatility_expansion_ratio="0.8") == "LOW_VOL"
    assert classify_volatility_v1(vix_level="28") == "HIGH_VOL"
    assert classify_volatility_v1(vix_change_pct="30") == "PANIC_VOL"
    assert classify_breadth_v1(advancing_issues="300", declining_issues="2500", breadth_down_pct="82") == "BREADTH_COLLAPSE"
    assert classify_breadth_v1(advancing_issues="2100", declining_issues="900", advance_decline_delta="1200") == "STRONG_BREADTH"
    macro = classify_macro_event_v1(day_utc=DAY, macro_calendar={"events": [{"date": DAY, "type": "FOMC", "risk_level": "EXTREME"}]})
    assert macro["macro_event_type"] == "FOMC"
    assert macro["macro_event_risk_level"] == "EXTREME"
    assert classify_regime_v1(volatility_label="PANIC_VOL", breadth_label="BREADTH_COLLAPSE", trend_metrics={"spy_return_pct": "-3.2"}, macro_context=macro) == "PANIC"


def test_snapshot_cli_writes_fail_closed_missing_input_snapshot(tmp_path: Path, capsys) -> None:
    assert snapshot_cli_main(["--truth_root", str(tmp_path), "--day_utc", DAY, "--generated_at_utc", NOW]) == 0
    out = json.loads(capsys.readouterr().out)
    payload = json.loads(Path(out["path"]).read_text(encoding="utf-8"))

    assert payload["stale_data_status"] == "MISSING_INPUT"
    assert payload["regime_label"] == "UNKNOWN"
    assert payload["broker_submit_required"] is False


def test_snapshot_cli_derives_truth_lineage_without_fabricating_stale_market_data(tmp_path: Path, capsys) -> None:
    manifest_path = tmp_path / "market_data_snapshot_v1" / "dataset_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps({"source_snapshot_utc": "2026-05-15T13:45:00Z"}), encoding="utf-8")
    for symbol, close in (("SPY", "505"), ("QQQ", "430")):
        path = tmp_path / "market_data_snapshot_v1" / symbol / "2026.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"symbol": symbol, "timestamp_utc": "2026-05-15T00:00:00Z", "close": close, "ingested_utc": "2026-05-15T13:45:00Z"})
            + "\n",
            encoding="utf-8",
        )

    assert snapshot_cli_main(["--truth_root", str(tmp_path), "--day_utc", "2026-05-18", "--generated_at_utc", "2026-05-18T16:00:00Z"]) == 0
    out = json.loads(capsys.readouterr().out)
    payload = json.loads(Path(out["path"]).read_text(encoding="utf-8"))
    validate_event_market_snapshot_v1(payload)

    lineage_types = {entry["artifact_type"] for entry in payload["source_lineage"]}
    assert payload["stale_data_status"] in {"STALE", "MISSING_INPUT"}
    assert "market_context_demand_v1" in lineage_types
    assert "market_context_source" in lineage_types
    assert payload["current_prices"]["SPY"] == ""
    assert payload["current_prices"]["QQQ"] == ""
    assert any(code == "MARKET_CONTEXT_SOURCE_STALE" or code.startswith("CONTEXT_NOT_FETCHED:") for code in payload["reason_codes"])
    assert any(code == "MISSING_INPUT:spy_price" or code.startswith("CONTEXT_NOT_FETCHED:spy_") for code in payload["reason_codes"])
    assert payload["broker_submit_required"] is False


def test_event_monitor_consumes_snapshot_and_blocks_stale_context(tmp_path: Path) -> None:
    snapshot = _snapshot(source_timestamp_utc="2026-05-15T17:00:00Z")
    assert snapshot["stale_data_status"] == "STALE"

    result = run_event_monitor_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        market_snapshot=snapshot,
        monitor_run_id="monitor-market-context",
        timestamp_utc=NOW,
    )

    status = result["monitoring_status"]
    assert status["market_context"]["regime_label"] == "TRENDING_UP"
    assert status["market_snapshot_freshness_status"] == "STALE"
    assert any(code.endswith(":STALE_DATA") for code in status["pass_fail_reason_codes"])
    assert status["triggered_events"] == []
    assert status["broker_submit_required"] is False


def test_ai_feedback_and_sleeve_review_include_market_context() -> None:
    snapshot = _snapshot()
    report = _sleeve_report()
    gate = build_evidence_gate_v1(
        review_type="EOD",
        period_start=DAY,
        period_end=DAY,
        generated_at_utc=NOW,
        sleeve_performance_reports=[report],
        market_context_snapshots=[snapshot],
        input_artifact_refs=[{"artifact_type": "event_market_snapshot_v1", "path": "/tmp/snapshot"}],
    )
    review = build_ai_feedback_review_v1(
        review_type="EOD",
        period_start=DAY,
        period_end=DAY,
        generated_at_utc=NOW,
        sleeve_performance_reports=[report],
        evidence_gate=gate,
        market_context_snapshots=[snapshot],
        input_artifact_refs=[{"artifact_type": "event_market_snapshot_v1", "path": "/tmp/snapshot"}],
    )
    sleeve_review = build_eod_sleeve_review_v1(
        day_utc=DAY,
        generated_at_utc=NOW,
        sleeve_performance_report=report,
        market_context_snapshots=[snapshot],
    )

    validate_evidence_gate_v1(gate)
    validate_ai_feedback_review_v1(review)
    validate_eod_sleeve_review_v1(sleeve_review)
    assert gate["market_context_summary"]["regime_label"] == "TRENDING_UP"
    assert "TRENDING_UP" in review["regimes_reviewed"]
    assert review["market_context_summary"]["macro_event_type"] == "CPI"
    assert sleeve_review["market_context_summary"]["volatility_classification"] == "NORMAL_VOL"


def test_chatgpt_control_packet_exposes_market_context(tmp_path: Path) -> None:
    snapshot = _snapshot()
    write_event_market_snapshot_v1(truth_root=tmp_path, payload=snapshot)
    _write_packet_sources(tmp_path, snapshot)

    packet = build_aegis_chatgpt_control_packet_v1(truth_root=tmp_path, day_utc=DAY, generated_at_utc=NOW)

    assert packet["runtime_truth_classification"] == "PARTIAL_CONTEXT"
    assert packet["market_context_status"]["regime_label"] == "TRENDING_UP"
    assert packet["market_context_status"]["macro_event_risk_level"] == "HIGH"
    assert packet["trade_advice_allowed"] is False


def _sleeve_report() -> dict[str, object]:
    rows = [
        {
            "trade_id": "trade-1",
            "lifecycle_status": "EXECUTED_CLOSED",
            "sleeve_id": "sleeve-a",
            "source_hypothesis_id": "hypothesis-a",
            "symbol": "SPY",
            "outcome_status": "loss",
            "return_pct": "-1.0",
            "regime_state": "TRENDING_UP",
            "event_type": "MACRO_EVENT_REACTION",
            "event_id": "event-1",
            "failure_reason": "false positive around CPI",
            "entry_slippage_respected": True,
            "max_entry_slippage_respected": True,
            "stop_entered": True,
            "stop_matched_recommendation": True,
            "edge_overlap_attribution": "LOW_OVERLAP",
        }
    ]
    return {
        "schema_id": "sleeve_performance_report",
        "schema_version": "v1",
        "artifact_id": "sleeve_performance_report_v1",
        "day_utc": DAY,
        "generated_at_utc": NOW,
        "portfolio_summary": {"total_recommended_trades": 1, "total_executed_trades": 1, "missing_receipt_count": 0, "missing_outcome_count": 0},
        "sleeve_summary": [{"sleeve_id": "sleeve-a", "total_return": "-1.0"}],
        "execution_quality": {},
        "trade_lifecycle_rows": rows,
    }


def _write_packet_sources(root: Path, snapshot: dict[str, object]) -> None:
    sources = {
        "reports/aegis_lite_operating_status_v1/2026-05-15/aegis_lite_operating_status.v1.json": {
            "schema_id": "aegis_lite_operating_status",
            "artifact_id": "aegis_lite_operating_status_v1",
            "day_utc": DAY,
            "generated_at_utc": NOW,
            "broker_mode": "MANUAL_ONLY",
            "manual_execution_only": True,
            "broker_submit_required": False,
        },
        "reports/aegis_lite_eod_report_v1/2026-05-15/aegis_lite_eod_report.v1.json": {
            "schema_id": "aegis_lite_eod_report",
            "artifact_id": "aegis_lite_eod_report_v1",
            "day_utc": DAY,
            "generated_at_utc": NOW,
            "broker_mode": "MANUAL_ONLY",
            "manual_execution_only": True,
            "broker_submit_required": False,
        },
        "reports/operator_execution_queue_v1/2026-05-15/operator_execution_queue.v1.json": {
            "schema_id": "operator_execution_queue",
            "artifact_id": "operator_execution_queue_v1",
            "day_utc": DAY,
            "generated_at_utc": NOW,
            "execution_queue": [],
        },
        "reports/event_monitoring_status_v1/2026-05-15/run/event_monitoring_status.v1.json": {
            "schema_id": "event_monitoring_status",
            "artifact_id": "event_monitoring_status_v1",
            "day_utc": DAY,
            "timestamp_utc": NOW,
            "monitor_run_id": "event-run",
            "triggered_events": [],
            "blocked_events": [],
            "market_context": snapshot,
            "market_snapshot_freshness_status": snapshot["stale_data_status"],
            "canonical_eod_state_mutated": False,
        },
        "reports/event_rules_registry_v1/2026-05-15/run/event_rules_registry.v1.json": {
            "schema_id": "event_rules_registry",
            "artifact_id": "event_rules_registry_v1",
            "day_utc": DAY,
            "generated_at_utc": NOW,
            "event_rules": [],
        },
    }
    for relpath, payload in sources.items():
        path = root / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
