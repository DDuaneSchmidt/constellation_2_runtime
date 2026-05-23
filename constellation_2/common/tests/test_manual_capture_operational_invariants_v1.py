from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.operator_state.canonical_operator_state_builder_v1 import build_operator_state_snapshot_v1
from ops.aegis.operator_state.manual_capture_record_v1 import (
    append_manual_capture_record_v1,
    latest_manual_capture_record_v1,
    list_manual_capture_records_v1,
)


ROOT = Path(__file__).resolve().parents[3]
DAY = "2026-05-20"
INTENT_ID = "c2_trend_eq_amt_2026-05-20_v1"
INTENT_HASH = "a" * 64


def _write(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _seed_current_selected_exposure(root: Path) -> None:
    intent_path = root / "intents_v1" / "snapshots" / DAY / f"{INTENT_HASH}.exposure_intent.v1.json"
    _write(
        intent_path,
        {
            "schema_id": "exposure_intent",
            "schema_version": "v1",
            "intent_id": INTENT_ID,
            "engine": {"engine_id": "C2_TREND_EQ_PRIMARY_V1"},
            "underlying": {"symbol": "AMT", "currency": "USD"},
        },
    )
    rows = [
        {
            "candidate_id": INTENT_ID,
            "symbol": "AMT",
            "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
            "engine_id": "C2_TREND_EQ_PRIMARY_V1",
            "direction": "LONG",
            "score": 31.3333,
            "confidence": "UNKNOWN",
            "rank_before_arbitration": 1,
            "selected_by_gate": "YES",
            "suppression_code": "SELECTED",
            "portfolio_gate_decision": "ALLOW",
            "reason_codes": ["SCORING_EXECUTABLE_ELIGIBLE"],
            "evidence_paths": [str(intent_path.resolve())],
        },
        {
            "candidate_id": "suppressed-spy",
            "symbol": "SPY",
            "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
            "engine_id": "C2_TREND_EQ_PRIMARY_V1",
            "direction": "LONG",
            "score": 10,
            "selected_by_gate": "NO",
            "suppression_code": "one_primary_per_regime_bucket_suppressed",
            "portfolio_gate_decision": "SUPPRESS",
            "reason_codes": ["ONE_PRIMARY_PER_REGIME_BUCKET_SUPPRESSED"],
            "competing_selected_candidate_id": INTENT_ID,
        },
    ]
    _write(
        root / "reports" / "portfolio_gate_candidate_report_v1" / DAY / "portfolio_gate_candidate_report.v1.json",
        {
            "schema_id": "portfolio_gate_candidate_report",
            "schema_version": "portfolio_gate_candidate_report.v1",
            "day_utc": DAY,
            "selected_candidate_id": INTENT_ID,
            "candidate_count": 2,
            "selected_count": 1,
            "suppressed_count": 1,
            "candidate_rows": rows,
        },
    )
    _write(
        root / "reports" / "exposure_intent_paper_submission_package_v1" / DAY / "attempt" / "exposure_intent_paper_submission_package.v1.json",
        {
            "schema_id": "exposure_intent_paper_submission_package",
            "schema_version": "v1",
            "day_utc": DAY,
            "status": "BLOCKED",
            "exposure_intent_id": INTENT_ID,
            "symbol": "AMT",
            "paper_trade_intent_created": False,
            "blocker_code": "STALE_MARKET_DATA_BLOCKS_CONVERSION",
            "blocker_message": "AMT market data is not current for 2026-05-20; observed_session=2026-04-02.",
            "market_data_status": {"expected_session": DAY, "observed_session": "2026-04-02", "status": "STALE_OR_MISSING"},
        },
    )


def _pages_source() -> str:
    return (ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")


def _server_source() -> str:
    return (ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py").read_text(encoding="utf-8")


def test_selected_exposure_visible_and_stale_market_data_blocker_still_visible(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_current_selected_exposure(root)
    snapshot = build_operator_state_snapshot_v1(truth_root=root, day_utc=DAY)

    assert snapshot["manual_capture_candidate"]["selected_exposure_intent_id"] == INTENT_ID
    assert snapshot["manual_capture_candidate"]["symbol"] == "AMT"
    assert snapshot["manual_capture_candidate"]["blocker_code"] == "STALE_MARKET_DATA_BLOCKS_CONVERSION"
    assert snapshot["market_data_freshness"]["latest_market_session"] == "2026-04-02"


def test_editable_quantity_and_fill_fields_are_visible_in_manual_capture_ui() -> None:
    pages = _pages_source()
    block = pages.split("function renderManualCaptureRecordForm", 1)[1].split("function renderOperatorReadinessProjectionPanel", 1)[0]

    assert "Mark capture complete" in block
    assert 'name="quantity"' in block
    assert 'name="fill_price"' in block
    assert 'name="fill_time"' in block
    assert 'name="notes"' in block
    assert 'name="operator_id"' in block
    assert "Mark capture complete" in block


def test_save_manual_capture_record_rejects_without_active_lineage_or_submit_boundary(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_current_selected_exposure(root)
    try:
        append_manual_capture_record_v1(
            truth_root=root,
            day_utc=DAY,
            generated_at_utc="2026-05-20T14:30:00Z",
            request_payload={
                "selected_exposure_intent_id": INTENT_ID,
                "capture_status": "blocked",
                "quantity": "7",
                "fill_price": "184.25",
                "fill_time": "2026-05-20T14:29:00Z",
                "stop_price": "179.50",
                "notes": "operator recorded external paper fill",
                "operator_id": "David",
                "external_reference": "manual-paper-1",
            },
        )
    except ValueError as exc:
        payload = json.loads(str(exc))
    else:
        raise AssertionError("manual capture save without lineage/precheck was accepted")

    listed = list_manual_capture_records_v1(truth_root=root, day_utc=DAY, selected_exposure_intent_id=INTENT_ID)
    assert listed["record_count"] == 0
    assert payload["blocker_code"] in {"STALE_RUNTIME", "MISSING_SUBMIT_BOUNDARY", "MISSING_MARKET_FRESHNESS", "MISSING_CONVERSION"}
    assert payload["broker_execution_allowed"] is False
    assert payload["order_routing_allowed"] is False
    assert payload["autonomous_execution_allowed"] is False
    assert not (root / "reports" / "exposure_intent_paper_submit_v1").exists()


def test_current_truth_migration_does_not_remove_edit_workflow_or_routes() -> None:
    pages = _pages_source()
    server = _server_source()

    assert "renderManualCaptureProjectionPanel" in pages
    assert "renderManualCaptureRecordForm(manual" in pages
    assert "executeManualCaptureRecordWorkflow" in pages
    assert '"/api/aegis/operator/manual-capture-records/latest"' in server
    assert '"/api/aegis/operator/manual-capture-records"' in server
    assert "append_manual_capture_record_v1" in server


def test_stale_fallback_cannot_override_current_exposure(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed_current_selected_exposure(root)
    qqq_path = root / "intents_v1" / "snapshots" / "2026-05-19" / f"{'q' * 64}.exposure_intent.v1.json"
    _write(qqq_path, {"schema_id": "exposure_intent", "schema_version": "v1", "intent_id": "c2_cross_asset_trend_qqq_2026-05-19_v1", "underlying": {"symbol": "QQQ"}})
    _write(
        root / "pointers" / "selected_intent_pointer.v1.json",
        {"schema_id": "selected_intent_pointer", "schema_version": "v1", "day_utc": "2026-05-19", "selected_intent": {"intent_id": "c2_cross_asset_trend_qqq_2026-05-19_v1", "intent_path": str(qqq_path.resolve()), "symbol": "QQQ"}},
    )

    snapshot = build_operator_state_snapshot_v1(truth_root=root, day_utc=DAY)

    assert snapshot["manual_capture_candidate"]["selected_exposure_intent_id"] == INTENT_ID
    assert snapshot["manual_capture_candidate"]["symbol"] == "AMT"


def test_no_packet_may_remove_manual_capture_ui_silently() -> None:
    pages = _pages_source()
    contract = (ROOT / "constellation_2/phaseL/ui/docs/ui_sturdiness_contract_v1.md").read_text(encoding="utf-8")
    invariants = (ROOT / "docs/aegis/operational_invariants_v1.md").read_text(encoding="utf-8")

    assert "Operator workflows are protected invariants" in contract
    assert "Manual Capture Workflow Invariants" in invariants
    assert "manual-capture-record-form" in pages
    assert "Aegis did not execute this trade" in pages


def test_opportunities_fails_if_selected_exposure_exists_but_no_capture_workflow_visible() -> None:
    pages = _pages_source()
    today_block = pages.split("function renderAegisTodayWorkflow", 1)[1].split("function dashboardCaptureProjection", 1)[0]
    capture_block = pages.split("function renderManualCaptureProjectionPanel", 1)[1].split("function renderManualCaptureRecordForm", 1)[0]

    assert "renderDashboardSystemStatus(payload" in today_block
    assert "capture_ticket_count" in pages
    assert "trade_ticket_projection_v1" in capture_block
    assert "renderManualCaptureRecordForm" in pages
