from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.operator_state.canonical_operator_state_builder_v1 import build_operator_state_snapshot_v1
from ops.aegis.operator_state.manual_capture_record_v1 import append_manual_capture_record_v1
from ops.aegis.operator_state.trade_candidate_projection_v1 import build_trade_candidate_projection_v1

DAY = "2026-05-20"
INTENT_ID = "c2_trend_eq_amt_2026-05-20_v1"
INTENT_HASH = "a" * 64


def _write(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _seed(
    root: Path,
    *,
    market_status: str = "CURRENT",
    observed_session: str = DAY,
    entry_price: str = "180.25",
    stop_price: str = "174.50",
    quantity: int | None = 3,
    conversion_blocker: str = "",
    stale_qqq_converter: bool = False,
) -> None:
    intent_path = root / "intents_v1" / "snapshots" / DAY / f"{INTENT_HASH}.exposure_intent.v1.json"
    _write(
        intent_path,
        {
            "schema_id": "exposure_intent",
            "schema_version": "v1",
            "intent_id": INTENT_ID,
            "engine": {"engine_id": "C2_TREND_EQ_PRIMARY_V1"},
            "exposure_type": "LONG_EQUITY",
            "underlying": {"symbol": "AMT", "currency": "USD"},
            "target_notional_pct": "0.01",
            "constraints": {"max_risk_pct": "0.01"},
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
            "rank_before_arbitration": 1,
            "selected_by_gate": "YES",
            "portfolio_gate_decision": "ALLOW",
            "reason_codes": ["SCORING_EXECUTABLE_ELIGIBLE"],
            "evidence_paths": [str(intent_path.resolve())],
        },
        {
            "candidate_id": "c2_cross_asset_trend_qqq_2026-05-19_v1",
            "symbol": "QQQ",
            "sleeve_id": "C2_CROSS_ASSET_TREND_V1",
            "engine_id": "C2_CROSS_ASSET_TREND_V1",
            "direction": "LONG",
            "rank_before_arbitration": 2,
            "selected_by_gate": "NO",
            "portfolio_gate_decision": "SUPPRESS",
            "suppression_code": "one_primary_per_regime_bucket_suppressed",
            "suppression_reason": "Suppressed watchlist row remains visible.",
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
            "suppression_code_counts": {"one_primary_per_regime_bucket_suppressed": 1},
            "candidate_rows": rows,
        },
    )
    conv_id = "c2_cross_asset_trend_qqq_2026-05-19_v1" if stale_qqq_converter else INTENT_ID
    conv_symbol = "QQQ" if stale_qqq_converter else "AMT"
    conv = {
        "schema_id": "exposure_intent_paper_submission_package",
        "schema_version": "v1",
        "day_utc": DAY,
        "status": "BLOCKED" if conversion_blocker else "READY",
        "exposure_intent_id": conv_id,
        "symbol": conv_symbol,
        "entry_reference_price": entry_price,
        "stop_price": stop_price,
        "quantity": quantity,
        "paper_trade_intent_created": False,
        "blocker_code": conversion_blocker,
        "blocker_message": f"{conv_symbol} blocker",
        "market_data_status": {
            "expected_session": DAY,
            "observed_session": observed_session,
            "status": market_status,
            "path": f"/tmp/{conv_symbol}/quote.json",
        },
    }
    if quantity is None:
        conv.pop("quantity", None)
    if entry_price == "":
        conv.pop("entry_reference_price", None)
    if stop_price == "":
        conv.pop("stop_price", None)
    _write(root / "reports" / "exposure_intent_paper_submission_package_v1" / DAY / "attempt" / "exposure_intent_paper_submission_package.v1.json", conv)
    market_day = observed_session if not stale_qqq_converter else DAY
    (root / "market_data_snapshot_v1" / "AMT").mkdir(parents=True, exist_ok=True)
    (root / "market_data_snapshot_v1" / "AMT" / "2026.jsonl").write_text(json.dumps({"schema_id": "market_data_snapshot_v1", "symbol": "AMT", "timestamp_utc": f"{market_day}T00:00:00Z", "close": entry_price or "180.25", "volume": 1000000}, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    _write(root / "market_data_snapshot_v1" / "dataset_manifest.json", {"schema_id": "market_data_snapshot_manifest", "symbols": ["AMT"], "files": [{"symbol": "AMT", "file": "AMT/2026.jsonl"}]})
    _write(root / "allocation_v1" / "capital_authority_allocation_v1" / DAY / "capital_authority_allocation.v1.json", {"schema_id": "capital_authority_allocation", "decision_chain": {"authorized_trade_intents": [{"intent_id": INTENT_ID, "authorization_outcome": "APPROVED", "authorized_quantity": quantity or 0}]}})
    _write(root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json", {"schema_id": "submit_boundary_status", "schema_version": "v1", "status": "PASS"})


def _codes(payload: dict) -> set[str]:
    return {str(row.get("code")) for row in payload.get("blockers", []) if isinstance(row, dict)}


def test_every_selected_exposure_produces_trade_candidate_projection_v1(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root)
    payload = build_operator_state_snapshot_v1(truth_root=root, day_utc=DAY)
    projection = payload["trade_candidate_projection_v1"]
    assert projection["schema_id"] == "trade_candidate_projection"
    assert projection["selected_exposure_intent_id"] == INTENT_ID
    assert projection["symbol"] == "AMT"
    assert (root / "reports" / "trade_candidate_projection_v1" / DAY / "trade_candidate_projection.v1.json").exists()


def test_amt_selected_exposure_renders_required_fields_or_blockers(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, entry_price="", stop_price="", quantity=None)
    projection = build_trade_candidate_projection_v1(truth_root=root, day_utc=DAY)
    assert projection["symbol"] == "AMT"
    assert {"MISSING_ENTRY_REFERENCE_PRICE", "MISSING_STOP_OR_INVALIDATION_LEVEL"} <= _codes(projection)


def test_missing_position_size_creates_explicit_blocker(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, quantity=None)
    intent_path = root / "intents_v1" / "snapshots" / DAY / f"{INTENT_HASH}.exposure_intent.v1.json"
    intent = json.loads(intent_path.read_text(encoding="utf-8"))
    intent.pop("target_notional_pct", None)
    intent["constraints"] = {}
    intent_path.write_text(json.dumps(intent, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    projection = build_trade_candidate_projection_v1(truth_root=root, day_utc=DAY)
    assert "MISSING_POSITION_SIZE" in _codes(projection)
    assert "MISSING_RISK_BUDGET" in _codes(projection)


def test_missing_price_and_stop_create_explicit_blockers(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, entry_price="", stop_price="")
    projection = build_trade_candidate_projection_v1(truth_root=root, day_utc=DAY)
    assert "MISSING_ENTRY_REFERENCE_PRICE" in _codes(projection)
    assert "MISSING_STOP_OR_INVALIDATION_LEVEL" in _codes(projection)


def test_captured_manually_validation_requires_quantity_fill_and_stop(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root)
    base = {"selected_exposure_intent_id": INTENT_ID, "capture_status": "captured_manually", "fill_time": f"{DAY}T15:30:00Z", "operator_id": "test"}
    for missing_payload, expected in [
        ({"fill_price": "180.50", "stop_price": "174.50", "operator_id": "test"}, "Quantity is required."),
        ({"quantity": "3", "stop_price": "174.50", "operator_id": "test"}, "Fill price is required."),
        ({"quantity": "3", "fill_price": "180.50"}, "A stop or invalidation level is required"),
    ]:
        try:
            append_manual_capture_record_v1(truth_root=root, day_utc=DAY, request_payload={**base, **missing_payload})
        except ValueError as exc:
            assert expected in str(exc)
        else:
            raise AssertionError("captured_manually validation did not fail")


def test_stale_market_data_prevents_ready_and_capture(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, market_status="STALE_OR_MISSING", observed_session="2026-04-02", conversion_blocker="STALE_MARKET_DATA_BLOCKS_CONVERSION")
    projection = build_trade_candidate_projection_v1(truth_root=root, day_utc=DAY)
    assert projection["lifecycle_status"] == "review_only_blocked"
    assert "STALE_MARKET_DATA_BLOCKS_CONVERSION" in _codes(projection)
    try:
        append_manual_capture_record_v1(truth_root=root, day_utc=DAY, request_payload={"selected_exposure_intent_id": INTENT_ID, "capture_status": "captured_manually", "quantity": "3", "fill_price": "180.50", "fill_time": f"{DAY}T15:30:00Z", "stop_price": "174.50", "operator_id": "test"})
    except ValueError as exc:
        assert "NOT_CAPTURE_READY" in str(exc)
    else:
        raise AssertionError("stale market data did not block captured_manually")


def test_old_qqq_stale_candidate_cannot_become_current(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, stale_qqq_converter=True, market_status="STALE_OR_MISSING", observed_session="2026-05-19", conversion_blocker="STALE_MARKET_DATA_BLOCKS_CONVERSION")
    projection = build_trade_candidate_projection_v1(truth_root=root, day_utc=DAY)
    assert projection["symbol"] == "AMT"
    assert projection["selected_exposure_intent_id"] == INTENT_ID
    assert "STALE_MARKET_DATA_BLOCKS_CONVERSION" not in _codes(projection)



def test_amt_stale_market_data_produces_blocked_missing_market_data_ticket(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, market_status="STALE_OR_MISSING", observed_session="2026-04-02", entry_price="", stop_price="", quantity=None, conversion_blocker="STALE_MARKET_DATA_BLOCKS_CONVERSION")
    projection = build_trade_candidate_projection_v1(truth_root=root, day_utc=DAY)
    ticket = projection["trade_ticket_projection_v1"]

    assert projection["symbol"] == "AMT"
    assert ticket["trade_ticket_status"] == "blocked_missing_market_data"
    assert ticket["capture_status"] == "Not capture-ready"
    assert {row["field"] for row in ticket["missing_fields"]} >= {"current_market_data", "entry_reference_price", "suggested_quantity", "stop_price", "risk_estimate"}
    assert "Refresh market data" in ticket["next_required_action"]


def test_incomplete_ticket_cannot_be_saved_as_captured_manually(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root, quantity=None)
    projection = build_trade_candidate_projection_v1(truth_root=root, day_utc=DAY)
    assert projection["trade_ticket_status"] == "incomplete"

    try:
        append_manual_capture_record_v1(truth_root=root, day_utc=DAY, request_payload={"selected_exposure_intent_id": INTENT_ID, "capture_status": "captured_manually", "quantity": "3", "fill_price": "180.50", "fill_time": f"{DAY}T15:30:00Z", "stop_price": "174.50", "operator_id": "test"})
    except ValueError as exc:
        assert "STALE_RUNTIME" in str(exc) or "ACTIVE_CURRENT" in str(exc)
    else:
        raise AssertionError("incomplete ticket allowed captured_manually")


def test_complete_fixture_displays_entry_qty_stop_risk_and_allows_capture(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root)
    projection = build_trade_candidate_projection_v1(truth_root=root, day_utc=DAY)
    ticket = projection["trade_ticket_projection_v1"]

    assert ticket["trade_ticket_status"] == "complete"
    assert ticket["entry_reference_price"] == "180.25"
    assert ticket["suggested_quantity"] == 3
    assert ticket["suggested_notional"] == "540.75"
    assert ticket["stop_price"] == "174.5"
    assert ticket["risk_per_share"] == "5.75"
    assert ticket["max_loss_estimate"] == "17.25"

    try:
        append_manual_capture_record_v1(truth_root=root, day_utc=DAY, request_payload={"selected_exposure_intent_id": INTENT_ID, "capture_status": "captured_manually", "quantity": "3", "fill_price": "180.50", "fill_time": f"{DAY}T15:30:00Z", "stop_price": "174.50", "operator_id": "test"})
    except ValueError as exc:
        assert "STALE_RUNTIME" in str(exc) or "ACTIVE_CURRENT" in str(exc)
    else:
        raise AssertionError("manual capture saved without ACTIVE_CURRENT lineage")


def test_ui_displays_missing_upstream_causes_and_required_ticket_fields() -> None:
    pages = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")
    block = pages.split("function renderTradeTicketSummary", 1)[1].split("function renderSuppressedWatchlistProjectionPanel", 1)[0]

    for text in ["Entry:", "Qty:", "Notional:", "Stop:", "Risk:", "Status:", "why_missing", "upstream_step", "Next action"]:
        assert text in block
    assert "Manual capture only" in pages

def test_manual_capture_route_requires_active_lineage_and_has_no_side_effects(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    _seed(root)
    request = {"selected_exposure_intent_id": INTENT_ID, "capture_status": "not_captured", "notes": "review only", "operator_id": "test"}
    try:
        append_manual_capture_record_v1(truth_root=root, day_utc=DAY, request_payload=request)
    except ValueError as exc:
        assert "STALE_RUNTIME" in str(exc) or "ACTIVE_CURRENT" in str(exc)
        assert "broker_execution_allowed" in str(exc)
    else:
        raise AssertionError("manual capture saved without ACTIVE_CURRENT lineage")


def test_ui_uses_trade_candidate_projection_and_shows_required_controls() -> None:
    pages = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")
    server = (ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py").read_text(encoding="utf-8")
    projection_block = pages.split("function renderManualCaptureProjectionPanel", 1)[1].split("function renderSuppressedWatchlistProjectionPanel", 1)[0]
    form_block = pages.split("function renderManualCaptureRecordForm", 1)[1].split("function renderOperatorReadinessProjectionPanel", 1)[0]
    assert "snapshot.trade_candidate_projection_v1" in projection_block
    assert "manual_capture_candidate_v1" not in projection_block
    assert "Entry reference" in form_block
    assert "Quantity" in form_block
    assert "Fill price" in form_block
    assert "Fill time" in form_block
    assert "ticket_lineage_hash" in form_block
    assert "paper_trade_construction_id" not in form_block
    assert "data-manual-capture-draft-field=\"fill_time\"" in form_block
    assert "data-manual-capture-validation-summary" in form_block
    assert "Record Manual Capture" in form_block
    assert "<dialog" in form_block
    assert "manual-capture-dialog" in form_block
    assert "data-manual-capture-open" in form_block
    assert "manual-capture-primary-row" in form_block
    assert "manual-capture-primary-cta" in form_block
    assert "Record manual capture" in form_block
    assert 'name="capture_status" data-manual-capture-draft-field="capture_status" value="${escapeHtml(captureStatusValue === "partial" ? "partial" : "captured")}"' in form_block
    assert 'value="captured_manually"' not in form_block
    assert "manual-capture-modal-footer" in form_block
    assert "Technical evidence" not in form_block
    assert "Recording this only documents your manual action" in form_block
    assert "trade_candidate_projection_v1" in server


def test_manual_capture_page_promotes_operator_cta_before_diagnostics() -> None:
    pages = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")
    css = (ROOT / "constellation_2/phaseL/ui/static/aegis.css").read_text(encoding="utf-8")
    projection_block = pages.split("function renderManualCaptureProjectionPanel", 1)[1].split("function renderSuppressedWatchlistProjectionPanel", 1)[0]

    cta_index = projection_block.index("renderManualCaptureRecordForm")
    diagnostics_index = projection_block.index("Technical diagnostics")
    ticket_summary_index = projection_block.index("renderTradeTicketSummary")
    assert cta_index < diagnostics_index < ticket_summary_index
    assert '<details class="raw-drawer manual-capture-diagnostics"' in projection_block
    assert '<details class="raw-drawer manual-capture-diagnostics" style="margin-top:12px;">' in projection_block
    assert "manual-capture-primary-row" in pages
    assert "manual-capture-primary-cta" in pages
    assert "grid-template-columns: minmax(0, 1fr) auto" in css
    assert ".manual-capture-primary-row" in css
    assert "justify-content: flex-start" in css
    assert ".manual-capture-operator-facts" in css
    assert "margin-top: 0" in css


def test_manual_capture_browser_payload_and_draft_hooks_are_present() -> None:
    pages = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")
    main = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js").read_text(encoding="utf-8")
    css = (ROOT / "constellation_2/phaseL/ui/static/aegis.css").read_text(encoding="utf-8")

    assert "export function buildManualCaptureRecordPayload" in pages
    assert "fill_time_local" in pages
    assert "fill_time_utc" in pages
    assert "new Date(raw)" in pages
    assert "toISOString()" in pages
    assert "Fill time is required." in pages
    assert "Fill time format is invalid." in pages
    assert "Fill price must be positive." in pages
    assert "Quantity must be positive." in pages
    assert "saveManualCaptureDraft" in main
    assert "localStorage.setItem(key, JSON.stringify" in main
    assert "applyManualCaptureFieldErrors" in main
    assert "openManualCaptureDialog" in main
    assert "closeManualCaptureDialog" in main
    assert "manualCaptureHasUnsavedDraft" in main
    assert "renderManualCaptureSuccess" in main
    assert "data-manual-capture-save-button" in main
    assert "validateManualCaptureForm" in main
    assert "operatorManualCaptureMessage" in main
    assert "requestSubmit" in main
    assert "setManualCaptureSaving" in main
    assert "Discard unsaved manual capture draft?" in main
    assert ".manual-capture-dialog" in css
    assert "max-height: calc(100dvh - 32px)" in css
    assert ".manual-capture-modal-shell" in css
    assert "grid-template-rows: auto minmax(0, 1fr) auto" in css
    assert ".manual-capture-modal-body" in css
    assert "overflow-y: auto" in css
    assert "overflow-x: hidden" in css
    assert ".manual-capture-modal-footer" in css
    assert "max-width: calc(100vw - 32px)" in css
    assert "body.manual-capture-modal-open" in css


def test_manual_capture_modal_viewport_constraints_keep_footer_visible() -> None:
    pages = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")
    main = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js").read_text(encoding="utf-8")
    css = (ROOT / "constellation_2/phaseL/ui/static/aegis.css").read_text(encoding="utf-8")

    assert "Record capture" in pages
    assert "Save manual capture record" not in pages
    assert "Refresh ticket" not in pages
    assert "Cancel" in pages
    assert 'evidence"><summary>Technical evidence</summary>' not in pages
    assert 'document.body.classList.add("manual-capture-modal-open")' in main
    assert 'document.body.classList.remove("manual-capture-modal-open")' in main
    assert 'event.key === "Enter"' in main
    assert "[data-manual-capture-save-button]" in main
    assert 'event.key === "Escape"' in main
    assert "width: min(760px, calc(100vw - 32px))" in css
    assert "max-height: calc(100dvh - 32px)" in css
    assert "overflow: hidden" in css
    assert "grid-template-rows: auto minmax(0, 1fr) auto" in css
    assert ".manual-capture-modal-footer" in css
    assert "@media (max-width: 760px)" in css


def test_completed_capture_is_historical_not_active_workflow() -> None:
    pages = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")
    css = (ROOT / "constellation_2/phaseL/ui/static/aegis.css").read_text(encoding="utf-8")
    projection_block = pages.split("function renderManualCaptureProjectionPanel", 1)[1].split("function renderSuppressedWatchlistProjectionPanel", 1)[0]
    completed_branch = projection_block.split("if (completedCapture)", 1)[1].split("const chips = [", 1)[0]

    assert 'title: "Manual Capture Completed"' in completed_branch
    assert 'subtitle: "No further action is required."' in completed_branch
    assert 'aria-label="Completed Captures"' in completed_branch
    assert 'sourceKey: "captured_ticket_projection_v1"' in completed_branch
    assert 'title: "Trade Lifecycle Case / Manual Capture"' not in completed_branch
    assert "Technical diagnostics" not in completed_branch
    assert "Record manual capture" not in completed_branch
    assert "CAPTURE_READY" not in completed_branch
    assert "completed-capture-card" in pages
    assert "Capture recorded successfully" in pages
    assert "No further action is required" in pages
    assert "Export capture" in pages
    assert "View evidence" in pages
    assert ".completed-capture-card" in css
    assert "rgba(34, 197, 94" in css


def test_operator_strip_counts_completed_captures_separately() -> None:
    pages = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")
    summary_block = pages.split("function renderOpportunitiesSummaryStrip", 1)[1].split("function operatorSnapshotHasUnresolvedHealth", 1)[0]

    assert "manualCaptureCompleted" in summary_block
    assert "manual.selected_exposure_intent_id || manual.capture_record_id ? 1 : 0" in summary_block
    assert "const manualCaptureTickets = manualCaptureCompleted ? 0 : rawManualCaptureTickets" in summary_block
    assert "Completed captures today" in summary_block
    assert "completedCaptures" in summary_block


def test_manual_capture_operator_modal_is_transactional_not_diagnostics() -> None:
    pages = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js").read_text(encoding="utf-8")
    main = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js").read_text(encoding="utf-8")
    css = (ROOT / "constellation_2/phaseL/ui/static/aegis.css").read_text(encoding="utf-8")
    form_block = pages.split("function renderManualCaptureRecordForm", 1)[1].split("function renderOperatorReadinessProjectionPanel", 1)[0]

    assert "Capture status" not in form_block
    assert "Operator ID" not in form_block
    assert "Technical evidence" not in form_block
    assert "Record capture" in form_block
    assert "Save manual capture record" not in form_block
    assert "validateManualCaptureForm(form, { showErrors: true })" in main
    assert "saveButton.disabled" in main
    assert "Recording capture..." in main
    assert "Manual capture recorded" in main
    assert "No broker action was taken" in main
    assert ".compact-transaction-fields" in css
    assert ".manual-capture-success-ticket" in css


def test_manual_capture_modal_qa_script_exists() -> None:
    qa = (ROOT / "constellation_2/phaseL/ui/docs/manual_capture_modal_qa_v1.md").read_text(encoding="utf-8")
    assert "1366x768" in qa
    assert "1440x900" in qa
    assert "1600x900" in qa
    assert "1920x1080" in qa
    assert "Record capture" in qa
    assert "fill_time_local" in qa
    assert "fill_time_utc" in qa
    assert "No broker action was taken" in qa
