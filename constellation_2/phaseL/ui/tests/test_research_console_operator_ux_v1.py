from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from constellation_2.phaseL.ui.tests.operator_shell_test_sources import pages_source_v1

from ops.aegis.operator_action_command_contracts_v1 import command_registry_v1

from ops.aegis.research_lab.research_console_v1 import (
    blocked_evidence_v1,
    convert_hypothesis_v1,
    paper_trials_v1,
    research_backlog_v1,
    research_console_v1,
    evidence_v1,
    research_hypothesis_queue_v1,
    research_intake_dossier_v1,
    review_hypothesis_v1,
    assess_hypothesis_v1,
    sleeve_review_center_v1,
    start_research_v1,
)
from research_lab.storage.manifest_io import append_jsonl
from ops.aegis.research_lab.research_pipeline_v1 import _operator_blocker_v1

REPO = Path(__file__).resolve().parents[4]
PAGES = REPO / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "pages" / "index.js"
NAVIGATION = REPO / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "navigation_schema.js"
SERVER = REPO / "constellation_2" / "phaseL" / "ui" / "server" / "run_ops_dashboard_v1.py"


def _seed_ready_research_store(store: Path) -> dict:
    append_jsonl(store / "registries" / "cost_model_snapshots.jsonl", {"cost_model_snapshot_id": "cm_default", "schema_version": "cost_model_snapshot.v1", "content_hash": "abc123abc123"})
    dataset_dir = store / "datasets" / "ds_ready"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    (dataset_dir / "manifest.json").write_text(
        '{"quality_status":"pass","row_count":120,"symbols_loaded":["SPY","QQQ","IWM","TLT","GLD"],"symbol_count":5,"start_date":"2024-01-01","end_date":"2024-06-30"}',
        encoding="utf-8",
    )
    append_jsonl(
        store / "registries" / "dataset_snapshots.jsonl",
        {
            "dataset_snapshot_id": "ds_ready",
            "quality_status": "pass",
            "row_count": 120,
            "symbols": ["SPY", "QQQ", "IWM", "TLT", "GLD"],
            "symbol_count": 5,
            "start_date": "2024-01-01",
            "end_date": "2024-06-30",
        },
    )
    universe_dir = store / "universes" / "us_ready"
    universe_dir.mkdir(parents=True, exist_ok=True)
    (universe_dir / "universe_snapshot.json").write_text(
        '{"universe_snapshot_id":"us_ready","universe_name":"Ready ETF universe","symbols":["SPY","QQQ","IWM","TLT","GLD"],"symbol_count":5}',
        encoding="utf-8",
    )
    append_jsonl(
        store / "registries" / "universe_snapshots.jsonl",
        {
            "universe_snapshot_id": "us_ready",
            "universe_name": "Ready ETF universe",
            "symbol_count": 5,
        },
    )
    return start_research_v1({"idea": "Volatility compression breakout", "symbols": "SPY QQQ IWM TLT GLD", "priority": "watchlist"}, store_root=store)


def test_research_blocker_classification_is_operator_actionable() -> None:
    market = _operator_blocker_v1(
        hypothesis={"missing_symbols": ["DBC", "USO", "XLE"]},
        gate="TESTING",
        status="BLOCKED",
        blocker="missing_required_symbols",
        spec={"universe": ["DBC", "USO", "XLE"]},
    )
    assert market["title"] == "Waiting for current intraday market data"
    assert market["recovery_strategy"] == "WAITING_ON_MARKET_REFRESH"
    assert market["operator_action_required"] is False
    assert market["missing_items"] == ["DBC", "USO", "XLE"]
    assert "No operator action required" not in market["next_action"]
    assert market["data_acquisition_plan"]["source_type"] == "AEGIS_CAN_FETCH_AUTOMATICALLY"
    assert market["data_acquisition_plan"]["action_label"] == "Fetch market data now"
    assert market["data_acquisition_plan"]["action_available"] is True
    assert market["data_acquisition_plan"]["retry_schedule"]

    upload = _operator_blocker_v1(
        hypothesis={"missing_datasets": ["Macro event calendar"]},
        gate="TESTING",
        status="BLOCKED",
        blocker="macro_event_calendar_missing",
        spec={"universe": []},
    )
    assert upload["title"] == "Blocked: External dataset required"
    assert upload["recovery_strategy"] == "WAITING_ON_OPERATOR_UPLOAD"
    assert upload["operator_action_required"] is True
    assert upload["action_kind"] == "UPLOAD_DATASET"
    assert upload["data_acquisition_plan"]["source_type"] == "USER_MUST_UPLOAD"
    assert upload["data_acquisition_plan"]["accepted_format"] == "CSV with columns: date,event_name,country,impact"
    assert upload["data_acquisition_plan"]["action_available"] is False

    unsupported = _operator_blocker_v1(
        hypothesis={"missing_datasets": ["Satellite refinery outage feed"]},
        gate="TESTING",
        status="BLOCKED",
        blocker="satellite_refinery_feed_not_supported",
        spec={"universe": []},
    )
    assert unsupported["title"] == "Blocked: Data source not currently supported"
    assert unsupported["data_acquisition_plan"]["source_type"] == "NOT_CURRENTLY_SUPPORTED"
    assert unsupported["data_acquisition_plan"]["action_available"] is False

    provider = _operator_blocker_v1(
        hypothesis={"missing_symbols": ["DBC"]},
        gate="TESTING",
        status="BLOCKED",
        blocker="PROVIDER_NOT_CONFIGURED",
        spec={"universe": ["DBC"]},
    )
    assert provider["title"] == "Blocked: Market data provider is not configured"
    assert provider["data_acquisition_plan"]["source_type"] == "USER_MUST_CONFIGURE_PROVIDER"
    assert "AEGIS_MARKET_DATA_PRIMARY_PROVIDER" in provider["data_acquisition_plan"]["required_config_keys"]

    stale = _operator_blocker_v1(
        hypothesis={"missing_symbols": ["SPY"]},
        gate="TESTING",
        status="BLOCKED",
        blocker="stale_market_data",
        spec={"universe": ["SPY"]},
    )
    assert stale["title"] == "Blocked: Market data is stale"
    assert stale["recovery_strategy"] == "WAITING_ON_MARKET_REFRESH"
    assert stale["operator_action_required"] is False
    assert stale["data_acquisition_plan"]["last_attempt_status"] == "STALE"


def test_research_left_nav_is_single_operator_pipeline() -> None:
    source = NAVIGATION.read_text(encoding="utf-8")
    for label in ["Dashboard", "Hypotheses", "Captured Trades", "Evidence", "System Health"]:
        assert label in source
    primary = source.split("export function flattenNavigation", 1)[0]
    for subsystem_label in ["Hypothesis Queue", "Research Plans", "Paper Trials", "Sleeve Reviews", "Blocked Work", "Research Backlog"]:
        assert subsystem_label not in primary


def test_research_console_renders_without_search() -> None:
    source = pages_source_v1(REPO)
    assert "New Research Idea" in source
    assert "What are you investigating?" in source
    assert "Start Research" in source
    assert "Advanced research settings" in source
    assert "What deserves attention now" in source


def test_start_research_form_creates_hypothesis_proposal_chain() -> None:
    with TemporaryDirectory() as tmp:
        store = Path(tmp)
        append_jsonl(store / "registries" / "cost_model_snapshots.jsonl", {"cost_model_snapshot_id": "cm_default", "schema_version": "cost_model_snapshot.v1", "content_hash": "abc123abc123"})
        result = start_research_v1({"idea": "Oil shock reversals", "symbols": "USO XLE DBC SPY", "priority": "watchlist"}, store_root=store)
        assert result["ok"] is True
        assert result["event_observation_id"]
        assert result["event_cluster_id"]
        assert result["intent_candidate_id"]
        assert result["hypothesis_proposal_id"]
        queue = research_hypothesis_queue_v1(store_root=store)
        assert any(row["hypothesis_proposal_id"] == result["hypothesis_proposal_id"] for row in queue["queue"])


def test_hypothesis_queue_displays_all_proposals_not_single_card() -> None:
    with TemporaryDirectory() as tmp:
        store = Path(tmp)
        append_jsonl(store / "registries" / "cost_model_snapshots.jsonl", {"cost_model_snapshot_id": "cm_default", "schema_version": "cost_model_snapshot.v1", "content_hash": "abc123abc123"})
        first = start_research_v1({"idea": "Oil shock reversals", "symbols": "USO XLE DBC SPY", "priority": "watchlist"}, store_root=store)
        second = start_research_v1({"idea": "Gap-fill after large overnight moves", "symbols": "SPY QQQ", "priority": "watchlist"}, store_root=store)
        queue = research_hypothesis_queue_v1(store_root=store)
        ids = {row["hypothesis_proposal_id"] for row in queue["queue"]}
        assert first["hypothesis_proposal_id"] in ids
        assert second["hypothesis_proposal_id"] in ids
        assert len(queue["queue"]) >= 2


def test_hypothesis_queue_lanes_and_oil_shock_visible() -> None:
    queue = research_hypothesis_queue_v1()
    assert "proposed" in queue["lanes"]
    assert "watchlist" in queue["lanes"]
    assert "needs_data" in queue["lanes"]
    assert any(row["hypothesis_proposal_id"] == "ehp_cdbd8fe683acb622" for row in queue["queue"])


def test_dossier_shows_readiness_and_intraday_data_status() -> None:
    dossier = research_intake_dossier_v1(hypothesis_proposal_id="ehp_cdbd8fe683acb622")
    assert dossier["ok"] is True
    assert "readiness" in dossier
    assert dossier["readiness"].get("blocking_items", []) == []
    assert dossier["readiness"].get("operator_data_status") == "Intraday market data available."


def test_accept_is_enabled_but_convert_remains_gated_without_dataset_snapshot() -> None:
    queue = research_hypothesis_queue_v1()
    oil = next(row for row in queue["queue"] if row["hypothesis_proposal_id"] == "ehp_cdbd8fe683acb622")
    assert oil["accept_for_research_enabled"] is True
    assert oil["convert_to_research_plan_enabled"] is False
    with pytest.raises(Exception):
        convert_hypothesis_v1("ehp_cdbd8fe683acb622", {"approve": True})


def test_paper_trial_panel_and_due_outcomes_surface() -> None:
    panel = paper_trials_v1()
    assert panel["paper_trials"]
    assert any(row["paper_trial_id"] for row in panel["paper_trials"])
    assert all("due_outcomes" in row for row in panel["paper_trials"])


def test_sleeve_review_center_shows_current_sleeve_state() -> None:
    panel = sleeve_review_center_v1()
    assert panel["sleeves"]
    assert any(row["sleeve_id"] == "slv_etf_drop_reversion_v1" for row in panel["sleeves"])


def test_blocked_evidence_distinguishes_external_macro_blockers() -> None:
    blocked = blocked_evidence_v1()
    assert not any(row.get("event_family_id") == "oil_shock" for row in blocked["blocked_items"])
    assert any("missing_macro_event_calendar" in row.get("why_blocked", []) for row in blocked["blocked_items"])


def test_research_backlog_has_priority_items() -> None:
    backlog = research_backlog_v1()
    assert backlog["backlog"]
    assert all("recommended_next_action" in row for row in backlog["backlog"])


def test_evidence_page_payload_is_readable_and_safe() -> None:
    evidence = evidence_v1()
    assert evidence["ok"] is True
    assert "Event Studies" in evidence["tabs"]
    assert "Backtests and model outputs are hypothetical research evidence" in evidence["hypothetical_evidence_label"]


def test_research_console_safety_labels_and_no_forbidden_routes() -> None:
    console = research_console_v1()
    labels = set(console["safety_labels"])
    assert {"READ-ONLY GOVERNANCE", "NO BROKER EXECUTION", "NO LIVE TRADING", "MANUAL REVIEW REQUIRED"}.issubset(labels)
    server = SERVER.read_text(encoding="utf-8")
    forbidden_near_research = ["place_trade", "submit_order", "mutate_sleeve", "allocate_capital", "promote automatically"]
    assert not any(term in server.lower() for term in forbidden_near_research)


def test_operator_research_routes_are_first_class_pages() -> None:
    source = pages_source_v1(REPO)
    assert '"/research-lab": "/aegis-edge-lab"' not in source
    assert 'case "research_lab"' in source
    assert 'case "research_start"' in source
    assert 'case "research_hypothesis_queue"' in source
    assert "What deserves attention now" in source
    assert "Advanced / Provenance" in source


def test_research_console_action_response_reports_visible_transition() -> None:
    with TemporaryDirectory() as tmp:
        store = Path(tmp)
        created = _seed_ready_research_store(store)
        hypothesis_id = created["hypothesis_proposal_id"]
        before = research_hypothesis_queue_v1(store_root=store)
        before_row = next(row for row in before["queue"] if row["hypothesis_proposal_id"] == hypothesis_id)
        assert before_row["lane"] == "proposed"
        assert before_row["accept_for_research_enabled"] is True

        result = review_hypothesis_v1(hypothesis_id, {"decision": "accept_for_research", "reason": "Ready for governed research."}, store_root=store)

        assert result["ok"] is True
        assert result["success"] is True
        assert result["previous_state"] == "IDEA"
        assert result["new_state"] == "RESEARCHING"
        assert result["previous_lane"] == "proposed"
        assert result["new_lane"] == "accepted_for_research"
        assert result["updated_lane"] == "accepted_for_research"
        assert result["projection_refreshed"] is True
        assert result["operator_message"] == "Accepted for Research."
        assert result["broker_execution_allowed"] is False
        assert result["autonomous_trading_allowed"] is False

        after = research_hypothesis_queue_v1(store_root=store)
        assert hypothesis_id in {row["hypothesis_proposal_id"] for row in after["lanes"]["accepted_for_research"]}


def test_research_console_watchlist_reject_archive_transitions_are_explicit() -> None:
    for decision, expected_lane, expected_state in [("watchlist", "watchlist", "WATCHLIST"), ("reject", "rejected", "ARCHIVED"), ("archive", "archived", "ARCHIVED")]:
        with TemporaryDirectory() as tmp:
            store = Path(tmp)
            created = _seed_ready_research_store(store)
            result = review_hypothesis_v1(created["hypothesis_proposal_id"], {"decision": decision, "reason": f"Move to {decision}."}, store_root=store)
            assert result["previous_lane"] == "proposed"
            assert result["new_lane"] == expected_lane
            assert result["new_state"] == expected_state
            assert result["operator_message"]


def test_research_console_assess_readiness_returns_refresh_contract() -> None:
    with TemporaryDirectory() as tmp:
        store = Path(tmp)
        created = _seed_ready_research_store(store)
        result = assess_hypothesis_v1(created["hypothesis_proposal_id"], store_root=store)
        assert result["ok"] is True
        assert result["projection_refreshed"] is True
        assert result["previous_lane"] == result["new_lane"]
        assert result["operator_message"] == "Readiness assessed. The Research Pipeline was refreshed."


def test_research_console_convert_to_plan_returns_visible_transition() -> None:
    with TemporaryDirectory() as tmp:
        store = Path(tmp)
        created = _seed_ready_research_store(store)
        hypothesis_id = created["hypothesis_proposal_id"]
        review_hypothesis_v1(hypothesis_id, {"decision": "accept_for_research", "reason": "Ready for governed research."}, store_root=store)

        result = convert_hypothesis_v1(hypothesis_id, {"approve": True}, store_root=store)

        assert result["ok"] is True
        assert result["success"] is True
        assert result["previous_state"] == "RESEARCHING"
        assert result["new_state"] == "VALIDATING"
        assert result["operator_message"] == "Converted to Research Plan."
        assert result["conversion_inputs"] == {
            "dataset_snapshot_id": "ds_ready",
            "universe_snapshot_id": "us_ready",
            "start": "2024-01-01T00:00:00Z",
            "end": "2024-06-30T00:00:00Z",
        }
        assert result["research_plan"]["research_plan_id"]
        assert result["broker_execution_allowed"] is False
        assert result["autonomous_trading_allowed"] is False



def test_research_queue_api_exposes_discovery_counts_and_lifecycle() -> None:
    with TemporaryDirectory() as tmp:
        store = Path(tmp)
        append_jsonl(store / "registries" / "cost_model_snapshots.jsonl", {"cost_model_snapshot_id": "cm_default", "schema_version": "cost_model_snapshot.v1", "content_hash": "abc123abc123"})
        first = start_research_v1({"idea": "Index volatility breakout", "symbols": "SPY QQQ", "priority": "high"}, store_root=store)
        second = start_research_v1({"idea": "Treasury duration squeeze", "symbols": "TLT IEF", "priority": "low"}, store_root=store)
        review_hypothesis_v1(second["hypothesis_proposal_id"], {"decision": "archive", "reason": "Archive test item."}, store_root=store)

        queue = research_hypothesis_queue_v1(store_root=store)

        assert queue["count"] == len(queue["queue"])
        assert queue["archived_count"] == 1
        assert queue["non_archived_count"] == queue["discoverable_count"] == queue["count"] - 1
        assert queue["counts_by_lifecycle"].get("Archived") == 1
        row = next(item for item in queue["queue"] if item["hypothesis_proposal_id"] == first["hypothesis_proposal_id"])
        assert row["lifecycle_state"] in {"Ideas", "Blocked"}
        assert row["tier"] in {"High", "Blocked", "Watchlist"}
        assert row["created_at"]
        assert row["updated_at"]
        assert "spy" in row["search_text"]
        assert isinstance(row["operator_action_required"], bool)
        assert row["operator_attention_required"] == row["operator_action_required"]
        assert "priority" in row
        assert "recent" in row


def test_research_console_api_exposes_normalized_inventory_contract() -> None:
    payload = research_console_v1()
    rows = payload["all_hypotheses"]

    assert payload["combined_hypothesis_count"] == len(rows)
    assert payload["search_index_integrity_status"] == "ok"
    assert payload["counts_by_source"]
    assert payload["counts_by_lifecycle"]
    assert payload["counts_by_attention"]["operator_action_required"] + payload["counts_by_attention"]["no_operator_action_required"] == len(rows)

    required = {
        "hypothesis_id",
        "title",
        "source_type",
        "symbols",
        "event_type",
        "lifecycle_state",
        "tier",
        "rank",
        "data_status",
        "blocker_summary",
        "next_action",
        "operator_action_required",
        "created_at",
        "updated_at",
        "search_text",
        "research_run_state",
        "user_facing_status",
        "user_facing_explanation",
        "last_research_run",
        "trigger_source",
        "started_by_user",
        "primary_action_label",
    }
    assert rows
    assert required <= set(rows[0])
    assert payload["research_run_ledger"]["schema_id"] == "research_run_ledger.v1"
    assert "research_run_ledger.v1.jsonl" in payload["research_run_ledger"]["path"]
    model = payload["hypothesis_view_model_v1"]
    assert model["schema_id"] == "hypothesis_view_model.v1"
    assert set(model["sections"]) >= {"recommendations_ready", "ready_to_start", "researching", "waiting", "blocked", "completed"}
    ready = model["sections"]["ready_to_start"]
    assert model["raw_hypothesis_count"] == len(rows)
    assert model["rendered_hypothesis_count"] == sum(len(section["visible_items"]) for section in model["sections"].values())
    assert model["raw_hypothesis_count"] == model["rendered_hypothesis_count"]
    assert model["unmapped_hypothesis_ids"] == []
    assert all(section["count"] == len(section["visible_items"]) for section in model["sections"].values())
    if ready["count"]:
        assert ready["top_item"] == ready["visible_items"][0]
    assert all(row["source_type"] in {"Governed Research", "Research Store"} for row in rows)
    assert all(row["hypothesis_id"] and row["title"] and row["search_text"] for row in rows)
    assert all(row["research_run_state"].get("schema_id") == "hypothesis_run_state_projection.v1" for row in rows)


def test_research_ui_is_hypotheses_first_with_diagnostics_secondary() -> None:
    source = pages_source_v1(REPO)
    render_order = source[source.index("function renderResearchDiscoveryPanels"):source.index("function researchBlockedWhy")]
    workspace = source[source.index("function renderHypothesesWorkspace"):source.index("function researchFilterTokens")]

    assert "return [renderHypothesesWorkspace(payload, state)]" in render_order
    for label in ["ready_to_start", "researching", "waiting", "recommendations_ready", "blocked", "completed"]:
        assert label in source
    primary = source[source.index("function hypothesisPrimaryAction"):source.index("function hypothesisSecondaryActions")]
    action_bar = source[source.index("function renderHypothesisActionBar"):source.index("function renderHypothesisCard")]
    commands = command_registry_v1()["commands_by_id"]
    for command_id in ["START_RESEARCH", "VIEW_QUEUE", "VIEW_PROGRESS", "VIEW_WAITING_REASON", "VIEW_FINDINGS", "VIEW_BLOCKER", "VIEW_RECOMMENDATION"]:
        assert commands[command_id]["label"]
    assert "row.primary_command" in primary
    assert "hypothesis-more-menu" in action_bar
    assert "renderResearchInventoryPanel(payload)" not in render_order
    assert "renderResearchFocusAttentionPanel(payload)" not in render_order
    assert "renderResearchPipelineSummaryPanel(payload)" not in render_order
    assert '<details class="hypotheses-diagnostics"' in source
    diagnostics = source[source.index("function renderHypothesesDiagnosticsPanel"):source.index("function renderHypothesesWorkspace")]
    assert "renderResearchInventoryPanel(payload)" in diagnostics
    assert "renderResearchPipelineSummaryPanel(payload)" in diagnostics
    assert "renderHypothesesDiagnosticsPanel(payload, state)" not in workspace

def test_research_inventory_visual_contract_and_search_filtering() -> None:
    source = pages_source_v1(REPO)
    css = (REPO / "constellation_2" / "phaseL" / "ui" / "static" / "aegis.css").read_text(encoding="utf-8")
    main = (REPO / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "main.js").read_text(encoding="utf-8")

    inventory_block = source[source.index("function renderResearchInventoryPanel"):source.index("function renderResearchHypothesisSearchControl")]
    assert "researchAllHypothesisRows(payload)" in inventory_block
    assert "filter((row) => !researchIsArchived(row))" in inventory_block
    assert "<th>Title</th>" in inventory_block
    assert "<th>Source</th>" in inventory_block
    assert "<th>Symbols</th>" in inventory_block
    assert "<th>Lifecycle</th>" in inventory_block
    assert "<th>Tier / Rank</th>" in inventory_block
    assert "<th>Data Status</th>" in inventory_block
    assert "<th>Blocker</th>" in inventory_block
    assert "<th>Next Action</th>" in inventory_block
    assert "<th>Operator Action Required</th>" in inventory_block
    assert "<th>Action</th>" in inventory_block
    assert "<th>Last Updated</th>" in inventory_block
    assert "<th>Open</th>" in inventory_block
    assert "No hypotheses found." in inventory_block
    assert "data-research-datasource-fatal" in inventory_block
    assert "researchInventoryDatasourceError" in source
    assert ".research-inventory-table" in css
    assert ".research-datasource-fatal" in css
    assert ".research-inventory-scroll" in css
    assert "const inventoryRows = Array.from(pageRoot.querySelectorAll(\"[data-research-inventory-row]\"))" in main
    assert 'const activeFilter = query ? "all"' in main
    assert 'filterRegion.dataset.activeFilter = "all"' in main
    assert "rawQuery ? `${visibleCount} results for ${rawQuery}`" in main
    assert "data-research-no-results" in main
    assert "research-result-count" in source
    assert ".research-result-count" in css




def test_research_inventory_operator_action_labels_are_display_only() -> None:
    source = pages_source_v1(REPO)
    helper_block = source[source.index("function researchOperatorActionDescriptor"):source.index("function researchFilterTokens")]
    inventory_block = source[source.index("function renderResearchInventoryPanel"):source.index("function renderResearchHypothesisSearchControl")]

    assert "ready_for_human_review" in helper_block
    assert "Review and decide" in helper_block
    assert "Review inconclusive result" in helper_block
    assert "Review result" in helper_block
    assert "Provide earnings/event calendar" in helper_block
    assert "View data format" in helper_block
    assert "Upload required dataset" in helper_block
    assert "View requirements" in helper_block
    assert "Data required before testing" in helper_block
    assert "View missing data" in helper_block
    assert "No action required" in helper_block
    assert "researchOperatorActionButton(row)" in inventory_block
    assert "researchOperatorDataStatusText(row)" in inventory_block
    assert "research-row-action" in source

def test_research_inventory_search_is_global_metadata_driven() -> None:
    source = pages_source_v1(REPO)
    helper_block = source[source.index("function researchNormalizeInventoryRow"):source.index("function renderResearchInventoryPanel")]

    assert "payload.all_hypotheses" in helper_block
    assert "researchQueuePayload" not in helper_block
    assert "researchAegisRows" not in helper_block
    assert "row.search_text" in helper_block
    assert "row.title" in helper_block
    assert "row.hypothesis_id" in helper_block
    assert "row.event_type" in helper_block
    assert "row.source_type" in helper_block
    assert "row.blocker_summary" in helper_block
    assert "row.next_action" in helper_block
    assert "safeList(row.symbols" in helper_block
    assert "researchDiscoverySearchText(row).includes(needle)" in source


def test_research_inventory_has_no_hypothesis_specific_special_case() -> None:
    source = pages_source_v1(REPO)
    helper_block = source[source.index("function researchNormalizeInventoryRow"):source.index("function researchBlockedWhy")]
    forbidden = ["NVDA", "NVIDIA", "rh-nvidia"]
    assert "researchQueuePayload" not in helper_block
    assert "researchAegisRows" not in helper_block
    assert not any(term in helper_block for term in forbidden)

def test_research_pipeline_action_ui_has_feedback_and_lane_board() -> None:
    source = pages_source_v1(REPO)
    main = (REPO / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "main.js").read_text(encoding="utf-8")
    assert "renderResearchConsoleActionFeedback" in source
    assert "research-proposal-lanes" in source
    assert "data-research-proposal-card" in source
    assert "Accepting hypothesis..." in main
    assert "Research Pipeline action completed." in main
    assert "research-action-pending" in main
    assert "renderHypothesesWorkspace(consolePayload, state)" in source
    assert "renderHypothesisQueueConsolePanel(payload, state)" in source
    assert 'dataset_snapshot_id: String(formData?.get("dataset_snapshot_id")' in source
    assert 'name="dataset_snapshot_id"' in source
    assert 'name="universe_snapshot_id"' in source
    assert "Conversion needs a ready dataset, universe, and date range." in source
    assert "const canAccept = ready && !accepted" in source
