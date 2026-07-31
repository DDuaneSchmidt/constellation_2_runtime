from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
CSS = ROOT / "constellation_2/phaseL/ui/static/aegis.css"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"
SCHEMA = ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/narrative_operational_analytics.v1.schema.json"


def test_performance_page_orders_summary_narrative_charts_before_trade_tables() -> None:
    source = PAGES.read_text(encoding="utf-8")
    block = source[source.index("function renderPaperTradeEvaluationWorkspace"):source.index("function exitReviewDetailId")]
    assert block.index('title: "P&L Summary"') < block.index('title: "What happened and why"')
    assert block.index('title: "What happened and why"') < block.index('title: "Performance trends"')
    assert block.index('title: "Performance trends"') < block.index('title: "Open Trades"')
    assert 'renderNarrativeStatementList(payload, ["PERFORMANCE", "SLEEVE_ATTRIBUTION", "EXIT_REVIEW"])' in block
    assert 'renderNarrativeCharts(payload, "performance")' in block


def test_candidate_funnel_page_orders_summary_narrative_charts_before_drilldown() -> None:
    source = PAGES.read_text(encoding="utf-8")
    block = source[source.index("function renderCandidateFunnelWorkspace"):source.index("if (typeof window !==")]
    assert block.index('title: "Candidate Funnel"') < block.index('title: "What happened and why"')
    assert block.index('title: "What happened and why"') < block.index('title: "Candidate funnel trends"')
    assert block.index('title: "Candidate funnel trends"') < block.index('title: "Blocked / Excluded Candidates"')
    assert 'renderNarrativeStatementList(payload, ["CANDIDATE_FUNNEL", "CERTIFICATION"])' in block
    assert 'renderNarrativeCharts(payload, "candidate_funnel")' in block


def test_narrative_ui_renders_evidence_linked_claims_and_empty_chart_states() -> None:
    source = PAGES.read_text(encoding="utf-8")
    for text in [
        "function renderNarrativeStatementList",
        "data-narrative-operational-analytics",
        "supporting_artifact_path",
        "supporting_metric",
        "function renderNarrativeChart",
        "data-chart-status=\"INSUFFICIENT_HISTORY\"",
        "Not enough history yet.",
        "renderEvidenceTrigger",
        "data-evidence-payload",
    ]:
        assert text in source



def test_narrative_main_surface_hides_raw_paths_behind_evidence_drawer() -> None:
    source = PAGES.read_text(encoding="utf-8")
    statement_block = source[source.index("function renderNarrativeStatementList"):source.index("function renderNarrativeChart")]
    chart_block = source[source.index("function renderNarrativeChart"):source.index("function renderNarrativeCharts")]
    assert '<div><dt>Evidence</dt><dd>${escapeHtml(row.supporting_artifact_path || "missing")}</dd></div>' not in statement_block
    assert '<p class="muted-mini">Evidence:' not in chart_block
    assert "Evidence available" in source
    assert "View Evidence" in source


def test_shared_evidence_drawer_markup_and_runtime_handler_exist() -> None:
    index = (ROOT / "constellation_2/phaseL/ui/static/index.html").read_text(encoding="utf-8")
    main = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js").read_text(encoding="utf-8")
    css = CSS.read_text(encoding="utf-8")
    assert 'id="evidenceDrawer"' in index
    assert 'id="evidenceDrawerBody"' in index
    assert "function openEvidenceDrawer" in main
    assert "data-evidence-payload" in main
    assert "Copy path" in main
    assert "Copy hash" in main
    assert ".evidence-drawer-dialog" in css
    assert ".evidence-summary" in css

def test_narrative_charts_have_dedicated_non_dashboard_heavy_styles() -> None:
    css = CSS.read_text(encoding="utf-8")
    for text in [
        ".narrative-statement-list",
        ".narrative-statement",
        ".narrative-chart-grid",
        ".narrative-chart",
        ".narrative-chart-track",
    ]:
        assert text in css


def test_server_builds_and_exposes_narrative_artifact() -> None:
    server = SERVER.read_text(encoding="utf-8")
    assert "build_narrative_operational_analytics_v1" in server
    assert "write_narrative_operational_analytics_v1" in server
    assert 'payload["narrative_operational_analytics_v1"]' in server
    assert 'payload["source_paths"]["narrative_operational_analytics_v1"]' in server


def test_schema_requires_evidence_for_narrative_statements() -> None:
    schema = SCHEMA.read_text(encoding="utf-8")
    assert "narrative_operational_analytics" in schema
    assert "supporting_artifact_path" in schema
    assert "minLength" in schema
    assert "broker_submit_transmit_allowed" in schema
    assert "autonomous_execution_allowed" in schema
