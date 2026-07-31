from __future__ import annotations

import json
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest
from ops.tools.research_hypotheses_clickthrough_qa_v1 import CdpClient, wait_for_target

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.change_control_v1 import build_report, load_register, validate_register

PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
MAIN = ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js"
ROUTES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/route_metadata.js"
NAV = ROOT / "constellation_2/phaseL/ui/static/operator_shell/navigation_schema.js"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"
DOMAIN_CLIENT = ROOT / "constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js"
REGISTER = ROOT / "aegis/change_control/aegis_change_control_register_v1.json"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_http_ok(url: str, timeout: float = 15.0) -> None:
    import urllib.request

    deadline = time.time() + timeout
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except Exception as exc:
            last_error = exc
        time.sleep(0.25)
    raise RuntimeError(f"Server did not become ready: {last_error}")


def test_change_control_route_is_registered_and_rendered() -> None:
    routes = _text(ROUTES)
    pages = _text(PAGES)
    nav = _text(NAV)
    server = _text(SERVER)

    assert 'path: "/aegis-change-control"' in routes
    assert 'id: "aegis_change_control"' in routes
    assert 'case "aegis_change_control"' in pages
    assert 'renderAegisChangeControlPage' in pages
    assert 'route: "/aegis-change-control"' in nav
    assert '"/aegis-change-control"' in server


def test_change_control_api_is_read_only_and_loads_register() -> None:
    server = _text(SERVER)
    domain = _text(DOMAIN_CLIENT)
    assert '"/api/aegis/change-control"' in server
    assert '"/api/aegis/change-control/latest"' in server
    assert 'fetchAegisChangeControl' in domain
    assert 'recordAegisChangeControlDecision' in domain
    assert 'postJson("/api/aegis/change-control/decision"' in domain
    assert 'patchJson("/api/aegis/change-control' not in domain
    assert 'do_POST' in server
    post_block = server.split('def do_POST', 1)[-1]
    assert '/api/aegis/change-control/decision' in post_block
    for forbidden in ['/api/aegis/change-control/close', '/api/aegis/change-control/validate', '/api/aegis/change-control/implement']:
        assert forbidden not in post_block


def test_change_control_register_report_contains_priority_and_backlog_sections() -> None:
    payload = load_register(REGISTER)
    validation = validate_register(payload)
    report = build_report(payload)

    assert validation["ok"] is True
    assert report["summary"]["total_items"] >= 10
    assert report["summary"]["open_p0_p1_count"] >= 1
    assert report["summary"]["v1_1_backlog_count"] >= 1
    assert "lifecycle_counts" in report
    assert "VALIDATING" in report["lifecycle_counts"]
    assert "validation_dashboard" in report
    assert "decision_dashboard" in report
    assert "relationship_graph" in report
    assert any(row["id"] == "ACC-20260530-008" for row in report["relationship_graph"]["parents"])
    assert isinstance(report["recently_closed"], list)


def test_change_control_page_has_required_sections_and_empty_states() -> None:
    pages = _text(PAGES)
    for required in [
        "Open P0/P1",
        "Lifecycle Pipeline Summary",
        "Dependency Graph",
        "Parent / child records",
        "Completion rollup",
        "Required child items",
        "Validation Dashboard",
        "Decision Dashboard",
        "Closure Evidence",
        "Awaiting Decision",
        "Awaiting Validation",
        "V1.1 Backlog",
        "Recently Closed",
        "Deferred / Rejected",
        "All Items",
        "No change-control items recorded.",
        "No open P0/P1 items.",
        "No items awaiting validation.",
    ]:
        assert required in pages


def test_change_control_page_exposes_validation_required_and_read_only_details() -> None:
    pages = _text(PAGES)
    for required in [
        "Lifecycle status",
        "Decision status",
        "Implementation status",
        "Validation status",
        "Evidence completeness",
        "Implemented does not mean closed",
        "Decision readiness",
        "Implementation readiness",
        "Operator value",
        "Strategic value",
        "Required tests",
        "Required screenshots",
        "Validation state",
        "Linked screenshots",
        "Linked tests",
        "Safety proof",
        "Closure reason",
        "Required children",
        "Required Child Records",
        "Parent validation blocked",
        "Blocked by",
        "Parent cannot move to VALIDATED/CLOSED until required children are complete.",
        "Dependencies",
        "Blockers",
        "This page can record approve, reject, defer, prioritize, and decision-note records.",
        "It cannot create enhancement items, close, validate, implement, or modify evidence-driven records.",
    ]:
        assert required in pages


def test_change_control_page_has_safe_error_state_without_raw_json_primary_ui() -> None:
    pages = _text(PAGES)
    assert "Change Control is unavailable" in pages
    assert "No stale or fake items are shown" in pages
    assert "JSON.stringify" not in pages[pages.index("async function renderAegisChangeControlPage"):pages.index("function renderBlockedDomain")]


def test_change_control_no_forbidden_mutation_controls_exist() -> None:
    pages = _text(PAGES)
    block = pages[pages.index("async function renderAegisChangeControlPage"):pages.index("function renderBlockedDomain")]
    for allowed in ["Approve", "Reject", "Defer", "Prioritize", "Add decision notes", "Record Decision"]:
        assert allowed in pages
    forbidden = ["Save", "Delete", "Close item", "Validate item", "Implement item", "Assign item", "drag", "drop", "data-api-endpoint"]
    for item in forbidden:
        assert item not in block


def test_change_control_strategic_enhancement_records_are_tracked() -> None:
    payload = load_register(REGISTER)
    categories = {row.get("strategic_category") for row in payload["intake_register"]}
    for category in [
        "ACC-RESEARCH-VALIDATION",
        "ACC-RESEARCH-HYPOTHESIS-GENERATION",
        "ACC-PERFORMANCE-HYPOTHESIS-ATTRIBUTION",
        "ACC-PORTFOLIO-ESTIMATED-VALUATION",
        "ACC-EVIDENCE-AUDIT-WORKSPACE",
    ]:
        assert category in categories

    expected_categories = {
        "ACC-RESEARCH-VALIDATION",
        "ACC-RESEARCH-HYPOTHESIS-GENERATION",
        "ACC-PERFORMANCE-HYPOTHESIS-ATTRIBUTION",
        "ACC-PORTFOLIO-ESTIMATED-VALUATION",
        "ACC-EVIDENCE-AUDIT-WORKSPACE",
    }
    strategic = [row for row in payload["intake_register"] if row.get("strategic_category") in expected_categories]
    assert any(row["title"] == "Research Validation Engine" and row["priority"] == "P1" for row in strategic)
    assert any(row["title"] == "Automated Hypothesis Generation" and row["affected_domain"] == "RESEARCH_WORKFLOW" for row in strategic)
    assert any(row["title"] == "Portfolio Attribution by Hypothesis" and row["affected_domain"] == "PERFORMANCE_ANALYTICS" for row in strategic)
    ranks = {row["id"]: row.get("strategic_priority_rank") for row in strategic}
    assert ranks["ACC-20260530-008"] == 1
    assert ranks["ACC-20260530-009"] == 2
    assert ranks["ACC-20260530-010"] == 3
    assert ranks["ACC-20260530-005"] == 4
    assert ranks["ACC-20260530-003"] == 5

    required_fields = [
        "problem_statement",
        "operator_value",
        "strategic_value",
        "non_goals",
        "dependencies",
        "risks",
        "safety_impact",
        "decision_required",
        "decision_question",
        "recommended_option",
        "rejected_options",
        "rationale",
        "implementation_phase",
        "estimated_complexity",
        "required_tests",
        "required_screenshots",
        "required_audit_proof",
        "required_safety_proof",
        "screens_impacted",
        "artifacts_apis_impacted",
        "why_long_term",
    ]
    for row in strategic:
        for field in required_fields:
            assert row.get(field) not in (None, "", []), f"{row['id']} missing {field}"


def test_change_control_v1_1_page_remains_read_only() -> None:
    pages = _text(PAGES)
    block = pages[pages.index("async function renderAegisChangeControlPage"):pages.index("function renderBlockedDomain")]
    assert "Lifecycle Pipeline Summary" in pages
    assert "Validation Dashboard" in pages
    assert "Decision Dashboard" in pages
    for forbidden in ["Edit item", "Close item", "Validate item", "Assign item", "data-drag", "data-drop"]:
        assert forbidden not in block


def test_change_control_parent_expanded_card_shows_required_children_prominently() -> None:
    pages = _text(PAGES)
    block_start = pages.index('function renderChangeControlRequiredChildRecords')
    block_end = pages.index('function renderChangeControlDecisionForm')
    block = pages[block_start:block_end]
    for required in [
        'Required Child Records',
        'Completion rollup',
        'Parent validation blocked',
        'Blocked by',
        '${completeCount}/${children.length} complete',
        'Parent cannot move to VALIDATED/CLOSED until required children are complete.',
        'data-change-control-required-child',
        'renderChangeControlRecordLink(child.id || "", context)',
        'Linked status:',
    ]:
        assert required in block
    item_block = pages[pages.index('function renderChangeControlItemCards'):pages.index('function renderChangeControlTable')]
    assert 'hasRequiredChildren ? " open"' in item_block
    assert 'renderChangeControlRequiredChildRecords(row, context)' in item_block


def test_change_control_dependency_tree_links_open_records_without_searching() -> None:
    pages = _text(PAGES)
    main = _text(MAIN)
    for required in [
        'function changeControlRecordAnchorId',
        'function renderChangeControlRecordLink',
        'data-change-control-record-link',
        'change-control-item-${String(id || "").trim()',
        'renderChangeControlRelationshipNavigator(row, context)',
        'Linked Change Records',
        '<h5>Parent</h5>',
        '<h5>Children</h5>',
        '<h5>Blockers</h5>',
        '<h5>Dependencies</h5>',
    ]:
        assert required in pages
    for child_id in [
        'ACC-20260530-008A',
        'ACC-20260530-008B',
        'ACC-20260530-008C',
        'ACC-20260530-008D',
    ]:
        assert child_id in _text(REGISTER)
    for required in [
        'function revealChangeControlRecordTarget',
        'target.open = true',
        'data-change-control-record-link',
        'await navigateTo(changeControlRecordLink.getAttribute("href") || "/aegis-change-control")',
        'revealCurrentHashTarget(hashPart)',
        'revealCurrentHashTarget();',
    ]:
        assert required in main


def test_change_control_browser_click_opens_child_and_links_back_to_parent() -> None:
    chromium = shutil.which("chromium") or shutil.which("chromium-browser")
    if not chromium:
        pytest.skip("Chromium is not installed")
    server_port = _free_port()
    cdp_port = _free_port()
    server = subprocess.Popen(
        [
            "python3",
            "-B",
            "-m",
            "constellation_2.phaseL.ui.server.run_ops_dashboard_v1",
            "--host",
            "127.0.0.1",
            "--port",
            str(server_port),
        ],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    profile = tempfile.TemporaryDirectory()
    browser = None
    try:
        _wait_http_ok(f"http://127.0.0.1:{server_port}/healthz")
        browser = subprocess.Popen(
            [
                chromium,
                "--headless=new",
                "--disable-gpu",
                "--no-sandbox",
                "--disable-application-cache",
                "--disk-cache-size=0",
                "--media-cache-size=0",
                f"--remote-debugging-port={cdp_port}",
                f"--user-data-dir={profile.name}",
                "--window-size=1440,1100",
                "about:blank",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        cdp = CdpClient(wait_for_target(cdp_port, timeout=10))
        cdp.command("Runtime.enable")
        cdp.command("Page.enable")
        cdp.command("Network.enable")
        cdp.command("Network.setCacheDisabled", {"cacheDisabled": True})
        cdp.command("Page.navigate", {"url": f"http://127.0.0.1:{server_port}/aegis-change-control?day=2026-05-30"})
        time.sleep(4)
        expression = """
(async () => {
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const waitFor = async (predicate) => {
    for (let i = 0; i < 60; i += 1) {
      if (predicate()) return true;
      await sleep(100);
    }
    return false;
  };
  const parent = document.querySelector('[data-change-control-item="ACC-20260530-008"]');
  const childLink = parent?.querySelector('[data-change-control-required-child="ACC-20260530-008A"] [data-change-control-record-link]');
  const parentVisibleText = parent?.innerText || '';
  const rollupVisibleText = parent?.querySelector('[data-change-control-required-children]')?.textContent || '';
  childLink?.click();
  await waitFor(() => window.location.hash === '#change-control-item-ACC-20260530-008A' && document.querySelector('[data-change-control-item="ACC-20260530-008A"]')?.open === true);
  const child = document.querySelector('[data-change-control-item="ACC-20260530-008A"]');
  const childText = child?.innerText || '';
  const parentBackLink = child?.querySelector('[data-change-control-linked-records] [data-change-control-record-link="ACC-20260530-008"]');
  parentBackLink?.click();
  await waitFor(() => window.location.hash === '#change-control-item-ACC-20260530-008' && document.querySelector('[data-change-control-item="ACC-20260530-008"]')?.open === true);
  return {
    parentHasRequiredChildren: /Required Child Records/.test(parentVisibleText),
    parentHasAllChildren: ['ACC-20260530-008A','ACC-20260530-008B','ACC-20260530-008C','ACC-20260530-008D'].every((id) => parentVisibleText.includes(id)),
    parentHasRollup: /Completion rollup/i.test(rollupVisibleText) && rollupVisibleText.includes('0/4 complete'),
    childOpened: child?.open === true,
    childHasParentLink: !!parentBackLink,
    returnedToParent: window.location.hash === '#change-control-item-ACC-20260530-008',
    childText,
  };
})()
"""
        result = cdp.command("Runtime.evaluate", {"expression": expression, "awaitPromise": True, "returnByValue": True})
        value = result["result"].get("value") or {}
        assert value.get("parentHasRequiredChildren") is True
        assert value.get("parentHasAllChildren") is True
        assert value.get("parentHasRollup") is True
        assert value.get("childOpened") is True
        assert value.get("childHasParentLink") is True
        assert value.get("returnedToParent") is True
        assert "Linked Change Records" in value.get("childText", "")
    finally:
        if browser is not None:
            browser.terminate()
            try:
                browser.wait(timeout=5)
            except subprocess.TimeoutExpired:
                browser.kill()
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
        profile.cleanup()



def test_change_control_intelligence_route_api_and_read_only_ui_are_registered() -> None:
    routes = _text(ROUTES)
    pages = _text(PAGES)
    nav = _text(NAV)
    server = _text(SERVER)
    domain = _text(DOMAIN_CLIENT)

    assert 'path: "/aegis-change-control-lab"' in routes
    assert 'id: "aegis_change_control_lab"' in routes
    assert 'case "aegis_change_control_lab"' in pages
    assert 'renderAegisChangeControlLabPage' in pages
    assert 'route: "/aegis-change-control-lab"' in nav
    assert '"/aegis-change-control-lab"' in server
    assert '"/api/aegis/change-control/intelligence/latest"' in server
    assert 'fetchAegisChangeControlIntelligence' in domain
    lab_block = pages[pages.index('function renderChangeControlLabRecommendations'):pages.index('async function renderAegisChangeControlPage')]
    for required in [
        'Recommended Next Actions',
        'Blocked Parent Records',
        'Records Needing David Decision',
        'Highest Risk Items',
        'Stale / Contradictory Records',
        'Suggested Codex Prompt',
        'advisory. The UI does not send it, approve work, validate work, close records, or mutate Change Control',
    ]:
        assert required in lab_block
    for forbidden in ['Record Decision', 'Approve', 'Reject', 'Defer', 'Prioritize', 'Close item', 'Validate item', 'Implement item']:
        assert forbidden not in lab_block


def test_change_control_intelligence_browser_renders_top_recommendation_and_blocked_parent() -> None:
    chromium = shutil.which("chromium") or shutil.which("chromium-browser")
    if not chromium:
        pytest.skip("Chromium is not installed")
    server_port = _free_port()
    cdp_port = _free_port()
    server = subprocess.Popen(
        [
            "python3",
            "-B",
            "-m",
            "constellation_2.phaseL.ui.server.run_ops_dashboard_v1",
            "--host",
            "127.0.0.1",
            "--port",
            str(server_port),
        ],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    profile = tempfile.TemporaryDirectory()
    browser = None
    try:
        _wait_http_ok(f"http://127.0.0.1:{server_port}/healthz")
        browser = subprocess.Popen(
            [
                chromium,
                "--headless=new",
                "--disable-gpu",
                "--no-sandbox",
                "--disable-application-cache",
                "--disk-cache-size=0",
                "--media-cache-size=0",
                f"--remote-debugging-port={cdp_port}",
                f"--user-data-dir={profile.name}",
                "--window-size=1440,1100",
                "about:blank",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        cdp = CdpClient(wait_for_target(cdp_port, timeout=10))
        cdp.command("Runtime.enable")
        cdp.command("Page.enable")
        cdp.command("Network.enable")
        cdp.command("Network.setCacheDisabled", {"cacheDisabled": True})
        cdp.command("Page.navigate", {"url": f"http://127.0.0.1:{server_port}/aegis-change-control-lab?day=2026-05-30"})
        time.sleep(4)
        expression = """
(() => {
  const text = document.body.innerText || '';
  return {
    hasTop: text.includes('ACC-20260530-008A'),
    hasBlockedParent: /Blocked Parent Records/i.test(text) && text.includes('ACC-20260530-008'),
    hasRollup: text.includes('0/4 complete'),
    hasPrompt: /Suggested Codex Prompt/i.test(text),
    hasNoMutationCopy: text.includes('read-only') || text.includes('does not send it'),
    hasForbiddenMutation: /Record Decision|Close item|Validate item|Implement item/.test(text),
    text: text.slice(0, 4000),
  };
})()
"""
        result = cdp.command("Runtime.evaluate", {"expression": expression, "returnByValue": True})["result"]["value"]
        assert result["hasTop"], result["text"]
        assert result["hasBlockedParent"], result["text"]
        assert result["hasRollup"], result["text"]
        assert result["hasPrompt"], result["text"]
        assert result["hasNoMutationCopy"], result["text"]
        assert not result["hasForbiddenMutation"], result["text"]
    finally:
        if browser:
            browser.terminate()
            browser.wait(timeout=5)
        server.terminate()
        server.wait(timeout=5)
        profile.cleanup()
