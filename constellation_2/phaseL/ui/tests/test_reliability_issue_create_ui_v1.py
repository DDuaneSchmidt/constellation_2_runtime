from __future__ import annotations

from pathlib import Path
from constellation_2.phaseL.ui.tests.operator_shell_test_sources import pages_source_v1


ROOT = Path(__file__).resolve().parents[4]


def test_reliability_issue_create_button_and_form_are_present() -> None:
    pages = pages_source_v1(ROOT)

    assert "+ New Issue" in pages
    assert 'name="reliability_action" value="create_issue"' in pages
    for field in [
        'name="title"',
        'name="type"',
        'name="category"',
        'name="severity"',
        'name="canonical_key"',
        'name="readiness_blocker"',
        'name="impact_summary"',
        'name="expected_behavior"',
        'name="actual_behavior"',
        'name="created_by"',
    ]:
        assert field in pages


def test_reliability_issue_create_uses_existing_post_endpoint() -> None:
    pages = pages_source_v1(ROOT)
    domain_client = (
        ROOT / "constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js"
    ).read_text(encoding="utf-8")

    assert "createReliabilityIssue," in pages
    assert "result = await createReliabilityIssue(payload);" in pages
    assert 'postJson("/api/reliability/issues", payload)' in domain_client


def test_reliability_issue_create_refreshes_and_surfaces_errors() -> None:
    pages = pages_source_v1(ROOT)
    main = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js").read_text(encoding="utf-8")

    assert "const payload = await fetchReliabilityIssues(filters);" in pages
    assert "Issue Created" in pages
    assert "The issue list was refreshed from the reliability API." in pages
    assert "Last Reliability Issue Create Error" in pages
    assert "lastFormInput" in pages
    assert "lastFormInput: submitted" in main
    assert "await renderRoute();" in main


def test_reliability_issue_create_uses_allowed_enum_values() -> None:
    pages = pages_source_v1(ROOT)

    for value in [
        "bug",
        "missing_functionality",
        "regression",
        "paper_trading_incident",
        "readiness_blocker",
        "strategy",
        "execution",
        "data",
        "infrastructure",
        "ui_missing_functionality",
        "low",
        "medium",
        "high",
        "critical",
    ]:
        assert f'value="{value}"' in pages
