from __future__ import annotations

import src.short_research_review as short_research_review
from src.short_research_review import (
    build_historical_short_research_review_markdown,
    candidate_ideas_for_aegis,
    failed_short_themes,
    successful_short_themes,
    write_historical_short_research_review,
)


def test_successful_short_themes_include_required_research_themes() -> None:
    theme_names = [str(theme["name"]).lower() for theme in successful_short_themes()]

    assert theme_names == [
        "collapsing earnings revisions",
        "accounting weakness",
        "deteriorating profitability",
        "broken momentum",
        "excessive valuation",
    ]


def test_theme_contracts_are_complete_and_ranked() -> None:
    themes = successful_short_themes()

    assert [theme["frequency_rank"] for theme in themes] == [1, 2, 3, 4, 5]
    for theme in themes:
        assert theme["theme_id"]
        assert theme["why_it_worked"]
        assert theme["observable_signals"]
        assert theme["failure_modes"]
        assert theme["aegis_candidate_idea"]
        assert theme["expected_best_use"]
        assert theme["research_priority"] in {"HIGH", "MEDIUM", "LOW"}


def test_failed_themes_capture_likely_short_failure_modes() -> None:
    failed = failed_short_themes()
    names = {theme["name"] for theme in failed}

    assert "Valuation-only shorts" in names
    assert "Crowded consensus shorts" in names
    assert "Low-quality companies without timing evidence" in names
    assert "Macro beta mistaken for company failure" in names
    assert all(theme["aegis_guardrail"] for theme in failed)


def test_candidate_ideas_are_generated_for_each_successful_theme() -> None:
    ideas = candidate_ideas_for_aegis()

    assert len(ideas) == 5
    assert [idea["priority"] for idea in ideas].count("HIGH") == 3
    assert ideas[0]["candidate_id"] == "aegis_short_candidate_earnings_revision_collapse_v1"
    assert ideas[-1]["source_theme"] == "Excessive valuation"


def test_markdown_contains_required_outputs() -> None:
    markdown, summary = build_historical_short_research_review_markdown()

    assert "# Historical Short Research Review" in markdown
    assert "## Most Common Successful Short Themes" in markdown
    assert "## Themes Likely To Fail" in markdown
    assert "## Candidate Ideas For Aegis" in markdown
    assert "Runtime truth currently blocks trade advice and execution" in markdown
    assert summary == {
        "successful_theme_count": 5,
        "failed_theme_count": 4,
        "candidate_idea_count": 5,
        "local_assumption_count": 4,
    }


def test_write_historical_short_research_review_writes_requested_file(tmp_path) -> None:
    output = tmp_path / "review.md"

    result = write_historical_short_research_review(output)

    assert result["path"] == str(output)
    assert output.read_text(encoding="utf-8").startswith("# Historical Short Research Review")


def test_no_api_or_network_surface_is_exposed() -> None:
    module_source = short_research_review.__loader__.get_source(short_research_review.__name__) or ""

    assert "requests" not in module_source
    assert "urllib" not in module_source
    assert "http.client" not in module_source
    assert "import requests" not in module_source
    assert "portfolio123.com" not in module_source.lower()
