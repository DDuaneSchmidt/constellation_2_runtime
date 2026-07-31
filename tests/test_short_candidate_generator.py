from __future__ import annotations

import src.short_candidate_generator as short_candidate_generator
from src.short_candidate_generator import (
    DEFAULT_UNIVERSE_RULES,
    TEMPLATE_IDS,
    generate_by_template_id,
    generate_templates,
    render_markdown,
)


REQUIRED_FIELDS = {
    "template_id",
    "title",
    "status",
    "thesis",
    "portfolio123_rules",
    "rank_formula",
    "entry_review_notes",
    "borrow_risk_notes",
}


def test_generate_templates_returns_ten_draft_only_templates() -> None:
    templates = generate_templates()

    assert len(templates) == 10
    assert [template["template_id"] for template in templates] == [
        f"short_{template_id}_v1" for template_id in TEMPLATE_IDS
    ]
    assert {template["status"] for template in templates} == {"DRAFT_ONLY"}


def test_every_template_contains_required_contract_fields() -> None:
    for template in generate_templates():
        assert set(template) == REQUIRED_FIELDS
        assert template["title"]
        assert template["thesis"]
        assert template["portfolio123_rules"]
        assert template["rank_formula"]
        assert template["entry_review_notes"]
        assert template["borrow_risk_notes"]


def test_common_universe_constraints_exclude_microcaps_and_low_liquidity() -> None:
    constraints = "\n".join(DEFAULT_UNIVERSE_RULES)

    assert "Close(0) > 5" in constraints
    assert "AvgDailyTot(60) > 2000000" in constraints
    assert "MktCap > 1000" in constraints


def test_generation_is_deterministic_and_filterable() -> None:
    assert generate_templates() == generate_templates()
    assert generate_by_template_id("  NEGATIVE_REVISIONS  ") == generate_by_template_id("negative_revisions")


def test_render_markdown_includes_all_templates_and_draft_boundary() -> None:
    markdown = render_markdown()

    assert markdown.startswith("# Short Candidate Templates\n")
    assert markdown.count("## Template ") == 10
    assert markdown.count("Status: DRAFT_ONLY") == 11
    assert "No API calls" not in markdown
    assert "no API calls" in markdown
    assert "AvgDailyTot(60) > 2000000" in markdown


def test_no_api_or_network_surface_is_exposed() -> None:
    module_source = short_candidate_generator.__loader__.get_source(short_candidate_generator.__name__) or ""

    assert "requests" not in module_source
    assert "urllib" not in module_source
    assert "httpx" not in module_source
    assert "Portfolio123 calls" in module_source
