from src.research.public_formula_library import (
    formula_library_summary,
    formulas_by_category,
    get_formula,
    public_formula_library,
    render_markdown,
    search_formulas,
)


def test_public_formula_library_covers_required_categories() -> None:
    summary = formula_library_summary()

    assert summary["activation_status"] == "research_only_not_ranked"
    assert summary["ranking_activation_allowed"] is False
    assert set(summary["categories"]) == {
        "Forensic",
        "Growth",
        "Industry",
        "Liquidity",
        "Momentum",
        "Quality",
        "Stability",
        "Value",
    }
    assert summary["formula_count"] == len(public_formula_library())


def test_public_formula_lookup_and_search() -> None:
    pegy = get_formula("growth_pegy_ratio_v1")

    assert pegy["exact_formula"] == "PEGY = P/E / (projected EPS growth rate + dividend yield)"
    assert pegy["category"] == "Growth"
    assert formulas_by_category("value")
    assert search_formulas("Europe momentum")


def test_render_markdown_contains_required_sections() -> None:
    markdown = render_markdown()

    assert "# Public Formula Library" in markdown
    assert "## Evidence Boundary" in markdown
    assert "| Category | Count |" in markdown
    assert "## Liquidity" in markdown
    assert "PEGY = P/E / (projected EPS growth rate + dividend yield)" in markdown
