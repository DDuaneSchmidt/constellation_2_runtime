from __future__ import annotations

from pathlib import Path

PAGE = Path("constellation_2/phaseL/ui/static/operator_shell/pages/index.js")


def _text() -> str:
    return PAGE.read_text(encoding="utf-8")


def test_performance_ui_consumes_semantic_invariant_gate() -> None:
    text = _text()
    assert "semanticInvariantFailures" in text
    assert "renderSemanticInvariantUnavailable" in text
    assert "Analytics Unavailable" in text
    assert "fetchAegisSurfaceReadiness(routeParams)" in text


def test_sleeve_analytics_ui_hides_scorecard_when_blocking_invariant_fails() -> None:
    text = _text()
    assert "Analytics Unavailable" in text
    assert 'data-semantic-invariant-blocked' in text
    assert '${sleeveSemanticBlocked ? "" : `<section class="operator-section sleeve-scorecard">' in text


def test_raw_semantic_invariant_evidence_is_collapsed() -> None:
    text = _text()
    assert '<details class="operator-disclosure"><summary>Raw invariant evidence' in text
