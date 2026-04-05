from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def test_ops_dashboard_readonly_markup_contract() -> None:
    index_html = (ROOT / "constellation_2" / "phaseL" / "ui" / "static" / "index.html").read_text(encoding="utf-8")
    app_js = (ROOT / "constellation_2" / "phaseL" / "ui" / "static" / "app.js").read_text(encoding="utf-8")

    assert 'id="tabAdvisor"' in index_html
    assert 'id="viewAdvisor"' in index_html

    for required_id in [
        "advisorDecisionShell",
        "advisorSystemState",
        "advisorAllowedAction",
        "advisorWhy",
        "advisorLastValidated",
        "advisorChangedSinceLastRun",
        "advisorEvidenceSpine",
        "advisorEvidenceExecutionTruth",
        "advisorEvidenceGateAuthority",
        "advisorEvidencePublicationPromotion",
        "advisorEvidenceReplayIntegrity",
        "advisorEvidenceTraceIntegrity",
        "advisorEvidenceDiagnostics",
    ]:
        assert f'id="{required_id}"' in index_html

    assert "No advisor artifacts found" in index_html
    assert "Advisor panel is read-only and artifact-backed" in index_html
    assert 'setView("advisor")' in app_js
    assert 'renderAdvisor(payload, previousAdvisorChain);' in app_js
