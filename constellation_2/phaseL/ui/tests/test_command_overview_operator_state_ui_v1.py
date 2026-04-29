from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
COMMAND_OVERVIEW = ROOT / "constellation_2/phaseL/ui/static/operator_shell/components/command_overview.js"


def test_command_overview_has_explicit_operator_state_classification() -> None:
    source = COMMAND_OVERVIEW.read_text(encoding="utf-8")

    for state in [
        "MARKET_CLOSED / OUT_OF_SESSION",
        "STALE_DATA",
        "PARTIAL_STATE",
        "READY",
        "BLOCKED",
    ]:
        assert state in source

    assert "classifyOperatorState" in source
    assert "OperatorStateSummary" in source
    assert "System is not currently evaluating readiness because the market is closed." in source


def test_command_overview_collapses_unavailable_secondary_tiles() -> None:
    source = COMMAND_OVERVIEW.read_text(encoding="utf-8")

    assert "Secondary readiness tiles are hidden" in source
    assert "collapsedSecondary ? \"\" : StatusStrip" in source
    assert "Raw artifact evidence remains available in the context rail." in source
    assert "Secondary tiles are collapsed to avoid implying live readiness." in source

