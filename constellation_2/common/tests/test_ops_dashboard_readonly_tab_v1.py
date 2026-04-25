from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def test_operator_shell_single_entry_markup_contract() -> None:
    index_html = (ROOT / "constellation_2" / "phaseL" / "ui" / "static" / "index.html").read_text(encoding="utf-8")
    app_js = (ROOT / "constellation_2" / "phaseL" / "ui" / "static" / "app.js").read_text(encoding="utf-8")
    shell_main = (ROOT / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "main.js").read_text(encoding="utf-8")

    for required_id in [
        "app",
        "environmentBadge",
        "currentDayValue",
        "refreshValue",
        "brokerIndicator",
        "statusStrip",
        "alertTray",
        "workspaceNav",
        "workspaceTitle",
        "workspaceContent",
        "detailDrawer",
        "drawerTitle",
        "drawerContent",
    ]:
        assert f'id="{required_id}"' in index_html

    assert "Constellation Operator Shell" in index_html
    assert 'type="module"' in index_html
    assert 'bootOperatorShell();' in app_js
    assert 'fetchJson("/api/system/summary")' in shell_main
    assert 'fetchJson("/api/shared/status-semantics")' in shell_main
    assert 'fetchJson("/api/operations")' in shell_main
    assert 'fetchJson("/api/advisory")' in shell_main
    assert 'fetchJson("/api/orders")' in shell_main
    assert 'fetchJson("/api/positions")' in shell_main
    assert 'fetchJson("/api/reconciliation")' in shell_main
    assert 'fetchJson("/api/alerts")' in shell_main
    assert 'fetchJson("/api/integrity")' in shell_main
    assert 'fetchJson("/api/system/actions")' in shell_main
