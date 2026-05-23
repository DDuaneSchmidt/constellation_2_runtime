from __future__ import annotations

import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
HARNESS = REPO / "ops" / "tools" / "research_ui_live_validation_v1.py"
PACKAGE = REPO / "package.json"


def test_npm_script_points_to_live_ui_validation_harness() -> None:
    package = json.loads(PACKAGE.read_text(encoding="utf-8"))
    assert package["scripts"]["aegis:validate-live-ui"] == "python3 ops/tools/research_ui_live_validation_v1.py"


def test_live_ui_validation_harness_is_forensic_and_ocr_gated() -> None:
    source = HARNESS.read_text(encoding="utf-8")

    assert "DEFAULT_URL = \"http://127.0.0.1:3917/research-lab\"" in source
    assert "DEFAULT_SEARCH_TERM = \"NVDA\"" in source
    assert "DEFAULT_EXPECTED_TEXT = \"NVIDIA Earnings Event Dislocation\"" in source
    assert "--user-data-dir" in source
    assert "Network.setCacheDisabled" in source
    assert "about:blank" in source
    assert "Input.insertText" in source
    assert "Page.captureScreenshot" in source
    assert "tesseract" in source
    assert "missing_ocr_text" in source
    assert "return 0 if result[\"ok\"] else 1" in source


def test_live_ui_validation_harness_uses_inventory_dom_and_single_datasource() -> None:
    harness = HARNESS.read_text(encoding="utf-8")
    page_source = (REPO / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "pages" / "index.js").read_text(encoding="utf-8")

    assert "[data-research-inventory-row]" in harness
    assert "data-research-hypothesis-search" in harness
    assert "data-research-hypothesis-search-count" in harness
    css = (REPO / "constellation_2" / "phaseL" / "ui" / "static" / "aegis.css").read_text(encoding="utf-8")

    assert "payload.all_hypotheses" in page_source
    assert ".research-inventory-table th:first-child" in css
    assert "white-space: nowrap" in css
    assert "researchQueuePayload" not in page_source[page_source.index("function researchAllHypothesisRows"):page_source.index("function researchIsArchived")]
    assert "researchAegisRows" not in page_source[page_source.index("function researchAllHypothesisRows"):page_source.index("function researchIsArchived")]
