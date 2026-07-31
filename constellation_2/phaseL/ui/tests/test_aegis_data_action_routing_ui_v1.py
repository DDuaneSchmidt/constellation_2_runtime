from __future__ import annotations

from pathlib import Path

from constellation_2.phaseL.ui.server import run_ops_dashboard_v1 as server
from ops.aegis.data_action_routing_v1 import build_data_action_routing_v1, write_data_action_routing_v1

ROOT = Path(__file__).resolve().parents[4]
UI = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
TRUTH = Path("/home/node/constellation_runtime_data/truth")
DAY = "2026-06-01"


def test_ui_renders_data_action_routing_fields_for_generated_hypotheses() -> None:
    text = UI.read_text(encoding="utf-8")
    assert "function commandCenterDataActionRoutingRows" in text
    assert "data_action_routing_v1" in text
    assert "Classification" in text
    assert "Owner" in text
    assert "David action" in text
    assert "data_action_next_step" in text


def test_ui_has_macro_and_oil_shock_owner_action_explanation_contract() -> None:
    text = UI.read_text(encoding="utf-8")
    block = text[text.index("function commandCenterApplyDataActionRouting"):text.index("function renderOperatorDecisionTodayResult")]
    assert "data_action_classification" in block
    assert "missing_data_description" in block
    assert "david_action_required" in block
    assert "owner" in block


def test_operator_payload_exposes_data_action_routing() -> None:
    routing_payload = build_data_action_routing_v1(truth_root=TRUTH, day_utc=DAY, computed_at_utc="2026-06-01T12:00:00Z")
    write_data_action_routing_v1(truth_root=TRUTH, day_utc=DAY, payload=routing_payload)
    payload = server._today_build_operator_envelope_v1(TRUTH, DAY, DAY)
    routing = payload.get("data_action_routing_v1")
    assert isinstance(routing, dict)
    assert routing.get("schema_id") == "aegis_data_action_routing"
    assert payload.get("source_paths", {}).get("data_action_routing", "").endswith("data_action_routing.v1.json")
