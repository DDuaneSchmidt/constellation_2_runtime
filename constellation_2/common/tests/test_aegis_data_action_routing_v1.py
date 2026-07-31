from __future__ import annotations

from pathlib import Path

from ops.aegis.data_action_routing_v1 import build_data_action_routing_v1, write_data_action_routing_v1
from ops.aegis.intelligence_common_v1 import write_json_v1

DAY = "2026-06-01"


def _report(root: Path, family: str, filename: str) -> Path:
    return root / "reports" / family / DAY / filename


def _seed(root: Path, *, oil_action: bool = False) -> None:
    actions = [{
        "action_type": "PROVIDE_DATA_SOURCE",
        "hypothesis_id": "ehp_macro_calendar_fixture",
        "hypothesis_name": "Macro Calendar",
        "why_action_needed": "macro event calendar is missing",
        "required_fields": ["event_name", "event_type", "release_datetime"],
        "exact_buttons": ["Connect Source", "Upload Dataset", "Mark Not Available", "Defer"],
        "source_artifact_paths": [str(_report(root, "aegis_hypothesis_workflow_state_v1", "hypothesis_workflow_state.v1.json"))],
    }]
    if oil_action:
        actions.append({
            "action_type": "PROVIDE_DATA_SOURCE",
            "hypothesis_id": "ehp_oil",
            "hypothesis_name": "Oil shock reversals across energy ETFs",
            "why_action_needed": "Oil Shock explicit source action exists",
            "required_fields": ["market_data_source"],
            "exact_buttons": ["Connect Source", "Defer"],
        })
    write_json_v1(_report(root, "aegis_operator_action_queue_v1", "operator_action_queue.v1.json"), {
        "day_utc": DAY,
        "summary": {"action_count": len(actions)},
        "actions": actions,
    })
    write_json_v1(_report(root, "aegis_generated_hypothesis_throughput_v1", "generated_hypothesis_throughput.v1.json"), {
        "day_utc": DAY,
        "generated_hypotheses": [],
    })
    write_json_v1(_report(root, "aegis_oil_shock_candidate_flow_v1", "oil_shock_candidate_flow.v1.json"), {
        "day_utc": DAY,
        "oil_shock": {
            "hypothesis_id": "ehp_oil",
            "hypothesis_name": "Oil shock reversals across energy ETFs",
            "exact_blocker": "MISSING_DATA",
            "david_action_required": False,
            "due_to": {"missing_data": True},
            "required_evidence_fields": ["instrument_universe", "data_availability"],
            "next_expected_step": "provide missing Oil Shock source data, then rerun candidate producer",
        },
    })


def test_macro_calendar_missing_data_routes_to_operator_provided_data(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_data_action_routing_v1(truth_root=tmp_path, day_utc=DAY, computed_at_utc="2026-06-01T12:00:00Z")
    macro = next(row for row in payload["routing_rows"] if row["hypothesis_id"] == "ehp_macro_calendar_fixture")
    assert macro["blocker_code"] == "MISSING_DATA"
    assert macro["data_action_classification"] == "OPERATOR_PROVIDED_DATA_REQUIRED"
    assert macro["david_action_required"] is True
    assert macro["owner"] == "DAVID"
    assert macro["missing_data_description"] == "Macro Calendar needs a macro event calendar source."
    assert macro["exact_buttons"] == ["Connect Source", "Upload Dataset", "Mark Not Available", "Defer"]


def test_oil_shock_missing_data_routes_to_non_david_when_no_explicit_action(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_data_action_routing_v1(truth_root=tmp_path, day_utc=DAY)
    oil = next(row for row in payload["routing_rows"] if row["hypothesis_id"] == "ehp_oil")
    assert oil["blocker_code"] == "MISSING_DATA"
    assert oil["data_action_classification"] == "WAITING_FOR_MARKET_DATA"
    assert oil["david_action_required"] is False
    assert oil["owner"] == "MARKET_CONDITIONS"
    assert "No David action is required" in oil["missing_data_description"]


def test_oil_shock_system_data_pipeline_required_routes_to_non_david_system(tmp_path: Path) -> None:
    _seed(tmp_path)
    write_json_v1(_report(tmp_path, "aegis_oil_shock_candidate_flow_v1", "oil_shock_candidate_flow.v1.json"), {
        "day_utc": DAY,
        "oil_shock": {
            "hypothesis_id": "ehp_oil",
            "hypothesis_name": "Oil shock reversals across energy ETFs",
            "exact_blocker": "SYSTEM_DATA_PIPELINE_REQUIRED",
            "david_action_required": False,
            "due_to": {"missing_data": True},
            "required_evidence_fields": ["instrument_universe", "data_availability"],
            "next_expected_step": "repair system market-data evidence for USO",
        },
    })
    payload = build_data_action_routing_v1(truth_root=tmp_path, day_utc=DAY)
    oil = next(row for row in payload["routing_rows"] if row["hypothesis_id"] == "ehp_oil")
    assert oil["blocker_code"] == "SYSTEM_DATA_PIPELINE_REQUIRED"
    assert oil["data_action_classification"] == "SYSTEM_DATA_PIPELINE_REQUIRED"
    assert oil["david_action_required"] is False
    assert oil["owner"] == "AEGIS_SYSTEM"


def test_oil_shock_explicit_action_overrides_to_david_owned(tmp_path: Path) -> None:
    _seed(tmp_path, oil_action=True)
    payload = build_data_action_routing_v1(truth_root=tmp_path, day_utc=DAY)
    oil = next(row for row in payload["routing_rows"] if row["hypothesis_id"] == "ehp_oil")
    assert oil["data_action_classification"] == "OPERATOR_PROVIDED_DATA_REQUIRED"
    assert oil["david_action_required"] is True
    assert oil["owner"] == "DAVID"


def test_no_generic_missing_data_ambiguity_and_safety_gates_unchanged(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_data_action_routing_v1(truth_root=tmp_path, day_utc=DAY)
    assert payload["summary"]["ambiguous_missing_data_count"] == 0
    assert payload["no_broker_execution"] is True
    assert payload["no_trade_advice"] is True
    assert payload["no_live_trading"] is True
    assert payload["no_safety_gate_change"] is True
    assert payload["safety_gates_changed"] is False
    assert payload["broker_execution_allowed"] is False
    assert payload["trade_advice_allowed"] is False


def test_artifact_write_path(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_data_action_routing_v1(truth_root=tmp_path, day_utc=DAY)
    path = write_data_action_routing_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert path.exists()
    assert path.name == "data_action_routing.v1.json"
