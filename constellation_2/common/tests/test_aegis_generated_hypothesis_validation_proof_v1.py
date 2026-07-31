from __future__ import annotations

from pathlib import Path

from ops.aegis.generated_hypothesis_validation_proof_v1 import (
    build_generated_hypothesis_validation_proof_v1,
    write_generated_hypothesis_validation_proof_v1,
)
from ops.aegis.intelligence_common_v1 import write_json_v1

DAY = "2026-06-01"


def _report(root: Path, family: str, filename: str) -> Path:
    return root / "reports" / family / DAY / filename


def _seed(root: Path) -> None:
    write_json_v1(_report(root, "aegis_generated_hypothesis_throughput_v1", "generated_hypothesis_throughput.v1.json"), {
        "day_utc": DAY,
        "generated_hypotheses": [
            {
                "hypothesis_id": "ehp_macro",
                "hypothesis_name": "Macro calendar event dislocation watch",
                "proposal_state": "NEEDS_DATA",
                "throughput_status": "NEEDS_DATA",
                "next_expected_step": "candidate generation",
                "included_validation_samples": 0,
            },
            {
                "hypothesis_id": "ehp_oil",
                "hypothesis_name": "Oil shock reversals across energy ETFs",
                "proposal_state": "PAPER_TRACKING_READY",
                "approval_state": "PAPER_PROMOTION_APPROVED",
                "paper_setup_state": "PAPER_TRACKING_READY",
                "throughput_status": "BLOCKED",
                "candidate_count": 0,
                "paper_observation_count": 0,
                "closed_outcomes": 0,
                "included_validation_samples": 0,
            },
        ],
    })
    write_json_v1(_report(root, "aegis_data_action_routing_v1", "data_action_routing.v1.json"), {
        "day_utc": DAY,
        "routing_rows": [
            {
                "hypothesis_id": "ehp_macro",
                "hypothesis_name": "Macro calendar event dislocation watch",
                "owner": "DAVID",
                "david_action_required": True,
                "missing_data_description": "Macro Calendar needs a macro event calendar source.",
                "next_step": "provide macro calendar source or mark unavailable",
                "source_artifact_paths": [],
            },
            {
                "hypothesis_id": "ehp_oil",
                "hypothesis_name": "Oil shock reversals across energy ETFs",
                "owner": "MARKET_CONDITIONS",
                "david_action_required": False,
                "missing_data_description": "Oil Shock requires system market data evidence. No David action is required unless Aegis creates a data-source action.",
                "next_step": "wait for qualifying market data/setup or run producer when evidence becomes available",
                "source_artifact_paths": [],
            },
        ],
    })
    write_json_v1(_report(root, "aegis_oil_shock_candidate_flow_v1", "oil_shock_candidate_flow.v1.json"), {
        "day_utc": DAY,
        "oil_shock": {
            "hypothesis_id": "ehp_oil",
            "hypothesis_name": "Oil shock reversals across energy ETFs",
            "current_state": "PAPER_TRACKING_READY",
            "paper_setup_status": "PAPER_TRACKING_READY",
            "candidate_flow_status": "BLOCKED",
            "candidate_count": 0,
            "paper_observation_count": 0,
            "david_action_required": False,
            "due_to": {"missing_data": True},
            "ui_message": "No David action required. Oil Shock producer ran; required market data is missing.",
            "next_expected_step": "provide missing Oil Shock source data, then rerun candidate producer",
        },
    })
    for family, filename, payload in [
        ("aegis_candidate_contracts_v1", "candidate_contracts.v1.json", {"candidate_contracts": []}),
        ("aegis_candidate_to_paper_lifecycle_v1", "candidate_to_paper_lifecycle.v1.json", {"summary": {}}),
        ("aegis_outcome_registry_v1", "outcome_registry.v1.json", {"outcomes": []}),
        ("aegis_validation_samples_v1", "validation_samples.v1.json", {"samples": []}),
        ("aegis_macro_calendar_data_readiness_v1", "macro_calendar_data_readiness.v1.json", {"status": "NEEDS_SOURCE"}),
    ]:
        write_json_v1(_report(root, family, filename), payload)


def test_generated_hypotheses_are_tracked_across_every_stage(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_generated_hypothesis_validation_proof_v1(truth_root=tmp_path, day_utc=DAY)
    assert payload["summary"]["generated_hypotheses_total"] == 2
    for row in payload["hypothesis_rows"]:
        for field in [
            "proposal_state",
            "shadow_validation_state",
            "approval_state",
            "paper_tracking_setup_state",
            "candidate_flow_state",
            "paper_observation_flow_state",
            "outcome_flow_state",
            "validation_sample_flow_state",
        ]:
            assert row[field] in {"NOT_STARTED", "READY", "FLOWING", "BLOCKED", "NEEDS_DATA", "WAITING_FOR_MARKET_CONDITIONS", "COMPLETE", "NOT_APPLICABLE"}


def test_oil_shock_reaches_paper_tracking_ready_but_not_validation_samples(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_generated_hypothesis_validation_proof_v1(truth_root=tmp_path, day_utc=DAY)
    oil = next(row for row in payload["hypothesis_rows"] if "Oil shock" in row["hypothesis_name"])
    assert oil["paper_tracking_setup_state"] == "READY"
    assert oil["candidate_flow_state"] == "WAITING_FOR_MARKET_CONDITIONS"
    assert oil["validation_sample_flow_state"] == "NOT_STARTED"
    assert oil["current_stop_stage"] == "candidate flow"
    assert oil["blocker_owner"] == "MARKET_CONDITIONS"
    assert oil["david_action_required"] is False
    assert oil["next_expected_step"] == "wait for qualifying market data/setup or run producer when evidence becomes available"



def test_oil_shock_started_flow_counts_as_candidate_flow_without_contract_mutation(tmp_path: Path) -> None:
    _seed(tmp_path)
    write_json_v1(_report(tmp_path, "aegis_oil_shock_candidate_flow_v1", "oil_shock_candidate_flow.v1.json"), {
        "day_utc": DAY,
        "oil_shock": {
            "hypothesis_id": "ehp_oil",
            "hypothesis_name": "Oil shock reversals across energy ETFs",
            "current_state": "PAPER_TRACKING_READY",
            "paper_setup_status": "PAPER_TRACKING_READY",
            "candidate_flow_status": "CANDIDATE_FLOW_STARTED",
            "candidate_flow_started": True,
            "candidate_count": 0,
            "paper_observation_count": 0,
            "david_action_required": False,
            "due_to": {"missing_data": False},
            "ui_message": "Oil Shock candidate flow started.",
            "next_expected_step": "route raw signal through signal evidence graph and candidate contracts",
        },
    })
    payload = build_generated_hypothesis_validation_proof_v1(truth_root=tmp_path, day_utc=DAY)
    oil = next(row for row in payload["hypothesis_rows"] if "Oil shock" in row["hypothesis_name"])
    assert oil["candidate_flow_state"] == "FLOWING"
    assert payload["summary"]["reached_candidate_flow_count"] == 1
    assert payload["no_candidate_mutation"] is True


def test_candidate_to_paper_proof_updates_oil_stop_reason_without_mutation(tmp_path: Path) -> None:
    _seed(tmp_path)
    write_json_v1(_report(tmp_path, "aegis_oil_shock_candidate_flow_v1", "oil_shock_candidate_flow.v1.json"), {
        "day_utc": DAY,
        "oil_shock": {
            "hypothesis_id": "ehp_oil",
            "hypothesis_name": "Oil shock reversals across energy ETFs",
            "paper_setup_status": "PAPER_TRACKING_READY",
            "candidate_flow_status": "CANDIDATE_FLOW_STARTED",
            "candidate_flow_started": True,
            "candidate_count": 1,
            "paper_observation_count": 1,
            "david_action_required": False,
            "due_to": {"missing_data": False},
        },
    })
    write_json_v1(_report(tmp_path, "aegis_generated_hypothesis_candidate_to_paper_v1", "generated_hypothesis_candidate_to_paper.v1.json"), {
        "oil_shock": {
            "hypothesis_id": "ehp_oil",
            "candidate_to_paper_status": "PAPER_CONSTRUCTION_FAILED",
            "candidate_contract_status": "CANDIDATE_CONTRACT_VALID",
            "entry_reference_price_status": "ENTRY_REFERENCE_CERTIFIED",
            "paper_construction_status": "PAPER_CONSTRUCTION_FAILED",
            "auto_promotion_status": "AUTO_PROMOTION_BLOCKED",
            "paper_observation_created": False,
            "outcome_row_created": False,
            "current_stop_stage": "paper construction",
            "current_stop_reason": "PAPER_CONSTRUCTION_FAILED: missing stop_price",
            "source_artifact_paths": {},
        }
    })
    payload = build_generated_hypothesis_validation_proof_v1(truth_root=tmp_path, day_utc=DAY)
    oil = next(row for row in payload["hypothesis_rows"] if "Oil shock" in row["hypothesis_name"])
    assert oil["candidate_flow_state"] == "FLOWING"
    assert oil["paper_observation_flow_state"] == "NOT_STARTED"
    assert oil["current_stop_stage"] == "paper construction"
    assert oil["current_stop_reason"] == "PAPER_CONSTRUCTION_FAILED: missing stop_price"
    assert oil["candidate_to_paper_status"] == "PAPER_CONSTRUCTION_FAILED"
    assert payload["no_paper_lifecycle_mutation"] is True


def test_candidate_to_paper_proof_marks_paper_observation_stage(tmp_path: Path) -> None:
    _seed(tmp_path)
    write_json_v1(_report(tmp_path, "aegis_oil_shock_candidate_flow_v1", "oil_shock_candidate_flow.v1.json"), {
        "day_utc": DAY,
        "oil_shock": {
            "hypothesis_id": "ehp_oil",
            "hypothesis_name": "Oil shock reversals across energy ETFs",
            "paper_setup_status": "PAPER_TRACKING_READY",
            "candidate_flow_status": "CANDIDATE_FLOW_STARTED",
            "candidate_flow_started": True,
            "candidate_count": 1,
            "paper_observation_count": 0,
            "david_action_required": False,
            "due_to": {"missing_data": False},
        },
    })
    write_json_v1(_report(tmp_path, "aegis_generated_hypothesis_candidate_to_paper_v1", "generated_hypothesis_candidate_to_paper.v1.json"), {
        "oil_shock": {
            "hypothesis_id": "ehp_oil",
            "candidate_to_paper_status": "PAPER_OBSERVATION_CREATED",
            "candidate_contract_status": "CANDIDATE_CONTRACT_VALID",
            "entry_reference_price_status": "ENTRY_REFERENCE_CERTIFIED",
            "paper_construction_status": "PAPER_CONSTRUCTION_READY",
            "auto_promotion_status": "AUTO_PROMOTED_TO_PAPER_TRACKING",
            "paper_observation_created": True,
            "outcome_row_created": True,
            "current_stop_stage": "validation sample flow",
            "current_stop_reason": "Oil Shock paper observation created through normal Aegis gates; outcome row exists.",
            "source_artifact_paths": {},
        }
    })
    payload = build_generated_hypothesis_validation_proof_v1(truth_root=tmp_path, day_utc=DAY)
    oil = next(row for row in payload["hypothesis_rows"] if "Oil shock" in row["hypothesis_name"])
    assert oil["paper_observation_flow_state"] == "FLOWING"
    assert oil["outcome_row_created"] is False
    assert payload["summary"]["reached_paper_observation_count"] == 1


def test_paper_observation_to_outcome_proof_advances_only_on_created_outcome(tmp_path: Path) -> None:
    _seed(tmp_path)
    write_json_v1(_report(tmp_path, "aegis_oil_shock_candidate_flow_v1", "oil_shock_candidate_flow.v1.json"), {
        "day_utc": DAY,
        "oil_shock": {
            "hypothesis_id": "ehp_oil",
            "hypothesis_name": "Oil shock reversals across energy ETFs",
            "paper_setup_status": "PAPER_TRACKING_READY",
            "candidate_flow_status": "CANDIDATE_FLOW_STARTED",
            "candidate_flow_started": True,
            "candidate_count": 1,
            "paper_observation_count": 1,
            "david_action_required": False,
        },
    })
    write_json_v1(_report(tmp_path, "aegis_generated_hypothesis_paper_observation_to_outcome_v1", "generated_hypothesis_paper_observation_to_outcome.v1.json"), {
        "oil_shock": {
            "hypothesis_id": "ehp_oil",
            "paper_observation_found": True,
            "outcome_readiness_status": "READY",
            "outcome_status": "OUTCOME_CREATED",
            "outcome_row_created": True,
            "validation_sample_status": "NOT_READY",
            "remaining_blocker": "NONE",
            "blocker_code": "NONE",
        }
    })
    payload = build_generated_hypothesis_validation_proof_v1(truth_root=tmp_path, day_utc=DAY)
    oil = next(row for row in payload["hypothesis_rows"] if "Oil shock" in row["hypothesis_name"])
    assert oil["outcome_flow_state"] == "FLOWING"
    assert oil["outcome_row_created"] is True
    assert oil["validation_sample_flow_state"] == "NOT_STARTED"
    assert payload["summary"]["furthest_stage_reached"] == "OUTCOME_FLOW"


def test_paper_observation_to_outcome_not_ready_keeps_outcome_blocked(tmp_path: Path) -> None:
    _seed(tmp_path)
    write_json_v1(_report(tmp_path, "aegis_oil_shock_candidate_flow_v1", "oil_shock_candidate_flow.v1.json"), {
        "day_utc": DAY,
        "oil_shock": {
            "hypothesis_id": "ehp_oil",
            "hypothesis_name": "Oil shock reversals across energy ETFs",
            "paper_setup_status": "PAPER_TRACKING_READY",
            "candidate_flow_status": "CANDIDATE_FLOW_STARTED",
            "candidate_flow_started": True,
            "candidate_count": 1,
            "paper_observation_count": 1,
            "david_action_required": False,
        },
    })
    write_json_v1(_report(tmp_path, "aegis_generated_hypothesis_paper_observation_to_outcome_v1", "generated_hypothesis_paper_observation_to_outcome.v1.json"), {
        "oil_shock": {
            "hypothesis_id": "ehp_oil",
            "paper_observation_found": True,
            "outcome_readiness_status": "NOT_READY",
            "outcome_status": "OUTCOME_NOT_READY",
            "outcome_row_created": False,
            "validation_sample_status": "NOT_READY",
            "remaining_blocker": "HOLDING_PERIOD_NOT_ELAPSED",
            "blocker_code": "HOLDING_PERIOD_NOT_ELAPSED",
            "blocker_reason": "same-day paper observation is not eligible",
        }
    })
    payload = build_generated_hypothesis_validation_proof_v1(truth_root=tmp_path, day_utc=DAY)
    oil = next(row for row in payload["hypothesis_rows"] if "Oil shock" in row["hypothesis_name"])
    assert oil["paper_observation_flow_state"] == "FLOWING"
    assert oil["outcome_flow_state"] == "BLOCKED"
    assert oil["outcome_row_created"] is False
    assert oil["paper_observation_to_outcome_blocker_code"] == "HOLDING_PERIOD_NOT_ELAPSED"
    assert oil["validation_sample_flow_state"] == "NOT_STARTED"

def test_macro_calendar_stops_at_data_readiness_with_david_action(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_generated_hypothesis_validation_proof_v1(truth_root=tmp_path, day_utc=DAY)
    macro = next(row for row in payload["hypothesis_rows"] if "Macro calendar" in row["hypothesis_name"])
    assert macro["current_stop_stage"] == "data readiness / shadow validation"
    assert macro["blocker_owner"] == "DAVID"
    assert macro["david_action_required"] is True
    assert "macro event calendar" in macro["current_stop_reason"]


def test_no_generated_hypothesis_is_falsely_marked_validation_producing(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_generated_hypothesis_validation_proof_v1(truth_root=tmp_path, day_utc=DAY)
    assert payload["summary"]["reached_validation_sample_count"] == 0
    assert all(row["validation_sample_flow_state"] == "NOT_STARTED" for row in payload["hypothesis_rows"])


def test_validation_proof_safety_gates_and_no_mutation_flags(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_generated_hypothesis_validation_proof_v1(truth_root=tmp_path, day_utc=DAY)
    assert payload["no_broker_execution"] is True
    assert payload["no_trade_advice"] is True
    assert payload["no_live_trading"] is True
    assert payload["no_candidate_mutation"] is True
    assert payload["no_paper_lifecycle_mutation"] is True
    assert payload["no_outcome_mutation"] is True
    assert payload["no_validation_mutation"] is True
    assert payload["no_allocation_mutation"] is True
    assert payload["no_workflow_state_mutation"] is True
    assert payload["safety_gates_changed"] is False


def test_artifact_write_path(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_generated_hypothesis_validation_proof_v1(truth_root=tmp_path, day_utc=DAY)
    path = write_generated_hypothesis_validation_proof_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert path.exists()
    assert path.name == "generated_hypothesis_validation_proof.v1.json"
