from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.sleeve_throughput_evidence_classification_scorecard_v1 import (
    build_sleeve_throughput_evidence_classification_scorecard_v1,
    sleeve_throughput_evidence_classification_scorecard_path_v1,
    write_sleeve_throughput_evidence_classification_scorecard_v1,
)

DAY = "2026-06-02"


def _report(root: Path, family: str, filename: str) -> Path:
    return root / "reports" / family / DAY / filename


def _seed(root: Path) -> None:
    write_json_v1(_report(root, "aegis_sleeve_throughput_diagnostics_v1", "sleeve_throughput_diagnostics.v1.json"), {
        "sleeves": [
            {
                "sleeve_id": "FLOW",
                "sleeve_name": "Flowing Sleeve",
                "throughput_status": "FLOWING",
                "blocker_code": "NONE",
                "raw_signal_count": 1,
                "candidate_count": 1,
                "paper_observation_count": 1,
                "outcome_count": 0,
                "validation_sample_count": 0,
                "owner": "NONE",
                "david_action_required": False,
            },
            {
                "sleeve_id": "C2_EVENT_DISLOCATION_V1",
                "sleeve_name": "Event Dislocation Repricing",
                "throughput_status": "BLOCKED",
                "blocker_code": "SIGNALS_PRESENT_BUT_NO_CANDIDATES",
                "raw_signal_count": 19,
                "candidate_count": 0,
                "paper_observation_count": 0,
                "outcome_count": 0,
                "validation_sample_count": 0,
                "owner": "AEGIS_SYSTEM",
                "david_action_required": False,
            },
            {
                "sleeve_id": "C2_DEFENSIVE_TAIL_V1",
                "sleeve_name": "Defensive Tail Convexity",
                "throughput_status": "DORMANT",
                "blocker_code": "NO_SIGNALS_GENERATED",
                "raw_signal_count": 0,
                "candidate_count": 0,
                "paper_observation_count": 0,
                "outcome_count": 0,
                "validation_sample_count": 0,
                "owner": "AEGIS_SYSTEM",
                "david_action_required": False,
            },
            {
                "sleeve_id": "C2_MARKET_NEUTRAL_SPREAD_V1",
                "sleeve_name": "Market-Neutral Spread Convergence",
                "throughput_status": "DORMANT",
                "blocker_code": "NO_SIGNALS_GENERATED",
                "raw_signal_count": 0,
                "candidate_count": 0,
                "paper_observation_count": 0,
                "outcome_count": 0,
                "validation_sample_count": 0,
                "owner": "AEGIS_SYSTEM",
                "david_action_required": False,
            },
            {
                "sleeve_id": "NEEDS_DATA_ROW",
                "sleeve_name": "Needs Data",
                "throughput_status": "BLOCKED",
                "blocker_code": "INSUFFICIENT_MARKET_DATA",
                "blocker_reason": "source missing",
                "raw_signal_count": 0,
                "candidate_count": 0,
                "paper_observation_count": 0,
                "outcome_count": 0,
                "validation_sample_count": 0,
                "owner": "DAVID",
                "david_action_required": True,
            },
            {
                "sleeve_id": "NEEDS_REPAIR_ROW",
                "sleeve_name": "Needs Repair",
                "throughput_status": "BLOCKED",
                "blocker_code": "SIGNALS_PRESENT_BUT_NO_CANDIDATES",
                "blocker_reason": "builder stopped",
                "raw_signal_count": 2,
                "candidate_count": 0,
                "paper_observation_count": 0,
                "outcome_count": 0,
                "validation_sample_count": 0,
                "owner": "AEGIS_SYSTEM",
                "david_action_required": False,
            },
        ]
    })
    write_json_v1(_report(root, "aegis_signal_evidence_graph_v1", "signal_evidence_graph.v1.json"), {
        "signals": [
            {"sleeve_id": "FLOW", "raw_signal_id": "sig-flow", "governance_status": "GOVERNED"},
            {"sleeve_id": "C2_EVENT_DISLOCATION_V1", "raw_signal_id": "sig-cifr", "symbol": "CIFR", "symbol_governance_status": "UNGOVERNED", "candidate_contract_status": "SUPPRESSED", "pre_contract_suppression_reason": "UNGOVERNED_SYMBOL_SUPPRESSED"},
            {"sleeve_id": "NEEDS_REPAIR_ROW", "raw_signal_id": "sig-repair-1", "governance_status": "GOVERNED"},
            {"sleeve_id": "NEEDS_REPAIR_ROW", "raw_signal_id": "sig-repair-2", "governance_status": "GOVERNED"},
        ]
    })
    write_json_v1(_report(root, "aegis_candidate_contracts_v1", "candidate_contracts.v1.json"), {
        "candidate_contracts": [
            {"sleeve_id": "FLOW", "candidate_id": "cand-flow", "contract_validation_status": "VALID"},
        ],
        "rejected_raw_signals": [],
        "pre_contract_suppressed_raw_signals": [
            {"sleeve_id": "C2_EVENT_DISLOCATION_V1", "raw_signal_id": "sig-cifr", "symbol": "CIFR", "pre_contract_suppression_reason": "UNGOVERNED_SYMBOL_SUPPRESSED"},
        ],
    })
    write_json_v1(_report(root, "aegis_event_dislocation_governed_universe_policy_repair_v1", "event_dislocation_governed_universe_policy_repair.v1.json"), {
        "sleeve_id": "C2_EVENT_DISLOCATION_V1",
        "signal_count_before_repair": 19,
        "governed_symbols_before_repair": ["GLD"],
        "suppressed_ungoverned_signal_count": 19,
        "candidate_count_after_repair": 0,
        "candidate_rejection_count_after_repair": 0,
        "repair_status": "REPAIRED",
        "post_repair_t06_status": "UNGOVERNED_SIGNALS_SUPPRESSED",
        "remaining_blocker": "NONE",
        "david_action_required": False,
    })


def test_scorecard_classifies_flowing_and_healthy_no_candidate(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_sleeve_throughput_evidence_classification_scorecard_v1(truth_root=tmp_path, day_utc=DAY)
    rows = {row["sleeve_id"]: row for row in payload["sleeves"]}

    assert rows["FLOW"]["evidence_classification"] == "FLOWING"
    assert rows["C2_EVENT_DISLOCATION_V1"]["evidence_classification"] == "HEALTHY_NO_CANDIDATE"
    assert rows["C2_EVENT_DISLOCATION_V1"]["blocker_code"] == "UNGOVERNED_SIGNALS_SUPPRESSED"
    assert rows["C2_EVENT_DISLOCATION_V1"]["raw_signal_count"] == 19
    assert rows["C2_EVENT_DISLOCATION_V1"]["governed_signal_count"] == 0
    assert rows["C2_EVENT_DISLOCATION_V1"]["ungoverned_signal_count"] == 19


def test_scorecard_normalizes_valid_no_signal_conditions(tmp_path: Path) -> None:
    _seed(tmp_path)
    rows = {
        row["sleeve_id"]: row
        for row in build_sleeve_throughput_evidence_classification_scorecard_v1(truth_root=tmp_path, day_utc=DAY)["sleeves"]
    }

    assert rows["C2_DEFENSIVE_TAIL_V1"]["evidence_classification"] == "HEALTHY_NO_CANDIDATE"
    assert rows["C2_DEFENSIVE_TAIL_V1"]["blocker_code"] == "VALID_NO_SIGNAL_CONDITIONS"
    assert rows["C2_MARKET_NEUTRAL_SPREAD_V1"]["evidence_classification"] == "HEALTHY_NO_CANDIDATE"
    assert rows["C2_MARKET_NEUTRAL_SPREAD_V1"]["blocker_code"] == "TRIGGER_THRESHOLDS_NOT_MET"


def test_scorecard_separates_needs_data_and_needs_repair(tmp_path: Path) -> None:
    _seed(tmp_path)
    rows = {
        row["sleeve_id"]: row
        for row in build_sleeve_throughput_evidence_classification_scorecard_v1(truth_root=tmp_path, day_utc=DAY)["sleeves"]
    }

    assert rows["NEEDS_DATA_ROW"]["evidence_classification"] == "NEEDS_DATA"
    assert rows["NEEDS_DATA_ROW"]["david_action_required"] is True
    assert rows["NEEDS_REPAIR_ROW"]["evidence_classification"] == "NEEDS_REPAIR"
    assert rows["NEEDS_REPAIR_ROW"]["owner"] == "AEGIS_SYSTEM"


def test_scorecard_summary_and_safety_invariants(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_sleeve_throughput_evidence_classification_scorecard_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["portfolio_summary"]["total_sleeves"] == 6
    assert payload["portfolio_summary"]["flowing_count"] == 1
    assert payload["portfolio_summary"]["healthy_no_candidate_count"] == 3
    assert payload["portfolio_summary"]["needs_data_count"] == 1
    assert payload["portfolio_summary"]["needs_repair_count"] == 1
    assert payload["read_only"] is True
    assert payload["no_candidate_mutation"] is True
    assert payload["no_threshold_mutation"] is True
    assert payload["broker_execution_allowed"] is False


def test_scorecard_writer_uses_required_path(tmp_path: Path) -> None:
    _seed(tmp_path)
    path = write_sleeve_throughput_evidence_classification_scorecard_v1(truth_root=tmp_path, day_utc=DAY)
    assert path == sleeve_throughput_evidence_classification_scorecard_path_v1(truth_root=tmp_path, day_utc=DAY)
    assert path.exists()
