from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.sleeve_throughput_diagnostics_v1 import (
    build_sleeve_throughput_diagnostics_v1,
    sleeve_throughput_diagnostics_path_v1,
    write_sleeve_throughput_diagnostics_v1,
)

DAY = "2026-06-02"


def _report(root: Path, family: str, filename: str) -> Path:
    return root / "reports" / family / DAY / filename


def _seed(root: Path) -> None:
    write_json_v1(_report(root, "aegis_research_portfolio_v1", "research_portfolio.v1.json"), {
        "sleeve_implementations": [
            {"sleeve_id": "FLOW", "hypothesis_id": "H_FLOW", "hypothesis_name": "Flowing Sleeve"},
            {"sleeve_id": "SIGNAL_ONLY", "hypothesis_id": "H_SIGNAL", "hypothesis_name": "Signal Only"},
            {"sleeve_id": "DORMANT", "hypothesis_id": "H_DORMANT", "hypothesis_name": "Dormant Sleeve"},
            {"sleeve_id": "PAPER_ONLY", "hypothesis_id": "H_PAPER", "hypothesis_name": "Paper Only"},
        ],
    })
    write_json_v1(_report(root, "aegis_hypothesis_workflow_state_v1", "hypothesis_workflow_state.v1.json"), {
        "hypotheses": [
            {"hypothesis_id": "H_FLOW", "display_name": "Flowing Sleeve", "current_state": "PAPER_OBSERVING"},
            {"hypothesis_id": "H_SIGNAL", "display_name": "Signal Only", "current_state": "PAPER_OBSERVING"},
            {"hypothesis_id": "H_DORMANT", "display_name": "Dormant Sleeve", "current_state": "PAPER_OBSERVING"},
            {"hypothesis_id": "H_PAPER", "display_name": "Paper Only", "current_state": "PAPER_OBSERVING"},
            {"hypothesis_id": "H_MACRO", "display_name": "Macro Generated", "source_type": "GENERATED_PROPOSAL", "current_state": "NEEDS_DATA"},
        ]
    })
    write_json_v1(_report(root, "aegis_generated_hypothesis_throughput_v1", "generated_hypothesis_throughput.v1.json"), {
        "generated_hypotheses": [
            {"hypothesis_id": "H_MACRO", "hypothesis_name": "Macro Generated", "throughput_status": "NEEDS_DATA", "candidate_count": 0}
        ]
    })
    write_json_v1(_report(root, "aegis_signal_evidence_graph_v1", "signal_evidence_graph.v1.json"), {
        "signals": [
            {"sleeve_id": "FLOW", "hypothesis_id": "H_FLOW", "raw_signal_id": "sig-flow", "generated_at_utc": "2026-06-02T10:00:00Z"},
            {"sleeve_id": "SIGNAL_ONLY", "hypothesis_id": "H_SIGNAL", "raw_signal_id": "sig-blocked"},
        ]
    })
    write_json_v1(_report(root, "aegis_candidate_contracts_v1", "candidate_contracts.v1.json"), {
        "candidate_contracts": [
            {"sleeve_id": "FLOW", "hypothesis_id": "H_FLOW", "candidate_id": "cand-flow", "contract_validation_status": "VALID"},
        ],
        "rejected_raw_signals": [
            {"sleeve_id": "SIGNAL_ONLY", "hypothesis_id": "H_SIGNAL", "raw_signal_id": "sig-blocked", "rejection_reason": "ENTRY_REFERENCE_PRICE_MISSING"}
        ],
    })
    write_json_v1(_report(root, "aegis_candidate_to_paper_lifecycle_v1", "candidate_to_paper_lifecycle.v1.json"), {
        "rows": [
            {"sleeve_id": "FLOW", "hypothesis_id": "H_FLOW", "candidate_id": "cand-flow", "paper_position_id": "paper-flow"},
            {"sleeve_id": "PAPER_ONLY", "hypothesis_id": "H_PAPER", "candidate_id": "cand-paper", "paper_position_id": "paper-only"},
        ]
    })
    write_json_v1(_report(root, "aegis_paper_position_ledger_v1", "paper_position_ledger.v1.json"), {
        "positions": [
            {"sleeve_id": "FLOW", "hypothesis_id": "H_FLOW", "candidate_id": "cand-flow", "position_id": "paper-flow"},
            {"sleeve_id": "PAPER_ONLY", "hypothesis_id": "H_PAPER", "candidate_id": "cand-paper", "position_id": "paper-only"},
        ]
    })
    write_json_v1(_report(root, "aegis_outcome_registry_v1", "outcome_registry.v1.json"), {
        "outcomes": [
            {"sleeve_id": "FLOW", "hypothesis_id": "H_FLOW", "candidate_id": "cand-flow", "outcome_id": "out-flow", "outcome_state": "CLOSED"},
        ]
    })
    write_json_v1(_report(root, "aegis_validation_samples_v1", "validation_samples.v1.json"), {
        "samples": [
            {"sleeve_id": "FLOW", "hypothesis_id": "H_FLOW", "candidate_id": "cand-flow", "sample_id": "sample-flow", "inclusion_status": "INCLUDED"},
        ]
    })
    write_json_v1(_report(root, "aegis_research_quality_engine_v1", "research_quality_engine.v1.json"), {
        "hypotheses": [
            {"hypothesis_id": "H_DORMANT", "hypothesis_name": "Dormant Sleeve", "quality_status": "UNDERPOWERED"},
        ]
    })
    write_json_v1(_report(root, "aegis_macro_calendar_data_readiness_v1", "macro_calendar_data_readiness.v1.json"), {
        "status": "NEEDS_SOURCE",
        "macro_calendar_ready": False,
    })
    write_json_v1(_report(root, "aegis_operator_action_queue_v1", "operator_action_queue.v1.json"), {
        "actions": [
            {"hypothesis_id": "H_MACRO", "hypothesis_name": "Macro Generated", "action_type": "PROVIDE_DATA_SOURCE", "why_action_needed": "macro event calendar is missing"}
        ]
    })


def test_sleeve_throughput_diagnostics_classifies_stops(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_sleeve_throughput_diagnostics_v1(truth_root=tmp_path, day_utc=DAY)
    rows = {row["sleeve_id"]: row for row in payload["sleeves"]}

    assert rows["FLOW"]["throughput_status"] == "FLOWING"
    assert rows["FLOW"]["validation_sample_count"] == 1
    assert rows["SIGNAL_ONLY"]["throughput_status"] == "BLOCKED"
    assert rows["SIGNAL_ONLY"]["blocker_code"] == "SIGNALS_PRESENT_BUT_NO_CANDIDATES"
    assert rows["DORMANT"]["throughput_status"] == "DORMANT"
    assert rows["DORMANT"]["blocker_code"] == "NO_SIGNALS_GENERATED"
    assert rows["PAPER_ONLY"]["throughput_status"] == "UNDERPRODUCING"
    assert rows["PAPER_ONLY"]["blocker_code"] == "NO_OUTCOMES_AVAILABLE"
    assert rows["H_MACRO"]["throughput_status"] == "BLOCKED"
    assert rows["H_MACRO"]["david_action_required"] is True


def test_sleeve_throughput_artifact_is_written_and_read_only(tmp_path: Path) -> None:
    _seed(tmp_path)
    path = write_sleeve_throughput_diagnostics_v1(truth_root=tmp_path, day_utc=DAY)
    assert path == sleeve_throughput_diagnostics_path_v1(truth_root=tmp_path, day_utc=DAY)
    payload = build_sleeve_throughput_diagnostics_v1(truth_root=tmp_path, day_utc=DAY)
    assert payload["read_only"] is True
    assert payload["no_sleeve_mutation"] is True
    assert payload["no_candidate_mutation"] is True
    assert "Why are only a minority" in payload["most_important_question"]["question"]
    assert payload["summary"]["total_sleeves"] >= 5
