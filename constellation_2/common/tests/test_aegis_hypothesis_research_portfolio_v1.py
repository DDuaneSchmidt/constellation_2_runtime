from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.hypothesis_registry_v1 import build_hypothesis_registry_v1
from ops.aegis.hypothesis_state_machine_v1 import transition_hypothesis_state_v1
from ops.aegis.research_allocation_score_v1 import score_hypothesis_allocation_v1
from ops.aegis.research_portfolio_manager_v1 import build_research_portfolio_v1
from ops.aegis.research_portfolio_self_check_v1 import research_portfolio_failures_v1
from ops.aegis.research_thesis_registry_v1 import build_research_thesis_registry_v1


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed(root: Path, day: str = "2026-05-31", sleeve: str = "C2_TREND_EQ_PRIMARY_V1") -> None:
    _write(root / "reports" / "sleeve_evaluation_kernel_v1" / day / sleeve / "sleeve_evaluation.v1.json", {"day_utc": day, "sleeve_id": sleeve})
    _write(root / "reports" / "aegis_candidate_contracts_v1" / day / "candidate_contracts.v1.json", {"day_utc": day, "candidate_contracts": [{"candidate_id": "candidate-1", "raw_signal_id": "signal-1", "sleeve_id": sleeve, "entry_reference_price_timestamp_utc": f"{day}T12:00:00Z"}]})
    pos = {"position_id": "position-1", "candidate_id": "candidate-1", "sleeve_id": sleeve, "candidate_lineage": {"candidate_id": "candidate-1", "sleeve_id": sleeve}}
    _write(root / "reports" / "aegis_paper_position_ledger_v1" / day / "paper_position_ledger.v1.json", {"day_utc": day, "positions": [pos], "open_positions": [pos], "closed_positions": []})
    _write(root / "reports" / "aegis_research_validation_samples_v1" / day / "research_validation_samples.v1.json", {"day_utc": day, "samples": [{"sample_id": "sample-1", "hypothesis_id": "HYP_LARGE_CAP_EQUITY_MOMENTUM_20_60D_V1", "sleeve_id": sleeve, "candidate_id": "candidate-1", "outcome_id": "outcome-1"}]})
    _write(root / "reports" / "aegis_research_validation_result_v1" / day / "research_validation_result.v1.json", {"day_utc": day, "results": [{"result_id": "result-1", "hypothesis_id": "HYP_LARGE_CAP_EQUITY_MOMENTUM_20_60D_V1", "artifact_binding_failures": []}]})


def test_thesis_registry_generation(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_research_thesis_registry_v1(truth_root=tmp_path, day_utc="2026-05-31")
    assert payload["summary"]["thesis_count"] == 1
    assert payload["theses"][0]["thesis_id"] == "THESIS_TREND_PERSISTENCE_V1"


def test_hypothesis_registry_generation(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_hypothesis_registry_v1(truth_root=tmp_path, day_utc="2026-05-31")
    assert payload["summary"]["hypothesis_count"] == 1
    assert payload["hypotheses"][0]["hypothesis_id"] == "HYP_LARGE_CAP_EQUITY_MOMENTUM_20_60D_V1"


def test_sleeve_to_hypothesis_mapping(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_hypothesis_registry_v1(truth_root=tmp_path, day_utc="2026-05-31")
    assert payload["sleeve_implementations"][0]["mapping_confidence"] == "LEGACY_INFERRED"
    assert payload["sleeve_implementations"][0]["hypothesis_id"] == "HYP_LARGE_CAP_EQUITY_MOMENTUM_20_60D_V1"


def test_candidate_to_hypothesis_mapping(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_hypothesis_registry_v1(truth_root=tmp_path, day_utc="2026-05-31")
    assert payload["candidate_hypothesis_links"][0]["candidate_id"] == "candidate-1"
    assert payload["candidate_hypothesis_links"][0]["hypothesis_id"] == "HYP_LARGE_CAP_EQUITY_MOMENTUM_20_60D_V1"


def test_paper_position_inheritance(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_hypothesis_registry_v1(truth_root=tmp_path, day_utc="2026-05-31")
    assert payload["paper_position_hypothesis_links"][0]["position_id"] == "position-1"
    assert payload["paper_position_hypothesis_links"][0]["thesis_id"] == "THESIS_TREND_PERSISTENCE_V1"


def test_validation_sample_binding(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_hypothesis_registry_v1(truth_root=tmp_path, day_utc="2026-05-31")
    hyp = payload["hypotheses"][0]
    assert hyp["linked_validation_samples"] == ["sample-1"]
    assert hyp["linked_validation_results"] == ["result-1"]


def test_state_machine_transition() -> None:
    transition = transition_hypothesis_state_v1({"hypothesis_id": "h", "status": "INVESTIGATING", "linked_candidates": ["c"], "linked_paper_positions": [], "linked_validation_samples": [], "linked_validation_results": []})
    assert transition["new_state"] == "ACCUMULATING_EVIDENCE"
    assert transition["transition_reason"] == "CANDIDATE_EVIDENCE_PRESENT"


def test_allocation_scoring_reason_codes() -> None:
    scored = score_hypothesis_allocation_v1({"linked_candidates": [str(i) for i in range(12)], "linked_paper_positions": ["p"], "linked_validation_samples": [], "linked_validation_results": [], "linked_outcomes": [], "linked_sleeves": ["s"], "evidence_objects": [{} for _ in range(3)]}, "ACCUMULATING_EVIDENCE")
    assert scored["allocation_recommendation"] in {"INCREASE", "MAINTAIN", "REDUCE", "PAUSE", "RETIRE", "INVESTIGATE_MORE"}
    assert "HIGH_CANDIDATE_YIELD" in scored["reason_codes"]


def test_self_check_failures() -> None:
    failures = research_portfolio_failures_v1({"theses": [], "hypotheses": [{"hypothesis_id": "h", "thesis_id": "missing"}], "sleeve_implementations": [{"sleeve_id": "s", "hypothesis_id": "missing"}], "candidate_hypothesis_links": [{"candidate_id": "c", "hypothesis_id": "missing"}], "paper_position_hypothesis_links": [{"position_id": "p", "hypothesis_id": "missing"}], "research_allocation_recommendations": [{"hypothesis_id": "h", "allocation_recommendation": "MAINTAIN"}]})
    assert {row["check_id"] for row in failures} >= {"hypothesis_without_thesis", "orphan_sleeve", "orphan_candidate", "paper_position_missing_hypothesis", "allocation_missing_reason_codes"}


def test_legacy_migration_behavior(tmp_path: Path) -> None:
    _seed(tmp_path, sleeve="C2_CROSS_ASSET_TREND_V1")
    portfolio = build_research_portfolio_v1(truth_root=tmp_path, day_utc="2026-05-31")
    assert portfolio["hypotheses"][0]["mapping_confidence"] == "LEGACY_INFERRED"
    assert portfolio["hypotheses"][0]["thesis_id"] == "THESIS_TREND_PERSISTENCE_V1"
