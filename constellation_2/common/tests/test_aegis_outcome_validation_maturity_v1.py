from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.hypothesis_outcome_ledger_v1 import build_hypothesis_outcome_ledger_v1
from ops.aegis.hypothesis_state_machine_v1 import transition_hypothesis_state_v1
from ops.aegis.outcome_registry_v1 import build_outcome_registry_v1
from ops.aegis.outcome_validation_maturity_self_check_v1 import outcome_validation_maturity_failures_v1
from ops.aegis.statistical_sufficiency_engine_v1 import build_statistical_sufficiency_v1
from ops.aegis.validation_sample_generator_v1 import build_validation_samples_v1


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed(root: Path, *, closed: bool = False, bad_lineage: bool = False) -> None:
    day = "2026-05-31"
    sleeve = "C2_TREND_EQ_PRIMARY_V1"
    _write(root / "reports" / "sleeve_evaluation_kernel_v1" / day / sleeve / "sleeve_evaluation.v1.json", {"day_utc": day, "sleeve_id": sleeve})
    _write(root / "reports" / "aegis_candidate_contracts_v1" / day / "candidate_contracts.v1.json", {"day_utc": day, "candidate_contracts": [{"candidate_id": "candidate-1", "raw_signal_id": "signal-1", "sleeve_id": sleeve}]})
    position = {
        "position_id": "position-1",
        "candidate_id": "candidate-1",
        "sleeve_id": "" if bad_lineage else sleeve,
        "hypothesis_id": "" if bad_lineage else "HYP_LARGE_CAP_EQUITY_MOMENTUM_20_60D_V1",
        "thesis_id": "" if bad_lineage else "THESIS_TREND_PERSISTENCE_V1",
        "entry_price": "100",
        "current_certified_mark": "105",
        "entry_time": "2026-05-30T14:00:00Z",
        "quantity": "10",
        "exit_time": "2026-05-31T14:00:00Z" if closed else "",
        "exit_price": "110" if closed else "",
        "realized_pnl": "100" if closed else "",
    }
    _write(root / "reports" / "aegis_paper_position_ledger_v1" / day / "paper_position_ledger.v1.json", {"day_utc": day, "positions": [position], "open_positions": [] if closed else [position], "closed_positions": [position] if closed else []})


def test_outcome_registry_generation(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_outcome_registry_v1(truth_root=tmp_path, day_utc="2026-05-31")
    assert payload["summary"]["paper_position_count"] == 1
    assert payload["outcomes"][0]["outcome_state"] == "OPEN"


def test_outcome_state_classification_closed_win(tmp_path: Path) -> None:
    _seed(tmp_path, closed=True)
    payload = build_outcome_registry_v1(truth_root=tmp_path, day_utc="2026-05-31")
    assert payload["outcomes"][0]["outcome_state"] == "CLOSED_WIN"
    assert payload["outcomes"][0]["realized_return"] == 0.1


def test_hypothesis_outcome_ledger_aggregation(tmp_path: Path) -> None:
    _seed(tmp_path, closed=True)
    outcomes = build_outcome_registry_v1(truth_root=tmp_path, day_utc="2026-05-31")
    samples = build_validation_samples_v1(truth_root=tmp_path, day_utc="2026-05-31", outcome_registry=outcomes)
    ledger = build_hypothesis_outcome_ledger_v1(truth_root=tmp_path, day_utc="2026-05-31", outcome_registry=outcomes, validation_samples=samples)
    row = ledger["hypotheses"][0]
    assert row["closed_positions_count"] == 1
    assert row["wins"] == 1
    assert row["usable_validation_sample_count"] == 1


def test_validation_sample_creation(tmp_path: Path) -> None:
    _seed(tmp_path, closed=True)
    outcomes = build_outcome_registry_v1(truth_root=tmp_path, day_utc="2026-05-31")
    samples = build_validation_samples_v1(truth_root=tmp_path, day_utc="2026-05-31", outcome_registry=outcomes)
    assert samples["samples"][0]["sample_state"] == "INCLUDED"
    assert samples["samples"][0]["return_value"] == 0.1


def test_sample_exclusion_reasons(tmp_path: Path) -> None:
    _seed(tmp_path)
    outcomes = build_outcome_registry_v1(truth_root=tmp_path, day_utc="2026-05-31")
    samples = build_validation_samples_v1(truth_root=tmp_path, day_utc="2026-05-31", outcome_registry=outcomes)
    assert samples["samples"][0]["sample_state"] == "EXCLUDED_OPEN_POSITION"
    assert samples["samples"][0]["exclusion_reason"] == "OPEN_POSITION_NOT_RESOLVED"


def test_statistical_sufficiency_state_classification(tmp_path: Path) -> None:
    _seed(tmp_path, closed=True)
    outcomes = build_outcome_registry_v1(truth_root=tmp_path, day_utc="2026-05-31")
    samples = build_validation_samples_v1(truth_root=tmp_path, day_utc="2026-05-31", outcome_registry=outcomes)
    ledger = build_hypothesis_outcome_ledger_v1(truth_root=tmp_path, day_utc="2026-05-31", outcome_registry=outcomes, validation_samples=samples)
    suff = build_statistical_sufficiency_v1(truth_root=tmp_path, day_utc="2026-05-31", ledger=ledger)
    assert suff["hypotheses"][0]["sufficiency_state"] == "ACCUMULATING"
    assert "INSUFFICIENT_CLOSED_SAMPLES" in suff["hypotheses"][0]["state_reason_codes"]


def test_hypothesis_state_transition_from_sufficiency() -> None:
    transition = transition_hypothesis_state_v1({"hypothesis_id": "h", "status": "ACCUMULATING_EVIDENCE", "linked_candidates": [], "linked_paper_positions": ["p"], "linked_validation_samples": [], "linked_validation_results": []}, {"sufficiency_state": "VALIDATION_READY"}, "/tmp/statistical_sufficiency.v1.json")
    assert transition["new_state"] == "VALIDATION_READY"
    assert transition["sufficiency_artifact"]


def test_self_check_success_path(tmp_path: Path) -> None:
    _seed(tmp_path, closed=True)
    outcomes = build_outcome_registry_v1(truth_root=tmp_path, day_utc="2026-05-31")
    samples = build_validation_samples_v1(truth_root=tmp_path, day_utc="2026-05-31", outcome_registry=outcomes)
    ledger = build_hypothesis_outcome_ledger_v1(truth_root=tmp_path, day_utc="2026-05-31", outcome_registry=outcomes, validation_samples=samples)
    suff = build_statistical_sufficiency_v1(truth_root=tmp_path, day_utc="2026-05-31", ledger=ledger)
    failures = outcome_validation_maturity_failures_v1(outcomes, samples, ledger, suff, {"transitions": []})
    assert failures == []


def test_self_check_failure_path() -> None:
    failures = outcome_validation_maturity_failures_v1({"outcomes": [{"outcome_id": "o", "position_id": "p", "outcome_state": "UNKNOWN_BLOCKED"}]}, {"samples": [{"sample_id": "s", "sample_state": "INCLUDED"}]}, {"hypotheses": []}, {"hypotheses": [{"hypothesis_id": "h", "sufficiency_state": "UNDERPOWERED", "state_reason_codes": []}]}, {"transitions": []})
    ids = {row["check_id"] for row in failures}
    assert "unknown_blocked_without_reason" in ids
    assert "validation_sample_broken_lineage" in ids
    assert "sufficiency_missing_reason_codes" in ids


def test_deterministic_rerun_stability(tmp_path: Path) -> None:
    _seed(tmp_path, closed=True)
    first = build_outcome_registry_v1(truth_root=tmp_path, day_utc="2026-05-31")
    second = build_outcome_registry_v1(truth_root=tmp_path, day_utc="2026-05-31")
    assert first["content_hash"] == second["content_hash"]
    assert first["outcomes"][0]["outcome_id"] == second["outcomes"][0]["outcome_id"]
