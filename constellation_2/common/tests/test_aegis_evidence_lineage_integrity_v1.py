from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.evidence_lineage_integrity_v1 import build_evidence_lineage_integrity_v1, integrity_failures_v1


def _write(root: Path, family: str, day: str, filename: str, payload: dict) -> None:
    path = root / "reports" / family / day / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed_traceable_chain(root: Path, day: str, *, mismatch: bool = False) -> None:
    _write(root, "aegis_candidate_contracts_v1", day, "candidate_contracts.v1.json", {
        "day_utc": day,
        "generated_at_utc": f"{day}T12:00:00Z",
        "candidate_contracts": [{
            "candidate_id": "candidate-1",
            "raw_signal_id": "signal-1",
            "sleeve_id": "SLEEVE_A",
            "entry_reference_price_timestamp_utc": f"{day}T12:01:00Z",
        }],
    })
    position = {
        "position_id": "position-1",
        "candidate_id": "candidate-1",
        "sleeve_id": "SLEEVE_A",
        "candidate_lineage": {"candidate_id": "candidate-1", "sleeve_id": "SLEEVE_A", "raw_signal_id": "signal-1"},
        "current_certified_mark": 101.25,
        "mark_timestamp_utc": f"{day}T21:00:00Z",
        "mark_source_path": "/tmp/market_data.v1.json",
        "mark_certification_status": "CERTIFIED",
    }
    _write(root, "aegis_paper_position_ledger_v1", day, "paper_position_ledger.v1.json", {
        "day_utc": day,
        "positions": [position],
        "open_positions": [position],
        "closed_positions": [],
    })
    _write(root, "aegis_research_validation_samples_v1", day, "research_validation_samples.v1.json", {
        "day_utc": day,
        "samples": [{
            "sample_id": "sample-1",
            "hypothesis_id": "hypothesis-1",
            "candidate_id": "candidate-1",
            "sleeve_id": "SLEEVE_A",
            "outcome_id": "outcome-1",
            "forward_return_1d": 0.01,
        }],
    })
    _write(root, "aegis_research_validation_result_v1", day, "research_validation_result.v1.json", {
        "day_utc": day,
        "results": [{
            "result_id": "result-1",
            "hypothesis_id": "hypothesis-1",
            "artifact_binding_failures": ["HYPOTHESIS_SAMPLE_BINDING_MISMATCH"] if mismatch else [],
            "hypothesis_binding": {"binding_status": "MISMATCH" if mismatch else "PASS"},
        }],
    })
    _write(root, "aegis_sleeve_analytics_v1", day, "sleeve_analytics.v1.json", {
        "day_utc": day,
        "summary": {"total_sleeves": 1},
        "sleeves": [{"sleeve_id": "SLEEVE_A"}],
    })
    _write(root, "aegis_sleeve_performance_truth_v1", day, "sleeve_performance_truth.v1.json", {
        "day_utc": day,
        "sleeves": [{"sleeve_id": "SLEEVE_A"}],
    })


def test_evidence_lineage_integrity_passes_traceable_chain(tmp_path: Path) -> None:
    day = "2026-05-31"
    _seed_traceable_chain(tmp_path, day)

    payload = build_evidence_lineage_integrity_v1(truth_root=tmp_path, day_utc=day)

    assert payload["evidence_coverage_panel"]["current_integrity_status"] == "GREEN"
    assert payload["evidence_coverage_panel"]["unknown_position_count"] == 0
    assert payload["evidence_coverage_panel"]["sample_binding_errors"] == 0
    assert integrity_failures_v1(payload) == []


def test_evidence_lineage_integrity_flags_validation_binding_mismatch(tmp_path: Path) -> None:
    day = "2026-05-31"
    _seed_traceable_chain(tmp_path, day, mismatch=True)

    payload = build_evidence_lineage_integrity_v1(truth_root=tmp_path, day_utc=day)
    failures = integrity_failures_v1(payload)

    assert payload["evidence_coverage_panel"]["sample_binding_errors"] == 1
    assert any("validation_binding_integrity" in failure for failure in failures)
    assert any("scorecard_traceability_integrity" in failure for failure in failures)
