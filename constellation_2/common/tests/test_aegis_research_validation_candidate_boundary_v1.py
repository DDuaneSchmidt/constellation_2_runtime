from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.candidate_observability_v1 import build_candidate_generation_manifest_v1
from ops.aegis.research_lab.research_validation_engine_v1 import build_research_promotion_gate_v1, research_promotion_gate_path_v1

DAY = "2026-05-30"
HYPOTHESIS = "rh-process-test-etf-drop-mean-reversion-v1"


def _write_gate(root: Path, status: str) -> None:
    result_payload = {
        "results": [
            {
                "result_id": "result-1",
                "hypothesis_id": HYPOTHESIS,
                "hypothesis_version": "v1",
                "validation_status": status,
                "source_artifacts": [{"path": "source.json", "sha256": "a" * 64}],
            }
        ]
    }
    gate = build_research_promotion_gate_v1(day_utc=DAY, result_payload=result_payload)
    path = research_promotion_gate_path_v1(truth_root=root, day_utc=DAY)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(gate), encoding="utf-8")


def test_research_derived_candidate_is_suppressed_without_supported_gate(tmp_path: Path) -> None:
    _write_gate(tmp_path, "UNDER_SAMPLED")
    manifest = build_candidate_generation_manifest_v1(
        day_utc=DAY,
        environment="TEST",
        truth_root=tmp_path,
        outcomes=[{"engine_id": "research_sleeve", "status": "INTENT_CREATED", "hypothesis_id": HYPOTHESIS, "output_intents": [{"intent_id": "i1", "symbol": "SPY"}]}],
        run_id="run-1",
        produced_at_utc="2026-05-30T12:00:00Z",
    )
    row = manifest["candidate_rows"][0]

    assert row["research_validation_required"] is True
    assert row["research_validation_enforcement_status"] == "REJECTED_RESEARCH_NOT_SUPPORTED"
    assert row["status"] == "SUPPRESSED"
    assert row["manual_capture_eligible"] is False


def test_research_derived_candidate_can_be_candidate_review_only_when_supported(tmp_path: Path) -> None:
    _write_gate(tmp_path, "SUPPORTED")
    manifest = build_candidate_generation_manifest_v1(
        day_utc=DAY,
        environment="TEST",
        truth_root=tmp_path,
        outcomes=[{"engine_id": "research_sleeve", "status": "INTENT_CREATED", "hypothesis_id": HYPOTHESIS, "output_intents": [{"intent_id": "i1", "symbol": "SPY"}]}],
        run_id="run-1",
        produced_at_utc="2026-05-30T12:00:00Z",
    )
    row = manifest["candidate_rows"][0]

    assert row["research_validation_required"] is True
    assert row["research_validation_enforcement_status"] == "PASS"
    assert row["status"] == "CANDIDATE_CREATED"
    assert manifest["execution_authority_granted"] is False
    assert manifest["order_submission_attempted"] is False
    assert manifest["trading_behavior_changed"] is False


def test_non_research_candidate_declares_not_research_derived(tmp_path: Path) -> None:
    manifest = build_candidate_generation_manifest_v1(
        day_utc=DAY,
        environment="TEST",
        truth_root=tmp_path,
        outcomes=[{"engine_id": "technical_sleeve", "status": "INTENT_CREATED", "output_intents": [{"intent_id": "i1", "symbol": "QQQ"}]}],
        run_id="run-1",
        produced_at_utc="2026-05-30T12:00:00Z",
    )
    row = manifest["candidate_rows"][0]

    assert row["research_validation_required"] is False
    assert row["research_validation_enforcement_status"] == "NOT_RESEARCH_DERIVED"
