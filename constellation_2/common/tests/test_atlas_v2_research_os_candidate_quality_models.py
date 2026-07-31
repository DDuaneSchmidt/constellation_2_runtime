from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from constellation_2.common.atlas_v2_research_os.candidate_quality_models import CandidateQualityBaseline, CandidateQualityEvaluation, CandidateQualityTreatment, MEASUREMENT_ONLY_LIMITATION

NOW = "2026-06-04T00:00:00Z"
WINDOW = {"start": "2026-06-01", "end": "2026-06-04"}


def test_creates_baseline_model() -> None:
    row = CandidateQualityBaseline("b1", NOW, "test", [], 10, 2, 4, 4, 1, 1, ["HISTORICAL_REPLAY"], 2, 1, 3, ["REGIME_MISMATCH"], WINDOW, "u1", "cf1").to_dict()
    assert row["baseline_id"] == "b1"
    assert row["governance_status"] == "MEASUREMENT_ONLY"


def test_creates_treatment_model() -> None:
    row = CandidateQualityTreatment("t1", NOW, "test", [], 10, 3, 3, 4, 1, 2, ["HISTORICAL_REPLAY"], 2, 2, 2, ["REGIME_MISMATCH"], WINDOW, "u1", "cf1", ["learn1"], ["mem1"]).to_dict()
    assert row["learning_input_ids"] == ["learn1"]


def test_creates_evaluation_model_and_required_fields_exist() -> None:
    row = CandidateQualityEvaluation("e1", NOW, "test", [], "b1", "t1", WINDOW, "u1", "cf1", ["learn1"], {}, {}, "PASS", [MEASUREMENT_ONLY_LIMITATION], {}, "EVALUATED", True, [], True, False, {"status": "MEASUREMENT_ONLY_PASS"}).to_dict()
    for field in ["evaluation_id", "created_at", "created_by", "source_artifact_ids", "baseline_id", "treatment_id", "measurement_window", "signal_universe_id", "candidate_factory_version", "learning_input_ids", "metric_set", "delta", "governance_status", "limitations", "metadata"]:
        assert field in row


def test_measurement_artifacts_do_not_imply_readiness() -> None:
    row = CandidateQualityBaseline("b2", NOW, "test", [], 10, 2, 4, 4, 1, 1, ["HISTORICAL_REPLAY"], 2, 1, 3, ["UNKNOWN"], WINDOW, "u1", "cf1").to_dict()
    assert "readiness" not in row
    assert row["metadata"].get("candidate_promotion_authorized") is not True
