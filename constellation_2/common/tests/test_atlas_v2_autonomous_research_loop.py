from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.atlas.v2_autonomous_research_loop_runner import AtlasV2AutonomousResearchLoopRunner, default_claim_payload
from ops.atlas.v2_core import AtlasV2Ledger, AtlasV2ValidationError, validate_object

NOW = "2026-06-04T00:00:00Z"
SCHEMA_PATH = REPO_ROOT / "governance/04_DATA/SCHEMAS/ATLAS/V2/atlas_v2_autonomous_research_loop.v1.schema.json"


def test_autonomous_research_loop_completes_claim_to_learning_record(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    result = AtlasV2AutonomousResearchLoopRunner(ledger).run(created_at=NOW)

    assert result.run["status"] == "COMPLETED_SAFE_RESEARCH_LOOP"
    assert result.run["mode"] == "mock"
    assert result.run["claims_seen"] == 1
    assert result.run["hypotheses_created"] == 1
    assert result.run["specs_created"] == 1
    assert result.run["results_created"] == 1
    assert result.run["experience_events_created"] == 1
    assert result.run["learning_estimates_created"] > 0
    assert result.label_integrity_report is not None
    assert result.summary_record["label_integrity_report_id"] == result.label_integrity_report["report_id"]
    assert ledger.records("ExternalStrategyClaim")
    assert ledger.records("ExternalStrategyMechanism")
    assert ledger.records("ResearchHypothesis")
    assert ledger.records("CheapExperimentSpec")
    assert ledger.records("ExperimentResult")
    assert ledger.records("ExperienceEvent")
    assert ledger.records("LearningEstimate")
    assert ledger.records("LabelIntegrityReport")
    assert ledger.audit_cheap_experiment_authority_boundaries().ok
    assert ledger.audit_forbidden_artifacts().ok


@pytest.mark.parametrize("mode", ["mock", "historical"])
def test_autonomous_research_loop_requires_safe_mode(tmp_path: Path, mode: str) -> None:
    result = AtlasV2AutonomousResearchLoopRunner(AtlasV2Ledger(tmp_path)).run(mode=mode, created_at=NOW)
    assert result.run["mode"] == mode

    with pytest.raises(AtlasV2ValidationError, match="mock or historical"):
        AtlasV2AutonomousResearchLoopRunner(AtlasV2Ledger(tmp_path / "bad")).run(mode="live", created_at=NOW)


def test_autonomous_research_loop_max_loop_default_and_bound(tmp_path: Path) -> None:
    claims = [
        {
            **default_claim_payload(created_at=NOW),
            "claim_id": f"arl-claim-{index:02d}",
            "claim_text": f"Opening range replay claim variant {index} with VWAP filter.",
            "entry_rule": f"price breaks above the opening range high after candle close variant {index}",
        }
        for index in range(12)
    ]
    result = AtlasV2AutonomousResearchLoopRunner(AtlasV2Ledger(tmp_path)).run(claims=claims, created_at=NOW)

    assert result.run["max_loop_count"] == 10
    assert result.run["claims_seen"] == 10
    assert result.run["results_created"] == 10

    with pytest.raises(AtlasV2ValidationError, match="between 0 and 10"):
        AtlasV2AutonomousResearchLoopRunner(AtlasV2Ledger(tmp_path / "too_many")).run(max_loop_count=11, created_at=NOW)


def test_autonomous_research_loop_stops_on_authority_violation(tmp_path: Path) -> None:
    bad_claim = {**default_claim_payload(created_at=NOW), "candidate_id": "forbidden-candidate"}

    result = AtlasV2AutonomousResearchLoopRunner(AtlasV2Ledger(tmp_path)).run(claims=[bad_claim], created_at=NOW)

    assert result.stopped_on_authority_violation is True
    assert result.run["status"] == "STOPPED_AUTHORITY_VIOLATION"
    assert result.run["results_created"] == 0
    assert result.run["experience_events_created"] == 0
    assert not AtlasV2Ledger(tmp_path).records("CheapExperiment")


def test_autonomous_research_loop_schema_validation_passes(tmp_path: Path) -> None:
    result = AtlasV2AutonomousResearchLoopRunner(AtlasV2Ledger(tmp_path)).run(mode="historical", created_at=NOW)
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)

    for record in (result.run, result.summary_record):
        validate_object(record)
        validator.validate(record)


def test_autonomous_research_loop_cli_runs_fixture_loop(tmp_path: Path) -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "ops.atlas.v2_autonomous_research_loop_runner",
            "--ledger-root",
            str(tmp_path),
            "--mode",
            "mock",
            "--max-loop-count",
            "3",
            "--created-at",
            NOW,
        ],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    summary = json.loads(completed.stdout)

    assert summary["status"] == "COMPLETED_SAFE_RESEARCH_LOOP"
    assert summary["claims_seen"] == 1
    assert summary["experience_events_created"] == 1
    assert (tmp_path / "AutonomousResearchLoopRun.jsonl").exists()
