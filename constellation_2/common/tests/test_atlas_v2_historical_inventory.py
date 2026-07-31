from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.atlas.v2_core import AtlasV2Ledger
from ops.atlas.v2_historical_experience_inventory import (
    build_historical_inventory,
    write_historical_inventory_report,
)

DAY = "2026-06-04"


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_inventory_does_not_convert_by_default(tmp_path: Path) -> None:
    _write(
        tmp_path / "research_journal/failures/FAIL_0001.yaml",
        "id: FAIL_0001\ndate: 2026-06-01\nwhat_we_expected: Evidence maturity would be high.\nwhat_failed: Evidence maturity was low.\nwhy_failed: Historical mismatch.\n",
    )
    payload = build_historical_inventory(tmp_path, day=DAY)
    ledger = AtlasV2Ledger(tmp_path / "ledger")

    assert payload["historical_records_discovered"] == 1
    assert payload["eligible_count"] == 1
    assert payload["authority_boundary"]["conversion_performed"] is False
    assert payload["authority_boundary"]["experience_events_created"] == 0
    assert ledger.records("HistoricalExperienceRecord") == []
    assert ledger.records("ExperienceEvent") == []


def test_missing_expectation_is_classified_correctly(tmp_path: Path) -> None:
    _write(
        tmp_path / "research_journal/observations/OBS_0001.yaml",
        "id: OBS_0001\ndate: 2026-06-01\nobservation: Outcome flow bottleneck was visible.\nwhy_interesting: It was actionable as learning context.\n",
    )
    payload = build_historical_inventory(tmp_path, day=DAY)
    candidate = payload["candidate_artifacts"][0]

    assert candidate["source_type"] == "OBSERVATION"
    assert candidate["has_expectation"] is False
    assert candidate["has_outcome"] is True
    assert candidate["conversion_eligibility"] == "INCOMPLETE_EXPECTATION"


def test_missing_outcome_is_classified_correctly(tmp_path: Path) -> None:
    _write(
        tmp_path / "research_journal/knowledge/KNW_0001.yaml",
        "id: KNW_0001\ndate: 2026-06-01\nstatement: Claim requires outcome-linked evidence before learning.\nwhy_it_matters: Prevents fabricated experience.\n",
    )
    payload = build_historical_inventory(tmp_path, day=DAY)
    candidate = payload["candidate_artifacts"][0]

    assert candidate["source_type"] == "KNOWLEDGE"
    assert candidate["has_expectation"] is True
    assert candidate["has_outcome"] is False
    assert candidate["conversion_eligibility"] == "INCOMPLETE_OUTCOME"


def test_provenance_detection_works(tmp_path: Path) -> None:
    _write(
        tmp_path / "research_journal/reports/review_001.md",
        "# Review\n\nExpected outcome: evidence coverage improves.\n\nActual outcome: evidence coverage improved after the run.\n",
    )
    payload = build_historical_inventory(tmp_path, day=DAY)
    candidate = payload["candidate_artifacts"][0]

    assert candidate["has_provenance"] is True
    assert candidate["source_path"] == "research_journal/reports/review_001.md"
    assert candidate["conversion_eligibility"] == "ELIGIBLE"


def test_inventory_authority_audit_passes(tmp_path: Path) -> None:
    _write(
        tmp_path / "research_journal/failures/FAIL_0001.yaml",
        "id: FAIL_0001\nwhat_we_expected: Expected learning.\nwhat_failed: Actual miss.\n",
    )
    payload = build_historical_inventory(tmp_path, day=DAY)
    ledger = AtlasV2Ledger(tmp_path / "ledger")

    assert payload["authority_boundary"]["trading_authority"] is False
    assert payload["authority_boundary"]["broker_execution_allowed"] is False
    assert payload["authority_boundary"]["autonomous_execution_allowed"] is False
    assert payload["authority_boundary"]["candidate_authority"] is False
    assert ledger.audit_all().ok


def test_inventory_report_files_are_written(tmp_path: Path) -> None:
    _write(
        tmp_path / "research_journal/failures/FAIL_0001.yaml",
        "id: FAIL_0001\nwhat_we_expected: Expected learning.\nwhat_failed: Actual miss.\n",
    )
    paths = write_historical_inventory_report(tmp_path, output_root=tmp_path / "reports/atlas_v2_historical_inventory/2026-06-04", day=DAY)

    assert paths["json"].exists()
    assert paths["summary"].exists()
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["historical_records_discovered"] == 1
    assert "conversion_performed: false" in paths["summary"].read_text(encoding="utf-8")
