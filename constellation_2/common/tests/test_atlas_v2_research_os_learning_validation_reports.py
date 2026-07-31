from __future__ import annotations

import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.learning_validation_reports import audit_learning_validation_report, write_learning_validation_report


def _snapshot(day: str, failures: int, duplicates: int, regimes: int, evidence: float, survival: float, quality: float) -> dict:
    return {
        "snapshot_id": f"s-{day}",
        "observed_at": f"{day}T00:00:00Z",
        "repeated_failures": failures,
        "duplicate_ideas": duplicates,
        "regime_gaps": regimes,
        "evidence_maturity_score": evidence,
        "hypothesis_survival_rate": survival,
        "candidate_quality_score": quality,
    }


def test_learning_validation_report_written(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    paths = write_learning_validation_report([
        _snapshot("2026-06-01", 10, 8, 6, 0.3, 0.4, 0.45),
        _snapshot("2026-06-05", 5, 4, 3, 0.5, 0.6, 0.7),
    ], horizon="7_day", day="2026-06-05")
    report = json.loads(paths["json"].read_text())
    assert report["trend_direction"] == "IMPROVING"
    assert report["snapshot_count"] == 2
    assert report["certification"]["certification_type"] == "CONTINUOUS_LEARNING_VALIDATION_CERTIFICATION"
    assert report["improvements"]
    assert paths["latest_json"].exists()
    assert "no trading" in paths["summary"].read_text().lower()
    assert audit_learning_validation_report(report)["learning_validation_audit_ok"] is True
