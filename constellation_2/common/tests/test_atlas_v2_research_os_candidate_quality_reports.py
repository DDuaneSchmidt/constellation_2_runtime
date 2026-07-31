from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

import json
import subprocess
import sys

from constellation_2.common.atlas_v2_research_os.candidate_quality_reports import audit_candidate_quality_reports, write_candidate_quality_report
from constellation_2.common.atlas_v2_research_os.cli import seed_demo_candidate_quality


def test_creates_json_report(tmp_path: Path) -> None:
    root = tmp_path / "quality"
    seed_demo_candidate_quality(root)
    paths = write_candidate_quality_report(root=root, baseline_id="cq-baseline-demo", treatment_id="cq-treatment-demo", day="2026-06-04")
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["governance_audit_result"] == "PASS"
    assert payload["certification_status"] == "MEASUREMENT_ONLY_PASS"
    assert payload["recommendation"] == "Continue measurement."


def test_creates_markdown_summary_and_latest_files(tmp_path: Path) -> None:
    root = tmp_path / "quality"
    seed_demo_candidate_quality(root)
    paths = write_candidate_quality_report(root=root, baseline_id="cq-baseline-demo", treatment_id="cq-treatment-demo", day="2026-06-04")
    assert "Atlas Candidate Quality Measurement Report" in paths["summary"].read_text(encoding="utf-8")
    assert paths["latest_json"].exists()
    assert paths["latest_summary"].exists()


def test_audit_passes(tmp_path: Path) -> None:
    root = tmp_path / "quality"
    seed_demo_candidate_quality(root)
    write_candidate_quality_report(root=root, baseline_id="cq-baseline-demo", treatment_id="cq-treatment-demo", day="2026-06-04")
    assert audit_candidate_quality_reports(root)["candidate_quality_audit_ok"] is True


def test_cli_seed_report_and_audit() -> None:
    seed = subprocess.run([sys.executable, "-m", "constellation_2.common.atlas_v2_research_os.cli", "--seed-demo-candidate-quality"], check=True, capture_output=True, text=True)
    assert json.loads(seed.stdout)["baseline_id"] == "cq-baseline-demo"
    report = subprocess.run([sys.executable, "-m", "constellation_2.common.atlas_v2_research_os.cli", "--candidate-quality-report"], check=True, capture_output=True, text=True)
    assert "latest_json" in json.loads(report.stdout)
    audit = subprocess.run([sys.executable, "-m", "constellation_2.common.atlas_v2_research_os.cli", "--candidate-quality-audit"], check=True, capture_output=True, text=True)
    assert json.loads(audit.stdout)["candidate_quality_audit_ok"] is True
