from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import json
import subprocess
import sys
from constellation_2.common.atlas_v2_research_os.learning_feedback_reports import write_learning_feedback_report


def test_learning_feedback_report_contains_lineage(tmp_path: Path) -> None:
    root = tmp_path / "store"
    paths = write_learning_feedback_report(root, day="2026-06-04")
    report = json.loads(paths["json"].read_text())
    assert report["lineage_links"]["memory_to_backlog"]
    assert report["candidate_quality_inputs"]
    assert report["recommendation"] == "Continue measurement."
    assert paths["latest_json"].exists()


def test_learning_feedback_cli_commands(tmp_path: Path) -> None:
    root = tmp_path / "store"
    demo = subprocess.run([sys.executable, "-m", "constellation_2.common.atlas_v2_research_os.cli", "--root", str(root), "--learning-feedback-demo"], check=True, capture_output=True, text=True)
    assert json.loads(demo.stdout)["governance_result"] == "PASS"
    report = subprocess.run([sys.executable, "-m", "constellation_2.common.atlas_v2_research_os.cli", "--root", str(root), "--learning-feedback-report"], check=True, capture_output=True, text=True)
    assert Path(json.loads(report.stdout)["latest_json"]).exists()
    audit = subprocess.run([sys.executable, "-m", "constellation_2.common.atlas_v2_research_os.cli", "--root", str(root), "--learning-feedback-audit"], check=True, capture_output=True, text=True)
    assert json.loads(audit.stdout)["learning_feedback_audit_ok"] is True
