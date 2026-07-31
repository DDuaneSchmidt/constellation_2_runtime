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

from constellation_2.common.atlas_v2_research_os.cli import seed_demo_memory
from constellation_2.common.atlas_v2_research_os.memory_reports import write_memory_report


def test_creates_json_report(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    seed_demo_memory(root)
    paths = write_memory_report(root, day="2026-06-04")
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["governance_audit_result"] == "PASS"
    assert payload["repeated_failure_counts"] == 1
    assert payload["likely_duplicate_counts"] >= 1


def test_creates_markdown_summary_and_latest_files(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    seed_demo_memory(root)
    paths = write_memory_report(root, day="2026-06-04")
    assert "Atlas V2 Research Memory Report" in paths["summary"].read_text(encoding="utf-8")
    assert paths["latest_json"].exists()
    assert paths["latest_summary"].exists()


def test_memory_cli_commands_do_not_run_autonomous_research(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    subprocess.run([sys.executable, "-m", "constellation_2.common.atlas_v2_research_os.cli", "--root", str(root), "--seed-demo-memory"], check=True, text=True, capture_output=True)
    audit = subprocess.run([sys.executable, "-m", "constellation_2.common.atlas_v2_research_os.cli", "--root", str(root), "--memory-audit"], check=True, text=True, capture_output=True)
    assert json.loads(audit.stdout)["memory_integrity_ok"] is True
    report = subprocess.run([sys.executable, "-m", "constellation_2.common.atlas_v2_research_os.cli", "--root", str(root), "--memory-report"], check=True, text=True, capture_output=True)
    assert "latest_json" in json.loads(report.stdout)
    duplicates = subprocess.run([sys.executable, "-m", "constellation_2.common.atlas_v2_research_os.cli", "--root", str(root), "--find-duplicates"], check=True, text=True, capture_output=True)
    assert json.loads(duplicates.stdout)[0]["status"] == "LIKELY_DUPLICATE"
    failures = subprocess.run([sys.executable, "-m", "constellation_2.common.atlas_v2_research_os.cli", "--root", str(root), "--list-repeated-failures"], check=True, text=True, capture_output=True)
    assert json.loads(failures.stdout)[0]["failure_id"] == "failure-demo-opening-range"
