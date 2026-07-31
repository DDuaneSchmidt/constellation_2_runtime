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

from constellation_2.common.atlas_v2_research_os.artifact_models import ArtifactType
from constellation_2.common.atlas_v2_research_os.artifact_store import ArtifactStore
from constellation_2.common.atlas_v2_research_os.reports import write_foundation_report
from constellation_2.common.atlas_v2_research_os.research_backlog import ResearchBacklog

NOW = "2026-06-04T00:00:00Z"


def _seed(root: Path) -> None:
    store = ArtifactStore(root)
    store.create_artifact(artifact_id="q-001", artifact_type=ArtifactType.QUESTION.value, created_at=NOW, created_by="test", is_root=True)
    ResearchBacklog(root).create_backlog_item(backlog_item_id="bi-001", item_type="RESEARCH_QUESTION", title="Question", description="Desc", created_at=NOW, created_by="test", state="READY", source_artifact_ids=["q-001"], expected_learning_value=0.5)


def test_creates_json_report(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    _seed(root)
    paths = write_foundation_report(root, day="2026-06-04")
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["governance_audit_result"] == "PASS"
    assert payload["artifact_counts"] == {"Question": 1}


def test_creates_markdown_summary(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    _seed(root)
    paths = write_foundation_report(root, day="2026-06-04")
    assert "Atlas V2 Research OS Foundation Report" in paths["summary"].read_text(encoding="utf-8")


def test_updates_latest_files(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    _seed(root)
    paths = write_foundation_report(root, day="2026-06-04")
    assert paths["latest_json"].exists()
    assert paths["latest_summary"].exists()


def test_cli_audit_report_seed_and_select(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    subprocess.run([sys.executable, "-m", "constellation_2.common.atlas_v2_research_os.cli", "--root", str(root), "--seed-demo-backlog"], check=True, text=True, capture_output=True)
    audit = subprocess.run([sys.executable, "-m", "constellation_2.common.atlas_v2_research_os.cli", "--root", str(root), "--audit"], check=True, text=True, capture_output=True)
    assert json.loads(audit.stdout)["lineage_ok"] is True
    report = subprocess.run([sys.executable, "-m", "constellation_2.common.atlas_v2_research_os.cli", "--root", str(root), "--report"], check=True, text=True, capture_output=True)
    assert "latest_json" in json.loads(report.stdout)
    selected = subprocess.run([sys.executable, "-m", "constellation_2.common.atlas_v2_research_os.cli", "--root", str(root), "--select-next", "--limit", "1"], check=True, text=True, capture_output=True)
    assert json.loads(selected.stdout)[0]["backlog_item_id"] == "bi-demo-opening-range"
