from __future__ import annotations

from pathlib import Path
import json
import subprocess
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.artifact_models import ArtifactType
from constellation_2.common.atlas_v2_research_os.artifact_store import ArtifactStore
from constellation_2.common.atlas_v2_research_os.certification_reports import write_research_os_certification_report
from constellation_2.common.atlas_v2_research_os.research_backlog import ResearchBacklog

NOW = "2026-06-04T00:00:00Z"


def _seed(root: Path) -> None:
    store = ArtifactStore(root)
    store.create_artifact(artifact_id="q-001", artifact_type=ArtifactType.QUESTION.value, created_at=NOW, created_by="test", is_root=True)
    ResearchBacklog(root).create_backlog_item(backlog_item_id="bi-001", item_type="RESEARCH_QUESTION", title="Question", description="Desc", created_at=NOW, created_by="test", state="READY", source_artifact_ids=["q-001"], expected_learning_value=0.5)


def test_writes_required_certification_report_paths(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    _seed(root)
    paths = write_research_os_certification_report(root, day="2026-06-04")
    assert paths["json"] == root / "certification" / "2026-06-04" / "research_os_certification.v1.json"
    assert paths["summary"] == root / "certification" / "2026-06-04" / "research_os_certification_summary.md"
    assert paths["latest_json"] == root / "certification" / "latest.json"
    assert paths["latest_summary"] == root / "certification" / "latest_summary.md"
    assert paths["json"].exists()
    assert paths["summary"].exists()


def test_certification_summary_states_non_authoritative_boundary(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    _seed(root)
    paths = write_research_os_certification_report(root, day="2026-06-04")
    text = paths["summary"].read_text(encoding="utf-8")
    assert "trading certified: False" in text
    assert "capital certified: False" in text
    assert "candidate promotion certified: False" in text


def test_cli_certify_and_report(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    _seed(root)
    certify = subprocess.run([sys.executable, "-m", "constellation_2.common.atlas_v2_research_os.cli", "--root", str(root), "--research-os-certify"], check=True, text=True, capture_output=True)
    assert "authority_boundary" in json.loads(certify.stdout)
    report = subprocess.run([sys.executable, "-m", "constellation_2.common.atlas_v2_research_os.cli", "--root", str(root), "--research-os-certification-report"], check=True, text=True, capture_output=True)
    assert json.loads(report.stdout)["latest_json"].endswith("certification/latest.json")
