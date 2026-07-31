from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.paper_forward_outcome_reports import (
    audit_paper_forward_outcome_report,
    demo_paper_forward_outcomes,
    write_paper_forward_outcome_report,
)


def test_creates_report_summary_and_latest_pointers(tmp_path) -> None:
    paths = write_paper_forward_outcome_report(demo_paper_forward_outcomes(), root=tmp_path / "reports", feedback_root=tmp_path / "store")
    assert paths["json"].exists()
    assert paths["summary"].exists()
    assert paths["latest_json"].exists()
    assert paths["latest_summary"].exists()
    text = paths["summary"].read_text(encoding="utf-8")
    assert "Outcomes recorded" in text
    assert "Survived" in text
    audit = audit_paper_forward_outcome_report(tmp_path / "reports")
    assert audit["paper_forward_outcome_audit_ok"] is True
