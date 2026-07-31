from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.historical_replay_reports import audit_historical_replay_report, demo_historical_replay_results, write_historical_replay_report


def test_creates_report_summary_and_latest_pointers(tmp_path) -> None:
    paths = write_historical_replay_report(demo_historical_replay_results(), root=tmp_path / "reports", backlog_root=tmp_path / "store")
    assert paths["json"].exists()
    assert paths["summary"].exists()
    assert paths["latest_json"].exists()
    assert paths["latest_summary"].exists()
    text = paths["summary"].read_text(encoding="utf-8")
    assert "Positive replays" in text
    assert "Recommended failure analyses" in text
    audit = audit_historical_replay_report(tmp_path / "reports")
    assert audit["historical_replay_audit_ok"] is True
