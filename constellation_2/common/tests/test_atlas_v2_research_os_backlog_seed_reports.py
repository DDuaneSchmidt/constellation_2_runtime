from pathlib import Path

from constellation_2.common.atlas_v2_research_os.backlog_seed_reports import build_backlog_seeding_report, write_seed_report


def test_write_seed_report_creates_json_summary_and_latest(tmp_path: Path):
    report = {
        "created_at": "2026-06-05T00:00:00Z",
        "profile": "SMALL_REVIEW",
        "generated_count": 1,
        "written_count": 1,
        "duplicate_count": 0,
        "blocked_count": 0,
        "ready_count_after_seeding": 1,
        "ready_count_by_type": {"MECHANISM_VARIATION": 1},
        "ready_count_by_mechanism": {"OPENING_RANGE": 1},
        "governance_result": {"status": "PASS", "violations": [], "warnings": []},
        "top_ready_items": [],
    }
    paths = write_seed_report(report, tmp_path, day="2026-06-05")
    assert paths["json"].exists()
    assert paths["summary"].exists()
    assert paths["latest_json"].exists()
    assert "Items written: 1" in paths["summary"].read_text(encoding="utf-8")
    latest = build_backlog_seeding_report(tmp_path)
    assert latest["written_count"] == 1
