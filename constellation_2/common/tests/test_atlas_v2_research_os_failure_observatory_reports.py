from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.cli import main
from constellation_2.common.atlas_v2_research_os.failure_observatory import record_certification_block, record_failure, record_governance_block, record_scheduler_failure, record_worker_failure
from constellation_2.common.atlas_v2_research_os.failure_observatory_reports import summarize_failures_for_day, write_failure_report


def test_creates_daily_json_report_markdown_summary_and_latest_files(tmp_path: Path) -> None:
    record_worker_failure(root=tmp_path, error_type="WorkerError", error_message="worker failed", timestamp="2026-06-04T12:00:00Z", worker_run_id="wr-1")
    record_scheduler_failure(root=tmp_path, error_type="SchedulerError", error_message="scheduler failed", timestamp="2026-06-04T12:01:00Z", scheduler_trigger_id="st-1")
    record_governance_block(root=tmp_path, error_message="blocked", timestamp="2026-06-04T12:02:00Z", backlog_item_id="bi-1")
    record_certification_block(root=tmp_path, error_message="cert blocked", timestamp="2026-06-04T12:03:00Z")
    record_failure(root=tmp_path, component="LINEAGE", severity="CRITICAL", error_type="LINEAGE_VALIDATION_FAILED", error_message="missing source", timestamp="2026-06-04T12:04:00Z")
    record_failure(root=tmp_path, component="LINEAGE", severity="ERROR", error_type="LINEAGE_VALIDATION_FAILED", error_message="missing source again", timestamp="2026-06-04T12:05:00Z", metadata={"regime": "RISK_OFF"})

    summary = summarize_failures_for_day(tmp_path, day="2026-06-04")
    paths = write_failure_report(tmp_path, day="2026-06-04")

    assert summary["total_failures"] == 6
    assert summary["critical_failures"] == 1
    assert summary["governance_blocks"] == 1
    assert summary["certification_blocks"] == 1
    assert summary["worker_failures"] == 1
    assert summary["scheduler_failures"] == 1
    assert "Review failed worker run." in summary["recommended_human_review_items"]
    assert "Inspect blocked backlog item." in summary["recommended_human_review_items"]
    assert "Check repeated lineage failure." in summary["recommended_human_review_items"]
    assert summary["failures_by_regime"]["RISK_OFF"] == 1
    assert paths["json"].exists()
    assert paths["summary"].read_text(encoding="utf-8").startswith("# Atlas V2 Research OS Failure Summary")
    assert (tmp_path / "failures" / "latest.json").exists()
    assert (tmp_path / "failures" / "latest_summary.md").exists()


def test_cli_commands_run_successfully(tmp_path: Path, capsys) -> None:
    record_worker_failure(root=tmp_path, error_type="WorkerError", error_message="worker failed", timestamp=f"{date.today().isoformat()}T00:00:00Z")

    assert main(["--root", str(tmp_path), "--failure-summary"]) == 0
    assert json.loads(capsys.readouterr().out)["total_failures"] == 1
    assert main(["--root", str(tmp_path), "--list-unresolved-failures"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert main(["--root", str(tmp_path), "--failure-report"]) == 0
    assert "latest_summary" in json.loads(capsys.readouterr().out)
    assert main(["--root", str(tmp_path), "--failure-audit"]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "PASS"
