from __future__ import annotations

from pathlib import Path

from constellation_2.common.atlas_v2_research_os.cli import main
from constellation_2.common.atlas_v2_research_os.family_learning_engine import build_family_learning_report, run_family_learning
from constellation_2.common.atlas_v2_research_os.family_learning_reports import (
    audit_family_learning_report,
    render_family_learning_summary,
    write_family_learning_report,
)
from constellation_2.common.tests.test_atlas_v2_research_os_family_learning_engine import NOW, _seed_inputs


def test_family_learning_report_writes_latest_and_dated_outputs(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    report = build_family_learning_report(root=tmp_path, created_at=NOW)

    paths = write_family_learning_report(report, root=tmp_path)

    assert paths["json"].name == "family_learning_report.json"
    assert paths["summary"].name == "family_learning_summary.md"
    assert (tmp_path / "family_learning" / "latest.json").exists()
    assert (tmp_path / "family_learning" / "latest_summary.md").exists()
    assert "No trade recommendation." in paths["summary"].read_text(encoding="utf-8")
    assert audit_family_learning_report(tmp_path)["family_learning_report_audit_ok"] is True


def test_family_learning_summary_renders_required_completion_counts(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    report = build_family_learning_report(root=tmp_path, created_at=NOW)

    summary = render_family_learning_summary(report)

    assert "Families processed: 4" in summary
    assert "Families strengthened: 1" in summary
    assert "Families needing data: 1" in summary
    assert "No candidate production promotion." in summary


def test_run_family_learning_and_cli_write_report(tmp_path: Path, capsys) -> None:
    _seed_inputs(tmp_path)

    report = run_family_learning(root=tmp_path, created_at=NOW)
    assert report["summary"]["families_processed"] == 4

    rc = main(["--root", str(tmp_path), "--family-learning-report"])
    captured = capsys.readouterr()
    assert rc == 0
    assert "family_learning/latest.json" in captured.out
