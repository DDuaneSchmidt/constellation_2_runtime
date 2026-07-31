import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.autonomous_research_reports import (
    build_autonomous_research_execution_report,
    write_autonomous_research_execution_report,
)


def _result(status="COMPLETED"):
    return {
        "execution_id": "exec-1",
        "created_at": "2026-06-05T00:00:00Z",
        "started_at": "2026-06-05T00:00:00Z",
        "completed_at": "2026-06-05T00:00:01Z",
        "status": status,
        "selected_backlog_item_id": "bi1",
        "selected_worker_id": "atlas_v2_claim_worker_adapter",
        "output_artifact_ids": ["c1"],
        "memory_updates": [{"memory_id": "m1"}],
        "backlog_updates": [{"backlog_item_id": "bi1"}],
        "certification_result": {"status": "WARNING"},
        "governance_result": {"status": "PASS"},
        "lineage_result": {"status": "PASS"},
        "safety_gate_results": [{"result": "PASS"}],
    }


def test_build_report_contains_required_summary_fields():
    report = build_autonomous_research_execution_report(_result(), day="2026-06-05")
    assert report["execution_id"] == "exec-1"
    assert report["selected_backlog_item"] == "bi1"
    assert report["selected_worker"] == "atlas_v2_claim_worker_adapter"
    assert report["artifacts_created"] == ["c1"]
    assert report["certification_status"] == "WARNING"
    assert report["recommendation"] == "Continue research execution."


def test_write_report_creates_latest_pointers(tmp_path: Path):
    paths = write_autonomous_research_execution_report(_result("FAILED_SAFETY_GATE"), tmp_path, day="2026-06-05")
    assert paths["json"].exists()
    assert paths["summary"].exists()
    assert paths["latest_json"].exists()
    payload = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert payload["recommendation"] == "Review blocked backlog item."
    summary = paths["latest_summary"].read_text(encoding="utf-8")
    assert "Execution id: exec-1" in summary
