import json

from constellation_2.common.atlas_v2_research_os.worker_reports import build_worker_connection_report, write_worker_connection_report


def test_worker_connection_report_lists_connected_and_placeholder_workers(tmp_path):
    report = build_worker_connection_report(tmp_path, day="2026-06-05")
    assert report["connected_worker_count"] == 5
    assert report["placeholder_worker_count"] == 3
    assert {row["worker_type"] for row in report["connected_workers"]} >= {
        "ClaimWorker",
        "HypothesisWorker",
        "ExperimentDesignWorker",
        "LearningWorker",
        "EvaluationWorker",
    }
    paths = write_worker_connection_report(tmp_path, day="2026-06-05")
    assert paths["json"].exists()
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["forbidden_artifact_attempts"] == 0
