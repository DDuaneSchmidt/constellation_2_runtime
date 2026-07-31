import json

from constellation_2.common.atlas_v2_research_os.worker_execution_records import (
    WorkerExecutionRecord,
    read_worker_run_index,
    validate_worker_run_record_integrity,
    write_worker_execution_record,
    write_worker_execution_report,
)


def test_worker_execution_record_writes_index_and_report(tmp_path):
    record = WorkerExecutionRecord(
        worker_run_id="wr-1",
        worker_type="ClaimWorker",
        started_at="2026-06-05T00:00:00Z",
        completed_at="2026-06-05T00:00:01Z",
        status="COMPLETED",
        input_artifact_ids=["q1"],
        output_artifact_ids=["c1"],
        lineage_result={"status": "PASS"},
        governance_result={"status": "PASS"},
        errors=[],
        warnings=[],
        metadata={},
    )
    paths = write_worker_execution_record(record, tmp_path)
    assert paths["record"].exists()
    index = read_worker_run_index(tmp_path)
    assert index["worker_runs"][0]["worker_run_id"] == "wr-1"
    ok, failures = validate_worker_run_record_integrity(tmp_path)
    assert ok, failures
    report_paths = write_worker_execution_report(tmp_path, day="2026-06-05")
    payload = json.loads(report_paths["json"].read_text(encoding="utf-8"))
    assert payload["worker_execution_counts"] == {"ClaimWorker": 1}
