from __future__ import annotations

from pathlib import Path

from constellation_2.common.atlas_v2_research_os.failure_observatory import (
    get_repeated_failures,
    list_failures,
    list_failures_by_component,
    list_failures_by_severity,
    list_unresolved_failures,
    mark_failure_resolved,
    record_certification_block,
    record_exception,
    record_failure,
    record_governance_block,
    record_scheduler_failure,
    record_worker_failure,
)


def test_records_generic_failure(tmp_path: Path) -> None:
    record = record_failure(root=tmp_path, component="ORCHESTRATOR", severity="ERROR", error_type="UnitError", error_message="unit failure", run_id="run-1")

    assert record["failure_id"]
    assert record["component"] == "ORCHESTRATOR"
    assert list_failures(tmp_path)[0]["error_type"] == "UnitError"
    assert (tmp_path / "failures" / "failure_registry.jsonl").exists()
    assert (tmp_path / "failures" / "failure_index.json").exists()


def test_records_exception_with_stack_trace(tmp_path: Path) -> None:
    try:
        raise RuntimeError("boom")
    except RuntimeError as exc:
        record = record_exception(exc, root=tmp_path, component="WORKER", worker_run_id="wr-1")

    assert record["error_type"] == "RuntimeError"
    assert "RuntimeError: boom" in record["stack_trace"]


def test_records_specialized_blocks_and_worker_scheduler_failures(tmp_path: Path) -> None:
    record_governance_block(root=tmp_path, error_message="governed block", backlog_item_id="bi-1")
    record_certification_block(root=tmp_path, error_message="cert block", certification_id="cert-1")
    record_worker_failure(root=tmp_path, error_type="WorkerError", error_message="worker failed", worker_run_id="wr-1")
    record_scheduler_failure(root=tmp_path, error_type="SchedulerError", error_message="scheduler failed", scheduler_trigger_id="st-1")

    assert len(list_failures(tmp_path)) == 4
    assert len(list_failures_by_component("WORKER", tmp_path)) == 1
    assert len(list_failures_by_severity("GOVERNANCE_BLOCK", tmp_path)) == 1


def test_lists_unresolved_marks_resolved_and_detects_repeated(tmp_path: Path) -> None:
    first = record_failure(root=tmp_path, component="LINEAGE", severity="ERROR", error_type="LINEAGE_VALIDATION_FAILED", error_message="missing source")
    record_failure(root=tmp_path, component="LINEAGE", severity="ERROR", error_type="LINEAGE_VALIDATION_FAILED", error_message="missing source again")

    assert len(list_unresolved_failures(tmp_path)) == 2
    resolved = mark_failure_resolved(first["failure_id"], root=tmp_path, resolution_notes="fixed fixture")

    assert resolved["resolved"] is True
    assert len(list_unresolved_failures(tmp_path)) == 1
    repeated = get_repeated_failures(tmp_path)
    assert repeated[0]["error_type"] == "LINEAGE_VALIDATION_FAILED"
    assert repeated[0]["count"] == 2
