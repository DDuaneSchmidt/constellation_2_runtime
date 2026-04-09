from __future__ import annotations

import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common import execution_journal_v1 as journal
from constellation_2.common import performance_projection_v1 as performance_projection


def test_performance_projection_from_stage_duration_events() -> None:
    identity = {
        "day_utc": "2026-04-08",
        "day_attempt_id": "day_attempt:2026-04-08:A001",
        "pipeline_run_id": "pipeline_run:2026-04-08:R001",
        "release_id": "release-001",
        "git_sha": "a" * 40,
    }
    event = journal.build_event_record_v1(
        **identity,
        event_seq=1,
        event_type="STAGE_DURATION_RECORDED",
        event_source="paper_session_startup_flow_v1",
        generated_at_utc="2026-04-08T13:00:02Z",
        status="RECORDED",
        payload={
            "source_artifact_path": "/tmp/ledger.json",
            "source_artifact_sha256": "b" * 64,
            "source_generated_at_utc": "2026-04-08T13:00:02Z",
            "stage_name": "STARTUP_MATERIALIZATION_TO_LEDGER_ELAPSED",
            "started_at_utc": "2026-04-08T13:00:00Z",
            "ended_at_utc": "2026-04-08T13:00:02Z",
            "duration_ms": 2000,
        },
    )
    journal_payload = journal.build_execution_journal_payload_v1(
        **identity,
        generated_at_utc="2026-04-08T13:00:02Z",
        events=[event],
        producer_module="test.module",
    )
    payload = performance_projection.build_performance_projection_v1(
        journal_payload=journal_payload,
        journal_ref="/tmp/execution_journal.v1.json",
        journal_generated_at_utc="2026-04-08T13:00:02Z",
        generated_at_utc="2026-04-08T13:01:00Z",
        producer_module="test.module",
    )
    assert payload["overall_wall_time_ms"] == 2000
    assert payload["per_stage_durations"][0]["stage_name"] == "STARTUP_MATERIALIZATION_TO_LEDGER_ELAPSED"
