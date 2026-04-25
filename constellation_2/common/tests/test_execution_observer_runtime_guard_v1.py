from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.ib.c2_execution_observer_v1 as observer_module
import ops.tools.run_session_readiness_refresh_v1 as session_refresh_module

DAY = "2026-04-16"


class _FakeCanonicalWriter:
    created_days: list[str] = []

    def __init__(self, *, day_utc: str, **_: object) -> None:
        self.day_utc = day_utc
        self.records: list[dict[str, object]] = []
        self.closed = False
        type(self).created_days.append(day_utc)

    def write_payload(self, record: dict[str, object]) -> None:
        self.records.append(record)

    def close(self) -> None:
        self.closed = True


class _FakeLegacyWriter:
    created_paths: list[str] = []

    def __init__(self, *, log_path: Path, broker: dict[str, object]) -> None:
        self.log_path = log_path
        self.broker = broker
        self.sequence_number = 0
        self.records: list[dict[str, object]] = []
        self.closed = False
        type(self).created_paths.append(str(log_path))

    def write_record(self, record: dict[str, object]) -> None:
        self.records.append(record)

    def close(self) -> None:
        self.closed = True


def test_observer_fanout_writer_rolls_to_new_day_when_service_keeps_running() -> None:
    _FakeCanonicalWriter.created_days = []
    _FakeLegacyWriter.created_paths = []
    broker = {"client_id": 179, "environment": "PAPER", "name": "INTERACTIVE_BROKERS"}

    with patch.object(observer_module, "BrokerRawEvidenceJournalWriterV1", _FakeCanonicalWriter), patch.object(
        observer_module, "JsonlRawWriter", _FakeLegacyWriter
    ), patch.object(observer_module, "day_utc", side_effect=["2026-04-16", "2026-04-17"]):
        initial_canonical = _FakeCanonicalWriter(day_utc="2026-04-16")
        initial_legacy = _FakeLegacyWriter(
            log_path=Path("/tmp/broker_events/2026-04-16/broker_event_log.v1.jsonl"),
            broker=broker,
        )
        writer = observer_module.ObserverFanoutWriter(
            repo_root=SOURCE_ROOT,
            execution_root_path=Path("/tmp/execution_root"),
            environment="PAPER",
            sleeve_id="PRIMARY",
            canonical_writer=initial_canonical,
            broker=broker,
            legacy_writer=initial_legacy,
            legacy_log_root=Path("/tmp/broker_events"),
            fixed_day_utc="",
        )

        writer.write_raw("heartbeat", ["ok"])

    assert _FakeCanonicalWriter.created_days == ["2026-04-16", "2026-04-17"]
    assert _FakeLegacyWriter.created_paths[-1].endswith("/2026-04-17/broker_event_log.v1.jsonl")


def test_session_refresh_skips_bootstrap_when_canonical_observer_service_is_active() -> None:
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **_: object) -> dict[str, object]:
        calls.append(cmd)
        return {"cmd": cmd, "returncode": 0, "stdout": "", "stderr": ""}

    with patch.object(session_refresh_module, "resolve_single_paper_ib_account_from_sleeve_registry", return_value="DUO847203"), patch.object(
        session_refresh_module, "_load_accounts", return_value={"PAPER": ["DUO847203"], "LIVE": []}
    ), patch.object(
        session_refresh_module, "_run", side_effect=fake_run
    ), patch.object(
        session_refresh_module, "_git_sha", return_value="abc1234"
    ), patch.object(
        session_refresh_module, "_resolve_paper_sleeve_truth_bindings", return_value=[]
    ), patch.object(
        session_refresh_module, "validate_against_repo_schema_v1", lambda *args, **kwargs: None
    ), patch.object(
        session_refresh_module, "_authority_lifecycle_result", return_value={"status": "OK", "incident_count": 0, "incidents": []}
    ), patch.object(
        session_refresh_module, "_active_execution_observer_service_status", return_value={
            "active": True,
            "state": "ACTIVE_MATCHED",
            "service_name": "c2-execution-observer.service",
            "main_pid": 123,
        }
    ), patch(
        "sys.argv", ["run_session_readiness_refresh_v1.py", "--day_utc", DAY]
    ):
        rc = session_refresh_module.main()

    assert rc in (0, 2)
    assert not any(
        len(cmd) > 1 and str(cmd[1]) == str(session_refresh_module.BROKER_EVENTS_BOOTSTRAP_TOOL)
        for cmd in calls
    )
