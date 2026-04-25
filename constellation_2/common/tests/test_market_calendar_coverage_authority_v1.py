from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path

from constellation_2.common.market_calendar_coverage_authority_v1 import (
    ACTION_EXTEND_GOVERNED_SOURCE,
    ACTION_NONE,
    ACTION_REFRESH_RUNTIME_FROM_SOURCE,
    COVERAGE_STATUS_BLOCKED,
    COVERAGE_STATUS_HEALTHY,
    COVERAGE_STATUS_WARNING,
    REASON_COVERAGE_BELOW_POLICY_BUFFER,
    REASON_RUNTIME_NOT_REFRESHED,
    REASON_SOURCE_AND_RUNTIME_RANGE_MISMATCH,
    REASON_SOURCE_NOT_EXTENDED,
    SEVERITY_CRITICAL,
    SEVERITY_INFO,
    build_market_calendar_coverage_status_payload_v1,
    refresh_market_calendar_coverage_v1,
    write_market_calendar_coverage_status_v1,
)
from constellation_2.common.session_authority_monitor_v1 import (
    ALERT_STATUS_ALERT,
    ALERT_STATUS_CLEAR,
    ALERT_STATUS_DEDUPED,
    build_session_authority_status_payload_v1,
    derive_session_authority_alert_payload_v1,
    write_session_authority_alert_v1,
    write_session_authority_status_v1,
)
from constellation_2.common.tests.test_session_authority_monitor_v1 import _write_state
import constellation_2.phaseJ.tools.market_calendar_ingest_v1 as ingest_module


NOW = datetime(2026, 4, 9, 16, 0, tzinfo=UTC)


def _write_csv(path: Path, rows: list[tuple[str, bool]]) -> str:
    body = ["day_utc,is_trading_session"]
    body.extend(f"{day},{'true' if is_trading else 'false'}" for day, is_trading in rows)
    raw = ("\n".join(body) + "\n").encode("utf-8")
    path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


def _write_source_bundle(source_root: Path, files: list[tuple[str, list[tuple[str, bool]]]]) -> None:
    source_root.mkdir(parents=True, exist_ok=True)
    source_hashes: dict[str, str] = {}
    source_files: list[str] = []
    all_days: list[str] = []
    for relpath, rows in files:
        csv_path = source_root / relpath
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        source_hashes[relpath] = _write_csv(csv_path, rows)
        source_files.append(relpath)
        all_days.extend(day for day, _ in rows)
    manifest = {
        "dataset_id": "market_calendar_source_dataset_v1:NYSE:2026",
        "effective_date_range": {"start": min(all_days), "end": max(all_days)},
        "exchange": "NYSE",
        "generated_at_utc": "2026-04-09T21:43:49Z",
        "schema_id": "market_calendar_source_dataset.v1",
        "schema_version": "v1",
        "source_file_hashes": source_hashes,
        "source_files": source_files,
        "status": "ACTIVE",
        "year": 2026,
    }
    (source_root / "dataset_manifest.json").write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")


def _ingest_runtime_rows(truth_root: Path, rows: list[tuple[str, bool]]) -> int:
    csv_path = truth_root / "runtime_source.csv"
    source_hash = _write_csv(csv_path, rows)
    return ingest_module.main(
        [
            "--dataset_version",
            "v1",
            "--run_utc",
            "2026-04-09T21:43:49Z",
            "--exchange",
            "NYSE",
            "--csv",
            str(csv_path),
            "--source_name",
            "governed_source_dataset_v1",
            "--source_hash",
            source_hash,
            "--truth_root",
            str(truth_root),
        ]
    )


def test_source_coverage_ends_before_required_target_day_blocks(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    _write_source_bundle(source_root, [("NYSE_calendar_2026_seed.csv", [("2026-04-09", True)])])
    assert _ingest_runtime_rows(tmp_path, [("2026-04-09", True)]) == 0

    payload = build_market_calendar_coverage_status_payload_v1(
        truth_root=tmp_path,
        source_root=source_root,
        required_target_day="2026-04-10",
        buffer_calendar_days=1,
        now=NOW,
    )
    assert payload["coverage_status"] == COVERAGE_STATUS_BLOCKED
    assert payload["severity"] == SEVERITY_CRITICAL
    assert payload["reason_codes"][0] == REASON_SOURCE_NOT_EXTENDED
    assert payload["source_status"] == COVERAGE_STATUS_BLOCKED
    assert payload["runtime_status"] == COVERAGE_STATUS_BLOCKED
    assert payload["source_required_target_day_covered"] is False
    assert payload["operator_action_code"] == ACTION_EXTEND_GOVERNED_SOURCE


def test_source_covers_target_day_but_runtime_is_not_refreshed_blocks(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    _write_source_bundle(
        source_root,
        [("NYSE_calendar_2026_seed.csv", [("2026-04-09", True), ("2026-04-10", True)])],
    )
    assert _ingest_runtime_rows(tmp_path, [("2026-04-09", True)]) == 0

    payload = build_market_calendar_coverage_status_payload_v1(
        truth_root=tmp_path,
        source_root=source_root,
        required_target_day="2026-04-10",
        buffer_calendar_days=1,
        now=NOW,
    )
    assert payload["coverage_status"] == COVERAGE_STATUS_BLOCKED
    assert REASON_RUNTIME_NOT_REFRESHED in payload["reason_codes"]
    assert payload["source_status"] == COVERAGE_STATUS_HEALTHY
    assert payload["runtime_status"] == COVERAGE_STATUS_BLOCKED
    assert payload["source_required_target_day_covered"] is True
    assert payload["runtime_required_target_day_covered"] is False
    assert payload["operator_action_code"] == ACTION_REFRESH_RUNTIME_FROM_SOURCE


def test_source_runtime_mismatch_is_detected(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    _write_source_bundle(
        source_root,
        [("NYSE_calendar_2026_seed.csv", [("2026-04-09", True), ("2026-04-10", True), ("2026-04-11", False)])],
    )
    assert _ingest_runtime_rows(tmp_path, [("2026-04-09", True), ("2026-04-10", True)]) == 0

    payload = build_market_calendar_coverage_status_payload_v1(
        truth_root=tmp_path,
        source_root=source_root,
        required_target_day="2026-04-10",
        buffer_calendar_days=1,
        now=NOW,
    )
    assert payload["coverage_status"] == COVERAGE_STATUS_WARNING
    assert REASON_SOURCE_AND_RUNTIME_RANGE_MISMATCH in payload["reason_codes"]


def test_healthy_coverage_yields_info(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    rows = [("2026-04-09", True), ("2026-04-10", True)]
    _write_source_bundle(source_root, [("NYSE_calendar_2026_seed.csv", rows)])
    assert _ingest_runtime_rows(tmp_path, rows) == 0

    payload = build_market_calendar_coverage_status_payload_v1(
        truth_root=tmp_path,
        source_root=source_root,
        required_target_day="2026-04-10",
        buffer_calendar_days=1,
        now=NOW,
    )
    assert payload["coverage_status"] == COVERAGE_STATUS_HEALTHY
    assert payload["severity"] == SEVERITY_INFO
    assert payload["source_status"] == COVERAGE_STATUS_HEALTHY
    assert payload["runtime_status"] == COVERAGE_STATUS_HEALTHY
    assert payload["source_required_target_day_covered"] is True
    assert payload["runtime_required_target_day_covered"] is True
    assert payload["operator_action_code"] == ACTION_NONE


def test_monitoring_incorporates_coverage_status(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    _write_source_bundle(source_root, [("NYSE_calendar_2026_seed.csv", [("2026-04-09", True)])])
    coverage_ref = write_market_calendar_coverage_status_v1(
        truth_root=tmp_path,
        payload=build_market_calendar_coverage_status_payload_v1(
            truth_root=tmp_path,
            source_root=source_root,
            required_target_day="2026-04-10",
            buffer_calendar_days=1,
            now=NOW,
        ),
    )
    _write_state(
        tmp_path,
        prior_active=True,
        artifact_rows=[
            {
                "artifact_id": "market_calendar_day",
                "artifact_name": "market_calendar_day",
                "required": True,
                "role_class": "REQUIRED_BINDING_INPUT",
                "classification": "TEST",
                "canonical_path": str((tmp_path / "market_calendar_day.json").resolve()),
                "authority_path": str((tmp_path / "market_calendar_day.json").resolve()),
                "path_family": "CANONICAL_RUNTIME_TRUTH_SUBPATH",
                "observed_status": "FAIL",
                "result_status": "FAIL",
                "blocker_codes": [REASON_SOURCE_NOT_EXTENDED],
                "blocking_reason_code": REASON_SOURCE_NOT_EXTENDED,
                "schema_status": "VALID",
                "schema_ref": "governance/test.schema.json",
                "freshness_rule": "TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
                "freshness_status": "STALE",
                "target_day_expected": "2026-04-10",
                "target_day_observed": "",
                "date_binding_status": "MISMATCH",
                "date_binding_value": "",
                "provenance_required": True,
                "provenance_summary": {"required": True, "present": False, "fields_present": [], "source": ""},
                "closure_status": "OPEN",
                "producer": {"module": "test", "git_sha": "abc123"},
                "source_refs": [coverage_ref.payload["source_manifest_ref"]],
                "observed_dependency_artifacts": [],
            }
        ],
        target_day="2026-04-10",
    )
    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER", now=NOW)
    assert payload["market_calendar_coverage_status"] == COVERAGE_STATUS_BLOCKED
    assert payload["market_calendar_required_target_day"] == "2026-04-10"
    assert payload["market_calendar_source_status"] == COVERAGE_STATUS_BLOCKED
    assert payload["market_calendar_runtime_status"] == COVERAGE_STATUS_BLOCKED
    assert payload["market_calendar_source_covers_required_target_day"] is False
    assert payload["market_calendar_runtime_covers_required_target_day"] is False
    assert payload["market_calendar_operator_action_code"] == ACTION_EXTEND_GOVERNED_SOURCE
    assert REASON_SOURCE_NOT_EXTENDED in payload["top_blocker_reason_codes"]


def test_alerting_escalates_coverage_failure(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    _write_source_bundle(source_root, [("NYSE_calendar_2026_seed.csv", [("2026-04-09", True)])])
    write_market_calendar_coverage_status_v1(
        truth_root=tmp_path,
        payload=build_market_calendar_coverage_status_payload_v1(
            truth_root=tmp_path,
            source_root=source_root,
            required_target_day="2026-04-10",
            buffer_calendar_days=1,
            now=NOW,
        ),
    )
    _write_state(
        tmp_path,
        prior_active=True,
        artifact_rows=[
            {
                "artifact_id": "market_calendar_day",
                "artifact_name": "market_calendar_day",
                "required": True,
                "role_class": "REQUIRED_BINDING_INPUT",
                "classification": "TEST",
                "canonical_path": str((tmp_path / "market_calendar_day.json").resolve()),
                "authority_path": str((tmp_path / "market_calendar_day.json").resolve()),
                "path_family": "CANONICAL_RUNTIME_TRUTH_SUBPATH",
                "observed_status": "FAIL",
                "result_status": "FAIL",
                "blocker_codes": [REASON_SOURCE_NOT_EXTENDED],
                "blocking_reason_code": REASON_SOURCE_NOT_EXTENDED,
                "schema_status": "VALID",
                "schema_ref": "governance/test.schema.json",
                "freshness_rule": "TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
                "freshness_status": "STALE",
                "target_day_expected": "2026-04-10",
                "target_day_observed": "",
                "date_binding_status": "MISMATCH",
                "date_binding_value": "",
                "provenance_required": True,
                "provenance_summary": {"required": True, "present": False, "fields_present": [], "source": ""},
                "closure_status": "OPEN",
                "producer": {"module": "test", "git_sha": "abc123"},
                "source_refs": [],
                "observed_dependency_artifacts": [],
            }
        ],
        target_day="2026-04-10",
    )
    status_ref = write_session_authority_status_v1(
        truth_root=tmp_path,
        payload=build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER", now=NOW),
    )
    alert_payload = derive_session_authority_alert_payload_v1(
        truth_root=tmp_path,
        environment="PAPER",
        status_ref=status_ref,
    )
    assert alert_payload["alert_status"] == ALERT_STATUS_ALERT
    assert REASON_SOURCE_NOT_EXTENDED in alert_payload["alert_reason_codes"]


def test_coverage_alert_dedupe_for_unchanged_failure(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    _write_source_bundle(source_root, [("NYSE_calendar_2026_seed.csv", [("2026-04-09", True)])])
    status_ref = write_session_authority_status_v1(
        truth_root=tmp_path,
        payload={
            **build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER", now=NOW),
            "market_calendar_coverage_status": COVERAGE_STATUS_BLOCKED,
            "market_calendar_coverage_severity": SEVERITY_CRITICAL,
            "market_calendar_required_target_day": "2026-04-10",
            "top_blocker_reason_codes": [REASON_SOURCE_NOT_EXTENDED],
        },
    )
    first_alert_ref = write_session_authority_alert_v1(
        truth_root=tmp_path,
        payload=derive_session_authority_alert_payload_v1(
            truth_root=tmp_path,
            environment="PAPER",
            status_ref=status_ref,
        ),
    )
    second_alert_payload = derive_session_authority_alert_payload_v1(
        truth_root=tmp_path,
        environment="PAPER",
        status_ref=status_ref,
        prior_alert_ref=first_alert_ref,
    )
    assert second_alert_payload["alert_status"] == ALERT_STATUS_DEDUPED


def test_coverage_alert_recovery_clears_after_restoration(tmp_path: Path) -> None:
    blocked_status_ref = write_session_authority_status_v1(
        truth_root=tmp_path,
        payload={
            **build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER", now=NOW),
            "market_calendar_coverage_status": COVERAGE_STATUS_BLOCKED,
            "market_calendar_coverage_severity": SEVERITY_CRITICAL,
            "market_calendar_required_target_day": "2026-04-10",
            "top_blocker_reason_codes": [REASON_SOURCE_NOT_EXTENDED],
            "status_severity": SEVERITY_CRITICAL,
        },
    )
    prior_alert_ref = write_session_authority_alert_v1(
        truth_root=tmp_path,
        payload=derive_session_authority_alert_payload_v1(
            truth_root=tmp_path,
            environment="PAPER",
            status_ref=blocked_status_ref,
        ),
    )
    healthy_status_ref = write_session_authority_status_v1(
        truth_root=tmp_path,
        payload={
            **build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER", now=NOW),
            "market_calendar_coverage_status": COVERAGE_STATUS_HEALTHY,
            "market_calendar_coverage_severity": SEVERITY_INFO,
            "market_calendar_required_target_day": "2026-04-10",
            "top_blocker_reason_codes": [],
            "status_severity": SEVERITY_INFO,
        },
    )
    recovered = derive_session_authority_alert_payload_v1(
        truth_root=tmp_path,
        environment="PAPER",
        status_ref=healthy_status_ref,
        prior_alert_ref=prior_alert_ref,
    )
    assert recovered["alert_status"] == ALERT_STATUS_CLEAR


def test_refresh_uses_approved_producer_flow_and_updates_status(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    _write_source_bundle(
        source_root,
        [("NYSE_calendar_2026_seed.csv", [("2026-04-10", True), ("2026-04-11", False)])],
    )
    payload = refresh_market_calendar_coverage_v1(
        truth_root=tmp_path,
        source_root=source_root,
        required_target_day="2026-04-10",
        buffer_calendar_days=1,
        now=NOW,
    )
    assert payload["refresh_actions"]
    assert payload["refresh_actions"][0]["producer"] == "constellation_2/phaseJ/tools/market_calendar_ingest_v1.py"
    assert payload["coverage_status"] in {COVERAGE_STATUS_HEALTHY, COVERAGE_STATUS_WARNING}


def test_check_mode_does_not_claim_refresh_or_write_status_artifact(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    _write_source_bundle(
        source_root,
        [("NYSE_calendar_2026_seed.csv", [("2026-04-09", True), ("2026-04-10", True)])],
    )
    assert _ingest_runtime_rows(tmp_path, [("2026-04-09", True)]) == 0

    payload = build_market_calendar_coverage_status_payload_v1(
        truth_root=tmp_path,
        source_root=source_root,
        required_target_day="2026-04-10",
        buffer_calendar_days=1,
        now=NOW,
    )
    assert REASON_RUNTIME_NOT_REFRESHED in payload["reason_codes"]
    assert payload["refresh_actions"] == []
    assert not (tmp_path / "market_calendar_coverage_status_v1" / "current.json").exists()


def test_wrapper_remains_session_authority_consumer_only() -> None:
    wrapper_path = Path("/home/node/constellation/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh")
    content = wrapper_path.read_text(encoding="utf-8")
    assert "run_session_authority_v1.py" in content
    assert 'DAY="$(TZ=America/New_York date +%F)"' not in content
