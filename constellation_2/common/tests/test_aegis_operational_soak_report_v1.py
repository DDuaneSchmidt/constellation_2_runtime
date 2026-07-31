from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from ops.aegis.operational_soak_report_v1 import (
    build_aegis_operational_soak_report_v1,
    write_aegis_operational_soak_report_v1,
)
from ops.tools.run_aegis_operational_soak_report_v1 import _latest_day

DAY = "2026-05-22"
SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_operational_soak_report.v1.schema.json"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _seed_minimal_cycle(root: Path, day: str = DAY) -> None:
    _write_json(
        root / "reports" / "operator_state_snapshot_v1" / day / "operator_state_snapshot.v1.json",
        {
            "schema_id": "operator_state_snapshot",
            "content_hash": "a" * 64,
            "candidate_funnel_projection": {
                "raw_candidate_count": 319,
                "excluded_candidate_count": 319,
                "promoted_candidate_count": 0,
                "capture_ticket_count": 0,
                "consumption_counts": {"EXCLUDED_UNCOVERED_SYMBOL": 302},
                "certified_universe": {"symbol_count": 43},
            },
        },
    )
    _write_json(
        root / "reports" / "replay_certification_gate_v1" / day / "replay_certification_gate.v1.json",
        {"status": "PASS", "content_hash": "b" * 64},
    )
    _write_json(
        root / "reports" / "aegis_market_data_v1" / day / "market_data_provider_attempts.v1.json",
        {"attempts": [{"symbol": "DOW", "provider": "TIINGO", "status": "SUCCESS", "duration_ms": 42}]},
    )
    _write_json(
        root / "reports" / "final_eod_market_data_v1" / day / "final_eod_market_data.v1.json",
        {"content_hash": "c" * 64, "validation_status": "PASS", "final_eod_symbols": ["DOW", "MSFT"]},
    )
    _write_json(
        root / "reports" / "dynamic_certification_queue_v1" / day / "dynamic_certification_queue.v1.json",
        {"certification_status": "PASS", "requested_symbols": ["CRWD"], "certified_symbols": ["CRWD"], "estimated_provider_load": 1},
    )
    _write_json(
        root / "reports" / "paper_trade_evaluation_projection_v1" / day / "paper_trade_evaluation_projection.v1.json",
        {"content_hash": "d" * 64, "trade_count": 1, "open_trade_count": 1, "closed_trade_count": 0},
    )
    _write_json(
        root / "reports" / "exit_review_projection_v1" / day / "exit_review_projection.v1.json",
        {"content_hash": "e" * 64, "open_position_count": 1, "review_needed_count": 0, "closed_outcomes": []},
    )


def _rule(report: dict, rule_id: str) -> dict:
    return next(rule for rule in report["acceptance_rule_results"] if rule["rule_id"] == rule_id)


def test_operational_soak_report_contains_acceptance_surface_and_validates_schema(tmp_path: Path) -> None:
    _seed_minimal_cycle(tmp_path)

    report = build_aegis_operational_soak_report_v1(truth_root=tmp_path, operational_day=DAY, ui_results={"ok": True, "results": [], "failures": []})
    paths = write_aegis_operational_soak_report_v1(truth_root=tmp_path, operational_day=DAY, payload=report)

    assert report["schema_id"] == "aegis_operational_soak_report"
    assert report["replay_status"] == "PASS"
    assert report["candidate_counts"]["raw_candidate_count"] == 319
    assert report["promoted_counts"]["promoted_candidate_count"] == 0
    assert report["dynamic_certified_symbols"] == ["CRWD"]
    assert report["broker_submit_transmit_allowed"] is False
    assert report["trade_advice_allowed"] is False
    assert Path(paths["aegis_operational_soak_report"]).exists()
    validate_against_repo_schema_v1(report, SOURCE_ROOT, SCHEMA)


def test_provider_timeout_is_recorded_as_bounded_warning(tmp_path: Path) -> None:
    _seed_minimal_cycle(tmp_path)
    _write_json(
        tmp_path / "reports" / "aegis_market_data_v1" / DAY / "market_data_provider_attempts.v1.json",
        {"attempts": [{"symbol": "CRWD", "provider": "TIINGO", "status": "TIMEOUT", "duration_ms": 30000, "exception_message": "timeout"}]},
    )

    report = build_aegis_operational_soak_report_v1(truth_root=tmp_path, operational_day=DAY)

    assert report["timeout_events"] == 1
    assert report["provider_status"] == "WARN"
    assert _rule(report, "provider_instability")["status"] == "WARN"


def test_runaway_dynamic_certification_growth_fails_acceptance(tmp_path: Path) -> None:
    _seed_minimal_cycle(tmp_path)
    _write_json(
        tmp_path / "reports" / "dynamic_certification_queue_v1" / DAY / "dynamic_certification_queue.v1.json",
        {
            "certification_status": "QUEUED",
            "requested_symbols": [f"S{i}" for i in range(21)],
            "certified_symbols": [],
            "estimated_provider_load": 21,
        },
    )

    report = build_aegis_operational_soak_report_v1(truth_root=tmp_path, operational_day=DAY)

    assert report["overall_operational_health"] == "FAIL"
    assert _rule(report, "runaway_dynamic_universe_growth")["status"] == "FAIL"


def test_content_hash_is_stable_across_resource_usage_sampling(tmp_path: Path) -> None:
    _seed_minimal_cycle(tmp_path)

    first = build_aegis_operational_soak_report_v1(truth_root=tmp_path, operational_day=DAY)
    second = build_aegis_operational_soak_report_v1(truth_root=tmp_path, operational_day=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_ui_failures_are_recorded_as_workflow_regressions(tmp_path: Path) -> None:
    _seed_minimal_cycle(tmp_path)

    report = build_aegis_operational_soak_report_v1(
        truth_root=tmp_path,
        operational_day=DAY,
        ui_results={"ok": False, "results": [], "failures": ["/aegis-exit-review: dead command"]},
    )

    assert report["overall_operational_health"] == "FAIL"
    assert report["ui_failures"] == ["/aegis-exit-review: dead command"]
    assert _rule(report, "browser_workflow_regressions")["status"] == "FAIL"


def test_ui_soak_records_skipped_api_commands_without_failure(tmp_path: Path) -> None:
    _seed_minimal_cycle(tmp_path)

    report = build_aegis_operational_soak_report_v1(
        truth_root=tmp_path,
        operational_day=DAY,
        ui_results={
            "ok": True,
            "failures": [],
            "results": [
                {
                    "route": "/aegis-candidate-funnel",
                    "ok": True,
                    "clicked_count": 2,
                    "skipped_api_commands": [{"command_id": "QUEUE_CERTIFICATION", "label": "Queue Certification"}],
                    "failures": [],
                }
            ],
        },
    )

    route = report["ui_workflow_soak"]["routes"][0]
    assert route["skipped_api_command_count"] == 1
    assert route["skipped_api_commands"][0]["command_id"] == "QUEUE_CERTIFICATION"
    assert _rule(report, "browser_workflow_regressions")["status"] == "PASS"


def test_historical_first_run_bundle_fail_closed_does_not_fail_current_health(tmp_path: Path) -> None:
    _seed_minimal_cycle(tmp_path, day="2026-05-21")
    _seed_minimal_cycle(tmp_path, day=DAY)
    _write_json(
        tmp_path / "reports" / "replay_certification_gate_v1" / "2026-05-21" / "replay_certification_gate.v1.json",
        {
            "status": "FAIL",
            "first_run": True,
            "reason_codes": ["REPLAY_CERT_FIRST_RUN", "REPLAY_CERT_BUNDLE_FAIL_CLOSED"],
            "gate_sha256": "f" * 64,
            "candidate_bundle_sha256": "0" * 64,
        },
    )
    _write_json(
        tmp_path / "reports" / "replay_certification_bundle_v1" / "2026-05-21" / "replay_certification_bundle.v1.json",
        {
            "status": "FAIL",
            "inputs": {
                "missing_types": ["input_manifest", "gate_stack_verdict"],
                "present_types": ["nav"],
            },
        },
    )

    report = build_aegis_operational_soak_report_v1(truth_root=tmp_path, operational_day=DAY, lookback_days=2)

    assert report["replay_status"] == "PASS"
    replay_rule = _rule(report, "replay_drift")
    assert replay_rule["status"] == "PASS"
    assert replay_rule["details"][0]["classification"] == "HISTORICAL_LEGACY_FAILURE"


def test_recovered_provider_failure_does_not_degrade_provider_status(tmp_path: Path) -> None:
    _seed_minimal_cycle(tmp_path)
    _write_json(
        tmp_path / "reports" / "aegis_market_data_v1" / DAY / "market_data_provider_attempts.v1.json",
        {
            "attempts": [
                {"symbol": "DOW", "provider": "STOOQ", "status": "TIMEOUT", "duration_ms": 30000, "exception_message": "timeout"},
                {"symbol": "DOW", "provider": "TIINGO", "status": "SUCCESS", "duration_ms": 200},
            ]
        },
    )

    report = build_aegis_operational_soak_report_v1(truth_root=tmp_path, operational_day=DAY)

    assert report["provider_status"] == "PASS"
    assert report["provider_failure_classification"]["recovered_failure_count"] == 1
    assert report["provider_failure_classification"]["active_failure_count"] == 0
    assert _rule(report, "provider_instability")["status"] == "PASS"


def test_historical_warnings_and_known_final_eod_pointer_alias_do_not_degrade_health(tmp_path: Path) -> None:
    _seed_minimal_cycle(tmp_path, day="2026-05-21")
    _seed_minimal_cycle(tmp_path, day=DAY)
    duplicate_pointer = {"schema_id": "final_eod_market_data_pointer", "content_hash": "9" * 64}
    _write_json(tmp_path / "reports" / "final_eod_market_data_v1" / DAY / "final_eod_market_data.v1.json", duplicate_pointer)
    _write_json(tmp_path / "reports" / "final_eod_market_data_v1" / DAY / "final_eod_market_data.current.v1.json", duplicate_pointer)
    _write_json(
        tmp_path / "reports" / "final_eod_certification_run_ledger_v1" / DAY / "final_eod_certification_run_ledger.latest.v1.json",
        {"schema_id": "final_eod_certification_run_ledger", "status": "PASS"},
    )
    _write_json(
        tmp_path / "reports" / "replay_certification_gate_v1" / "2026-05-21" / "replay_certification_gate.v1.json",
        {
            "status": "FAIL",
            "first_run": True,
            "reason_codes": ["REPLAY_CERT_FIRST_RUN", "REPLAY_CERT_BUNDLE_FAIL_CLOSED"],
            "gate_sha256": "f" * 64,
            "candidate_bundle_sha256": "0" * 64,
        },
    )
    _write_json(
        tmp_path / "reports" / "replay_certification_bundle_v1" / "2026-05-21" / "replay_certification_bundle.v1.json",
        {"status": "FAIL", "inputs": {"missing_types": ["gate_stack_verdict"], "present_types": []}},
    )

    report = build_aegis_operational_soak_report_v1(truth_root=tmp_path, operational_day=DAY, lookback_days=2)

    assert report["overall_operational_health"] == "PASS"
    duplicate_rule = _rule(report, "duplicate_artifacts")
    assert duplicate_rule["status"] == "PASS"
    assert duplicate_rule["details"][0]["classification"] == "DUPLICATE_ARTIFACT_WARNING"
    assert duplicate_rule["details"][0]["active"] is False
    assert _rule(report, "stale_operational_state")["status"] == "PASS"
    assert _rule(report, "unresolved_command_actions")["status"] == "PASS"


def test_latest_day_ignores_future_and_self_generated_soak_artifacts(tmp_path: Path) -> None:
    _seed_minimal_cycle(tmp_path, day=DAY)
    _write_json(
        tmp_path / "reports" / "aegis_operational_soak_report_v1" / "2099-01-01" / "aegis_operational_soak_report.v1.json",
        {"schema_id": "aegis_operational_soak_report", "operational_day": "2099-01-01"},
    )
    _write_json(
        tmp_path / "reports" / "capital_risk_envelope_v2" / "2099-01-01" / "capital_risk_envelope.v2.json",
        {"schema_id": "capital_risk_envelope", "day_utc": "2099-01-01"},
    )
    _write_json(
        tmp_path / "reports" / "aegis_operational_soak_report_v1" / "2026-05-25" / "aegis_operational_soak_report.v1.json",
        {"schema_id": "aegis_operational_soak_report", "operational_day": "2026-05-25"},
    )

    assert _latest_day(tmp_path) == DAY
