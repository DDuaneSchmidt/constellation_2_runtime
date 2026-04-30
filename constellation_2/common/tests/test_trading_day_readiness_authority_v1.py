from __future__ import annotations

import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.trading_day_readiness_authority_v1 import evaluate_trading_day_readiness_authority_v1
from constellation_2.common.pre_open_materializer_v1 import apply_trading_day_readiness_policy_to_pre_open_checks_v1
from ops.tools.run_submit_boundary_status_v1 import _canonical_blocker_for_boundary_v1


def test_future_target_day_uses_preopen_build_and_forbids_future_evidence(tmp_path: Path) -> None:
    payload = evaluate_trading_day_readiness_authority_v1(
        target_day="2026-05-01",
        truth_root=tmp_path / "truth",
        execution_root=tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER",
        environment="PAPER",
        current_time_utc="2026-04-30T22:00:00Z",
    )

    assert payload["session_state"] == "FUTURE_TARGET_DAY"
    assert payload["readiness_mode"] == "PREOPEN_BUILD"
    assert payload["requires_same_day_broker_event_log"] is False
    assert payload["requires_same_day_options_snapshot"] is False
    assert payload["submit_allowed_by_mode"] is False
    assert "future_broker_event_log" in payload["forbidden_future_sources"]


def test_intraday_mode_requires_same_day_broker_and_options_evidence(tmp_path: Path) -> None:
    payload = evaluate_trading_day_readiness_authority_v1(
        target_day="2026-05-01",
        truth_root=tmp_path / "truth",
        execution_root=tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER",
        environment="PAPER",
        current_time_utc="2026-05-01T15:00:00Z",
    )

    assert payload["session_state"] == "REGULAR_OPEN"
    assert payload["readiness_mode"] == "INTRADAY_SUBMIT_READY"
    assert payload["requires_same_day_broker_event_log"] is True
    assert payload["requires_same_day_options_snapshot"] is True
    assert payload["requires_live_account_truth"] is True
    assert payload["submit_allowed_by_mode"] is True


def test_after_hours_closure_does_not_require_market_open_quotes(tmp_path: Path) -> None:
    payload = evaluate_trading_day_readiness_authority_v1(
        target_day="2026-05-01",
        truth_root=tmp_path / "truth",
        execution_root=tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER",
        environment="PAPER",
        current_time_utc="2026-05-01T22:00:00Z",
    )

    assert payload["session_state"] == "AFTER_HOURS"
    assert payload["readiness_mode"] == "AFTER_HOURS_CLOSURE"
    assert payload["requires_same_day_options_snapshot"] is False
    assert payload["submit_allowed_by_mode"] is False


def test_submit_boundary_prefers_trading_day_mode_blocker() -> None:
    assert (
        _canonical_blocker_for_boundary_v1(
            [
                "BROKER_EVENT_LOG_MISSING",
                "STALE_ARTIFACT",
                "SUBMIT_NOT_ALLOWED_BY_TRADING_DAY_MODE",
            ]
        )
        == "SUBMIT_NOT_ALLOWED_BY_TRADING_DAY_MODE"
    )


def test_preopen_build_policy_does_not_require_future_ib_handshake() -> None:
    checks = [
        {
            "artifact_id": "ib_api_handshake_v1",
            "required": True,
            "observed_status": "MISSING",
            "result_status": "FAIL",
            "blocker_codes": ["BROKER_EVENTS_MISSING", "IB_API_HANDSHAKE_NOT_OK"],
            "blocking_reason_code": "REQUIRED_GATE_FAIL",
            "freshness_rule": "TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
            "freshness_status": "STALE",
            "date_binding_status": "MISSING",
            "provenance_required": True,
            "provenance_summary": {"required": True, "present": False, "fields_present": [], "source": ""},
            "closure_status": "OPEN",
            "source_refs": [],
        }
    ]
    readiness = {
        "readiness_mode": "PREOPEN_BUILD",
        "requires_same_day_broker_event_log": False,
        "requires_live_account_truth": False,
        "path": "/tmp/trading_day_readiness_authority.v1.json",
    }

    adjusted = apply_trading_day_readiness_policy_to_pre_open_checks_v1(checks=checks, readiness_payload=readiness)

    assert adjusted[0]["required"] is False
    assert adjusted[0]["result_status"] == "PASS"
    assert adjusted[0]["blocking_reason_code"] == ""
    assert adjusted[0]["blocker_codes"] == []
    assert adjusted[0]["freshness_status"] == "NOT_REQUIRED"
