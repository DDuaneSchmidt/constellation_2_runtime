from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch


SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_c2_daily_operator_gate_v1 as operator_gate_module
from constellation_2.common.session_authority_monitor_v1 import (
    build_session_authority_status_payload_v1,
    write_session_authority_status_v1,
)
from constellation_2.common.tests.test_next_day_readiness_consistency_gate_v1 import (
    DAY,
    _write_boundary,
    _write_control_plane,
    _write_kill_switch,
    _write_ledger,
)
from constellation_2.common.tests.test_session_authority_monitor_v1 import (
    _artifact_row,
    _write_state,
)


def _write_ready_day(truth_root: Path) -> None:
    _write_state(
        truth_root,
        prior_active=True,
        artifact_rows=[
            _artifact_row(truth_root, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(truth_root, "paper_policy_verdict_v1"),
            _artifact_row(truth_root, "trade_submit_readiness_c2_v1"),
            _artifact_row(truth_root, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    _write_kill_switch(truth_root, day_utc=DAY, canonical_active=False)
    _write_boundary(truth_root, DAY, authorized=True)
    _write_ledger(truth_root, DAY, authority_status="GRANTED")
    _write_control_plane(truth_root, DAY, final_start_decision="READY_NOW", ledger_authority_status="GRANTED")
    write_session_authority_status_v1(
        truth_root=truth_root,
        payload=build_session_authority_status_payload_v1(truth_root=truth_root, environment="PAPER"),
    )


def _write_non_trading_blocked_day(truth_root: Path, *, stale_generated_utc: str | None = None) -> None:
    _write_state(
        truth_root,
        prior_active=True,
        artifact_rows=[
            _artifact_row(
                truth_root,
                "market_calendar_day",
                role_class="REQUIRED_BINDING_INPUT",
                result_status="FAIL",
                blocking_reason_code="NON_TRADING_DAY",
                closure_status="OPEN",
            ),
            _artifact_row(truth_root, "paper_policy_verdict_v1"),
            _artifact_row(
                truth_root,
                "trade_submit_readiness_c2_v1",
                result_status="FAIL",
                blocking_reason_code="REQUIRED_GATE_FAIL",
                closure_status="OPEN",
            ),
            _artifact_row(
                truth_root,
                "trading_day_state_machine_v1",
                role_class="REQUIRED_EXECUTION_BOUNDARY",
                result_status="FAIL",
                blocking_reason_code="NON_TRADING_DAY",
                closure_status="OPEN",
            ),
        ],
        paper_authority_status="DENIED",
        paper_submission_authorized=False,
        paper_blocking_reason_codes=["NON_TRADING_DAY"],
    )
    _write_kill_switch(truth_root, day_utc=DAY, canonical_active=False)
    _write_boundary(truth_root, DAY, authorized=False, produced_at_utc="2026-04-11T22:11:51Z")
    _write_ledger(truth_root, DAY, authority_status="DENIED", evaluated_at_utc="2026-04-11T22:11:54Z")
    _write_control_plane(
        truth_root,
        DAY,
        final_start_decision="BLOCKED_VALID",
        ledger_authority_status="DENIED",
        evaluated_at_utc="2026-04-11T22:11:50Z",
    )
    payload = build_session_authority_status_payload_v1(truth_root=truth_root, environment="PAPER")
    if stale_generated_utc:
        payload["generated_utc"] = stale_generated_utc
    write_session_authority_status_v1(truth_root=truth_root, payload=payload)


def _write_stale_ready_status(truth_root: Path) -> None:
    payload = build_session_authority_status_payload_v1(
        truth_root=truth_root,
        environment="PAPER",
        now=datetime(2026, 4, 11, 3, 53, 22, tzinfo=UTC),
    )
    payload["generated_utc"] = "2026-04-11T03:53:22Z"
    write_session_authority_status_v1(truth_root=truth_root, payload=payload)


def test_operator_gate_matches_canonical_readiness_and_ignores_legacy_submission_index(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    state_root = tmp_path / "state"
    _write_ready_day(truth_root)

    with patch.object(operator_gate_module, "TRUTH", truth_root), patch.object(operator_gate_module, "STATE_ROOT", state_root):
        rc = operator_gate_module.main(["--day_utc", DAY])

    assert rc == 0
    payload = json.loads((state_root / f"operator_gate_{DAY}.v1.json").read_text(encoding="utf-8"))
    assert payload["status"] == "PASS"
    assert payload["consistency_gate"]["status"] == "PASS"
    assert payload["canonical_truth"]["submit_boundary_status"] == "AUTHORIZED"
    assert payload["canonical_truth"]["paper_day_control_plane_decision"] == "READY_NOW"
    assert "MISSING_SUBMISSION_INDEX_V1" not in payload["reason_codes"]


def test_operator_gate_fails_when_canonical_readiness_is_blocked(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    state_root = tmp_path / "state"
    _write_state(
        truth_root,
        prior_active=True,
        artifact_rows=[
            _artifact_row(truth_root, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(truth_root, "paper_policy_verdict_v1"),
            _artifact_row(truth_root, "trade_submit_readiness_c2_v1", result_status="FAIL", blocking_reason_code="REQUIRED_GATE_FAIL", closure_status="OPEN"),
            _artifact_row(truth_root, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    _write_kill_switch(truth_root, day_utc=DAY, canonical_active=False)
    _write_boundary(truth_root, DAY, authorized=False)
    _write_ledger(truth_root, DAY, authority_status="DENIED")
    _write_control_plane(truth_root, DAY, final_start_decision="BLOCKED_VALID", ledger_authority_status="DENIED")
    write_session_authority_status_v1(
        truth_root=truth_root,
        payload=build_session_authority_status_payload_v1(truth_root=truth_root, environment="PAPER"),
    )

    with patch.object(operator_gate_module, "TRUTH", truth_root), patch.object(operator_gate_module, "STATE_ROOT", state_root):
        rc = operator_gate_module.main(["--day_utc", DAY])

    assert rc == 1
    payload = json.loads((state_root / f"operator_gate_{DAY}.v1.json").read_text(encoding="utf-8"))
    assert payload["status"] == "FAIL"
    assert payload["canonical_truth"]["paper_day_control_plane_decision"] == "BLOCKED_VALID"
    assert "MISSING_SUBMISSION_INDEX_V1" not in payload["reason_codes"]


def test_operator_gate_non_trading_day_with_fresh_denied_authority_has_no_stale_consistency_failure(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    state_root = tmp_path / "state"
    _write_non_trading_blocked_day(truth_root)

    with patch.object(operator_gate_module, "TRUTH", truth_root), patch.object(operator_gate_module, "STATE_ROOT", state_root):
        rc = operator_gate_module.main(["--day_utc", DAY])

    assert rc == 1
    payload = json.loads((state_root / f"operator_gate_{DAY}.v1.json").read_text(encoding="utf-8"))
    assert payload["status"] == "FAIL"
    assert payload["session_day_blocker"] in {"NON_TRADING_DAY", "NO_ACTIVE_PAPER_SESSION"}
    assert payload["consistency_gate"]["status"] == "PASS"
    assert "SESSION_AUTHORITY_STATUS_STALE" not in payload["consistency_gate"]["blocking_reason_codes"]
    assert payload["reason_codes"] == [payload["session_day_blocker"]]


def test_operator_gate_trading_day_stale_status_still_fails_consistency_gate(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    state_root = tmp_path / "state"
    _write_ready_day(truth_root)
    _write_boundary(truth_root, DAY, authorized=True, produced_at_utc="2026-04-11T22:11:51Z")
    _write_ledger(truth_root, DAY, authority_status="GRANTED", evaluated_at_utc="2026-04-11T22:11:54Z")
    _write_control_plane(
        truth_root,
        DAY,
        final_start_decision="READY_NOW",
        ledger_authority_status="GRANTED",
        evaluated_at_utc="2026-04-11T22:11:50Z",
    )
    _write_stale_ready_status(truth_root)

    with patch.object(operator_gate_module, "TRUTH", truth_root), patch.object(operator_gate_module, "STATE_ROOT", state_root):
        rc = operator_gate_module.main(["--day_utc", DAY])

    assert rc == 1
    payload = json.loads((state_root / f"operator_gate_{DAY}.v1.json").read_text(encoding="utf-8"))
    assert payload["session_day_blocker"] == ""
    assert payload["consistency_gate"]["status"] == "FAIL"
    assert "SESSION_AUTHORITY_STATUS_STALE" in payload["consistency_gate"]["blocking_reason_codes"]


def test_operator_gate_missing_session_authority_fails_closed(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    state_root = tmp_path / "state"
    _write_ready_day(truth_root)
    (truth_root / "active_session_v1" / "current.json").unlink()
    (truth_root / "target_day_admission_v1" / f"{DAY}.json").unlink()

    with patch.object(operator_gate_module, "TRUTH", truth_root), patch.object(operator_gate_module, "STATE_ROOT", state_root):
        rc = operator_gate_module.main(["--day_utc", DAY])

    assert rc == 1
    payload = json.loads((state_root / f"operator_gate_{DAY}.v1.json").read_text(encoding="utf-8"))
    assert payload["status"] == "FAIL"
    assert payload["reason_codes"] == ["SESSION_AUTHORITY_MISSING"]


def test_operator_gate_non_trading_day_remains_fail_closed(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    state_root = tmp_path / "state"
    _write_non_trading_blocked_day(truth_root)

    with patch.object(operator_gate_module, "TRUTH", truth_root), patch.object(operator_gate_module, "STATE_ROOT", state_root):
        rc = operator_gate_module.main(["--day_utc", DAY])

    assert rc == 1
    payload = json.loads((state_root / f"operator_gate_{DAY}.v1.json").read_text(encoding="utf-8"))
    assert payload["status"] == "FAIL"
    assert payload["session_day_blocker"] in {"NON_TRADING_DAY", "NO_ACTIVE_PAPER_SESSION"}
    assert payload["reason_codes"] == [payload["session_day_blocker"]]
