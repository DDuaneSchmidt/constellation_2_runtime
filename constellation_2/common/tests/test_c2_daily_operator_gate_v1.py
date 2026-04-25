from __future__ import annotations

import json
import sys
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
    _write_control_plane,
    _write_kill_switch,
)
from constellation_2.common.tests.test_paper_day_control_plane_v1 import (
    _write_boundary,
    _write_ledger,
)
from constellation_2.common.tests.test_session_authority_monitor_v1 import _write_state
from constellation_2.common.tests.test_session_authority_v1 import _artifact_row


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
