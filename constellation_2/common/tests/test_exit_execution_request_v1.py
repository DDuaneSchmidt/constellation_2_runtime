from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.exit_execution_request_v1 import (  # noqa: E402
    derive_exit_execution_request_payload_v1,
    write_exit_execution_request_v1,
)


DAY = "2026-04-15"


def test_exit_execution_request_requires_canonical_actionable_decision(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    payload = derive_exit_execution_request_payload_v1(
        day_utc=DAY,
        position_id="native-1",
        exit_decision_payload={
            "decision_id": "decision-1",
            "origin": "NATIVE",
            "risk_basis": "R_NATIVE",
            "decision_action": "EXIT_FULL",
            "exit_execution_eligibility": "APPROVED",
            "scoring_eligibility": "INCLUDED",
            "reason_codes": ["STOP_BREACH"],
        },
    )
    ref = write_exit_execution_request_v1(truth_root=truth_root, payload=payload)
    written = json.loads(ref.path.read_text(encoding="utf-8"))
    assert written["execution_action"] == "CLOSE_POSITION"
    assert written["decision_action"] == "EXIT_FULL"


def test_exit_execution_request_rejects_non_actionable_decisions() -> None:
    with pytest.raises(ValueError, match="EXIT_DECISION_NOT_EXECUTION_ELIGIBLE"):
        derive_exit_execution_request_payload_v1(
            day_utc=DAY,
            position_id="native-1",
            exit_decision_payload={
                "decision_id": "decision-2",
                "origin": "NATIVE",
                "risk_basis": "R_NATIVE",
                "decision_action": "HOLD",
                "exit_execution_eligibility": "BLOCKED",
                "scoring_eligibility": "INCLUDED",
                "reason_codes": [],
            },
        )
