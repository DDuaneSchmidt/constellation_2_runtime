from __future__ import annotations

import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common import recurrence_fingerprint_v1 as recurrence_fingerprint


def test_recurrence_fingerprint_is_stable_for_same_family() -> None:
    first = recurrence_fingerprint.build_recurrence_fingerprint_record_v1(
        blocker_family="DEPLOYMENT_BLOCK",
        blocker_code="LIVE_EXECUTION_NOT_ACTIVE_ROOT",
        authority_source="deployment_state_machine_v1",
        stage_id="DEPLOYMENT",
    )
    second = recurrence_fingerprint.build_recurrence_fingerprint_record_v1(
        blocker_family="DEPLOYMENT_BLOCK",
        blocker_code="LIVE_EXECUTION_NOT_ACTIVE_ROOT",
        authority_source="deployment_state_machine_v1",
        stage_id="DEPLOYMENT",
    )
    assert first == second


def test_recurrence_fingerprint_separates_different_families() -> None:
    deploy = recurrence_fingerprint.recurrence_fingerprint_v1(
        blocker_family="DEPLOYMENT_BLOCK",
        blocker_code="LIVE_EXECUTION_NOT_ACTIVE_ROOT",
        authority_source="deployment_state_machine_v1",
        stage_id="DEPLOYMENT",
    )
    proof = recurrence_fingerprint.recurrence_fingerprint_v1(
        blocker_family="PROOF_INVALID",
        blocker_code="TARGET_DAY_NOT_LIVE_CURRENT_DAY",
        authority_source="trading_day_state_machine_v1",
        stage_id="TRADING_DAY_START",
    )
    assert deploy != proof
