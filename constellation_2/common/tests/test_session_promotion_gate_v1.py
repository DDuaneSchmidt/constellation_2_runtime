from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.session_promotion_gate_v1 import (
    PROMOTION_STATE_BLOCKED,
    PROMOTION_STATE_FAILED_VALIDATION,
    PROMOTION_STATE_PROMOTED,
    derive_session_promotion_decision_payload_v1,
)


DAY = "2026-04-16"


def _ref(path: Path, payload: dict, sha256: str = "a" * 64) -> SimpleNamespace:
    return SimpleNamespace(path=path, payload=payload, sha256=sha256)


def test_complete_bundle_and_admit_produce_promoted_decision(tmp_path: Path) -> None:
    pre_open_ref = _ref(
        tmp_path / "reports" / "pre_open_bundle_v1" / DAY / "pre_open_bundle.v1.json",
        {
            "target_day": DAY,
            "materialization_state": "COMPLETE",
            "completion_state": "COMPLETE",
            "blocking_reason_codes": [],
        },
    )
    admission_ref = _ref(
        tmp_path / "target_day_admission_v1" / f"{DAY}.json",
        {
            "target_day": DAY,
            "admission_status": "ADMIT",
            "blocking_reason_codes": [],
        },
    )

    payload = derive_session_promotion_decision_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        pre_open_bundle_ref=pre_open_ref,
        target_day_admission_ref=admission_ref,
        owner_tool="ops/tools/run_session_authority_v1.py",
    )

    assert payload["promotion_state"] == PROMOTION_STATE_PROMOTED
    assert payload["blocked_reason_codes"] == []
    assert payload["promoted_artifacts"] == payload["candidate_artifacts"]


def test_incomplete_bundle_blocks_promotion_and_preserves_reasons(tmp_path: Path) -> None:
    pre_open_ref = _ref(
        tmp_path / "reports" / "pre_open_bundle_v1" / DAY / "pre_open_bundle.v1.json",
        {
            "target_day": DAY,
            "materialization_state": "BLOCKED",
            "completion_state": "INCOMPLETE",
            "blocking_reason_codes": ["BROKER_EVENTS_MISSING", "TARGET_DAY_DATE_MISMATCH"],
        },
    )
    admission_ref = _ref(
        tmp_path / "target_day_admission_v1" / f"{DAY}.json",
        {
            "target_day": DAY,
            "admission_status": "BLOCKED",
            "blocking_reason_codes": ["BROKER_EVENTS_MISSING"],
        },
    )

    payload = derive_session_promotion_decision_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        pre_open_bundle_ref=pre_open_ref,
        target_day_admission_ref=admission_ref,
        owner_tool="ops/tools/run_session_authority_v1.py",
    )

    assert payload["promotion_state"] == PROMOTION_STATE_BLOCKED
    assert payload["promoted_artifacts"] == []
    assert payload["blocked_reason_codes"] == ["BROKER_EVENTS_MISSING", "TARGET_DAY_DATE_MISMATCH"]


def test_validation_failure_blocks_promotion_before_current_state_advances(tmp_path: Path) -> None:
    pre_open_ref = _ref(
        tmp_path / "reports" / "pre_open_bundle_v1" / DAY / "pre_open_bundle.v1.json",
        {
            "target_day": "2026-04-15",
            "materialization_state": "COMPLETE",
            "completion_state": "COMPLETE",
            "blocking_reason_codes": [],
        },
    )
    admission_ref = _ref(
        tmp_path / "target_day_admission_v1" / f"{DAY}.json",
        {
            "target_day": DAY,
            "admission_status": "ADMIT",
            "blocking_reason_codes": [],
        },
    )

    payload = derive_session_promotion_decision_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        pre_open_bundle_ref=pre_open_ref,
        target_day_admission_ref=admission_ref,
        owner_tool="ops/tools/run_session_authority_v1.py",
    )

    assert payload["promotion_state"] == PROMOTION_STATE_FAILED_VALIDATION
    assert payload["promoted_artifacts"] == []
    assert "PRE_OPEN_BUNDLE_TARGET_DAY_MISMATCH" in payload["blocked_reason_codes"]


def test_admit_with_incomplete_pre_open_remains_blocked_without_paper_override(tmp_path: Path) -> None:
    pre_open_ref = _ref(
        tmp_path / "reports" / "pre_open_bundle_v1" / DAY / "pre_open_bundle.v1.json",
        {
            "target_day": DAY,
            "materialization_state": "BLOCKED",
            "completion_state": "INCOMPLETE",
            "blocking_reason_codes": ["ROLLOVER_FAILED_STALE_AUTHORITY_HEAD", "TARGET_DAY_DATE_MISMATCH"],
        },
    )
    admission_ref = _ref(
        tmp_path / "target_day_admission_v1" / f"{DAY}.json",
        {
            "target_day": DAY,
            "admission_status": "ADMIT",
            "blocking_reason_codes": [],
        },
    )

    payload = derive_session_promotion_decision_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        pre_open_bundle_ref=pre_open_ref,
        target_day_admission_ref=admission_ref,
        owner_tool="ops/tools/run_session_authority_v1.py",
    )

    assert payload["promotion_state"] == PROMOTION_STATE_BLOCKED
    assert payload["promoted_artifacts"] == []
    assert "TARGET_DAY_DATE_MISMATCH" in payload["blocked_reason_codes"]


def test_paper_override_allows_promotion_when_admitted_despite_incomplete_pre_open(tmp_path: Path) -> None:
    pre_open_ref = _ref(
        tmp_path / "reports" / "pre_open_bundle_v1" / DAY / "pre_open_bundle.v1.json",
        {
            "target_day": DAY,
            "materialization_state": "BLOCKED",
            "completion_state": "INCOMPLETE",
            "blocking_reason_codes": ["ROLLOVER_FAILED_STALE_AUTHORITY_HEAD", "TARGET_DAY_DATE_MISMATCH"],
        },
    )
    admission_ref = _ref(
        tmp_path / "target_day_admission_v1" / f"{DAY}.json",
        {
            "target_day": DAY,
            "admission_status": "ADMIT",
            "blocking_reason_codes": [],
        },
    )

    payload = derive_session_promotion_decision_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        pre_open_bundle_ref=pre_open_ref,
        target_day_admission_ref=admission_ref,
        owner_tool="ops/tools/run_session_authority_v1.py",
        allow_pre_open_incomplete_if_admitted=True,
    )

    assert payload["promotion_state"] == PROMOTION_STATE_PROMOTED
    assert payload["blocked_reason_codes"] == []
    assert payload["promoted_artifacts"] == payload["candidate_artifacts"]
