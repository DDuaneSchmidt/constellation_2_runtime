from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from ops.tools.run_tomorrow_paper_startup_prep_v1 import (
    _broker_events_external_blocker,
    _capital_authority_failure_hint,
    _reconciled_trade_state_failure_hint,
    _resolve_post_bootstrap_blocker,
    _sleeve_edge_failure_hint,
)
from constellation_2.common.paper_session_path_alignment_v1 import resolve_paper_capital_seed_path
from constellation_2.common.session_authority_v1 import resolve_session_authority_tomorrow_target_day_v1
from constellation_2.common.tomorrow_paper_startup_prep_v1 import (
    BLOCKER_FIXABILITY_EXTERNAL,
    BLOCKER_FIXABILITY_REFRESHABLE,
    BLOCKER_FIXABILITY_UNKNOWN,
    BLOCKER_FIXABILITY_VERIFIABLE_ONLY,
    DAY_READINESS_BLOCKED_EXTERNAL,
    DAY_READINESS_BLOCKED_REPO_FIXABLE,
    DAY_READINESS_CLASSIFICATION_EXTERNAL_TIME_BOUND,
    DAY_READINESS_CLASSIFICATION_REFRESHABLE,
    DAY_READINESS_CLASSIFICATION_VERIFIABLE_ONLY,
    DAY_READINESS_READY_FOR_DAY,
    DAY_READINESS_WAITING_FOR_MARKET_DATA,
    classify_bootstrap_blocker_fixability_v1,
    classify_day_readiness_projection_v1,
    derive_tomorrow_readiness_state_v1,
    resolve_continuity_paper_seed_usd_v1,
    resolve_day_readiness_automation_path_v1,
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _seed_payload(*, day_utc: str, cash_total: str, nlv_total: str | None = None) -> dict[str, object]:
    return {
        "schema_id": "C2_PAPER_CAPITAL_SEED",
        "schema_version": 1,
        "day_utc": day_utc,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "environment": "PAPER",
        "ib_account": "DUO847203",
        "currency": "USD",
        "seed_mode": "EXPLICIT_USD",
        "cash_total": cash_total,
        "nlv_total": nlv_total or cash_total,
        "policy_ref": {
            "path": "/tmp/policy.json",
            "sha256": "a" * 64,
            "policy_id": "C2_PAPER_CAPITAL_SEED_POLICY_V1",
        },
        "notes": ["CAPITAL_SEED_V2: governed paper capital seed", f"seed_usd={cash_total}"],
    }


def test_resolve_session_authority_tomorrow_target_day_uses_new_york_day_boundary() -> None:
    now = datetime(2026, 4, 16, 23, 30, tzinfo=ZoneInfo("America/New_York"))
    assert resolve_session_authority_tomorrow_target_day_v1(now=now) == "2026-04-17"


def test_resolve_continuity_paper_seed_uses_latest_prior_day(tmp_path: Path) -> None:
    operator_input_root = tmp_path / "constellation_2"
    _write_json(
        resolve_paper_capital_seed_path(operator_input_root=operator_input_root, day_utc="2026-04-15"),
        _seed_payload(day_utc="2026-04-15", cash_total="100000.00"),
    )
    _write_json(
        resolve_paper_capital_seed_path(operator_input_root=operator_input_root, day_utc="2026-04-16"),
        _seed_payload(day_utc="2026-04-16", cash_total="250000.00"),
    )

    assert (
        resolve_continuity_paper_seed_usd_v1(
            operator_input_root=operator_input_root,
            target_day="2026-04-17",
        )
        == "250000.00"
    )


def test_resolve_continuity_paper_seed_fails_closed_on_invalid_latest_prior_seed(tmp_path: Path) -> None:
    operator_input_root = tmp_path / "constellation_2"
    _write_json(
        resolve_paper_capital_seed_path(operator_input_root=operator_input_root, day_utc="2026-04-15"),
        _seed_payload(day_utc="2026-04-15", cash_total="100000.00"),
    )
    _write_json(
        resolve_paper_capital_seed_path(operator_input_root=operator_input_root, day_utc="2026-04-16"),
        _seed_payload(day_utc="2026-04-16", cash_total="100000.00", nlv_total="99999.99"),
    )

    with pytest.raises(ValueError, match="paper_capital_seed_cash_nlv_mismatch"):
        resolve_continuity_paper_seed_usd_v1(
            operator_input_root=operator_input_root,
            target_day="2026-04-17",
        )


def test_classify_bootstrap_blocker_fixability_marks_refreshable_and_verifiable() -> None:
    assert classify_bootstrap_blocker_fixability_v1(
        owner_tool="/repo/ops/tools/ensure_paper_capital_seed_v1.py",
        blocker_class="MISSING_ARTIFACT",
        reason_codes=["PAPER_CAPITAL_SEED_MISSING"],
    ) == BLOCKER_FIXABILITY_REFRESHABLE
    assert classify_bootstrap_blocker_fixability_v1(
        owner_tool="/repo/ops/tools/run_pre_open_materializer_v1.py",
        blocker_class="BLOCKED_STATE",
        reason_codes=["C2_KILL_SWITCH_ACTIVE"],
    ) == BLOCKER_FIXABILITY_REFRESHABLE
    assert classify_bootstrap_blocker_fixability_v1(
        owner_tool="/repo/ops/tools/run_session_authority_v1.py",
        blocker_class="FAILED_VALIDATION",
        reason_codes=["TARGET_DAY_ARTIFACT_MISSING"],
    ) == BLOCKER_FIXABILITY_VERIFIABLE_ONLY


def test_day_readiness_classification_distinguishes_refreshable_verifiable_and_external_time_bound() -> None:
    assert classify_day_readiness_projection_v1(
        blocker_fixability=BLOCKER_FIXABILITY_REFRESHABLE,
        reason_codes=["PAPER_CAPITAL_SEED_MISSING"],
    ) == DAY_READINESS_CLASSIFICATION_REFRESHABLE
    assert classify_day_readiness_projection_v1(
        blocker_fixability=BLOCKER_FIXABILITY_VERIFIABLE_ONLY,
        reason_codes=["TARGET_DAY_ARTIFACT_MISSING"],
    ) == DAY_READINESS_CLASSIFICATION_VERIFIABLE_ONLY
    assert classify_day_readiness_projection_v1(
        blocker_fixability=BLOCKER_FIXABILITY_EXTERNAL,
        reason_codes=["BROKER_EVENTS_MISSING"],
    ) == DAY_READINESS_CLASSIFICATION_EXTERNAL_TIME_BOUND


def test_derive_tomorrow_readiness_state_requires_downstream_alignment_for_ready() -> None:
    assert derive_tomorrow_readiness_state_v1(
        bootstrap_status="READY",
        blocker_fixability=BLOCKER_FIXABILITY_UNKNOWN,
        reason_codes=[],
        downstream_ready=True,
    ) == DAY_READINESS_READY_FOR_DAY
    assert derive_tomorrow_readiness_state_v1(
        bootstrap_status="READY",
        blocker_fixability=BLOCKER_FIXABILITY_UNKNOWN,
        reason_codes=[],
        downstream_ready=False,
    ) == DAY_READINESS_BLOCKED_REPO_FIXABLE


def test_derive_tomorrow_readiness_state_surfaces_time_bound_market_data_wait() -> None:
    assert derive_tomorrow_readiness_state_v1(
        bootstrap_status="READY",
        blocker_fixability=BLOCKER_FIXABILITY_EXTERNAL,
        reason_codes=["STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:DEFAULT_EQUITY_REFERENCE_PRICE_UNAVAILABLE"],
        downstream_ready=False,
    ) == DAY_READINESS_WAITING_FOR_MARKET_DATA
    assert derive_tomorrow_readiness_state_v1(
        bootstrap_status="BLOCKED",
        blocker_fixability=BLOCKER_FIXABILITY_EXTERNAL,
        reason_codes=["IB_API_HANDSHAKE_NOT_OK"],
        downstream_ready=False,
    ) == DAY_READINESS_BLOCKED_EXTERNAL


def test_resolve_day_readiness_automation_path_uses_canonical_report_family(tmp_path: Path) -> None:
    path = resolve_day_readiness_automation_path_v1(truth_root=tmp_path / "truth", target_day="2026-04-17")
    assert path == (
        tmp_path
        / "truth"
        / "reports"
        / "day_readiness_automation_v1"
        / "2026-04-17"
        / "day_readiness_automation.v1.json"
    ).resolve()


def test_failure_hints_cover_capital_authority_chain() -> None:
    assert _capital_authority_failure_hint("FAIL: AUTHORITY_HEAD_DAY_MISMATCH head_day=2026-04-16") == "AUTHORITY_HEAD_DAY_MISMATCH"
    assert _capital_authority_failure_hint("FAIL: EXPOSURE_NET_MISSING: /tmp/exposure_net.v1.json") == "EXPOSURE_NET_MISSING"
    assert _capital_authority_failure_hint("FAIL: SLEEVE_EDGE_SNAPSHOT_DEPENDENCY:sleeve_id=C2_CROSS_ASSET_TREND") == "SLEEVE_EDGE_SNAPSHOT_DEPENDENCY"
    assert _sleeve_edge_failure_hint("ValueError: SLEEVE_EDGE_CORE2_SUMMARY_MISSING") == "SLEEVE_EDGE_CORE2_SUMMARY_MISSING"
    assert _reconciled_trade_state_failure_hint("ValueError: CORE1_HEALTH_MISSING:path=/tmp/health.json") == "CORE1_HEALTH_MISSING"


def test_broker_events_external_blocker_extracts_runtime_owner_and_path(tmp_path: Path) -> None:
    result = {
        "returncode": 1,
        "stdout": "",
        "stderr": (
            "Traceback (most recent call last):\n"
            "  File \"/home/node/constellation/ops/tools/run_broker_fact_spine_v1.py\", line 1, in <module>\n"
            "FileNotFoundError: /home/node/constellation_runtime_data/truth/execution_evidence_v1/broker_events/2026-04-17/broker_event_log.v1.jsonl"
        ),
    }

    blocker = _broker_events_external_blocker(day_utc="2026-04-17", truth_root=tmp_path, result=result)

    assert blocker is not None
    assert blocker["blocker"] == "broker_event_log_v1_jsonl"
    assert blocker["reason_codes"] == ["BROKER_EVENTS_MISSING"]
    assert blocker["artifact_path"].endswith("/execution_evidence_v1/broker_events/2026-04-17/broker_event_log.v1.jsonl")
    assert blocker["owner_tool"].endswith("/ops/ib/c2_execution_observer_v1.py")
    assert blocker["surfaced_by_owner_tool"].endswith("/ops/tools/run_broker_fact_spine_v1.py")


def test_post_bootstrap_blocker_marks_missing_target_day_reference_price_external(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _write_json(
        truth_root / "reports" / "startup_materialization_v1" / "2026-04-17" / "startup_materialization.v1.json",
        {
            "status": "MISSING_DEPENDENCY",
            "blocking_codes": [
                "STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:DEFAULT_EQUITY_REFERENCE_PRICE_UNAVAILABLE",
            ],
        },
    )
    _write_json(
        truth_root / "reports" / "startup_materialization_inputs_prep_v1" / "2026-04-17" / "startup_materialization_inputs_prep.v1.json",
        {
            "status": "BLOCKED_VALID",
            "blocking_codes": [
                "LIQUIDITY_SLIPPAGE_GATE:LIQPOL_PASS",
                "STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:DEFAULT_EQUITY_REFERENCE_PRICE_UNAVAILABLE",
            ],
        },
    )

    blocker = _resolve_post_bootstrap_blocker(day_utc="2026-04-17", truth_root=truth_root)

    assert blocker is not None
    assert blocker["blocker"] == "startup_materialization_inputs_prep_v1"
    assert blocker["fixability"] == BLOCKER_FIXABILITY_EXTERNAL
    assert blocker["owner_tool"].endswith("/ops/tools/run_startup_materialization_inputs_prep_v1.py")
    assert blocker["artifact_path"].endswith("/reports/startup_materialization_inputs_prep_v1/2026-04-17/startup_materialization_inputs_prep.v1.json")
    assert "09:30 America/New_York" in blocker["canonical_next_action"]
