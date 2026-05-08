from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.safety_state_authority_v1 import evaluate_safety_state_authority_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from ops.tools.run_submit_boundary_status_v1 import _canonical_blocker_for_boundary_v1


DAY = "2026-04-30"
PRIOR_DAY = "2026-04-29"
ACCOUNT = "DU1234567"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _base_safety_inputs(root: Path, *, current_nav_cents: int = 1000000, prior_nav_cents: int = 1000000) -> None:
    _write_json(root / "reports" / "broker_supply_v1" / DAY / "broker_supply.v1.json", {"day_utc": DAY, "status": "PASS"})
    _write_json(root / "reports" / "capital_supply_v1" / DAY / "capital_supply.v1.json", {"day_utc": DAY, "status": "PASS"})
    _write_json(
        root / "accounting_v2" / "nav" / DAY / "nav.v2.json",
        {"day_utc": DAY, "nav_total_cents": current_nav_cents},
    )
    _write_json(
        root / "accounting_v2" / "nav" / PRIOR_DAY / "nav.v2.json",
        {"day_utc": PRIOR_DAY, "nav_total_cents": prior_nav_cents},
    )
    _write_json(
        root / "reports" / "portfolio_account_authority_v1" / DAY / "portfolio_account_authority.v1.json",
        {"day_utc": DAY, "account_state": "READY", "account_values": {"net_liquidation_cents": current_nav_cents}},
    )
    _write_json(
        root / "reports" / "capital_risk_envelope_v2" / DAY / "capital_risk_envelope.v2.json",
        {"day_utc": DAY, "status": "PASS", "envelope": {"drawdown_limit_pct": "-0.100000"}},
    )
    _write_json(
        root / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json",
        {"day_utc": DAY, "state": "INACTIVE", "allow_entries": True, "allow_exits": True, "reason_codes": []},
    )
    _write_json(
        root / "trade_submit_readiness_c2_v1" / "_history" / "PAPER" / ACCOUNT / DAY / "status.json",
        {"day_utc": DAY, "status": "PASS", "state": "OK", "ok": True, "submit_allowed": True, "reason_codes": []},
    )


def _base_paper_execution_inputs(
    truth_root: Path,
    execution_root: Path,
    *,
    current_nav_cents: int = 101380203,
    prior_nav_cents: int = 101364311,
) -> None:
    _base_safety_inputs(execution_root, current_nav_cents=current_nav_cents, prior_nav_cents=prior_nav_cents)
    _write_json(
        execution_root / "accounting_v2" / "nav" / DAY / "nav.v2.json",
        {
            "day_utc": DAY,
            "status": "ACTIVE",
            "nav": {"nav_total_cents": current_nav_cents, "cash_total_cents": current_nav_cents, "currency": "USD"},
            "history": {"peak_nav_cents": prior_nav_cents},
        },
    )
    _write_json(
        execution_root / "accounting_v2" / "nav" / PRIOR_DAY / "nav.v2.json",
        {
            "day_utc": PRIOR_DAY,
            "status": "ACTIVE",
            "nav": {"nav_total_cents": prior_nav_cents, "cash_total_cents": prior_nav_cents, "currency": "USD"},
            "history": {"peak_nav_cents": prior_nav_cents},
        },
    )
    _write_json(
        execution_root / "reports" / "broker_supply_v1" / DAY / "broker_supply.v1.json",
        {
            "day_utc": DAY,
            "status": "PASS",
            "account_values": {
                "net_liquidation_cents": current_nav_cents,
                "total_cash_value_cents": current_nav_cents,
            },
        },
    )
    _write_json(
        execution_root / "reports" / "capital_supply_v1" / DAY / "capital_supply.v1.json",
        {
            "day_utc": DAY,
            "status": "PASS",
            "selected_source": {
                "source_type": "BROKER_ACCOUNT",
                "status": "VALID",
                "net_liquidation_cents": current_nav_cents,
            },
            "nav_evidence": {
                "status": "BROKER_SUPPLY_VALID",
                "nav_total_cents": current_nav_cents,
            },
        },
    )
    _write_json(
        truth_root / "accounting_v2" / "nav" / DAY / "nav.v2.json",
        {
            "day_utc": DAY,
            "status": "BOOTSTRAP",
            "nav": {"nav_total": 0, "cash_total": 0, "currency": "USD"},
            "history": {"peak_nav": 0},
        },
    )
    _write_json(
        truth_root / "accounting_v2" / "nav" / PRIOR_DAY / "nav.v2.json",
        {
            "day_utc": PRIOR_DAY,
            "status": "BOOTSTRAP",
            "nav": {"nav_total": 0, "cash_total": 0, "currency": "USD"},
            "history": {"peak_nav": 0},
        },
    )
    _write_json(
        truth_root / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json",
        {"day_utc": DAY, "state": "INACTIVE", "allow_entries": True, "allow_exits": True, "reason_codes": []},
    )


def test_nav_invalid_does_not_report_negative_100_drawdown_and_explains_kill_switch_dependency(tmp_path: Path) -> None:
    _base_safety_inputs(tmp_path, current_nav_cents=0, prior_nav_cents=1000000)
    _write_json(
        tmp_path / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json",
        {
            "day_utc": DAY,
            "state": "ACTIVE",
            "allow_entries": False,
            "allow_exits": True,
            "reason_codes": ["CANONICAL_KILL_SWITCH_ACTIVE"],
        },
    )

    payload = evaluate_safety_state_authority_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        execution_root=tmp_path,
        account=ACCOUNT,
        environment="PAPER",
    )

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "NAV_INVALID"
    assert payload["nav_valid"] is False
    assert payload["drawdown_pct"] is None
    assert payload["drawdown_status"] == "NAV_INVALID"
    assert "kill_switch_dependency=state:ACTIVE" in payload["root_cause"]


def test_paper_execution_root_nav_and_capital_override_canonical_bootstrap_zero(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _base_paper_execution_inputs(truth_root, execution_root)

    payload = evaluate_safety_state_authority_v1(
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        account=ACCOUNT,
        environment="PAPER",
    )

    assert payload["status"] == "PASS"
    assert payload["nav_valid"] is True
    assert payload["nav_current_cents"] == 101380203
    assert payload["nav_prior_cents"] == 101364311
    assert payload["nav_source"].startswith(f"accounting_nav_v2:{execution_root}")
    assert payload["upstream_artifact_paths"]["broker_supply_v1"].startswith(str(execution_root))
    assert payload["upstream_artifact_paths"]["capital_supply_v1"].startswith(str(execution_root))


def test_paper_execution_root_prior_nav_is_resolved_from_sleeve(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _base_paper_execution_inputs(truth_root, execution_root, prior_nav_cents=987654321)

    payload = evaluate_safety_state_authority_v1(
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        account=ACCOUNT,
        environment="PAPER",
    )

    assert payload["nav_prior_cents"] == 987654321
    assert payload["upstream_artifact_paths"]["nav_prior_v2"].startswith(str(execution_root))


def test_stale_paper_execution_broker_evidence_fails_closed(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _base_paper_execution_inputs(truth_root, execution_root)
    _write_json(
        execution_root / "reports" / "broker_supply_v1" / DAY / "broker_supply.v1.json",
        {
            "day_utc": PRIOR_DAY,
            "status": "PASS",
            "account_values": {"net_liquidation_cents": 101380203},
        },
    )

    payload = evaluate_safety_state_authority_v1(
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        account=ACCOUNT,
        environment="PAPER",
    )

    assert payload["status"] == "DEGRADED"
    assert payload["canonical_blocker"] == "SAFETY_INPUTS_DEGRADED"
    assert payload["allow_entries"] is False


def test_non_positive_paper_execution_capital_evidence_fails_closed(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _base_paper_execution_inputs(truth_root, execution_root)
    _write_json(
        execution_root / "reports" / "capital_supply_v1" / DAY / "capital_supply.v1.json",
        {
            "day_utc": DAY,
            "status": "PASS",
            "selected_source": {"source_type": "BROKER_ACCOUNT", "status": "VALID", "net_liquidation_cents": 0},
            "nav_evidence": {"status": "BROKER_SUPPLY_VALID", "nav_total_cents": 0},
        },
    )

    payload = evaluate_safety_state_authority_v1(
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        account=ACCOUNT,
        environment="PAPER",
    )

    assert payload["status"] == "DEGRADED"
    assert payload["canonical_blocker"] == "SAFETY_INPUTS_DEGRADED"
    assert payload["allow_entries"] is False


def test_real_drawdown_breach_blocks_without_disabling_kill_switch(tmp_path: Path) -> None:
    _base_safety_inputs(tmp_path, current_nav_cents=850000, prior_nav_cents=1000000)

    payload = evaluate_safety_state_authority_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        execution_root=tmp_path,
        account=ACCOUNT,
        environment="PAPER",
    )

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "DRAWDOWN_LIMIT_EXCEEDED"
    assert payload["drawdown_pct"] == "-0.150000"
    assert payload["allow_entries"] is False
    assert payload["kill_switch_state"] == "INACTIVE"


def test_submit_boundary_prefers_safety_authority_root_cause_over_legacy_safety_codes() -> None:
    assert (
        _canonical_blocker_for_boundary_v1(
            [
                "FAIL:BUNDLE_C_DRAWDOWN_LIMIT_EXCEEDED",
                "OPTIONS_SNAPSHOT_SYMBOL_MISSING",
                "NAV_INVALID",
            ]
        )
        == "NAV_INVALID"
    )


def test_accounting_nav_v2_nested_nav_total_is_canonical_nav_source(tmp_path: Path) -> None:
    _base_safety_inputs(tmp_path)
    _write_json(
        tmp_path / "accounting_v2" / "nav" / DAY / "nav.v2.json",
        {
            "schema_id": "C2_ACCOUNTING_NAV_V2",
            "schema_version": 2,
            "day_utc": DAY,
            "status": "ACTIVE",
            "nav": {"nav_total": 5000000, "cash_total": 5000000, "currency": "USD"},
            "history": {"peak_nav": 5000000, "drawdown_abs": 0, "drawdown_pct": "0.000000"},
        },
    )
    _write_json(
        tmp_path / "accounting_v2" / "nav" / PRIOR_DAY / "nav.v2.json",
        {
            "schema_id": "C2_ACCOUNTING_NAV_V2",
            "schema_version": 2,
            "day_utc": PRIOR_DAY,
            "status": "ACTIVE",
            "nav": {"nav_total": 0, "cash_total": 0, "currency": "USD"},
            "history": {"peak_nav": 0, "drawdown_abs": 0, "drawdown_pct": "0.000000"},
        },
    )

    payload = evaluate_safety_state_authority_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        execution_root=tmp_path,
        account=ACCOUNT,
        environment="PAPER",
    )

    assert payload["status"] == "PASS"
    assert payload["nav_source"].startswith("accounting_nav_v2:")
    assert payload["nav_current_cents"] == 500000000
    assert payload["nav_prior_cents"] == 500000000
    assert payload["drawdown_pct"] == "0.000000"


def test_bootstrap_zero_accounting_nav_falls_back_to_broker_capital_supply(tmp_path: Path) -> None:
    _base_safety_inputs(tmp_path, current_nav_cents=0, prior_nav_cents=1000000)
    _write_json(
        tmp_path / "accounting_v2" / "nav" / DAY / "nav.v2.json",
        {
            "schema_id": "C2_ACCOUNTING_NAV_V2",
            "schema_version": 2,
            "day_utc": DAY,
            "status": "BOOTSTRAP",
            "nav": {"nav_total": 0, "cash_total": 0, "currency": "USD"},
            "history": {"peak_nav": 0, "drawdown_abs": 0, "drawdown_pct": "0.000000"},
            "reason_codes": ["DAY0_BOOTSTRAP_MISSING_CASH_OR_POSITIONS_ALLOWED"],
        },
    )
    _write_json(
        tmp_path / "reports" / "capital_supply_v1" / DAY / "capital_supply.v1.json",
        {
            "day_utc": DAY,
            "status": "PASS",
            "selected_source": {
                "source_type": "BROKER_ACCOUNT",
                "status": "VALID",
                "net_liquidation_cents": 101347744,
                "cash_total_cents": 100766465,
            },
            "nav_evidence": {
                "status": "BROKER_SUPPLY_VALID",
                "nav_total_cents": 101347744,
                "cash_total_cents": 100766465,
            },
        },
    )

    payload = evaluate_safety_state_authority_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        execution_root=tmp_path,
        account=ACCOUNT,
        environment="PAPER",
    )

    assert payload["nav_valid"] is True
    assert payload["nav_source"].startswith("capital_supply_v1:")
    assert payload["nav_current_cents"] == 101347744
    assert payload["nav_prior_cents"] == 1000000


def test_prior_bootstrap_zero_accounting_nav_falls_back_to_prior_broker_capital_supply(tmp_path: Path) -> None:
    _base_safety_inputs(tmp_path, current_nav_cents=0, prior_nav_cents=0)
    _write_json(
        tmp_path / "accounting_v2" / "nav" / DAY / "nav.v2.json",
        {
            "schema_id": "C2_ACCOUNTING_NAV_V2",
            "schema_version": 2,
            "day_utc": DAY,
            "status": "BOOTSTRAP",
            "nav": {"nav_total": 0, "cash_total": 0, "currency": "USD"},
            "history": {"peak_nav": 0, "drawdown_abs": 0, "drawdown_pct": "0.000000"},
            "reason_codes": ["DAY0_BOOTSTRAP_MISSING_CASH_OR_POSITIONS_ALLOWED"],
        },
    )
    _write_json(
        tmp_path / "accounting_v2" / "nav" / PRIOR_DAY / "nav.v2.json",
        {
            "schema_id": "C2_ACCOUNTING_NAV_V2",
            "schema_version": 2,
            "day_utc": PRIOR_DAY,
            "status": "BOOTSTRAP",
            "nav": {"nav_total": 0, "cash_total": 0, "currency": "USD"},
            "history": {"peak_nav": 0, "drawdown_abs": 0, "drawdown_pct": "0.000000"},
            "reason_codes": ["DAY0_BOOTSTRAP_MISSING_CASH_OR_POSITIONS_ALLOWED"],
        },
    )
    for day in (DAY, PRIOR_DAY):
        _write_json(
            tmp_path / "reports" / "capital_supply_v1" / day / "capital_supply.v1.json",
            {
                "day_utc": day,
                "status": "PASS",
                "selected_source": {
                    "source_type": "BROKER_ACCOUNT",
                    "status": "VALID",
                    "net_liquidation_cents": 101347744,
                    "cash_total_cents": 100766465,
                },
                "nav_evidence": {
                    "status": "BROKER_SUPPLY_VALID",
                    "nav_total_cents": 101347744,
                    "cash_total_cents": 100766465,
                },
            },
        )

    payload = evaluate_safety_state_authority_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        execution_root=tmp_path,
        account=ACCOUNT,
        environment="PAPER",
    )

    assert payload["nav_valid"] is True
    assert payload["nav_source"].startswith("capital_supply_v1:")
    assert payload["nav_current_cents"] == 101347744
    assert payload["nav_prior_cents"] == 101347744
    assert payload["drawdown_pct"] == "0.000000"
    assert payload["upstream_artifact_paths"]["prior_capital_supply_v1"].endswith(
        f"/reports/capital_supply_v1/{PRIOR_DAY}/capital_supply.v1.json"
    )


def test_failed_envelope_negative_drawdown_does_not_override_canonical_nav_drawdown(tmp_path: Path) -> None:
    _base_safety_inputs(tmp_path)
    _write_json(
        tmp_path / "accounting_v2" / "nav" / DAY / "nav.v2.json",
        {
            "schema_id": "C2_ACCOUNTING_NAV_V2",
            "schema_version": 2,
            "day_utc": DAY,
            "status": "ACTIVE",
            "nav": {"nav_total": 5000000, "cash_total": 5000000, "currency": "USD"},
            "history": {"peak_nav": 5000000, "drawdown_abs": 0, "drawdown_pct": "0.000000"},
        },
    )
    _write_json(
        tmp_path / "reports" / "capital_risk_envelope_v2" / DAY / "capital_risk_envelope.v2.json",
        {
            "day_utc": DAY,
            "status": "FAIL",
            "reason_codes": ["B2_NAV_TOTAL_MISSING_OR_INVALID"],
            "envelope": {
                "nav_total_cents": 0,
                "peak_nav": 1013002,
                "drawdown_pct": "-1.000000",
                "drawdown_limit_pct": "-0.100000",
            },
        },
    )

    payload = evaluate_safety_state_authority_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        execution_root=tmp_path,
        account=ACCOUNT,
        environment="PAPER",
    )

    assert payload["drawdown_pct"] == "0.000000"
    assert payload["drawdown_status"] == "PASS"
    assert payload["canonical_blocker"] == "CAPITAL_RISK_ENVELOPE_NOT_PASS"


def test_safety_state_records_trading_day_readiness_mode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("C2_TRADING_DAY_READINESS_NOW_UTC", "2026-04-30T22:00:00Z")
    _base_safety_inputs(tmp_path)

    payload = evaluate_safety_state_authority_v1(
        day_utc="2026-05-01",
        truth_root=tmp_path,
        execution_root=tmp_path,
        account=ACCOUNT,
        environment="PAPER",
    )

    assert payload["readiness_mode"] == "PREOPEN_BUILD"
    assert payload["readiness_authority_path"].endswith(
        "/reports/trading_day_readiness_authority_v1/2026-05-01/trading_day_readiness_authority.v1.json"
    )
    assert payload["evidence_policy_used"]["policy_id"] == "PREOPEN_CARRY_FORWARD_V1"


def test_safety_state_payload_matches_schema(tmp_path: Path) -> None:
    _base_safety_inputs(tmp_path)

    payload = evaluate_safety_state_authority_v1(
        day_utc=DAY,
        truth_root=tmp_path,
        execution_root=tmp_path,
        account=ACCOUNT,
        environment="PAPER",
    )

    validate_against_repo_schema_v1(
        payload,
        SOURCE_ROOT,
        "governance/04_DATA/SCHEMAS/C2/REPORTS/safety_state_authority.v1.schema.json",
    )
    assert payload["status"] == "PASS"
