from __future__ import annotations

import json
import sys
from pathlib import Path

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
