from __future__ import annotations

import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from ops.tools.run_structure_decision_supply_v1 import (  # noqa: E402
    _near_itm_same_week_disallowed,
    _selected_legs_exist_in_snapshot,
)


def _snapshot() -> dict:
    return {
        "as_of_utc": "2026-04-29T14:30:00Z",
        "underlying": {"spot_price": "710.60"},
        "contracts": [
            {"contract_key": "SPY-20260430-P-712", "strike": "712.00", "right": "PUT", "ib": {"conId": 849302191}},
            {"contract_key": "SPY-20260430-P-710", "strike": "710.00", "right": "PUT", "ib": {"conId": 826251088}},
        ],
    }


def test_selected_legs_must_exist_in_latest_snapshot() -> None:
    selected = {
        "legs": [
            {"contract_key": "SPY-20260430-P-712", "ib_conId": 849302191},
            {"contract_key": "SPY-20260430-P-710", "ib_conId": 826251088},
        ]
    }

    assert _selected_legs_exist_in_snapshot(selected, _snapshot()) is True


def test_selection_rejects_absent_legs() -> None:
    selected = {"legs": [{"contract_key": "SPY-20260430-P-692", "ib_conId": 123}]}

    assert _selected_legs_exist_in_snapshot(selected, _snapshot()) is False


def test_near_itm_same_week_spread_requires_explicit_policy_approval() -> None:
    selected = {
        "expiry_utc": "2026-04-30T00:00:00Z",
        "legs": [{"action": "SELL", "right": "PUT", "strike": "712.00"}],
    }

    assert _near_itm_same_week_disallowed(selected, {"options_template": {"selection_policy": {}}}, _snapshot()) is True


def test_policy_can_explicitly_allow_near_itm_same_week_spread() -> None:
    selected = {
        "expiry_utc": "2026-04-30T00:00:00Z",
        "legs": [{"action": "SELL", "right": "PUT", "strike": "712.00"}],
    }
    policy = {"options_template": {"selection_policy": {"allow_near_itm_same_week": True}}}

    assert _near_itm_same_week_disallowed(selected, policy, _snapshot()) is False
