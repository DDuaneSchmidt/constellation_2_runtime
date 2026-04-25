from __future__ import annotations

from pathlib import Path
import sys

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import constellation_2.phaseD.lib.submit_boundary_paper_v4 as boundary_module


def _equity_plan_without_stop() -> dict[str, object]:
    return {
        "schema_id": "equity_order_plan",
        "schema_version": "v2",
        "structure": "EQUITY_SPOT",
        "symbol": "SPY",
        "currency": "USD",
        "action": "BUY",
        "qty_shares": 1,
        "order_terms": {"order_type": "LIMIT", "limit_price": "679.91", "time_in_force": "DAY"},
    }


def _equity_plan_with_stop() -> dict[str, object]:
    plan = _equity_plan_without_stop()
    plan["protective_stop"] = {
        "order_type": "STOP",
        "stop_price": "611.92",
        "time_in_force": "DAY",
        "basis": "ENTRY_REFERENCE_PRICE",
        "stop_loss_bps": 1000,
    }
    return plan


def test_equity_protective_stop_required_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(boundary_module, "get_allow_entry_only_paper_test_or_fail", lambda _engine_id: False)
    with pytest.raises(boundary_module.SubmitBoundaryV4Error, match="PROTECTIVE_STOP_REQUIRED_BUT_MISSING"):
        boundary_module._require_equity_protective_stop_or_fail(  # noqa: SLF001
            plan_obj=_equity_plan_without_stop(),
            engine_id="C2_TREND_EQ_PRIMARY_V1",
        )


def test_equity_protective_stop_accepts_valid_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(boundary_module, "get_allow_entry_only_paper_test_or_fail", lambda _engine_id: False)
    boundary_module._require_equity_protective_stop_or_fail(  # noqa: SLF001
        plan_obj=_equity_plan_with_stop(),
        engine_id="C2_TREND_EQ_PRIMARY_V1",
    )


def test_entry_only_allowed_only_when_explicit_policy_true(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(boundary_module, "get_allow_entry_only_paper_test_or_fail", lambda _engine_id: True)
    boundary_module._require_equity_protective_stop_or_fail(  # noqa: SLF001
        plan_obj=_equity_plan_without_stop(),
        engine_id="C2_TREND_EQ_PRIMARY_V1",
    )
