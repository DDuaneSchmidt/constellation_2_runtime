from __future__ import annotations

import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.phaseD.adapters.broker_adapter_v1 import BrokerConnectionSpec
from constellation_2.phaseD.adapters.ib_paper_adapter_v2 import IBAdapterError, IBPaperAdapterV2
from constellation_2.phaseD.lib.risk_budget_gate_v1 import enforce_risk_budget_against_whatif_v1


class _FakeStock:
    def __init__(self, *, symbol: str, currency: str, exchange: str) -> None:
        self.symbol = symbol
        self.currency = currency
        self.exchange = exchange


class _FakeContract:
    def __init__(self, *, secType: str, symbol: str, currency: str, exchange: str) -> None:
        self.secType = secType
        self.symbol = symbol
        self.currency = currency
        self.exchange = exchange
        self.comboLegs = []


class _FakeComboLeg:
    def __init__(self, *, conId: int, ratio: int, action: str, exchange: str) -> None:
        self.conId = conId
        self.ratio = ratio
        self.action = action
        self.exchange = exchange


class _FakeTagValue:
    def __init__(self, tag: str, value: str) -> None:
        self.tag = tag
        self.value = value


class _FakeOrder:
    def __init__(self) -> None:
        self.orderId = None
        self.action = None
        self.orderType = None
        self.lmtPrice = None
        self.auxPrice = None
        self.totalQuantity = None
        self.tif = None
        self.transmit = None
        self.parentId = None
        self.ocaGroup = None
        self.ocaType = None
        self.smartComboRoutingParams = None


class _FakeIB:
    def __init__(self, responses: dict[str, SimpleNamespace]) -> None:
        self._responses = responses
        self.calls: list[str] = []

    def whatIfOrder(self, _contract, order):
        self.calls.append(str(order.orderType))
        response = self._responses[str(order.orderType)]
        if isinstance(response, Exception):
            raise response
        return response


class _FakeSubmitIB:
    def __init__(self) -> None:
        self.placed: list[_FakeOrder] = []
        self.cancelled: list[int] = []
        self._next_order_id = 100

    def placeOrder(self, _contract, order):
        self.placed.append(order)
        order_id = self._next_order_id
        self._next_order_id += 1
        if not isinstance(order.parentId, int):
            order.parentId = 0
        trade = SimpleNamespace(
            order=SimpleNamespace(orderId=order_id, permId=order_id + 1000),
            orderStatus=SimpleNamespace(status="SUBMITTED"),
            advancedError="",
            orderState=SimpleNamespace(status="Submitted"),
            log=[],
        )
        return trade

    def sleep(self, _seconds: float) -> None:
        return None

    def cancelOrder(self, order):
        self.cancelled.append(order.orderId)
        return SimpleNamespace(orderStatus=SimpleNamespace(status="PENDINGCANCEL"))


def _install_fake_ib_insync(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_module = ModuleType("ib_insync")
    fake_module.Stock = _FakeStock
    fake_module.Contract = _FakeContract
    fake_module.ComboLeg = _FakeComboLeg
    fake_module.TagValue = _FakeTagValue
    fake_module.Order = _FakeOrder
    monkeypatch.setitem(sys.modules, "ib_insync", fake_module)


def _sample_limit_plan() -> dict[str, object]:
    return {
        "schema_id": "equity_order_plan",
        "schema_version": "v2",
        "structure": "EQUITY_SPOT",
        "symbol": "SPY",
        "currency": "USD",
        "action": "BUY",
        "qty_shares": 1,
        "engine_id": "C2_TREND_EQ_PRIMARY_V1",
        "order_terms": {
            "order_type": "LIMIT",
            "limit_price": "679.91",
            "time_in_force": "DAY",
        },
        "protective_stop": {
            "order_type": "STOP",
            "stop_price": "611.92",
            "time_in_force": "DAY",
            "basis": "ENTRY_REFERENCE_PRICE",
            "stop_loss_bps": 1000,
        },
        "take_profit": None,
        "bracket": {
            "enabled": True,
            "oca_group": None,
            "transmit_sequence": "PARENT_FALSE_FINAL_CHILD_TRUE",
        },
    }


def _sample_options_plan() -> dict[str, object]:
    return {
        "schema_id": "order_plan",
        "schema_version": "v1",
        "structure": "VERTICAL_SPREAD",
        "underlying": {"symbol": "SPY", "currency": "USD"},
        "legs": [
            {
                "action": "SELL",
                "ib_conId": 873301879,
                "right": "PUT",
                "strike": "712.00",
                "expiry_utc": "2026-04-27T00:00:00Z",
                "ratio": 1,
            },
            {
                "action": "BUY",
                "ib_conId": 873301825,
                "right": "PUT",
                "strike": "707.00",
                "expiry_utc": "2026-04-27T00:00:00Z",
                "ratio": 1,
            },
        ],
        "order_terms": {"order_type": "LIMIT", "limit_price": "1.12", "time_in_force": "DAY", "is_credit": True},
        "risk_proof": {"contracts": 1, "max_loss_usd": "388.00", "defined_risk_proven": True},
    }


def test_limit_whatif_falls_back_to_market_margin_probe_and_returns_finite_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_ib_insync(monkeypatch)
    adapter = IBPaperAdapterV2(conn=BrokerConnectionSpec(host="127.0.0.1", port=4002, client_id=7), env="PAPER")
    adapter._ib = _FakeIB(
        {
            "LMT": SimpleNamespace(
                initMarginChange="1.7976931348623157E308",
                maintMarginChange="1.7976931348623157E308",
            ),
            "MKT": SimpleNamespace(
                initMarginChange="170.01999999999998",
                maintMarginChange="170.01999999999998",
            ),
        }
    )

    result = adapter.whatif_order(order_plan=_sample_limit_plan())

    assert result.ok is True
    assert result.margin_change_usd == "170.01999999999998"
    assert result.raw["margin_source"] == "MARKET_MARGIN_PROBE"
    assert adapter._ib.calls == ["LMT", "MKT"]


def test_options_whatif_supported_for_vertical_spread(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_ib_insync(monkeypatch)
    adapter = IBPaperAdapterV2(conn=BrokerConnectionSpec(host="127.0.0.1", port=4002, client_id=7), env="PAPER")
    adapter._ib = _FakeIB(
        {
            "LMT": SimpleNamespace(
                initMarginChange="388.00",
                maintMarginChange="388.00",
            )
        }
    )

    result = adapter.whatif_order(order_plan=_sample_options_plan())

    assert result.ok is True
    assert result.margin_change_usd == "388.00"
    assert result.notional_usd == "388.00"
    assert isinstance(result.raw, dict)
    assert isinstance(result.raw.get("payload"), dict)
    assert result.raw["payload"]["bag"]["secType"] == "BAG"
    assert result.raw["payload"]["order"]["action"] == "BUY"
    assert result.raw["payload"]["routing"]["smart_combo_routing_params"] == [
        {"tag": "NonGuaranteed", "value": "1"}
    ]


def test_options_whatif_uses_defined_risk_fallback_when_ib_margin_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_ib_insync(monkeypatch)
    adapter = IBPaperAdapterV2(conn=BrokerConnectionSpec(host="127.0.0.1", port=4002, client_id=7), env="PAPER")
    adapter._ib = _FakeIB(
        {
            "LMT": SimpleNamespace(
                initMarginChange=None,
                maintMarginChange=None,
            )
        }
    )

    result = adapter.whatif_order(order_plan=_sample_options_plan())

    assert result.ok is True
    assert result.margin_change_usd == "388.00"
    assert result.notional_usd == "388.00"
    assert isinstance(result.raw, dict)
    assert result.raw["margin_source"] == "RISK_PROOF_MAX_LOSS_FALLBACK"


def test_options_whatif_error_201_blocks_as_riskless_combination(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_ib_insync(monkeypatch)
    adapter = IBPaperAdapterV2(conn=BrokerConnectionSpec(host="127.0.0.1", port=4002, client_id=7), env="PAPER")
    adapter._ib = _FakeIB({"LMT": RuntimeError("Error 201: Riskless combination orders are not allowed.")})

    with pytest.raises(IBAdapterError, match="IB_ERROR_201_RISKLESS_COMBINATION"):
        adapter.whatif_order(order_plan=_sample_options_plan())


def test_cancel_order_calls_ib_cancel_order_for_given_order_id(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_ib_insync(monkeypatch)
    fake_ib = _FakeSubmitIB()
    adapter = IBPaperAdapterV2(conn=BrokerConnectionSpec(host="127.0.0.1", port=4002, client_id=7), env="PAPER")
    adapter._ib = fake_ib

    result = adapter.cancel_order(order_id=123)

    assert result.ok is True
    assert result.status == "PENDINGCANCEL"
    assert result.order_id == 123
    assert fake_ib.cancelled == [123]


def test_options_whatif_fails_when_ib_margin_missing_and_defined_risk_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_ib_insync(monkeypatch)
    adapter = IBPaperAdapterV2(conn=BrokerConnectionSpec(host="127.0.0.1", port=4002, client_id=7), env="PAPER")
    adapter._ib = _FakeIB(
        {
            "LMT": SimpleNamespace(
                initMarginChange=None,
                maintMarginChange=None,
            )
        }
    )
    bad_plan = _sample_options_plan()
    bad_plan["risk_proof"] = {"defined_risk_proven": False}

    with pytest.raises(IBAdapterError, match="WHATIF_INVALID_MARGIN_CHANGE"):
        adapter.whatif_order(order_plan=bad_plan)


def test_valid_small_order_margin_does_not_exceed_sample_risk_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_ib_insync(monkeypatch)
    adapter = IBPaperAdapterV2(conn=BrokerConnectionSpec(host="127.0.0.1", port=4002, client_id=7), env="PAPER")
    adapter._ib = _FakeIB(
        {
            "LMT": SimpleNamespace(
                initMarginChange="1.7976931348623157E308",
                maintMarginChange="1.7976931348623157E308",
            ),
            "MKT": SimpleNamespace(
                initMarginChange="170.01999999999998",
                maintMarginChange="170.01999999999998",
            ),
        }
    )

    result = adapter.whatif_order(order_plan=_sample_limit_plan())
    risk_budget = json.loads(
        (SOURCE_ROOT / "constellation_2/phaseD/inputs/sample_risk_budget.v1.json").read_text(encoding="utf-8")
    )

    decision = enforce_risk_budget_against_whatif_v1(
        repo_root=SOURCE_ROOT,
        risk_budget=risk_budget,
        whatif_margin_change_usd=result.margin_change_usd,
        whatif_notional_usd=result.notional_usd,
        engine_id="C2_TREND_EQ_PRIMARY_V1",
    )

    assert decision.allow is True
    assert decision.reason_code is None


def test_invalid_limit_and_market_probe_margin_values_still_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_ib_insync(monkeypatch)
    adapter = IBPaperAdapterV2(conn=BrokerConnectionSpec(host="127.0.0.1", port=4002, client_id=7), env="PAPER")
    adapter._ib = _FakeIB(
        {
            "LMT": SimpleNamespace(
                initMarginChange="1.7976931348623157E308",
                maintMarginChange="1.7976931348623157E308",
            ),
            "MKT": SimpleNamespace(
                initMarginChange="1.7976931348623157E308",
                maintMarginChange="1.7976931348623157E308",
            ),
        }
    )

    with pytest.raises(IBAdapterError, match="WHATIF_INVALID_MARGIN_CHANGE"):
        adapter.whatif_order(order_plan=_sample_limit_plan())


def test_submit_order_builds_parent_and_stop_bracket_orders(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_ib_insync(monkeypatch)
    adapter = IBPaperAdapterV2(conn=BrokerConnectionSpec(host="127.0.0.1", port=4002, client_id=7), env="PAPER")
    fake_ib = _FakeSubmitIB()
    adapter._ib = fake_ib

    result = adapter.submit_order(order_plan=_sample_limit_plan())

    assert result.ok is True
    assert result.order_id == 100
    assert result.perm_id == 1100
    assert len(fake_ib.placed) == 2
    parent = fake_ib.placed[0]
    stop = fake_ib.placed[1]
    assert parent.orderType == "LMT"
    assert parent.transmit is False
    assert stop.orderType == "STP"
    assert stop.parentId == 100
    assert stop.transmit is True
    assert isinstance(result.raw, dict)
    assert isinstance(result.raw.get("broker_ids"), dict)
    assert result.raw["broker_ids"]["parent_order_id"] == 100
    assert result.raw["broker_ids"]["stop_order_id"] == 101


def test_submit_order_supports_options_vertical_spread(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_ib_insync(monkeypatch)
    adapter = IBPaperAdapterV2(conn=BrokerConnectionSpec(host="127.0.0.1", port=4002, client_id=7), env="PAPER")
    fake_ib = _FakeSubmitIB()
    adapter._ib = fake_ib

    result = adapter.submit_order(order_plan=_sample_options_plan())

    assert result.ok is True
    assert result.order_id == 100
    assert result.perm_id == 1100
    assert result.status == "SUBMITTED"
    assert len(fake_ib.placed) == 1
    placed = fake_ib.placed[0]
    assert placed.action == "BUY"
    assert placed.orderType == "LMT"
    assert placed.totalQuantity == 1
    assert [(row.tag, row.value) for row in placed.smartComboRoutingParams] == [("NonGuaranteed", "1")]
    assert isinstance(result.raw, dict)
    assert isinstance(result.raw.get("broker_ids"), dict)
    assert result.raw["payload"]["order"]["combo_action_convention"] == "BUY_PARENT_LEG_ACTIONS_ENCODE_SPREAD"
    assert result.raw["broker_ids"]["parent_order_id"] == 100
    assert result.raw["broker_ids"]["stop_order_id"] is None


def test_submit_order_error_201_is_broker_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_ib_insync(monkeypatch)

    class RejectingIB(_FakeSubmitIB):
        def placeOrder(self, _contract, order):
            self.placed.append(order)
            return SimpleNamespace(
                order=SimpleNamespace(orderId=94, permId=0),
                orderStatus=SimpleNamespace(status="Cancelled"),
                advancedError="Error 201: Riskless combination orders are not allowed.",
                orderState=SimpleNamespace(status="Cancelled", rejectReason="Riskless combination"),
                log=[],
            )

    adapter = IBPaperAdapterV2(conn=BrokerConnectionSpec(host="127.0.0.1", port=4002, client_id=7), env="PAPER")
    adapter._ib = RejectingIB()

    result = adapter.submit_order(order_plan=_sample_options_plan())

    assert result.ok is False
    assert result.order_id == 94
    assert result.perm_id == 0
    assert result.error_code == "IB_ERROR_201"
    assert result.raw["ib_error_201_detected"] is True


def test_submit_order_requires_protective_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_ib_insync(monkeypatch)
    adapter = IBPaperAdapterV2(conn=BrokerConnectionSpec(host="127.0.0.1", port=4002, client_id=7), env="PAPER")
    adapter._ib = _FakeSubmitIB()
    bad_plan = _sample_limit_plan()
    bad_plan.pop("protective_stop", None)

    with pytest.raises(IBAdapterError, match="PROTECTIVE_STOP_REQUIRED_BUT_MISSING"):
        adapter.submit_order(order_plan=bad_plan)


def test_submit_order_with_take_profit_uses_oca_linkage(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_ib_insync(monkeypatch)
    adapter = IBPaperAdapterV2(conn=BrokerConnectionSpec(host="127.0.0.1", port=4002, client_id=7), env="PAPER")
    fake_ib = _FakeSubmitIB()
    adapter._ib = fake_ib
    plan = _sample_limit_plan()
    plan["take_profit"] = {
        "order_type": "LIMIT",
        "limit_price": "750.00",
        "time_in_force": "DAY",
    }

    result = adapter.submit_order(order_plan=plan)

    assert result.ok is True
    assert len(fake_ib.placed) == 3
    parent = fake_ib.placed[0]
    stop = fake_ib.placed[1]
    target = fake_ib.placed[2]
    assert parent.transmit is False
    assert stop.parentId == 100
    assert target.parentId == 100
    assert stop.transmit is False
    assert target.transmit is True
    assert isinstance(stop.ocaGroup, str)
    assert stop.ocaGroup == target.ocaGroup
