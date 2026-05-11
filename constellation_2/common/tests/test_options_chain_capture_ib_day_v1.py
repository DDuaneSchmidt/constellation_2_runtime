from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
import sys

if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_options_chain_capture_ib_day_v1 as capture_module


def _contract(*, sec_type: str, con_id: int, local_symbol: str, trading_class: str = "SPY") -> SimpleNamespace:
    return SimpleNamespace(
        secType=sec_type,
        conId=con_id,
        localSymbol=local_symbol,
        tradingClass=trading_class,
        exchange="SMART",
        currency="USD",
        multiplier="100",
    )


def test_capture_falls_back_to_delayed_market_data_when_live_spot_missing(monkeypatch, tmp_path: Path) -> None:
    day_utc = "2026-04-24"

    class _FakeClient:
        def __init__(self) -> None:
            self.market_data_type = 1

        def connect_and_wait(self, *, host: str, port: int, client_id: int) -> None:
            return None

        def reqMarketDataType(self, marketDataType: int) -> None:  # noqa: N802
            self.market_data_type = int(marketDataType)

        def request_contract_details(self, contract: SimpleNamespace):
            if str(contract.secType) == "STK":
                return [SimpleNamespace(contract=_contract(sec_type="STK", con_id=101, local_symbol="SPY"))]
            return [SimpleNamespace(contract=_contract(sec_type="OPT", con_id=202, local_symbol="SPY   260430P00500000"))]

        def request_snapshot(
            self,
            contract: SimpleNamespace,
            generic_ticks: str,
            timeout_seconds: float = 4.0,
            snapshot: bool = True,
        ):
            if str(contract.secType) == "STK":
                if self.market_data_type == 1:
                    return {}
                return {"last": "500.12"}
            return {"bid": "1.00", "ask": "1.10", "volume": 1, "open_interest": 1, "observed_tick_types": [66, 67]}

        def request_secdef(self, *, symbol: str, underlying_con_id: int):
            return [{"trading_class": "SPY", "expirations": ["20260430"], "strikes": [500.0]}]

        def disconnect(self) -> None:
            return None

        def error_events(self):
            return []

    monkeypatch.setattr(capture_module, "_IbCaptureClient", _FakeClient)

    raw_path, payload = capture_module._capture_raw_chain(
        day_utc=day_utc,
        eval_time_utc=f"{day_utc}T20:00:00Z",
        symbol="SPY",
        truth_root=tmp_path,
        ib_host="127.0.0.1",
        ib_port=4002,
        ib_client_id=7,
    )

    assert raw_path.exists()
    assert payload["underlying"]["spot_price"] == "500.12"
    assert payload["provenance"]["market_data_type"] == 3
    assert len(payload["contracts"]) == 1


def test_capture_remains_fail_closed_when_spot_missing_for_all_market_data_types(monkeypatch, tmp_path: Path) -> None:
    day_utc = "2026-04-24"

    class _FakeClient:
        def __init__(self) -> None:
            self.market_data_type = 1

        def connect_and_wait(self, *, host: str, port: int, client_id: int) -> None:
            return None

        def reqMarketDataType(self, marketDataType: int) -> None:  # noqa: N802
            self.market_data_type = int(marketDataType)

        def request_contract_details(self, contract: SimpleNamespace):
            if str(contract.secType) == "STK":
                return [SimpleNamespace(contract=_contract(sec_type="STK", con_id=101, local_symbol="SPY"))]
            return [SimpleNamespace(contract=_contract(sec_type="OPT", con_id=202, local_symbol="SPY   260430P00500000"))]

        def request_snapshot(
            self,
            contract: SimpleNamespace,
            generic_ticks: str,
            timeout_seconds: float = 4.0,
            snapshot: bool = True,
        ):
            if str(contract.secType) == "STK":
                return {}
            return {"bid": "1.00", "ask": "1.10", "volume": 1, "open_interest": 1, "observed_tick_types": [66, 67]}

        def request_secdef(self, *, symbol: str, underlying_con_id: int):
            return [{"trading_class": "SPY", "expirations": ["20260430"], "strikes": [500.0]}]

        def disconnect(self) -> None:
            return None

        def error_events(self):
            return []

    monkeypatch.setattr(capture_module, "_IbCaptureClient", _FakeClient)

    with pytest.raises(capture_module.CaptureError, match="UNDERLYING_SPOT_MISSING"):
        capture_module._capture_raw_chain(
            day_utc=day_utc,
            eval_time_utc=f"{day_utc}T20:00:00Z",
            symbol="SPY",
            truth_root=tmp_path,
            ib_host="127.0.0.1",
            ib_port=4002,
            ib_client_id=7,
        )


def test_ib_tickprice_maps_delayed_bid_ask_last_close() -> None:
    client = capture_module._IbCaptureClient()

    client.tickPrice(42, 66, 499.95, None)
    client.tickPrice(42, 67, 500.05, None)
    client.tickPrice(42, 68, 500.00, None)
    client.tickPrice(42, 75, 498.88, None)

    row = client._ticks[42]
    assert row["bid"] == "499.95"
    assert row["ask"] == "500.05"
    assert row["last"] == "500.0"
    assert row["close"] == "498.88"


def test_capture_skips_per_contract_details_timeout_when_other_quotes_are_valid(monkeypatch, tmp_path: Path) -> None:
    day_utc = "2026-04-24"

    class _FakeClient:
        def __init__(self) -> None:
            self.market_data_type = 1

        def connect_and_wait(self, *, host: str, port: int, client_id: int) -> None:
            return None

        def reqMarketDataType(self, marketDataType: int) -> None:  # noqa: N802
            self.market_data_type = int(marketDataType)

        def request_contract_details(self, contract: SimpleNamespace):
            if str(contract.secType) == "STK":
                return [SimpleNamespace(contract=_contract(sec_type="STK", con_id=101, local_symbol="SPY"))]
            if float(contract.strike) < 500.0:
                raise capture_module.CaptureError("IB_CONTRACT_DETAILS_TIMEOUT:req_id=22")
            return [SimpleNamespace(contract=_contract(sec_type="OPT", con_id=202, local_symbol="SPY   260430P00500000"))]

        def request_snapshot(
            self,
            contract: SimpleNamespace,
            generic_ticks: str,
            timeout_seconds: float = 4.0,
            snapshot: bool = True,
        ):
            if str(contract.secType) == "STK":
                return {"last": "500.12"}
            return {"bid": "1.00", "ask": "1.10", "volume": 1, "open_interest": 1, "observed_tick_types": [66, 67]}

        def request_secdef(self, *, symbol: str, underlying_con_id: int):
            return [{"trading_class": "SPY", "expirations": ["20260430"], "strikes": [499.0, 500.0]}]

        def disconnect(self) -> None:
            return None

        def error_events(self):
            return []

    monkeypatch.setattr(capture_module, "_IbCaptureClient", _FakeClient)

    raw_path, payload = capture_module._capture_raw_chain(
        day_utc=day_utc,
        eval_time_utc=f"{day_utc}T20:00:00Z",
        symbol="SPY",
        truth_root=tmp_path,
        ib_host="127.0.0.1",
        ib_port=4002,
        ib_client_id=7,
    )

    assert raw_path.exists()
    assert len(payload["contracts"]) == 1


def test_capture_writes_diagnostic_artifact_on_fail_closed(monkeypatch, tmp_path: Path) -> None:
    day_utc = "2026-04-24"

    class _FakeClient:
        def __init__(self) -> None:
            self.market_data_type = 1

        def connect_and_wait(self, *, host: str, port: int, client_id: int) -> None:
            return None

        def reqMarketDataType(self, marketDataType: int) -> None:  # noqa: N802
            self.market_data_type = int(marketDataType)

        def request_contract_details(self, contract: SimpleNamespace):
            if str(contract.secType) == "STK":
                return [SimpleNamespace(contract=_contract(sec_type="STK", con_id=101, local_symbol="SPY"))]
            return [SimpleNamespace(contract=_contract(sec_type="OPT", con_id=202, local_symbol="SPY   260430P00500000"))]

        def request_snapshot(
            self,
            contract: SimpleNamespace,
            generic_ticks: str,
            timeout_seconds: float = 4.0,
            snapshot: bool = True,
        ):
            if str(contract.secType) == "STK":
                return {"last": "500.12", "snapshot_complete": True, "observed_tick_types": [68]}
            return {"last": "0.75", "snapshot_complete": True, "observed_tick_types": [68]}

        def request_secdef(self, *, symbol: str, underlying_con_id: int):
            return [{"trading_class": "SPY", "expirations": ["20260430"], "strikes": [500.0]}]

        def disconnect(self) -> None:
            return None

        def error_events(self):
            return [{"req_id": 1, "error_code": 354, "error_detail": "No market data permissions"}]

    monkeypatch.setattr(capture_module, "_IbCaptureClient", _FakeClient)

    with pytest.raises(capture_module.CaptureError, match="OPTIONS_CAPTURE_FAILED_ALL_MARKET_DATA_TYPES") as excinfo:
        capture_module._capture_raw_chain(
            day_utc=day_utc,
            eval_time_utc=f"{day_utc}T20:00:00Z",
            symbol="SPY",
            truth_root=tmp_path,
            ib_host="127.0.0.1",
            ib_port=4002,
            ib_client_id=7,
        )

    diagnostic_path = excinfo.value.diagnostic_path
    assert diagnostic_path is not None
    assert diagnostic_path.exists()


def test_capture_requests_option_snapshot_without_generic_ticks(monkeypatch, tmp_path: Path) -> None:
    day_utc = "2026-04-24"
    observed_option_generic_ticks = []

    class _FakeClient:
        def __init__(self) -> None:
            self.market_data_type = 3

        def connect_and_wait(self, *, host: str, port: int, client_id: int) -> None:
            return None

        def reqMarketDataType(self, marketDataType: int) -> None:  # noqa: N802
            self.market_data_type = int(marketDataType)

        def request_contract_details(self, contract: SimpleNamespace):
            if str(contract.secType) == "STK":
                return [SimpleNamespace(contract=_contract(sec_type="STK", con_id=101, local_symbol="SPY"))]
            return [SimpleNamespace(contract=_contract(sec_type="OPT", con_id=202, local_symbol="SPY   260430P00500000"))]

        def request_snapshot(
            self,
            contract: SimpleNamespace,
            generic_ticks: str,
            timeout_seconds: float = 4.0,
            snapshot: bool = True,
        ):
            if str(contract.secType) == "STK":
                return {"last": "500.12"}
            observed_option_generic_ticks.append(generic_ticks)
            return {"bid": "1.00", "ask": "1.10", "volume": 1, "open_interest": 1, "observed_tick_types": [66, 67]}

        def request_secdef(self, *, symbol: str, underlying_con_id: int):
            return [{"trading_class": "SPY", "expirations": ["20260430"], "strikes": [500.0]}]

        def disconnect(self) -> None:
            return None

        def error_events(self):
            return []

    monkeypatch.setattr(capture_module, "_IbCaptureClient", _FakeClient)

    raw_path, payload = capture_module._capture_raw_chain(
        day_utc=day_utc,
        eval_time_utc=f"{day_utc}T20:00:00Z",
        symbol="SPY",
        truth_root=tmp_path,
        ib_host="127.0.0.1",
        ib_port=4002,
        ib_client_id=7,
    )

    assert raw_path.exists()
    assert len(payload["contracts"]) == 1
    assert observed_option_generic_ticks == [""]


def test_capture_evaluates_all_governed_dte_window_expiries_by_default(monkeypatch, tmp_path: Path) -> None:
    day_utc = "2026-04-24"

    class _FakeClient:
        def connect_and_wait(self, *, host: str, port: int, client_id: int) -> None:
            return None

        def reqMarketDataType(self, marketDataType: int) -> None:  # noqa: N802
            return None

        def request_contract_details(self, contract: SimpleNamespace):
            if str(contract.secType) == "STK":
                return [SimpleNamespace(contract=_contract(sec_type="STK", con_id=101, local_symbol="SPY"))]
            con_id = int(str(contract.lastTradeDateOrContractMonth)[-2:] + "500")
            return [SimpleNamespace(contract=_contract(sec_type="OPT", con_id=con_id, local_symbol=f"SPY   {contract.lastTradeDateOrContractMonth}P00500000"))]

        def request_snapshot(
            self,
            contract: SimpleNamespace,
            generic_ticks: str,
            timeout_seconds: float = 4.0,
            snapshot: bool = True,
        ):
            if str(contract.secType) == "STK":
                return {"last": "500.12"}
            return {"bid": "1.00", "ask": "1.10", "volume": 1, "open_interest": 1, "observed_tick_types": [1, 2]}

        def request_secdef(self, *, symbol: str, underlying_con_id: int):
            return [{"trading_class": "SPY", "expirations": ["20260425", "20260426", "20260430", "20260510"], "strikes": [500.0]}]

        def disconnect(self) -> None:
            return None

        def error_events(self):
            return []

    monkeypatch.setattr(capture_module, "_IbCaptureClient", _FakeClient)

    raw_path, payload = capture_module._capture_raw_chain(
        day_utc=day_utc,
        eval_time_utc=f"{day_utc}T20:00:00Z",
        symbol="SPY",
        truth_root=tmp_path,
        ib_host="127.0.0.1",
        ib_port=4002,
        ib_client_id=7,
    )

    coverage = payload["policy"]["dte_window_policy"]
    assert raw_path.exists()
    assert [row["expiry_yyyymmdd"] for row in coverage["expiries_evaluated"]] == ["20260425", "20260426", "20260430"]
    assert coverage["expiries_omitted"] == []
    assert len(payload["contracts"]) == 3


def test_capture_reports_bounded_dte_window_omissions(monkeypatch, tmp_path: Path) -> None:
    day_utc = "2026-04-24"

    class _FakeClient:
        def connect_and_wait(self, *, host: str, port: int, client_id: int) -> None:
            return None

        def reqMarketDataType(self, marketDataType: int) -> None:  # noqa: N802
            return None

        def request_contract_details(self, contract: SimpleNamespace):
            if str(contract.secType) == "STK":
                return [SimpleNamespace(contract=_contract(sec_type="STK", con_id=101, local_symbol="SPY"))]
            con_id = int(str(contract.lastTradeDateOrContractMonth)[-2:] + "501")
            return [SimpleNamespace(contract=_contract(sec_type="OPT", con_id=con_id, local_symbol=f"SPY   {contract.lastTradeDateOrContractMonth}P00500000"))]

        def request_snapshot(
            self,
            contract: SimpleNamespace,
            generic_ticks: str,
            timeout_seconds: float = 4.0,
            snapshot: bool = True,
        ):
            if str(contract.secType) == "STK":
                return {"last": "500.12"}
            return {"bid": "1.00", "ask": "1.10", "volume": 1, "open_interest": 1, "observed_tick_types": [1, 2]}

        def request_secdef(self, *, symbol: str, underlying_con_id: int):
            return [{"trading_class": "SPY", "expirations": ["20260425", "20260426", "20260430"], "strikes": [500.0]}]

        def disconnect(self) -> None:
            return None

        def error_events(self):
            return []

    monkeypatch.setattr(capture_module, "_IbCaptureClient", _FakeClient)

    raw_path, payload = capture_module._capture_raw_chain(
        day_utc=day_utc,
        eval_time_utc=f"{day_utc}T20:00:00Z",
        symbol="SPY",
        truth_root=tmp_path,
        ib_host="127.0.0.1",
        ib_port=4002,
        ib_client_id=7,
        max_expiries_to_capture=2,
    )

    diagnostic_path = Path(payload["provenance"]["capture_diagnostic_path"])
    diagnostic = json.loads(diagnostic_path.read_text(encoding="utf-8"))
    coverage = payload["policy"]["dte_window_policy"]
    assert raw_path.exists()
    assert [row["expiry_yyyymmdd"] for row in coverage["expiries_evaluated"]] == ["20260425", "20260426"]
    assert coverage["expiries_omitted"] == [{"dte": 6, "expiry_yyyymmdd": "20260430", "reason": "MAX_EXPIRIES_TO_CAPTURE:2"}]
    assert diagnostic["dte_coverage"] == coverage
    assert len(payload["contracts"]) == 2


def test_tick_option_computation_handles_none_values() -> None:
    client = capture_module._IbCaptureClient()
    client.tickOptionComputation(
        7,
        13,
        0,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    payload = client._ticks[7]["option_computation"]["model"]
    assert payload["implied_vol"] is None
    assert payload["underlying_price"] is None
