#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import socket
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from ibapi.client import EClient
    from ibapi.contract import Contract
    from ibapi.wrapper import EWrapper
except Exception as exc:  # noqa: BLE001
    print(f"FAIL: IBAPI_IMPORT_ERROR:{exc}", file=sys.stderr)
    raise SystemExit(2)

from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root

GOVERNED_POLICY_DTE_MIN_DEFAULT = 1
GOVERNED_POLICY_DTE_MAX_DEFAULT = 7
GOVERNED_MAX_EXPIRIES_TO_CAPTURE_DEFAULT = 7
GOVERNED_MAX_CAPTURE_SECONDS_DEFAULT = 150
CAPTURE_TIME_BUDGET_OMISSION_REASON = "CAPTURE_TIME_BUDGET_EXHAUSTED"


class CaptureError(Exception):
    def __init__(self, message: str, *, diagnostic_path: Optional[Path] = None) -> None:
        super().__init__(message)
        self.diagnostic_path = diagnostic_path


@dataclass(frozen=True)
class ContractWithMeta:
    contract: Contract
    con_id: int
    local_symbol: str
    trading_class: str
    exchange: str
    currency: str
    multiplier: int
    expiry_yyyymmdd: str
    strike_2dp: str
    right: str


class _IbCaptureClient(EWrapper, EClient):
    def __init__(self) -> None:
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
        self._lock = threading.Lock()
        self._next_req_id: Optional[int] = None
        self._connected = threading.Event()
        self._fatal_error: Optional[str] = None

        self._secdef_rows: Dict[int, List[Dict[str, Any]]] = {}
        self._secdef_done: Dict[int, threading.Event] = {}

        self._contract_rows: Dict[int, List[Any]] = {}
        self._contract_done: Dict[int, threading.Event] = {}

        self._ticks: Dict[int, Dict[str, Any]] = {}
        self._ticks_done: Dict[int, threading.Event] = {}
        self._error_events: List[Dict[str, Any]] = []

    def _note_tick_type(self, row: Dict[str, Any], tick_type: int) -> None:
        seen = row.setdefault("observed_tick_types", [])
        tt = int(tick_type)
        if tt not in seen:
            seen.append(tt)

    def _optional_float(self, value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except Exception:  # noqa: BLE001
            return None

    def nextValidId(self, orderId: int) -> None:  # noqa: N802
        with self._lock:
            if self._next_req_id is None or orderId > self._next_req_id:
                self._next_req_id = int(orderId)
        self._connected.set()

    def error(self, reqId: int, errorCode: int, errorString: str, advancedOrderRejectJson: str = "") -> None:  # noqa: N802
        message = f"IB_ERROR:req_id={reqId}:code={errorCode}:detail={errorString}"
        self._error_events.append(
            {
                "observed_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                "req_id": int(reqId),
                "error_code": int(errorCode),
                "error_detail": str(errorString or ""),
            }
        )
        if errorCode in {501, 502, 504, 1100, 1300}:
            self._fatal_error = message
            self._connected.set()
        done = self._ticks_done.get(int(reqId))
        if done is not None:
            done.set()

    def securityDefinitionOptionParameter(  # noqa: N802
        self,
        reqId: int,
        exchange: str,
        underlyingConId: int,
        tradingClass: str,
        multiplier: str,
        expirations: Sequence[str],
        strikes: Sequence[float],
    ) -> None:
        rows = self._secdef_rows.setdefault(int(reqId), [])
        rows.append(
            {
                "exchange": str(exchange or "").strip(),
                "underlying_con_id": int(underlyingConId),
                "trading_class": str(tradingClass or "").strip(),
                "multiplier": str(multiplier or "").strip(),
                "expirations": [str(item or "").strip() for item in expirations],
                "strikes": [str(item).strip() for item in strikes],
            }
        )

    def securityDefinitionOptionParameterEnd(self, reqId: int) -> None:  # noqa: N802
        done = self._secdef_done.get(int(reqId))
        if done is not None:
            done.set()

    def contractDetails(self, reqId: int, contractDetails: Any) -> None:  # noqa: N802
        self._contract_rows.setdefault(int(reqId), []).append(contractDetails)

    def contractDetailsEnd(self, reqId: int) -> None:  # noqa: N802
        done = self._contract_done.get(int(reqId))
        if done is not None:
            done.set()

    def tickPrice(self, reqId: int, tickType: int, price: float, attrib: Any) -> None:  # noqa: N802
        row = self._ticks.setdefault(int(reqId), {})
        self._note_tick_type(row, int(tickType))
        if price is None or price <= 0:
            return
        tick_to_field = {
            1: "bid",
            2: "ask",
            4: "last",
            9: "close",
            # Delayed snapshot equivalents for bid/ask/last/close.
            66: "bid",
            67: "ask",
            68: "last",
            75: "close",
        }
        field = tick_to_field.get(int(tickType))
        if field:
            row[field] = str(price)
        delayed_field_by_tick = {
            66: "delayed_bid",
            67: "delayed_ask",
            68: "delayed_last",
            75: "delayed_close",
        }
        delayed_field = delayed_field_by_tick.get(int(tickType))
        if delayed_field:
            row[delayed_field] = str(price)

    def tickSize(self, reqId: int, tickType: int, size: int) -> None:  # noqa: N802
        row = self._ticks.setdefault(int(reqId), {})
        self._note_tick_type(row, int(tickType))
        if size is None or int(size) < 0:
            return
        tt = int(tickType)
        if tt in {29, 30, 8}:
            row["volume"] = max(int(row.get("volume", 0)), int(size))
        elif tt in {27, 28}:
            row["open_interest"] = max(int(row.get("open_interest", 0)), int(size))

    def tickGeneric(self, reqId: int, tickType: int, value: float) -> None:  # noqa: N802
        row = self._ticks.setdefault(int(reqId), {})
        self._note_tick_type(row, int(tickType))
        if value is None:
            return
        tt = int(tickType)
        if tt in {100, 105}:
            try:
                row["volume"] = max(int(row.get("volume", 0)), int(value))
            except Exception:  # noqa: BLE001
                return
        elif tt in {101, 104}:
            try:
                row["open_interest"] = max(int(row.get("open_interest", 0)), int(value))
            except Exception:  # noqa: BLE001
                return

    def tickOptionComputation(  # noqa: N802
        self,
        reqId: int,
        tickType: int,
        tickAttrib: int,
        impliedVol: float,
        delta: float,
        optPrice: float,
        pvDividend: float,
        gamma: float,
        vega: float,
        theta: float,
        undPrice: float,
    ) -> None:
        row = self._ticks.setdefault(int(reqId), {})
        self._note_tick_type(row, int(tickType))
        kind_by_tick = {10: "bid", 11: "ask", 12: "last", 13: "model"}
        kind = kind_by_tick.get(int(tickType), f"tick_{int(tickType)}")
        option_comp = row.setdefault("option_computation", {})
        option_comp[kind] = {
            "implied_vol": self._optional_float(impliedVol),
            "delta": self._optional_float(delta),
            "opt_price": self._optional_float(optPrice),
            "pv_dividend": self._optional_float(pvDividend),
            "gamma": self._optional_float(gamma),
            "vega": self._optional_float(vega),
            "theta": self._optional_float(theta),
            "underlying_price": self._optional_float(undPrice),
        }

    def marketDataType(self, reqId: int, marketDataType: int) -> None:  # noqa: N802
        row = self._ticks.setdefault(int(reqId), {})
        row["market_data_type_callback"] = int(marketDataType)

    def tickSnapshotEnd(self, reqId: int) -> None:  # noqa: N802
        done = self._ticks_done.get(int(reqId))
        if done is not None:
            done.set()

    def _alloc_req_id(self) -> int:
        with self._lock:
            if self._next_req_id is None:
                raise CaptureError("IB_REQ_ID_NOT_READY")
            rid = int(self._next_req_id)
            self._next_req_id += 1
            return rid

    def connect_and_wait(self, *, host: str, port: int, client_id: int, timeout_seconds: float = 8.0) -> None:
        self.connect(host, int(port), int(client_id))
        runner = threading.Thread(target=self.run, daemon=True)
        runner.start()
        if not self._connected.wait(timeout_seconds):
            raise CaptureError("IB_CONNECT_TIMEOUT")
        if self._fatal_error:
            raise CaptureError(self._fatal_error)

    def request_secdef(self, *, symbol: str, underlying_con_id: int, timeout_seconds: float = 8.0) -> List[Dict[str, Any]]:
        req_id = self._alloc_req_id()
        done = threading.Event()
        self._secdef_done[req_id] = done
        self._secdef_rows[req_id] = []
        self.reqSecDefOptParams(req_id, symbol, "", "STK", int(underlying_con_id))
        if not done.wait(timeout_seconds):
            raise CaptureError(f"IB_SECDEF_TIMEOUT:req_id={req_id}")
        return list(self._secdef_rows.get(req_id) or [])

    def request_contract_details(self, *, contract: Contract, timeout_seconds: float = 8.0) -> List[Any]:
        req_id = self._alloc_req_id()
        done = threading.Event()
        self._contract_done[req_id] = done
        self._contract_rows[req_id] = []
        self.reqContractDetails(req_id, contract)
        if not done.wait(timeout_seconds):
            raise CaptureError(f"IB_CONTRACT_DETAILS_TIMEOUT:req_id={req_id}")
        return list(self._contract_rows.get(req_id) or [])

    def request_snapshot(
        self,
        *,
        contract: Contract,
        generic_ticks: str,
        timeout_seconds: float = 4.0,
        snapshot: bool = True,
    ) -> Dict[str, Any]:
        req_id = self._alloc_req_id()
        done = threading.Event()
        self._ticks_done[req_id] = done
        self._ticks[req_id] = {}
        self.reqMktData(req_id, contract, generic_ticks, bool(snapshot), False, [])
        completed = bool(done.wait(timeout_seconds))
        self.cancelMktData(req_id)
        row = dict(self._ticks.get(req_id) or {})
        row["request_id"] = int(req_id)
        row["snapshot_complete"] = completed
        row["request_mode"] = "SNAPSHOT" if snapshot else "STREAMING_FALLBACK"
        return row

    def error_events(self) -> List[Dict[str, Any]]:
        return [dict(event) for event in self._error_events]


def _parse_day(day_utc: str) -> str:
    value = str(day_utc or "").strip()
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise CaptureError(f"BAD_DAY_UTC:{value!r}") from exc
    return value


def _parse_eval(eval_time_utc: str, day_utc: str) -> str:
    value = str(eval_time_utc or "").strip()
    if not value.endswith("Z") or "T" not in value:
        raise CaptureError(f"BAD_EVAL_TIME_UTC:{value!r}")
    if value[:10] != day_utc:
        raise CaptureError(f"EVAL_TIME_DAY_MISMATCH:eval={value}:day={day_utc}")
    return value


def _fmt_2dp(value: str, *, label: str) -> str:
    try:
        dec = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise CaptureError(f"DECIMAL_PARSE_FAIL:{label}:{value!r}") from exc
    return format(dec.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), "f")


def _strike_candidates(*, strikes: Sequence[str], spot: Decimal) -> List[str]:
    parsed: List[Decimal] = []
    for raw in strikes:
        try:
            parsed.append(Decimal(str(raw).strip()))
        except InvalidOperation:
            continue
    parsed = sorted(set(item for item in parsed if item > 0))
    if not parsed:
        return []
    lower = spot - Decimal("20")
    upper = spot + Decimal("3")
    windowed = [item for item in parsed if lower <= item <= upper]
    if len(windowed) >= 20:
        selected = windowed
    else:
        selected = sorted(parsed, key=lambda item: (abs(item - spot), item))[:40]
    return sorted({format(item.quantize(Decimal("0.01")), "f") for item in selected})


def _expiry_window_diagnostics(
    *,
    expirations: Sequence[str],
    day_utc: str,
    policy_dte_min: int,
    policy_dte_max: int,
    max_expiries_to_capture: int,
) -> Dict[str, Any]:
    base_day = date.fromisoformat(day_utc)
    candidates: List[Tuple[int, str]] = []
    for raw in expirations:
        value = str(raw or "").strip()
        if len(value) != 8 or not value.isdigit():
            continue
        exp_day = date(int(value[0:4]), int(value[4:6]), int(value[6:8]))
        dte = (exp_day - base_day).days
        if int(policy_dte_min) <= dte <= int(policy_dte_max):
            candidates.append((dte, value))
    ordered = sorted(candidates)
    cap = max(0, int(max_expiries_to_capture))
    evaluated = ordered[:cap]
    omitted = ordered[cap:]
    omission_reason = f"MAX_EXPIRIES_TO_CAPTURE:{cap}" if omitted else ""
    return {
        "policy_dte_min": int(policy_dte_min),
        "policy_dte_max": int(policy_dte_max),
        "max_expiries_to_capture": cap,
        "expiries_available": [{"dte": int(dte), "expiry_yyyymmdd": expiry} for dte, expiry in ordered],
        "expiries_evaluated": [{"dte": int(dte), "expiry_yyyymmdd": expiry} for dte, expiry in evaluated],
        "expiries_omitted": [
            {"dte": int(dte), "expiry_yyyymmdd": expiry, "reason": omission_reason}
            for dte, expiry in omitted
        ],
        "omission_reason": omission_reason,
    }


def _bounded_expiry_coverage(
    *,
    base_coverage: Dict[str, Any],
    completed_expiry_yyyymmdd: Sequence[str],
    time_budget_exhausted: bool,
) -> Dict[str, Any]:
    completed = {str(item) for item in completed_expiry_yyyymmdd}
    evaluated: List[Dict[str, Any]] = []
    omitted: List[Dict[str, Any]] = []
    for row in base_coverage.get("expiries_evaluated") or []:
        if not isinstance(row, dict):
            continue
        expiry = str(row.get("expiry_yyyymmdd") or "").strip()
        if expiry in completed:
            evaluated.append({"dte": int(row.get("dte") or 0), "expiry_yyyymmdd": expiry})
        else:
            omitted.append(
                {
                    "dte": int(row.get("dte") or 0),
                    "expiry_yyyymmdd": expiry,
                    "reason": CAPTURE_TIME_BUDGET_OMISSION_REASON if time_budget_exhausted else "NOT_EVALUATED",
                }
            )
    for row in base_coverage.get("expiries_omitted") or []:
        if not isinstance(row, dict):
            continue
        omitted.append(
            {
                "dte": int(row.get("dte") or 0),
                "expiry_yyyymmdd": str(row.get("expiry_yyyymmdd") or "").strip(),
                "reason": str(row.get("reason") or base_coverage.get("omission_reason") or "MAX_EXPIRIES_TO_CAPTURE"),
            }
        )
    omission_reason = ""
    if any(row.get("reason") == CAPTURE_TIME_BUDGET_OMISSION_REASON for row in omitted):
        omission_reason = CAPTURE_TIME_BUDGET_OMISSION_REASON
    elif omitted:
        omission_reason = str(base_coverage.get("omission_reason") or "MAX_EXPIRIES_TO_CAPTURE")
    return {
        "policy_dte_min": int(base_coverage.get("policy_dte_min") or 0),
        "policy_dte_max": int(base_coverage.get("policy_dte_max") or 0),
        "max_expiries_to_capture": int(base_coverage.get("max_expiries_to_capture") or 0),
        "expiries_available": list(base_coverage.get("expiries_available") or []),
        "expiries_evaluated": evaluated,
        "expiries_omitted": omitted,
        "omission_reason": omission_reason,
    }


def _expiry_candidates(
    *,
    expirations: Sequence[str],
    day_utc: str,
    policy_dte_min: int = GOVERNED_POLICY_DTE_MIN_DEFAULT,
    policy_dte_max: int = GOVERNED_POLICY_DTE_MAX_DEFAULT,
    max_expiries_to_capture: int = GOVERNED_MAX_EXPIRIES_TO_CAPTURE_DEFAULT,
) -> List[str]:
    diagnostics = _expiry_window_diagnostics(
        expirations=expirations,
        day_utc=day_utc,
        policy_dte_min=policy_dte_min,
        policy_dte_max=policy_dte_max,
        max_expiries_to_capture=max_expiries_to_capture,
    )
    return [str(row["expiry_yyyymmdd"]) for row in diagnostics["expiries_evaluated"]]


def _stock_contract(symbol: str) -> Contract:
    c = Contract()
    c.symbol = symbol
    c.secType = "STK"
    c.exchange = "SMART"
    c.currency = "USD"
    return c


def _option_contract(*, symbol: str, expiry_yyyymmdd: str, strike_2dp: str, right: str, trading_class: str) -> Contract:
    c = Contract()
    c.symbol = symbol
    c.secType = "OPT"
    c.exchange = "SMART"
    c.currency = "USD"
    c.lastTradeDateOrContractMonth = expiry_yyyymmdd
    c.right = right
    c.strike = float(strike_2dp)
    c.multiplier = "100"
    if trading_class:
        c.tradingClass = trading_class
    return c


def _select_spot(snapshot: Dict[str, Any]) -> str:
    for key in ("last", "close", "bid", "ask"):
        raw = snapshot.get(key)
        if raw is None:
            continue
        value = _fmt_2dp(str(raw), label=f"spot:{key}")
        if Decimal(value) > 0:
            return value
    raise CaptureError("UNDERLYING_SPOT_MISSING")


def _contract_from_details(*, detail: Any, expiry_yyyymmdd: str, strike_2dp: str, right: str) -> ContractWithMeta:
    contract = detail.contract
    con_id = int(contract.conId)
    local_symbol = str(contract.localSymbol or "").strip()
    trading_class = str(contract.tradingClass or "").strip()
    exchange = str(contract.exchange or "SMART").strip() or "SMART"
    currency = str(contract.currency or "USD").strip() or "USD"
    multiplier = int(str(contract.multiplier or "100").strip())
    if con_id <= 0 or not local_symbol:
        raise CaptureError("IB_CONTRACT_DETAILS_INVALID")
    return ContractWithMeta(
        contract=contract,
        con_id=con_id,
        local_symbol=local_symbol,
        trading_class=trading_class,
        exchange=exchange,
        currency=currency,
        multiplier=multiplier,
        expiry_yyyymmdd=expiry_yyyymmdd,
        strike_2dp=strike_2dp,
        right=right,
    )


def _quote_validity(*, meta: ContractWithMeta, ticks: Dict[str, Any]) -> Tuple[bool, str, Optional[str], Optional[str]]:
    bid_raw = ticks.get("bid")
    ask_raw = ticks.get("ask")
    if bid_raw is None or ask_raw is None:
        return (False, "MISSING_BID_ASK", None, None)
    try:
        bid = _fmt_2dp(str(bid_raw), label=f"bid:{meta.local_symbol}")
        ask = _fmt_2dp(str(ask_raw), label=f"ask:{meta.local_symbol}")
    except CaptureError as exc:
        return (False, str(exc), None, None)
    if Decimal(bid) <= 0 or Decimal(ask) <= 0:
        return (False, "NON_POSITIVE_BID_ASK", None, None)
    if Decimal(ask) < Decimal(bid):
        return (False, "CROSSED_BID_ASK", None, None)
    return (True, "", bid, ask)


def _contract_rows_to_raw_contracts(*, rows: Sequence[Tuple[ContractWithMeta, Dict[str, Any]]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for meta, ticks in rows:
        valid, _reason, bid, ask = _quote_validity(meta=meta, ticks=ticks)
        if not valid or bid is None or ask is None:
            continue
        volume = int(ticks.get("volume", 0))
        open_interest = int(ticks.get("open_interest", 0))
        expiry_utc = f"{meta.expiry_yyyymmdd[0:4]}-{meta.expiry_yyyymmdd[4:6]}-{meta.expiry_yyyymmdd[6:8]}T00:00:00Z"
        out.append(
            {
                "expiry_utc": expiry_utc,
                "strike": meta.strike_2dp,
                "right": meta.right,
                "bid": bid,
                "ask": ask,
                "open_interest": max(open_interest, 0),
                "volume": max(volume, 0),
                "ib": {
                    "conId": meta.con_id,
                    "localSymbol": meta.local_symbol,
                    "tradingClass": meta.trading_class,
                    "exchange": meta.exchange,
                    "currency": meta.currency,
                    "multiplier": meta.multiplier,
                },
            }
        )
    if not out:
        raise CaptureError("NO_VALID_OPTION_QUOTES_CAPTURED")
    return out


def _compact_contract_summary(detail: Any) -> Dict[str, Any]:
    contract = detail.contract
    return {
        "con_id": int(getattr(contract, "conId", 0) or 0),
        "symbol": str(getattr(contract, "symbol", "") or "").strip(),
        "expiry_yyyymmdd": str(getattr(contract, "lastTradeDateOrContractMonth", "") or "").strip(),
        "strike": str(getattr(contract, "strike", "") or ""),
        "right": str(getattr(contract, "right", "") or "").strip(),
        "exchange": str(getattr(contract, "exchange", "") or "").strip(),
        "trading_class": str(getattr(contract, "tradingClass", "") or "").strip(),
        "multiplier": str(getattr(contract, "multiplier", "") or "").strip(),
        "local_symbol": str(getattr(contract, "localSymbol", "") or "").strip(),
    }


def _write_capture_diagnostic(*, truth_root: Path, day_utc: str, run_id: str, diagnostic: Dict[str, Any]) -> Path:
    out_dir = (truth_root / "reports" / "options_chain_capture_ib_day_v1" / day_utc / run_id).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    path = (out_dir / "options_chain_capture_diagnostic.v1.json").resolve()
    path.write_text(json.dumps(diagnostic, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return path


def _capture_budget_exhausted(*, start_monotonic: float, max_capture_seconds: int) -> bool:
    return (time.monotonic() - float(start_monotonic)) >= float(max(1, int(max_capture_seconds)))


def _capture_budget_nearly_exhausted(*, start_monotonic: float, max_capture_seconds: int) -> bool:
    max_seconds = max(1, int(max_capture_seconds))
    elapsed = time.monotonic() - float(start_monotonic)
    remaining = float(max_seconds) - elapsed
    return remaining <= min(12.0, max(1.0, float(max_seconds) * 0.10))


def _capture_raw_chain(
    *,
    day_utc: str,
    eval_time_utc: str,
    symbol: str,
    truth_root: Path,
    ib_host: str,
    ib_port: int,
    ib_client_id: int,
    policy_dte_min: int = GOVERNED_POLICY_DTE_MIN_DEFAULT,
    policy_dte_max: int = GOVERNED_POLICY_DTE_MAX_DEFAULT,
    max_expiries_to_capture: int = GOVERNED_MAX_EXPIRIES_TO_CAPTURE_DEFAULT,
    max_capture_seconds: int = GOVERNED_MAX_CAPTURE_SECONDS_DEFAULT,
) -> Tuple[Path, Dict[str, Any]]:
    capture_tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"ib_capture_{symbol}_{capture_tag}_{uuid.uuid4().hex[:8]}"
    start_monotonic = time.monotonic()
    client = _IbCaptureClient()
    selected_market_data_type: Optional[int] = None
    spot_price_2dp = ""
    contracts: List[Dict[str, Any]] = []
    attempt_failures: List[str] = []
    diagnostic: Dict[str, Any] = {
        "schema_id": "options_chain_capture_diagnostic",
        "schema_version": "v1",
        "status": "RUNNING",
        "day_utc": day_utc,
        "environment": "PAPER",
        "symbol": symbol,
        "capture_run_id": run_id,
        "eval_time_utc": eval_time_utc,
        "ib_connection": {
            "host": ib_host,
            "port": int(ib_port),
            "client_id": int(ib_client_id),
            "ib_account": "UNKNOWN_NOT_CAPTURED_BY_THIS_TOOL",
            "connected": False,
        },
        "attempted_market_data_types": [],
        "attempts": [],
        "attempt_failures": [],
        "capture_bounds": {
            "max_expiries_to_capture": int(max_expiries_to_capture),
            "max_capture_seconds": int(max_capture_seconds),
            "nearest_expiry_first": True,
        },
    }
    diagnostic_path: Optional[Path] = None
    try:
        client.connect_and_wait(host=ib_host, port=ib_port, client_id=ib_client_id)
        diagnostic["ib_connection"]["connected"] = True

        stock_details = client.request_contract_details(contract=_stock_contract(symbol=symbol))
        if not stock_details:
            raise CaptureError(f"UNDERLYING_CONTRACT_DETAILS_MISSING:{symbol}")
        underlying = stock_details[0].contract
        underlying_con_id = int(underlying.conId)
        if underlying_con_id <= 0:
            raise CaptureError("UNDERLYING_CONID_INVALID")

        secdef_rows = client.request_secdef(symbol=symbol, underlying_con_id=underlying_con_id)
        if not secdef_rows:
            raise CaptureError(f"SECDEF_ROWS_MISSING:{symbol}")
        secdef = secdef_rows[0]
        expiry_coverage = _expiry_window_diagnostics(
            expirations=secdef.get("expirations") or [],
            day_utc=day_utc,
            policy_dte_min=int(policy_dte_min),
            policy_dte_max=int(policy_dte_max),
            max_expiries_to_capture=int(max_expiries_to_capture),
        )
        diagnostic["dte_coverage"] = dict(expiry_coverage)
        expiries = [str(row["expiry_yyyymmdd"]) for row in expiry_coverage["expiries_evaluated"]]
        if not expiries:
            raise CaptureError(f"NO_EXPIRY_IN_DTE_WINDOW:{symbol}")
        trading_class = str(secdef.get("trading_class") or "").strip()

        for market_data_type in (1, 3, 4):
            attempt: Dict[str, Any] = {
                "market_data_type_requested": int(market_data_type),
                "quote_snapshot_timeout_seconds": 4.0,
                "quote_streaming_fallback_timeout_seconds": 6.0,
                "contract_details_timeout_seconds": 8.0,
                "spot_snapshot": {},
                "strikes_selected_count": 0,
                "contract_request_count": 0,
                "contract_details_count": 0,
                "contract_details_timeouts": 0,
                "contract_details_missing": 0,
                "quote_request_count": 0,
                "valid_quote_count": 0,
                "invalid_quote_count": 0,
                "invalid_quote_reason_counts": {},
                "contract_samples": [],
                "quote_samples": [],
            }
            diagnostic["attempted_market_data_types"].append(int(market_data_type))
            diagnostic["attempts"].append(attempt)
            err_start = len(client.error_events())
            try:
                client.reqMarketDataType(market_data_type)
                spot_snapshot = client.request_snapshot(contract=underlying, generic_ticks="")
                attempt["spot_snapshot"] = {
                    "bid": spot_snapshot.get("bid"),
                    "ask": spot_snapshot.get("ask"),
                    "last": spot_snapshot.get("last"),
                    "close": spot_snapshot.get("close"),
                    "delayed_bid": spot_snapshot.get("delayed_bid"),
                    "delayed_ask": spot_snapshot.get("delayed_ask"),
                    "delayed_last": spot_snapshot.get("delayed_last"),
                    "delayed_close": spot_snapshot.get("delayed_close"),
                    "observed_tick_types": list(spot_snapshot.get("observed_tick_types") or []),
                    "market_data_type_callback": spot_snapshot.get("market_data_type_callback"),
                    "snapshot_complete": bool(spot_snapshot.get("snapshot_complete")),
                }
                spot_price_candidate = _select_spot(spot_snapshot)
                spot_dec = Decimal(spot_price_candidate)
                strikes = _strike_candidates(strikes=secdef.get("strikes") or [], spot=spot_dec)
                attempt["strikes_selected_count"] = int(len(strikes))
                if not strikes:
                    raise CaptureError(f"NO_STRIKES_AVAILABLE:{symbol}")

                captured_rows: List[Tuple[ContractWithMeta, Dict[str, Any]]] = []
                completed_expiries: List[str] = []
                time_budget_exhausted = False
                for expiry in expiries:
                    if completed_expiries and _capture_budget_nearly_exhausted(
                        start_monotonic=start_monotonic,
                        max_capture_seconds=int(max_capture_seconds),
                    ):
                        time_budget_exhausted = True
                        break
                    expiry_rows: List[Tuple[ContractWithMeta, Dict[str, Any]]] = []
                    for strike in strikes:
                        if _capture_budget_exhausted(
                            start_monotonic=start_monotonic,
                            max_capture_seconds=int(max_capture_seconds),
                        ):
                            time_budget_exhausted = True
                            break
                        attempt["contract_request_count"] = int(attempt["contract_request_count"]) + 1
                        option = _option_contract(
                            symbol=symbol,
                            expiry_yyyymmdd=expiry,
                            strike_2dp=strike,
                            right="PUT",
                            trading_class=trading_class,
                        )
                        try:
                            details = client.request_contract_details(contract=option)
                        except CaptureError as exc:
                            if str(exc).startswith("IB_CONTRACT_DETAILS_TIMEOUT"):
                                attempt["contract_details_timeouts"] = int(attempt["contract_details_timeouts"]) + 1
                                continue
                            raise
                        if not details:
                            attempt["contract_details_missing"] = int(attempt["contract_details_missing"]) + 1
                            continue
                        attempt["contract_details_count"] = int(attempt["contract_details_count"]) + 1
                        if len(attempt["contract_samples"]) < 10:
                            attempt["contract_samples"].append(_compact_contract_summary(details[0]))
                        try:
                            meta = _contract_from_details(
                                detail=details[0],
                                expiry_yyyymmdd=expiry,
                                strike_2dp=strike,
                                right="PUT",
                            )
                        except CaptureError:
                            continue
                        ticks = client.request_snapshot(contract=meta.contract, generic_ticks="")
                        if not list(ticks.get("observed_tick_types") or []):
                            fallback_ticks = client.request_snapshot(
                                contract=meta.contract,
                                generic_ticks="",
                                timeout_seconds=6.0,
                                snapshot=False,
                            )
                            if list(fallback_ticks.get("observed_tick_types") or []):
                                ticks = fallback_ticks
                        attempt["quote_request_count"] = int(attempt["quote_request_count"]) + 1
                        valid_quote, invalid_reason, _bid_norm, _ask_norm = _quote_validity(meta=meta, ticks=ticks)
                        if valid_quote:
                            attempt["valid_quote_count"] = int(attempt["valid_quote_count"]) + 1
                        else:
                            attempt["invalid_quote_count"] = int(attempt["invalid_quote_count"]) + 1
                            reason_counts = attempt["invalid_quote_reason_counts"]
                            reason_counts[invalid_reason] = int(reason_counts.get(invalid_reason, 0)) + 1
                        if len(attempt["quote_samples"]) < 10:
                            attempt["quote_samples"].append(
                                {
                                    "con_id": meta.con_id,
                                    "local_symbol": meta.local_symbol,
                                    "expiry_yyyymmdd": meta.expiry_yyyymmdd,
                                    "strike_2dp": meta.strike_2dp,
                                    "right": meta.right,
                                    "exchange": meta.exchange,
                                    "trading_class": meta.trading_class,
                                    "bid": ticks.get("bid"),
                                    "ask": ticks.get("ask"),
                                    "last": ticks.get("last"),
                                    "close": ticks.get("close"),
                                    "delayed_bid": ticks.get("delayed_bid"),
                                    "delayed_ask": ticks.get("delayed_ask"),
                                    "delayed_last": ticks.get("delayed_last"),
                                    "delayed_close": ticks.get("delayed_close"),
                                    "observed_tick_types": list(ticks.get("observed_tick_types") or []),
                                    "market_data_type_callback": ticks.get("market_data_type_callback"),
                                    "option_computation": ticks.get("option_computation") or {},
                                    "snapshot_complete": bool(ticks.get("snapshot_complete")),
                                    "request_mode": ticks.get("request_mode"),
                                    "valid_quote": bool(valid_quote),
                                    "invalid_reason": invalid_reason if not valid_quote else None,
                                }
                            )
                        expiry_rows.append((meta, ticks))
                        time.sleep(0.03)
                    if time_budget_exhausted:
                        break
                    captured_rows.extend(expiry_rows)
                    completed_expiries.append(expiry)
                    bounded_coverage = _bounded_expiry_coverage(
                        base_coverage=expiry_coverage,
                        completed_expiry_yyyymmdd=completed_expiries,
                        time_budget_exhausted=False,
                    )
                    diagnostic["dte_coverage"] = bounded_coverage
                    attempt["dte_coverage"] = bounded_coverage
                    _write_capture_diagnostic(
                        truth_root=truth_root,
                        day_utc=day_utc,
                        run_id=run_id,
                        diagnostic=diagnostic,
                    )

                if time_budget_exhausted:
                    expiry_coverage = _bounded_expiry_coverage(
                        base_coverage=expiry_coverage,
                        completed_expiry_yyyymmdd=completed_expiries,
                        time_budget_exhausted=True,
                    )
                    diagnostic["dte_coverage"] = dict(expiry_coverage)
                    attempt["dte_coverage"] = dict(expiry_coverage)
                    attempt["time_budget_exhausted"] = True
                    _write_capture_diagnostic(
                        truth_root=truth_root,
                        day_utc=day_utc,
                        run_id=run_id,
                        diagnostic=diagnostic,
                    )
                    if not completed_expiries:
                        raise CaptureError(f"OPTIONS_CAPTURE_TIMEOUT:{CAPTURE_TIME_BUDGET_OMISSION_REASON}")
                else:
                    expiry_coverage = _bounded_expiry_coverage(
                        base_coverage=expiry_coverage,
                        completed_expiry_yyyymmdd=completed_expiries,
                        time_budget_exhausted=False,
                    )
                    diagnostic["dte_coverage"] = dict(expiry_coverage)
                    attempt["dte_coverage"] = dict(expiry_coverage)
                contracts = _contract_rows_to_raw_contracts(rows=captured_rows)
                attempt["selected"] = True
                selected_market_data_type = market_data_type
                spot_price_2dp = spot_price_candidate
                break
            except CaptureError as exc:
                attempt["error"] = str(exc)
                attempt["ib_errors"] = client.error_events()[err_start:]
                attempt_failures.append(f"market_data_type={market_data_type}:{exc}")
                continue

        if selected_market_data_type is None:
            all_reasons = {
                failure.split(":", 1)[1].strip()
                for failure in attempt_failures
                if ":" in failure
            }
            if all_reasons == {"UNDERLYING_SPOT_MISSING"}:
                raise CaptureError("UNDERLYING_SPOT_MISSING")
            detail = "|".join(attempt_failures) if attempt_failures else "UNKNOWN_CAPTURE_FAILURE"
            raise CaptureError(f"OPTIONS_CAPTURE_FAILED_ALL_MARKET_DATA_TYPES:{detail}")
        diagnostic["status"] = "OK"
        diagnostic["selected_market_data_type"] = int(selected_market_data_type)
        diagnostic["valid_quote_count"] = int(len(contracts))
        diagnostic["attempt_failures"] = list(attempt_failures)
        diagnostic["ib_errors"] = client.error_events()
        diagnostic_path = _write_capture_diagnostic(
            truth_root=truth_root,
            day_utc=day_utc,
            run_id=run_id,
            diagnostic=diagnostic,
        )
    except CaptureError as exc:
        diagnostic["status"] = "FAIL"
        diagnostic["error"] = str(exc)
        diagnostic["attempt_failures"] = list(attempt_failures)
        diagnostic["ib_errors"] = client.error_events()
        diagnostic_path = _write_capture_diagnostic(
            truth_root=truth_root,
            day_utc=day_utc,
            run_id=run_id,
            diagnostic=diagnostic,
        )
        raise CaptureError(str(exc), diagnostic_path=diagnostic_path) from exc
    finally:
        try:
            client.disconnect()
        except Exception:  # noqa: BLE001
            pass

    out_dir = (truth_root / "options_chain_raw_v1" / day_utc / run_id).resolve()
    if out_dir.exists():
        raise CaptureError(f"OUTPUT_DIR_ALREADY_EXISTS:{out_dir}")
    out_dir.mkdir(parents=True, exist_ok=False)
    raw_path = (out_dir / "raw_chain.json").resolve()
    payload = {
        "as_of_utc": eval_time_utc,
        "underlying": {
            "symbol": symbol,
            "spot_price": spot_price_2dp,
            "spot_as_of_utc": eval_time_utc,
        },
        "contracts": contracts,
        "provenance": {
            "source": "IBKR",
            "capture_method": "IBKR_SNAPSHOT_DAY_ANCHORED",
            "market_data_type": selected_market_data_type,
            "capture_host": socket.gethostname(),
            "capture_run_id": run_id,
            "capture_diagnostic_path": str(diagnostic_path) if diagnostic_path else "",
        },
        "policy": {
            "dte_method": "CALENDAR_DAYS_UTC",
            "dte_window_policy": {
                "policy_dte_min": int(policy_dte_min),
                "policy_dte_max": int(policy_dte_max),
                "max_expiries_to_capture": int(max_expiries_to_capture),
                "expiries_available": list(expiry_coverage.get("expiries_available", [])),
                "expiries_evaluated": list(expiry_coverage.get("expiries_evaluated", [])),
                "expiries_omitted": list(expiry_coverage.get("expiries_omitted", [])),
                "omission_reason": str(expiry_coverage.get("omission_reason") or ""),
            },
            "liquidity_policy": {
                "min_open_interest": 0,
                "min_volume": 1,
                "max_bid_ask_spread": "0.10",
            },
            "pricing_policy": {
                "mid_definition": "(bid+ask)/2",
            },
        },
    }
    raw_path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return raw_path, payload


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_options_chain_capture_ib_day_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--eval_time_utc", required=True)
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--truth_root", default=str(resolve_canonical_truth_root().resolve()))
    ap.add_argument("--ib_host", default="127.0.0.1")
    ap.add_argument("--ib_port", type=int, default=4002)
    ap.add_argument("--ib_client_id", type=int, default=7)
    ap.add_argument("--policy_dte_min", type=int, default=GOVERNED_POLICY_DTE_MIN_DEFAULT, help="Governed minimum calendar DTE to capture for option-chain coverage diagnostics.")
    ap.add_argument("--policy_dte_max", type=int, default=GOVERNED_POLICY_DTE_MAX_DEFAULT, help="Governed maximum calendar DTE to capture for option-chain coverage diagnostics.")
    ap.add_argument("--max_expiries_to_capture", type=int, default=GOVERNED_MAX_EXPIRIES_TO_CAPTURE_DEFAULT, help="Bound on evaluated expiries; omitted expiries are reported in artifacts.")
    ap.add_argument("--max_capture_seconds", type=int, default=GOVERNED_MAX_CAPTURE_SECONDS_DEFAULT, help="Bound on in-process IB capture expansion before publishing a partial evaluated-expiry snapshot or fail-closing.")
    args = ap.parse_args()

    try:
        day_utc = _parse_day(args.day_utc)
        eval_time_utc = _parse_eval(args.eval_time_utc, day_utc)
        symbol = str(args.symbol or "").strip().upper()
        if not symbol:
            raise CaptureError("SYMBOL_REQUIRED")
        truth_root = Path(str(args.truth_root)).expanduser().resolve()
        if not truth_root.exists() or not truth_root.is_dir():
            raise CaptureError(f"TRUTH_ROOT_MISSING:{truth_root}")

        raw_path, payload = _capture_raw_chain(
            day_utc=day_utc,
            eval_time_utc=eval_time_utc,
            symbol=symbol,
            truth_root=truth_root,
            ib_host=str(args.ib_host or "127.0.0.1").strip(),
            ib_port=int(args.ib_port),
            ib_client_id=int(args.ib_client_id),
            policy_dte_min=int(args.policy_dte_min),
            policy_dte_max=int(args.policy_dte_max),
            max_expiries_to_capture=int(args.max_expiries_to_capture),
            max_capture_seconds=int(args.max_capture_seconds),
        )
        print(
            json.dumps(
                {
                    "status": "OK",
                    "path": str(raw_path),
                    "symbol": symbol,
                    "contracts": len(payload.get("contracts") or []),
                    "as_of_utc": payload.get("as_of_utc"),
                },
                sort_keys=True,
            )
        )
        return 0
    except CaptureError as exc:
        diag = f":DIAG_PATH={exc.diagnostic_path}" if exc.diagnostic_path else ""
        print(f"FAIL: {exc}{diag}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
