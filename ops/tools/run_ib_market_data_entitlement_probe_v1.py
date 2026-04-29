#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_execution_authority_v1 import resolve_governed_paper_execution_profile
from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from constellation_2.common.runtime_contract_v1 import resolve_runtime_data_root
from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry

SCHEMA_VERSION = "ib_market_data_entitlement_probe.v1"
ERROR_MAP = {
    10089: "API_MARKET_DATA_SUBSCRIPTION_MISSING",
    10091: "PARTIAL_MARKET_DATA_SUBSCRIPTION_MISSING",
    10167: "MARKET_DATA_NOT_SUBSCRIBED_DELAYED_AVAILABLE",
}
LIVE_DATA_TYPE = 1
DIAGNOSTIC_DATA_TYPES = (2, 3, 4)
DEFAULT_PROBE_CLIENT_ID = 181


@dataclass(frozen=True)
class EntitlementProbeConfig:
    day_utc: str
    environment: str
    symbol: str
    host: str
    port: int
    client_id: int
    account: str
    timeout_seconds: float


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n", encoding="utf-8")


def entitlement_probe_path_v1(*, truth_root: Path, day_utc: str, symbol: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "ib_market_data_entitlement_probe_v1"
        / str(day_utc).strip()
        / f"{str(symbol).strip().upper()}_ib_market_data_entitlement_probe.v1.json"
    ).resolve()


def _stock_contract(symbol: str) -> Any:
    from ibapi.contract import Contract  # type: ignore

    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"
    return contract


def _option_contract(symbol: str) -> Any:
    from ibapi.contract import Contract  # type: ignore

    contract = Contract()
    contract.symbol = symbol
    contract.secType = "OPT"
    contract.exchange = "SMART"
    contract.currency = "USD"
    # Diagnostic-only: SPY weekly near-the-money contract qualification/quote request.
    # If the concrete contract is unavailable, entitlement errors are still captured.
    contract.lastTradeDateOrContractMonth = ""
    contract.right = "CALL"
    contract.multiplier = "100"
    return contract


class _IbMarketDataProbeClient:  # pragma: no cover - live IB path; pure evaluator is unit-tested
    def __init__(self, config: EntitlementProbeConfig) -> None:
        try:
            from ibapi.client import EClient  # type: ignore
            from ibapi.wrapper import EWrapper  # type: ignore
        except Exception as exc:
            raise RuntimeError(f"ibapi_import_failed:{type(exc).__name__}:{exc}") from exc

        class Client(EWrapper, EClient):  # type: ignore[misc, valid-type]
            def __init__(self, outer: "_IbMarketDataProbeClient") -> None:
                EWrapper.__init__(self)
                EClient.__init__(self, wrapper=self)
                self.outer = outer

            def nextValidId(self, orderId: int) -> None:  # noqa: N802
                self.outer.next_req_id = int(orderId)
                self.outer.connected.set()

            def tickPrice(self, reqId: int, tickType: int, price: float, attrib: Any) -> None:  # noqa: N802
                self.outer.record_tick(reqId, "price", tick_type=int(tickType), value=price)

            def tickSize(self, reqId: int, tickType: int, size: int) -> None:  # noqa: N802
                self.outer.record_tick(reqId, "size", tick_type=int(tickType), value=size)

            def marketDataType(self, reqId: int, marketDataType: int) -> None:  # noqa: N802
                self.outer.record_tick(reqId, "marketDataType", market_data_type=int(marketDataType))

            def tickSnapshotEnd(self, reqId: int) -> None:  # noqa: N802
                self.outer.done.setdefault(int(reqId), threading.Event()).set()

            def error(self, reqId: int, errorCode: int, errorString: str, advancedOrderRejectJson: str = "") -> None:  # noqa: N802
                self.outer.errors.append(
                    {
                        "observed_at_utc": _now_iso(),
                        "req_id": int(reqId),
                        "error_code": int(errorCode),
                        "error_message": str(errorString or ""),
                        "mapped_reason": ERROR_MAP.get(int(errorCode), ""),
                    }
                )
                self.outer.done.setdefault(int(reqId), threading.Event()).set()

        self.config = config
        self.client = Client(self)
        self.connected = threading.Event()
        self.next_req_id = 0
        self.ticks: dict[int, list[dict[str, Any]]] = {}
        self.done: dict[int, threading.Event] = {}
        self.errors: list[dict[str, Any]] = []

    def _alloc_req_id(self) -> int:
        self.next_req_id += 1
        return int(self.next_req_id)

    def record_tick(self, req_id: int, tick_kind: str, **fields: Any) -> None:
        self.ticks.setdefault(int(req_id), []).append({"observed_at_utc": _now_iso(), "tick_kind": tick_kind, **fields})

    def _request_snapshot(self, *, contract: Any, data_type: int, label: str) -> dict[str, Any]:
        self.client.reqMarketDataType(int(data_type))
        req_id = self._alloc_req_id()
        self.done[req_id] = threading.Event()
        start_error_count = len(self.errors)
        self.client.reqMktData(req_id, contract, "", True, False, [])
        completed = self.done[req_id].wait(float(self.config.timeout_seconds))
        try:
            self.client.cancelMktData(req_id)
        except Exception:
            pass
        ticks = list(self.ticks.get(req_id) or [])
        errors = [row for row in self.errors[start_error_count:] if int(row.get("req_id") or -1) in {-1, req_id}]
        has_bid_ask = any(int(row.get("tick_type") or -1) in {1, 2, 66, 67} for row in ticks)
        has_any_price = any(row.get("tick_kind") == "price" and row.get("value") not in (None, "", -1) for row in ticks)
        return {
            "label": label,
            "req_id": req_id,
            "requested_market_data_type": int(data_type),
            "snapshot_completed": bool(completed),
            "has_any_price": bool(has_any_price),
            "has_bid_ask": bool(has_bid_ask),
            "ticks": ticks[:20],
            "errors": errors,
        }

    def run(self) -> dict[str, Any]:
        started = _now_iso()
        thread: threading.Thread | None = None
        connected = False
        connect_error = ""
        requests: list[dict[str, Any]] = []
        try:
            self.client.connect(self.config.host, int(self.config.port), int(self.config.client_id))
            thread = threading.Thread(target=self.client.run, daemon=True)
            thread.start()
            connected = self.connected.wait(float(self.config.timeout_seconds))
            if not connected:
                connect_error = "IB_CONNECT_TIMEOUT"
            else:
                stock = _stock_contract(self.config.symbol)
                option = _option_contract(self.config.symbol)
                for data_type in (LIVE_DATA_TYPE, *DIAGNOSTIC_DATA_TYPES):
                    requests.append(self._request_snapshot(contract=stock, data_type=data_type, label="underlying"))
                    requests.append(self._request_snapshot(contract=option, data_type=data_type, label="option_bid_ask"))
        except Exception as exc:  # noqa: BLE001
            connect_error = f"{type(exc).__name__}:{exc}"
        finally:
            try:
                self.client.disconnect()
            except Exception:
                pass
            if thread is not None:
                thread.join(timeout=1.0)
        return {
            "connected": connected,
            "connect_error": connect_error,
            "requests": requests,
            "started_at_utc": started,
            "completed_at_utc": _now_iso(),
        }


def _error_rows(raw: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for request in raw.get("requests") or []:
        if not isinstance(request, dict):
            continue
        for err in request.get("errors") or []:
            if isinstance(err, dict):
                rows.append(err)
    return rows


def _codes(raw: dict[str, Any]) -> list[int]:
    out: list[int] = []
    for row in _error_rows(raw):
        try:
            code = int(row.get("error_code"))
        except Exception:
            continue
        out.append(code)
    return sorted(set(out))


def _has_live_quotes(raw: dict[str, Any]) -> bool:
    for row in raw.get("requests") or []:
        if not isinstance(row, dict):
            continue
        if int(row.get("requested_market_data_type") or 0) == LIVE_DATA_TYPE and row.get("has_any_price") is True:
            return True
    return False


def _has_delayed_quotes(raw: dict[str, Any]) -> bool:
    for row in raw.get("requests") or []:
        if not isinstance(row, dict):
            continue
        if int(row.get("requested_market_data_type") or 0) in DIAGNOSTIC_DATA_TYPES and row.get("has_any_price") is True:
            return True
    return 10167 in _codes(raw)


def evaluate_entitlement_probe_v1(config: EntitlementProbeConfig, raw: dict[str, Any]) -> dict[str, Any]:
    connected = bool(raw.get("connected") is True)
    codes = _codes(raw)
    live_available = _has_live_quotes(raw) and not any(code in {10089, 10091} for code in codes)
    delayed_available = _has_delayed_quotes(raw)
    blocker = ""
    if not connected:
        blocker = "IB_MARKET_DATA_CAPABILITY_UNKNOWN"
    elif any(code in {10089, 10091, 10167} for code in codes) and not live_available:
        blocker = "OPTIONS_MARKET_DATA_PERMISSION_DENIED"
    elif not live_available:
        blocker = "IB_MARKET_DATA_CAPABILITY_UNKNOWN"
    status = "PASS" if not blocker else "BLOCKED"
    return {
        "schema_id": "ib_market_data_entitlement_probe",
        "schema_version": SCHEMA_VERSION,
        "day_utc": config.day_utc,
        "environment": config.environment,
        "symbol": config.symbol,
        "status": status,
        "canonical_blocker": blocker,
        "provider": "IBKR",
        "account": config.account,
        "host": config.host,
        "port": int(config.port),
        "client_id": int(config.client_id),
        "tested_data_types": [
            {"market_data_type": 1, "name": "LIVE", "readiness_eligible": True},
            {"market_data_type": 2, "name": "FROZEN", "readiness_eligible": False},
            {"market_data_type": 3, "name": "DELAYED", "readiness_eligible": False},
            {"market_data_type": 4, "name": "DELAYED_FROZEN", "readiness_eligible": False},
        ],
        "live_data_available": bool(live_available),
        "delayed_data_available": bool(delayed_available),
        "delayed_data_accepted_by_policy": False,
        "ib_error_codes": codes,
        "ib_errors": _error_rows(raw),
        "error_mapping": [
            {"ib_error_code": code, "meaning": ERROR_MAP.get(code, "UNMAPPED_IB_ERROR")}
            for code in codes
        ],
        "requests": raw.get("requests") if isinstance(raw.get("requests"), list) else [],
        "connection": {
            "connected": connected,
            "connect_error": str(raw.get("connect_error") or ""),
        },
        "operator_next_action": (
            "Enable IBKR Client Portal market-data subscriptions and API market-data access for SPY underlying and options for the logged-in trading user/account."
            if blocker == "OPTIONS_MARKET_DATA_PERMISSION_DENIED"
            else ("Restore IBKR API market-data connectivity, then rerun the entitlement probe." if blocker else "")
        ),
        "started_at_utc": str(raw.get("started_at_utc") or ""),
        "completed_at_utc": str(raw.get("completed_at_utc") or _now_iso()),
    }


def _resolve_config(day_utc: str, environment: str, symbol: str, timeout_seconds: float) -> tuple[EntitlementProbeConfig, Path]:
    account = resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    profile = resolve_governed_paper_execution_profile(
        repo_root=REPO_ROOT,
        environment=environment,
        ib_account=account,
        sleeve_id="PRIMARY",
    )
    client_id = int(os.environ.get("C2_IB_MARKET_DATA_PROBE_CLIENT_ID") or DEFAULT_PROBE_CLIENT_ID)
    truth_root = resolve_runtime_data_root().resolve() / "truth"
    return (
        EntitlementProbeConfig(
            day_utc=day_utc,
            environment=environment,
            symbol=str(symbol).strip().upper(),
            host=str(profile.host),
            port=int(profile.port),
            client_id=client_id,
            account=str(profile.ib_account),
            timeout_seconds=float(timeout_seconds),
        ),
        truth_root,
    )


def run_entitlement_probe_v1(day_utc: str, environment: str, symbol: str, timeout_seconds: float = 6.0) -> tuple[Path, dict[str, Any]]:
    config, truth_root = _resolve_config(day_utc, environment, symbol, timeout_seconds)
    try:
        raw = _IbMarketDataProbeClient(config).run()
    except Exception as exc:  # noqa: BLE001
        raw = {
            "connected": False,
            "connect_error": f"{type(exc).__name__}:{exc}",
            "requests": [],
            "started_at_utc": _now_iso(),
            "completed_at_utc": _now_iso(),
        }
    payload = evaluate_entitlement_probe_v1(config, raw)
    path = entitlement_probe_path_v1(truth_root=truth_root, day_utc=day_utc, symbol=symbol)
    payload["path"] = str(path)
    _write_json(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_ib_market_data_entitlement_probe_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--symbol", default="SPY")
    parser.add_argument("--timeout_seconds", type=float, default=6.0)
    args = parser.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    environment = str(args.environment or "PAPER").strip().upper()
    symbol = str(args.symbol or "SPY").strip().upper()
    path, payload = run_entitlement_probe_v1(day_utc, environment, symbol, float(args.timeout_seconds))
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "path": str(path)}, sort_keys=True))
    return 0 if payload.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
