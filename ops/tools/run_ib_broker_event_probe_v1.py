#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
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


REQUIRED_EVENT_TYPES = {"nextValidId", "currentTime", "managedAccounts", "accountSummary", "accountSummaryEnd", "positionEnd"}
BLOCKERS = {
    "IB_SOCKET_CONNECT_FAILED",
    "IB_CLIENT_ID_IN_USE",
    "IB_ACCOUNT_MISMATCH",
    "IB_MANAGED_ACCOUNTS_MISSING",
    "IB_ACCOUNT_UPDATES_MISSING",
    "IB_POSITIONS_MISSING",
    "IB_OPEN_ORDERS_MISSING",
    "IB_EVENT_TIMEOUT",
    "IB_PERMISSION_DENIED",
    "IB_MARKET_DATA_UNAVAILABLE",
    "IB_UNKNOWN_HANDSHAKE_FAILURE",
}


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n", encoding="utf-8")


def probe_artifact_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "ib_broker_event_probe_v1"
        / str(day_utc).strip()
        / "ib_broker_event_probe.v1.json"
    ).resolve()


@dataclass(frozen=True)
class ProbeConfig:
    day_utc: str
    environment: str
    expected_account: str
    host: str
    port: int
    client_id: int
    timeout_seconds: float
    request_open_orders: bool


def _event(event_type: str, **fields: Any) -> dict[str, Any]:
    return {"event_type": event_type, "received_utc": _now_iso(), **fields}


class _IbProbeClient:  # pragma: no cover - exercised by live verification, pure evaluator is unit-tested
    def __init__(self, config: ProbeConfig) -> None:
        try:
            from ibapi.client import EClient  # type: ignore
            from ibapi.wrapper import EWrapper  # type: ignore
        except Exception as exc:
            raise RuntimeError(f"ibapi_import_failed:{exc!r}") from exc

        class Client(EWrapper, EClient):  # type: ignore[misc, valid-type]
            def __init__(self, outer: "_IbProbeClient") -> None:
                EWrapper.__init__(self)
                EClient.__init__(self, wrapper=self)
                self.outer = outer

            def nextValidId(self, orderId: int) -> None:  # noqa: N802
                self.outer.record("nextValidId", order_id=int(orderId))
                self.outer.request_after_handshake()

            def currentTime(self, time_: int) -> None:  # noqa: N802
                self.outer.record("currentTime", ib_time=int(time_))

            def managedAccounts(self, accountsList: str) -> None:  # noqa: N802
                accounts = [item.strip() for item in str(accountsList or "").split(",") if item.strip()]
                self.outer.record("managedAccounts", accounts=accounts)

            def accountSummary(self, reqId: int, account: str, tag: str, value: str, currency: str) -> None:  # noqa: N802
                self.outer.record("accountSummary", req_id=int(reqId), account=str(account), tag=str(tag), value=str(value), currency=str(currency))

            def accountSummaryEnd(self, reqId: int) -> None:  # noqa: N802
                self.outer.record("accountSummaryEnd", req_id=int(reqId))
                try:
                    self.cancelAccountSummary(reqId)
                except Exception:
                    pass

            def updateAccountValue(self, key: str, val: str, currency: str, accountName: str) -> None:  # noqa: N802
                self.outer.record("updateAccountValue", key=str(key), value=str(val), currency=str(currency), account=str(accountName))

            def updatePortfolio(self, contract, position, marketPrice: float, marketValue: float, averageCost: float, unrealizedPNL: float, realizedPNL: float, accountName: str) -> None:  # noqa: N802
                self.outer.record("updatePortfolio", account=str(accountName), contract=str(contract), position=str(position))

            def accountDownloadEnd(self, accountName: str) -> None:  # noqa: N802
                self.outer.record("accountDownloadEnd", account=str(accountName))

            def position(self, account: str, contract, position, avgCost: float) -> None:
                self.outer.record("position", account=str(account), contract=str(contract), position=str(position), avg_cost=float(avgCost))

            def positionEnd(self) -> None:  # noqa: N802
                self.outer.record("positionEnd")

            def openOrder(self, orderId: int, contract, order, orderState) -> None:  # noqa: N802
                self.outer.record("openOrder", order_id=int(orderId), contract=str(contract), order_state=str(orderState))

            def openOrderEnd(self) -> None:  # noqa: N802
                self.outer.record("openOrderEnd")

            def error(self, reqId: int, errorCode: int, errorString: str, advancedOrderRejectJson: str = "") -> None:  # noqa: N802
                self.outer.record("error", req_id=int(reqId), error_code=int(errorCode), error_string=str(errorString), advanced_order_reject_json=str(advancedOrderRejectJson))

        self.config = config
        self.events: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._requested = False
        self.client = Client(self)
        self.thread: threading.Thread | None = None

    def record(self, event_type: str, **fields: Any) -> None:
        with self._lock:
            self.events.append(_event(event_type, **fields))

    def request_after_handshake(self) -> None:
        if self._requested:
            return
        self._requested = True
        try:
            self.client.reqCurrentTime()
            self.record("reqCurrentTime")
        except Exception as exc:
            self.record("reqCurrentTime_error", error=repr(exc))
        try:
            self.client.reqManagedAccts()
            self.record("reqManagedAccounts")
        except Exception as exc:
            self.record("reqManagedAccounts_error", error=repr(exc))
        try:
            self.client.reqAccountSummary(9101, "All", "TotalCashValue,TotalCashBalance,NetLiquidation,AvailableFunds,ExcessLiquidity")
            self.record("reqAccountSummary", req_id=9101, group="All")
        except Exception as exc:
            self.record("reqAccountSummary_error", error=repr(exc))
        try:
            self.client.reqPositions()
            self.record("reqPositions")
        except Exception as exc:
            self.record("reqPositions_error", error=repr(exc))
        if self.config.request_open_orders:
            try:
                self.client.reqAllOpenOrders()
                self.record("reqAllOpenOrders")
            except Exception as exc:
                self.record("reqAllOpenOrders_error", error=repr(exc))

    def run(self) -> dict[str, Any]:
        started = _now_iso()
        connected = False
        connect_error = ""
        try:
            self.client.connect(self.config.host, int(self.config.port), int(self.config.client_id))
            connected = bool(self.client.isConnected())
            self.thread = threading.Thread(target=self.client.run, daemon=True)
            self.thread.start()
            deadline = time.monotonic() + float(self.config.timeout_seconds)
            while time.monotonic() < deadline:
                with self._lock:
                    event_types = {str(row.get("event_type") or "") for row in self.events}
                if REQUIRED_EVENT_TYPES.issubset(event_types):
                    break
                time.sleep(0.05)
        except Exception as exc:  # noqa: BLE001
            connect_error = repr(exc)
        finally:
            try:
                self.client.disconnect()
            except Exception:
                pass
        server_version = 0
        try:
            server_version = int(self.client.serverVersion())
        except Exception:
            server_version = 0
        with self._lock:
            events = list(self.events)
        return {
            "connected": connected,
            "connect_error": connect_error,
            "server_version": server_version,
            "events": events,
            "started_at_utc": started,
            "completed_at_utc": _now_iso(),
        }


def _event_types(events: list[dict[str, Any]]) -> set[str]:
    return {str(row.get("event_type") or "").strip() for row in events}


def _managed_accounts(events: list[dict[str, Any]]) -> list[str]:
    accounts: set[str] = set()
    for row in events:
        if row.get("event_type") == "managedAccounts":
            for account in row.get("accounts") or []:
                text = str(account or "").strip()
                if text:
                    accounts.add(text)
    return sorted(accounts)


def _account_summary_accounts(events: list[dict[str, Any]]) -> list[str]:
    accounts: set[str] = set()
    for row in events:
        if row.get("event_type") in {"accountSummary", "updateAccountValue", "updatePortfolio", "accountDownloadEnd"}:
            text = str(row.get("account") or "").strip()
            if text:
                accounts.add(text)
    return sorted(accounts)


def _error_codes(events: list[dict[str, Any]]) -> list[int]:
    out: list[int] = []
    for row in events:
        if row.get("event_type") == "error":
            try:
                out.append(int(row.get("error_code")))
            except Exception:
                continue
    return out


def evaluate_probe_v1(config: ProbeConfig, raw: dict[str, Any]) -> dict[str, Any]:
    events = [row for row in raw.get("events") or [] if isinstance(row, dict)]
    event_types = _event_types(events)
    managed = _managed_accounts(events)
    account_summary_accounts = _account_summary_accounts(events)
    errors = _error_codes(events)
    connected = bool(raw.get("connected") is True)
    blocker = ""

    if not connected:
        blocker = "IB_SOCKET_CONNECT_FAILED"
    elif 326 in errors or "client id is already in use" in json.dumps(events).lower():
        blocker = "IB_CLIENT_ID_IN_USE"
    elif any(code in {321, 322} for code in errors):
        blocker = "IB_PERMISSION_DENIED"
    elif "nextValidId" not in event_types:
        blocker = "IB_EVENT_TIMEOUT"
    elif not managed:
        blocker = "IB_MANAGED_ACCOUNTS_MISSING"
    elif config.expected_account not in managed:
        blocker = "IB_ACCOUNT_MISMATCH"
    elif config.expected_account not in account_summary_accounts:
        blocker = "IB_ACCOUNT_UPDATES_MISSING"
    elif "positionEnd" not in event_types:
        blocker = "IB_POSITIONS_MISSING"
    elif not REQUIRED_EVENT_TYPES.issubset(event_types):
        blocker = "IB_EVENT_TIMEOUT"

    status = "PASS" if not blocker else "BLOCKED"
    return {
        "schema_id": "ib_broker_event_probe",
        "schema_version": "v1",
        "day_utc": config.day_utc,
        "environment": config.environment,
        "status": status,
        "canonical_blocker": blocker,
        "expected_account": config.expected_account,
        "expected_host": config.host,
        "expected_port": int(config.port),
        "expected_client_id": int(config.client_id),
        "timeout_seconds": float(config.timeout_seconds),
        "freshness_window": "CURRENT_DAY_ONLY",
        "required_event_types": sorted(REQUIRED_EVENT_TYPES),
        "required_criteria": {
            "socket_connection": True,
            "nextValidId": True,
            "currentTime": True,
            "managedAccounts": True,
            "accountSummary_or_accountUpdates_for_expected_account": True,
            "positions_positionEnd": True,
            "openOrder_openOrderEnd": False,
            "market_data_tick": False,
            "historical_data_response": False,
        },
        "connection": {
            "connected": connected,
            "connect_error": str(raw.get("connect_error") or ""),
            "server_version": int(raw.get("server_version") or 0),
        },
        "observed": {
            "event_types": sorted(event_types),
            "managed_accounts": managed,
            "account_summary_accounts": account_summary_accounts,
            "error_codes": errors,
        },
        "events": events,
        "started_at_utc": str(raw.get("started_at_utc") or ""),
        "completed_at_utc": str(raw.get("completed_at_utc") or ""),
    }


def _resolve_config(day_utc: str, environment: str, timeout_seconds: float, request_open_orders: bool) -> tuple[ProbeConfig, Path]:
    ib_account = resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    profile = resolve_governed_paper_execution_profile(
        repo_root=REPO_ROOT,
        environment=environment,
        ib_account=ib_account,
        sleeve_id="PRIMARY",
    )
    truth_root = resolve_runtime_data_root().resolve() / "truth"
    config = ProbeConfig(
        day_utc=day_utc,
        environment=environment,
        expected_account=str(profile.ib_account),
        host=str(profile.host),
        port=int(profile.port),
        client_id=int(profile.client_id_observer),
        timeout_seconds=float(timeout_seconds),
        request_open_orders=bool(request_open_orders),
    )
    return config, truth_root


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_ib_broker_event_probe_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--timeout_seconds", type=float, default=12.0)
    parser.add_argument("--request_open_orders", default="NO", choices=["YES", "NO"])
    args = parser.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    environment = str(args.environment).strip().upper()
    config, truth_root = _resolve_config(
        day_utc,
        environment,
        float(args.timeout_seconds),
        str(args.request_open_orders).strip().upper() == "YES",
    )
    path = probe_artifact_path_v1(truth_root=truth_root, day_utc=day_utc)
    try:
        raw = _IbProbeClient(config).run()
    except Exception as exc:  # noqa: BLE001
        raw = {
            "connected": False,
            "connect_error": repr(exc),
            "server_version": 0,
            "events": [],
            "started_at_utc": _now_iso(),
            "completed_at_utc": _now_iso(),
        }
    payload = evaluate_probe_v1(config, raw)
    payload["path"] = str(path)
    _write_json(path, payload)
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "path": str(path)}, sort_keys=True))
    return 0 if payload["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
