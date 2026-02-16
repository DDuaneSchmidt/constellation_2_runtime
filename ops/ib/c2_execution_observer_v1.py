#!/usr/bin/env python3
"""
C2 IB Execution Observer (PAPER) — Canonical BROKER_EVENT_RAW writer
clientId: 79 (default)

Writes append-only JSONL:
constellation_2/runtime/truth/execution_evidence_v2/broker_events/<DAY>/broker_event_log.v1.jsonl

Contract:
- single-writer required for monotonic sequence_number
- append-only; fsync each record
- schema_id="BROKER_EVENT_RAW", schema_version=1
- captures orderStatus, execDetails, commissionReport, openOrder, error, connectionClosed, nextValidId
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.execution import Execution, ExecutionFilter
from ibapi.commission_report import CommissionReport


def utc_now_z() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def day_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).date().isoformat()


def canonical_dumps(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def tail_last_sequence_number(path: Path) -> int:
    if not path.exists() or path.stat().st_size == 0:
        return 0
    chunk = 256 * 1024
    size = path.stat().st_size
    start = max(0, size - chunk)
    with path.open("rb") as f:
        f.seek(start)
        data = f.read()
    text = data.decode("utf-8", errors="replace")
    lines = [ln for ln in text.splitlines() if ln.strip()]
    for ln in reversed(lines):
        try:
            o = json.loads(ln)
            if isinstance(o, dict) and "sequence_number" in o:
                s = int(o["sequence_number"])
                if s >= 0:
                    return s
        except Exception:
            continue
    return 0


class JsonlRawWriter:
    def __init__(self, log_path: Path, broker: Dict[str, Any]) -> None:
        self.log_path = log_path
        self.broker = broker
        self.sequence_number = tail_last_sequence_number(log_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        self.fh = log_path.open("a", encoding="utf-8", buffering=1)

    def close(self) -> None:
        try:
            self.fh.flush()
            os.fsync(self.fh.fileno())
        except Exception:
            pass
        try:
            self.fh.close()
        except Exception:
            pass

    def write_raw(self, event_type: str, ib_args: Any) -> None:
        """
        Record shape matches existing listener:
        {
          "schema_id":"BROKER_EVENT_RAW",
          "schema_version":1,
          "received_utc":"...",
          "sequence_number":N,
          "broker":{...},
          "event_type":"...",
          "ib_fields":{"args":[{"value":"..."}]},
          "sha256":"..."
        }
        """
        self.sequence_number += 1

        # Normalize ib_fields.args into list of {"value": "<string>"} (matches your existing log)
        args_list = []
        if isinstance(ib_args, list):
            for a in ib_args:
                args_list.append({"value": str(a)})
        else:
            args_list.append({"value": str(ib_args)})

        rec_wo_sha = {
            "broker": self.broker,
            "event_type": event_type,
            "ib_fields": {"args": args_list},
            "received_utc": utc_now_z(),
            "schema_id": "BROKER_EVENT_RAW",
            "schema_version": 1,
            "sequence_number": self.sequence_number,
        }

        # sha256 over canonical json WITHOUT sha256 field
        canon = canonical_dumps(rec_wo_sha)
        rec = dict(rec_wo_sha)
        rec["sha256"] = sha256_hex(canon)

        line = canonical_dumps(rec)
        self.fh.write(line + "\n")
        self.fh.flush()
        os.fsync(self.fh.fileno())


class Observer(EWrapper, EClient):
    def __init__(self, writer: JsonlRawWriter, poll_seconds: int) -> None:
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
        self.writer = writer
        self.poll_seconds = poll_seconds
        self._last_poll = 0.0

    def _poll(self) -> None:
        now = time.monotonic()
        if now - self._last_poll < float(self.poll_seconds):
            return
        self._last_poll = now
        try:
            self.reqAllOpenOrders()
            self.writer.write_raw("poll_reqAllOpenOrders", ["reqAllOpenOrders()"])
        except Exception as e:
            self.writer.write_raw("poll_reqAllOpenOrders_error", [repr(e)])
        try:
            flt = ExecutionFilter()
            self.reqExecutions(9002, flt)
            self.writer.write_raw("poll_reqExecutions", ["reqExecutions(reqId=9002, ExecutionFilter())"])
        except Exception as e:
            self.writer.write_raw("poll_reqExecutions_error", [repr(e)])

    # ---- callbacks ----

    def nextValidId(self, orderId: int) -> None:
        self.writer.write_raw("nextValidId", [f"orderId={orderId}"])
        try:
            self.reqAllOpenOrders()
            self.writer.write_raw("reqAllOpenOrders", ["reqAllOpenOrders()"])
        except Exception as e:
            self.writer.write_raw("reqAllOpenOrders_error", [repr(e)])
        try:
            flt = ExecutionFilter()
            self.reqExecutions(9001, flt)
            self.writer.write_raw("reqExecutions", ["reqExecutions(reqId=9001, ExecutionFilter())"])
        except Exception as e:
            self.writer.write_raw("reqExecutions_error", [repr(e)])

    def connectionClosed(self) -> None:
        self.writer.write_raw("connectionClosed", ["connectionClosed()"])

    def error(self, reqId: int, errorCode: int, errorString: str, advancedOrderRejectJson: str = "") -> None:
        self.writer.write_raw(
            "error",
            [f"reqId={reqId}", f"errorCode={errorCode}", f"errorString={errorString}", f"advancedOrderRejectJson={advancedOrderRejectJson}"],
        )

    def openOrder(self, orderId: int, contract: Contract, order: Order, orderState) -> None:
        self.writer.write_raw(
            "openOrder",
            [
                f"orderId={orderId}",
                f"contract={contract}",
                f"order={order}",
                f"orderState={getattr(orderState,'__dict__',str(orderState))}",
            ],
        )

    def openOrderEnd(self) -> None:
        self.writer.write_raw("openOrderEnd", ["openOrderEnd()"])

    def orderStatus(
        self,
        orderId: int,
        status: str,
        filled: float,
        remaining: float,
        avgFillPrice: float,
        permId: int,
        parentId: int,
        lastFillPrice: float,
        clientId: int,
        whyHeld: str,
        mktCapPrice: float,
    ) -> None:
        self.writer.write_raw(
            "orderStatus",
            [
                f"orderId={orderId}",
                f"status={status}",
                f"filled={filled}",
                f"remaining={remaining}",
                f"avgFillPrice={avgFillPrice}",
                f"permId={permId}",
                f"parentId={parentId}",
                f"lastFillPrice={lastFillPrice}",
                f"clientId={clientId}",
                f"whyHeld={whyHeld}",
                f"mktCapPrice={mktCapPrice}",
            ],
        )

    def execDetails(self, reqId: int, contract: Contract, execution: Execution) -> None:
        self.writer.write_raw(
            "execDetails",
            [
                f"reqId={reqId}",
                f"contract={contract}",
                f"execution={execution}",
            ],
        )

    def execDetailsEnd(self, reqId: int) -> None:
        self.writer.write_raw("execDetailsEnd", [f"reqId={reqId}"])

    def commissionReport(self, commissionReport: CommissionReport) -> None:
        self.writer.write_raw("commissionReport", [f"commissionReport={commissionReport}"])


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=4002)
    p.add_argument("--client-id", type=int, default=79)
    p.add_argument("--poll-seconds", type=int, default=10)
    p.add_argument(
        "--log-root",
        default="constellation_2/runtime/truth/execution_evidence_v1/broker_events",
    )
    p.add_argument("--environment", default="PAPER")
    p.add_argument("--day-utc", default="", help="Optional override YYYY-MM-DD. If set, writes under that day dir.")

    return p.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path.cwd().resolve()
    log_root = (repo_root / args.log_root).resolve()

    # Fail-closed: must be under repo
    try:
        log_root.relative_to(repo_root)
    except Exception:
        print(f"FATAL: log_root not under repo: {log_root}", file=sys.stderr)
        return 2

    d_override = str(getattr(args, "day_utc", "") or "").strip()
    if d_override != "":
        if len(d_override) != 10 or d_override[4] != "-" or d_override[7] != "-":
            print(f"FATAL: BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d_override!r}", file=sys.stderr)
            return 2
        day_dir = log_root / d_override
    else:
        day_dir = log_root / day_utc()


    log_path = day_dir / "broker_event_log.v1.jsonl"

    broker = {"client_id": int(args.client_id), "environment": str(args.environment), "name": "INTERACTIVE_BROKERS"}
    writer = JsonlRawWriter(log_path=log_path, broker=broker)

    app = Observer(writer=writer, poll_seconds=int(args.poll_seconds))

    stopping = {"stop": False}

    def _sig(_signum, _frame) -> None:
        stopping["stop"] = True
        try:
            writer.write_raw("signal", [f"signum={_signum}"])
        except Exception:
            pass
        try:
            app.disconnect()
        except Exception:
            pass

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    writer.write_raw("starting", [f"host={args.host}", f"port={args.port}", f"clientId={args.client_id}"])

    try:
        app.connect(args.host, int(args.port), clientId=int(args.client_id))
    except Exception as e:
        writer.write_raw("connect_failed", [repr(e)])
        writer.close()
        return 3

    # Run in a loop so we can poll without extra threads
    # EClient.run() is blocking; we do lightweight message processing manually.
    try:
        while not stopping["stop"]:
            app._poll()  # request executions/open orders periodically
            time.sleep(0.2)
            # Process inbound messages
            try:
                app.run()
                break
            except Exception:
                # If run() returns quickly or throws, continue loop; errors are captured via callbacks where possible.
                time.sleep(0.2)
                continue
    finally:
        writer.write_raw("stopped", ["stopped()"])
        writer.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
