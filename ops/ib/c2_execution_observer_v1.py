#!/usr/bin/env python3
"""
C2 IB Execution Observer (PAPER) — Core 1 canonical raw-evidence writer
clientId: 79 (default)

Primary append-only JSONL:
/home/node/constellation_runtime_data/truth_sleeves/<sleeve_id>/<mode>/broker_fact_spine_v1/raw_journal/<DAY>/broker_raw_evidence_envelope.v1.jsonl

Optional diagnostic mirror:
/home/node/constellation_runtime_data/truth_sleeves/<sleeve_id>/<mode>/execution_evidence_v1/broker_events/<DAY>/broker_event_log.v1.jsonl

Contract:
- single-writer required for monotonic canonical raw-journal sequence_number
- append-only; fsync each record
- canonical raw journal must write first
- BROKER_EVENT_RAW mirror is non-canonical diagnostic input only
- captures orderStatus, execDetails, commissionReport, openOrder, position, accountSummary, error, connectionClosed, nextValidId
"""

from __future__ import annotations

import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

if not (_REPO_ROOT_FROM_FILE / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT_FROM_FILE}")
if not (_REPO_ROOT_FROM_FILE / "governance").exists():
    raise SystemExit(f"FATAL: repo_root_missing_governance: derived={_REPO_ROOT_FROM_FILE}")

import argparse
import datetime as dt
import hashlib
import json
import os
import signal
import threading
import time
import uuid
from typing import Any, Dict, Optional

from constellation_2.common.broker_fact_spine_v1 import BrokerRawEvidenceJournalWriterV1
from constellation_2.common.execution_identity_binding_v1 import (
    resolve_governed_execution_identity_v1,
)
from constellation_2.common.paper_execution_authority_v1 import (
    resolve_governed_paper_execution_roots,
)
from constellation_2.common.truth_root_v1 import resolve_truth_root
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
        self.sequence_number += 1
        record = build_legacy_raw_record(
            broker=self.broker,
            event_type=event_type,
            ib_args=ib_args,
            sequence_number=self.sequence_number,
        )
        self.write_record(record)

    def write_record(self, record: Dict[str, Any]) -> None:
        line = canonical_dumps(record)
        self.fh.write(line + "\n")
        self.fh.flush()
        os.fsync(self.fh.fileno())


def build_legacy_raw_record(
    *,
    broker: Dict[str, Any],
    event_type: str,
    ib_args: Any,
    sequence_number: int,
) -> Dict[str, Any]:
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

    args_list = []
    if isinstance(ib_args, list):
        for a in ib_args:
            args_list.append({"value": str(a)})
    else:
        args_list.append({"value": str(ib_args)})
    rec_wo_sha = {
        "broker": broker,
        "event_type": event_type,
        "ib_fields": {"args": args_list},
        "received_utc": utc_now_z(),
        "schema_id": "BROKER_EVENT_RAW",
        "schema_version": 1,
        "sequence_number": int(sequence_number),
    }
    canon = canonical_dumps(rec_wo_sha)
    rec = dict(rec_wo_sha)
    rec["sha256"] = sha256_hex(canon)
    return rec


class ObserverFanoutWriter:
    def __init__(
        self,
        *,
        repo_root: Path,
        execution_root_path: Path,
        environment: str,
        sleeve_id: str,
        canonical_writer: BrokerRawEvidenceJournalWriterV1,
        broker: Dict[str, Any],
        legacy_writer: JsonlRawWriter | None,
        legacy_log_root: Path | None,
        fixed_day_utc: str,
    ) -> None:
        self.repo_root = repo_root
        self.execution_root_path = execution_root_path
        self.environment = str(environment).strip().upper()
        self.sleeve_id = str(sleeve_id).strip().upper()
        self.canonical_writer = canonical_writer
        self.legacy_writer = legacy_writer
        self.broker = dict(broker)
        self.legacy_log_root = legacy_log_root
        self.fixed_day_utc = str(fixed_day_utc or "").strip()
        self._current_day_utc = self.fixed_day_utc or day_utc()
        self._legacy_sequence_number = (
            int(legacy_writer.sequence_number) if legacy_writer is not None else 0
        )
        self._lock = threading.Lock()

    def close(self) -> None:
        with self._lock:
            self.canonical_writer.close()
            if self.legacy_writer is not None:
                self.legacy_writer.close()

    def _reopen_for_day_locked(self, next_day_utc: str) -> None:
        self.canonical_writer.close()
        if self.legacy_writer is not None:
            self.legacy_writer.close()
        self.canonical_writer = BrokerRawEvidenceJournalWriterV1(
            repo_root=self.repo_root,
            execution_root_path=self.execution_root_path,
            day_utc=next_day_utc,
            environment=self.environment,
            sleeve_id=self.sleeve_id,
            source_adapter_name="ib_execution_observer_v1",
            source_session_id=(
                f"ib_observer:{self.environment}:{self.sleeve_id}:"
                f"{self.broker['client_id']}:{next_day_utc}:{uuid.uuid4().hex[:12]}"
            ),
            source_path=f"observer://interactive_brokers/{self.sleeve_id}/{self.environment}",
        )
        if self.legacy_log_root is not None:
            self.legacy_writer = JsonlRawWriter(
                log_path=(self.legacy_log_root / next_day_utc / "broker_event_log.v1.jsonl"),
                broker=self.broker,
            )
            self._legacy_sequence_number = int(self.legacy_writer.sequence_number)
        else:
            self.legacy_writer = None
            self._legacy_sequence_number = 0
        self._current_day_utc = next_day_utc

    def _rotate_if_needed_locked(self) -> None:
        if self.fixed_day_utc:
            return
        next_day_utc = day_utc()
        if next_day_utc == self._current_day_utc:
            return
        self._reopen_for_day_locked(next_day_utc)

    def write_raw(self, event_type: str, ib_args: Any) -> None:
        with self._lock:
            self._rotate_if_needed_locked()
            self._legacy_sequence_number += 1
            record = build_legacy_raw_record(
                broker=self.broker,
                event_type=event_type,
                ib_args=ib_args,
                sequence_number=self._legacy_sequence_number,
            )
            self.canonical_writer.write_payload(record)
            if self.legacy_writer is not None:
                self.legacy_writer.sequence_number = self._legacy_sequence_number
                self.legacy_writer.write_record(record)


class Observer(EWrapper, EClient):
    def __init__(self, writer: ObserverFanoutWriter, poll_seconds: int) -> None:
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
        self.writer = writer
        self.poll_seconds = poll_seconds
        self._last_poll = 0.0
        self.handshake_seen = False
        self.open_orders_complete = False
        self.executions_complete = False
        self.positions_complete = False
        self.account_summary_complete = False

    def bootstrap_capture_complete(self) -> bool:
        return (
            self.handshake_seen
            and self.open_orders_complete
            and self.executions_complete
            and self.positions_complete
            and self.account_summary_complete
        )

    def _request_bootstrap_snapshots(self) -> None:
        try:
            self.reqCurrentTime()
            self.writer.write_raw("reqCurrentTime", ["reqCurrentTime()"])
        except Exception as e:
            self.writer.write_raw("reqCurrentTime_error", [repr(e)])
        try:
            self.reqManagedAccts()
            self.writer.write_raw("reqManagedAccounts", ["reqManagedAccts()"])
        except Exception as e:
            self.writer.write_raw("reqManagedAccounts_error", [repr(e)])
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
        try:
            self.reqPositions()
            self.writer.write_raw("reqPositions", ["reqPositions()"])
        except Exception as e:
            self.writer.write_raw("reqPositions_error", [repr(e)])
        try:
            self.reqAccountSummary(9003, "All", "TotalCashValue,TotalCashBalance,NetLiquidation,AvailableFunds,ExcessLiquidity")
            self.writer.write_raw(
                "reqAccountSummary",
                ["reqId=9003", "groupName=All", "tags=TotalCashValue,TotalCashBalance,NetLiquidation,AvailableFunds,ExcessLiquidity"],
            )
        except Exception as e:
            self.writer.write_raw("reqAccountSummary_error", [repr(e)])

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
        self.handshake_seen = True
        self.writer.write_raw("nextValidId", [f"orderId={orderId}"])
        self._request_bootstrap_snapshots()

    def connectionClosed(self) -> None:
        self.writer.write_raw("connectionClosed", ["connectionClosed()"])

    def currentTime(self, time_: int) -> None:  # noqa: N802
        self.writer.write_raw("currentTime", [f"time={time_}"])

    def managedAccounts(self, accountsList: str) -> None:  # noqa: N802
        self.writer.write_raw("managedAccounts", [f"accounts={accountsList}"])

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
        self.open_orders_complete = True
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
        self.executions_complete = True
        self.writer.write_raw("execDetailsEnd", [f"reqId={reqId}"])

    def commissionReport(self, commissionReport: CommissionReport) -> None:
        self.writer.write_raw("commissionReport", [f"commissionReport={commissionReport}"])

    def position(self, account: str, contract: Contract, position, avgCost: float) -> None:
        self.writer.write_raw(
            "position",
            [
                f"account={account}",
                f"contract={contract}",
                f"position={position}",
                f"avgCost={avgCost}",
            ],
        )

    def positionEnd(self) -> None:
        self.positions_complete = True
        self.writer.write_raw("positionEnd", ["positionEnd()"])

    def accountSummary(self, reqId: int, account: str, tag: str, value: str, currency: str) -> None:
        self.writer.write_raw(
            "accountSummary",
            [
                f"reqId={reqId}",
                f"account={account}",
                f"tag={tag}",
                f"value={value}",
                f"currency={currency}",
            ],
        )

    def accountSummaryEnd(self, reqId: int) -> None:
        self.account_summary_complete = True
        self.writer.write_raw("accountSummaryEnd", [f"reqId={reqId}"])
        try:
            self.cancelAccountSummary(reqId)
        except Exception:
            pass


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default="", help="Optional authoritative repo root override.")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=4002)
    p.add_argument("--client-id", type=int, default=79)
    p.add_argument("--poll-seconds", type=int, default=10)
    p.add_argument("--truth_root", default="", help="Optional canonical truth root override.")
    p.add_argument("--sleeve-id", default="PRIMARY")
    p.add_argument(
        "--log-root",
        default="",
    )
    p.add_argument("--disable-legacy-log", action="store_true")
    p.add_argument("--environment", default="PAPER")
    p.add_argument("--day-utc", default="", help="Optional override YYYY-MM-DD. If set, writes under that day dir.")
    p.add_argument("--bootstrap-handshake-only", action="store_true", help="Connect only long enough to capture handshake evidence.")
    p.add_argument("--handshake-timeout-seconds", type=int, default=15, help="Timeout for bootstrap handshake-only mode.")

    return p.parse_args()


def _resolve_log_root(*, repo_root: Path, truth_root_value: str, log_root_value: str) -> Path:
    log_root_raw = str(log_root_value or "").strip()
    if log_root_raw:
        raise SystemExit("FATAL: --log-root override is unsupported; canonical truth root only")
    truth_root_raw = str(truth_root_value or "").strip()
    if truth_root_raw:
        return (Path(truth_root_raw).resolve() / "execution_evidence_v1" / "broker_events").resolve()
    return (resolve_truth_root(repo_root=repo_root).resolve() / "execution_evidence_v1" / "broker_events").resolve()


def _run_bootstrap_handshake_loop(
    *,
    app: Observer,
    writer: ObserverFanoutWriter,
    timeout_seconds: int,
    stopping: Dict[str, bool],
) -> int:
    timeout_seconds = max(int(timeout_seconds), 1)

    def _watchdog() -> None:
        deadline = time.monotonic() + float(timeout_seconds)
        while not stopping["stop"] and time.monotonic() < deadline:
            if app.bootstrap_capture_complete():
                writer.write_raw("bootstrapHandshakeComplete", [f"timeoutSeconds={timeout_seconds}"])
                stopping["stop"] = True
                try:
                    app.disconnect()
                except Exception:
                    pass
                return
            time.sleep(0.1)
        writer.write_raw("bootstrapHandshakeTimeout", [f"timeoutSeconds={timeout_seconds}"])
        stopping["stop"] = True
        try:
            app.disconnect()
        except Exception:
            pass

    watcher = threading.Thread(target=_watchdog, daemon=True)
    watcher.start()
    try:
        app.run()
    except Exception as e:
        writer.write_raw("bootstrapRunError", [repr(e)])
        return 3
    finally:
        watcher.join(timeout=1.0)
    return 0 if app.bootstrap_capture_complete() else 2


def _run_observer_runtime_loop(
    *,
    app: Observer,
    writer: ObserverFanoutWriter,
    stopping: Dict[str, bool],
) -> int:
    def _poller() -> None:
        while not stopping["stop"]:
            try:
                if app.isConnected():
                    app._poll()
            except Exception as exc:
                writer.write_raw("pollLoopError", [repr(exc)])
            time.sleep(0.2)

    poller = threading.Thread(target=_poller, daemon=True)
    poller.start()
    try:
        app.run()
    except Exception as exc:
        writer.write_raw("runtimeRunError", [repr(exc)])
        return 3
    finally:
        stopping["stop"] = True
        poller.join(timeout=1.0)
    return 0


def main() -> int:
    args = parse_args()
    repo_root = (
        Path(str(args.repo_root).strip()).resolve()
        if str(args.repo_root).strip()
        else Path.cwd().resolve()
    )

    d_override = str(getattr(args, "day_utc", "") or "").strip()
    if d_override != "":
        if len(d_override) != 10 or d_override[4] != "-" or d_override[7] != "-":
            print(f"FATAL: BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d_override!r}", file=sys.stderr)
            return 2
        target_day = d_override
    else:
        target_day = day_utc()

    broker = {"client_id": int(args.client_id), "environment": str(args.environment), "name": "INTERACTIVE_BROKERS"}
    try:
        governed_identity = resolve_governed_execution_identity_v1(
            repo_root=repo_root,
            environment=str(args.environment).strip().upper(),
            sleeve_id=str(args.sleeve_id).strip().upper(),
        )
        governed_roots = resolve_governed_paper_execution_roots(
            repo_root=repo_root,
            environment=governed_identity.environment,
            ib_account=governed_identity.account_id,
            sleeve_id=governed_identity.sleeve_id,
        )
    except ValueError as exc:
        print(f"FATAL: CANONICAL_OBSERVER_IDENTITY_UNRESOLVED:{exc}", file=sys.stderr)
        return 2

    source_session_id = (
        f"ib_observer:{governed_identity.environment}:{governed_identity.sleeve_id}:"
        f"{governed_identity.client_id_observer}:{target_day}:{uuid.uuid4().hex[:12]}"
    )
    legacy_log_root: Path | None = None
    canonical_writer = BrokerRawEvidenceJournalWriterV1(
        repo_root=repo_root,
        execution_root_path=Path(governed_roots.execution_root_path),
        day_utc=target_day,
        environment=governed_identity.environment,
        sleeve_id=governed_identity.sleeve_id,
        source_adapter_name="ib_execution_observer_v1",
        source_session_id=source_session_id,
        source_path=f"observer://interactive_brokers/{governed_identity.sleeve_id}/{governed_identity.environment}",
    )
    legacy_writer: JsonlRawWriter | None = None
    if not bool(args.disable_legacy_log):
        try:
            legacy_log_root = _resolve_log_root(
                repo_root=repo_root,
                truth_root_value=args.truth_root,
                log_root_value=args.log_root,
            )
        except SystemExit as exc:
            print(str(exc), file=sys.stderr)
            canonical_writer.close()
            return 2
        legacy_writer = JsonlRawWriter(
            log_path=(legacy_log_root / target_day / "broker_event_log.v1.jsonl"),
            broker=broker,
        )
    writer = ObserverFanoutWriter(
        repo_root=repo_root,
        execution_root_path=Path(governed_roots.execution_root_path),
        environment=governed_identity.environment,
        sleeve_id=governed_identity.sleeve_id,
        canonical_writer=canonical_writer,
        broker=broker,
        legacy_writer=legacy_writer,
        legacy_log_root=legacy_log_root,
        fixed_day_utc=d_override,
    )

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

    if bool(args.bootstrap_handshake_only):
        try:
            return _run_bootstrap_handshake_loop(
                app=app,
                writer=writer,
                timeout_seconds=int(args.handshake_timeout_seconds),
                stopping=stopping,
            )
        finally:
            writer.write_raw("stopped", ["stopped()"])
            writer.close()

    try:
        return _run_observer_runtime_loop(app=app, writer=writer, stopping=stopping)
    finally:
        writer.write_raw("stopped", ["stopped()"])
        writer.close()


if __name__ == "__main__":
    raise SystemExit(main())
