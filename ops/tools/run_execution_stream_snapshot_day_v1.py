#!/usr/bin/env python3
"""
run_execution_stream_snapshot_day_v1.py

Bundle 1 / Execution Observer (pull snapshot) v1 — hardened.

Writes:
  constellation_2/runtime/truth/execution_stream_v1/<DAY>/
    <event_hash>.execution_event_stream_record.v1.json

On failure, writes governed failure artifact:
  constellation_2/runtime/truth/execution_stream_v1/failures/<DAY>/failure.json
Schema:
  governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_evidence_failure.v1.schema.json

Fail-closed:
- ib_insync missing
- broker connect/pull failure (timeouts, disconnects)
- submissions day dir missing
- any broker order cannot be attributed to known submission evidence
- schema validation failure
- overwrite attempts with different bytes
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_STREAM = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_event_stream_record.v1.schema.json"
SCHEMA_FAILURE = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_evidence_failure.v1.schema.json"

SUBMISSIONS_DAY_ROOT = (TRUTH / "execution_evidence_v1" / "submissions").resolve()
OUT_ROOT = (TRUTH / "execution_stream_v1").resolve()
FAIL_ROOT = (OUT_ROOT / "failures").resolve()


def _git_sha() -> str:
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception:
        return "UNKNOWN"


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _sha256_dir_deterministic(root: Path) -> str:
    if not root.exists() or not root.is_dir():
        return _sha256_bytes(b"")
    items: List[Tuple[str, str]] = []
    for p in root.rglob("*"):
        if p.is_file():
            rel = str(p.relative_to(root)).replace("\\", "/")
            items.append((rel, _sha256_file(p)))
    items.sort(key=lambda x: x[0])
    h = hashlib.sha256()
    for rel, fsha in items:
        h.update(rel.encode("utf-8"))
        h.update(b"\n")
        h.update(fsha.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def _read_json_obj(p: Path) -> Dict[str, Any]:
    o = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(o, dict):
        raise RuntimeError(f"TOP_LEVEL_NOT_OBJECT: {p}")
    return o


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_immutable(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = path.read_bytes()
        if _sha256_bytes(existing) == _sha256_bytes(payload):
            return
        raise RuntimeError(f"REFUSE_OVERWRITE_DIFFERENT_BYTES: {path}")
    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    tmp.write_bytes(payload)
    import os
    os.replace(tmp, path)


def _event_hash_key(fields: List[str]) -> str:
    h = hashlib.sha256()
    for f in fields:
        h.update(f.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def _write_failure(
    *,
    day: str,
    produced_utc: str,
    code: str,
    message: str,
    details: Dict[str, Any],
    input_manifest: List[Dict[str, Any]],
    attempted_outputs: List[Dict[str, Any]],
) -> None:
    fail_obj: Dict[str, Any] = {
        "schema_id": "C2_EXECUTION_EVIDENCE_FAILURE_V1",
        "schema_version": 1,
        "produced_utc": produced_utc,
        "day_utc": day,
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_execution_stream_snapshot_day_v1.py"},
        "status": "FAIL_CORRUPT_INPUTS",
        "reason_codes": [code],
        "input_manifest": list(input_manifest),
        "failure": {
            "code": code,
            "message": message,
            "details": dict(details),
            "attempted_outputs": list(attempted_outputs),
        },
    }
    validate_against_repo_schema_v1(fail_obj, REPO_ROOT, SCHEMA_FAILURE)
    payload = canonical_json_bytes_v1(fail_obj) + b"\n"
    out_path = (FAIL_ROOT / day / "failure.json").resolve()
    _write_immutable(out_path, payload)


def _list_submission_dirs(day: str) -> List[Path]:
    d = (SUBMISSIONS_DAY_ROOT / day).resolve()
    if not d.exists() or not d.is_dir():
        raise RuntimeError(f"MISSING_SUBMISSIONS_DAY_DIR: {d}")
    return sorted([p for p in d.iterdir() if p.is_dir()])


def _build_orderid_index(day: str) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for subdir in _list_submission_dirs(day):
        bsr_p = subdir / "broker_submission_record.v2.json"
        if not bsr_p.exists():
            continue
        bsr = _read_json_obj(bsr_p)

        submission_id = str(bsr.get("submission_id") or "").strip() or subdir.name
        binding_hash = str(bsr.get("binding_hash") or "").strip()

        broker = bsr.get("broker") if isinstance(bsr.get("broker"), dict) else {}
        env = str(broker.get("environment") or "PAPER").strip()

        engine_id = ""
        source_intent_id = ""
        intent_sha256 = ""

        evt_p = subdir / "execution_event_record.v1.json"
        if evt_p.exists():
            evt = _read_json_obj(evt_p)
            engine_id = str(evt.get("engine_id") or "").strip()
            source_intent_id = str(evt.get("source_intent_id") or "").strip()
            intent_sha256 = str(evt.get("intent_sha256") or "").strip()

        if not engine_id or not source_intent_id or not intent_sha256:
            plan_p = subdir / "equity_order_plan.v1.json"
            if plan_p.exists():
                plan = _read_json_obj(plan_p)
                engine_id = engine_id or str(plan.get("engine_id") or "").strip()
                source_intent_id = source_intent_id or str(plan.get("source_intent_id") or "").strip()
                intent_sha256 = intent_sha256 or str(plan.get("intent_sha256") or "").strip()

        broker_ids = bsr.get("broker_ids") if isinstance(bsr.get("broker_ids"), dict) else {}
        order_id = broker_ids.get("order_id")
        perm_id = broker_ids.get("perm_id")

        base = {
            "submission_id": submission_id,
            "binding_hash": binding_hash,
            "engine_id": engine_id,
            "source_intent_id": source_intent_id,
            "intent_sha256": intent_sha256,
            "broker_env": env,
        }
        if isinstance(order_id, int) and order_id >= 0:
            idx[f"order_id:{order_id}"] = base
        if isinstance(perm_id, int) and perm_id >= 0:
            idx[f"perm_id:{perm_id}"] = base
    return idx


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_execution_stream_snapshot_day_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--ib_host", default="127.0.0.1")
    ap.add_argument("--ib_port", type=int, default=4002)
    ap.add_argument("--ib_client_id", type=int, default=7)
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    produced_utc = f"{day}T00:00:00Z"
    observed_at = _now_iso_z()

    out_day = (OUT_ROOT / day).resolve()
    out_day.mkdir(parents=True, exist_ok=True)
    attempted_outputs = [{"path": str(out_day), "sha256": None}]
    input_manifest: List[Dict[str, Any]] = []

    # Inputs: phaseD submissions root + day dir hash
    day_dir = (SUBMISSIONS_DAY_ROOT / day).resolve()
    subs_root_sha = _sha256_dir_deterministic(SUBMISSIONS_DAY_ROOT)
    input_manifest.append({"type": "phaseD_submissions_root", "path": str(SUBMISSIONS_DAY_ROOT), "sha256": subs_root_sha, "day_utc": None, "producer": "phaseD_submissions_root"})
    input_manifest.append({"type": "phaseD_submission_dir", "path": str(day_dir), "sha256": _sha256_dir_deterministic(day_dir), "day_utc": day, "producer": "phaseD_submission_dir"})

    try:
        idx = _build_orderid_index(day)
    except Exception as e:
        _write_failure(
            day=day,
            produced_utc=produced_utc,
            code="EXEC_STREAM_MISSING_SUBMISSIONS_DAY_DIR",
            message="Missing or unreadable submissions day dir",
            details={"error": str(e), "day_dir": str(day_dir)},
            input_manifest=input_manifest,
            attempted_outputs=attempted_outputs,
        )
        print(f"FAIL: {e}", file=sys.stderr)  # type: ignore[name-defined]
        return 2

    # Import ib_insync
    try:
        from ib_insync import IB  # type: ignore
    except Exception as e:  # noqa: BLE001
        _write_failure(
            day=day,
            produced_utc=produced_utc,
            code="EXEC_STREAM_IB_INSYNC_IMPORT_FAILED",
            message="ib_insync import failed",
            details={"error": repr(e)},
            input_manifest=input_manifest,
            attempted_outputs=attempted_outputs,
        )
        print(f"FAIL: ib_insync import failed: {e!r}")
        return 2

    # Connect with bounded retry/backoff
    ib = IB()
    last_err: Optional[str] = None
    for backoff in (1, 2, 5):
        try:
            ok = ib.connect(str(args.ib_host), int(args.ib_port), clientId=int(args.ib_client_id))
            if ok:
                last_err = None
                break
            last_err = "connect returned false"
        except Exception as e:  # noqa: BLE001
            last_err = repr(e)
        time.sleep(backoff)

    if last_err is not None:
        _write_failure(
            day=day,
            produced_utc=produced_utc,
            code="EXEC_STREAM_BROKER_CONNECT_FAILED",
            message="Broker connect failed (timeouts/disconnect)",
            details={"host": str(args.ib_host), "port": int(args.ib_port), "client_id": int(args.ib_client_id), "error": last_err},
            input_manifest=input_manifest,
            attempted_outputs=attempted_outputs,
        )
        print(f"FAIL: BROKER_CONNECT_FAILED: {last_err}")
        return 2

    # Pull broker state
    try:
        trades = list(ib.trades())
        executions = list(ib.executions())
    except Exception as e:  # noqa: BLE001
        ib.disconnect()
        _write_failure(
            day=day,
            produced_utc=produced_utc,
            code="EXEC_STREAM_BROKER_PULL_FAILED",
            message="Broker pull failed",
            details={"error": repr(e)},
            input_manifest=input_manifest,
            attempted_outputs=attempted_outputs,
        )
        print(f"FAIL: BROKER_PULL_FAILED: {e!r}")
        return 2

    ib.disconnect()

    wrote = 0

    def write_record(
        *,
        event_type: str,
        order_id: Optional[int],
        perm_id: Optional[int],
        order_state: Dict[str, Any],
        fill: Dict[str, Any],
        raw: Dict[str, Any],
    ) -> None:
        nonlocal wrote

        key = None
        if isinstance(perm_id, int):
            key = f"perm_id:{perm_id}"
        elif isinstance(order_id, int):
            key = f"order_id:{order_id}"

        if (not key) or (key not in idx):
            raise RuntimeError(f"UNATTRIBUTABLE_BROKER_EVENT: event_type={event_type} order_id={order_id} perm_id={perm_id}")

        meta = idx[key]
        submission_id = meta["submission_id"]
        binding_hash = meta["binding_hash"]
        engine_id = meta["engine_id"]
        source_intent_id = meta["source_intent_id"]
        intent_sha256 = meta["intent_sha256"]

        if not (engine_id and source_intent_id and intent_sha256):
            raise RuntimeError(f"MISSING_LINEAGE_FOR_SUBMISSION: submission_id={submission_id}")

        event_hash = _event_hash_key(
            [
                day,
                event_type,
                submission_id,
                str(order_id) if order_id is not None else "",
                str(perm_id) if perm_id is not None else "",
                str(order_state.get("status") or ""),
                str(order_state.get("filled_qty") or 0),
                str(fill.get("fill_qty") or 0),
                str(fill.get("fill_price") or "0"),
                str(raw.get("event_time_utc") or observed_at),
            ]
        )

        rec: Dict[str, Any] = {
            "schema_id": "C2_EXECUTION_EVENT_STREAM_RECORD_V1",
            "schema_version": 1,
            "produced_utc": produced_utc,
            "day_utc": day,
            "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_execution_stream_snapshot_day_v1.py"},
            "status": "OK",
            "reason_codes": [],
            "submission_id": submission_id,
            "binding_hash": binding_hash,
            "engine_id": engine_id,
            "source_intent_id": source_intent_id,
            "intent_sha256": intent_sha256,
            "broker": {"name": "INTERACTIVE_BROKERS", "environment": str(meta["broker_env"] or "PAPER")},
            "event_type": event_type,
            "event_time_utc": str(raw.get("event_time_utc") or observed_at),
            "observed_at_utc": observed_at,
            "broker_ids": {"order_id": order_id, "perm_id": perm_id},
            "order_state": order_state,
            "fill": fill,
            "canonical_json_hash": "",
        }
        rec["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(rec)
        validate_against_repo_schema_v1(rec, REPO_ROOT, SCHEMA_STREAM)

        payload = canonical_json_bytes_v1(rec) + b"\n"
        out_path = (out_day / f"{event_hash}.execution_event_stream_record.v1.json").resolve()
        _write_immutable(out_path, payload)
        wrote += 1

    # Trades -> ORDER_STATUS
    try:
        for t in trades:
            os = getattr(t, "orderStatus", None)
            o = getattr(t, "order", None)
            st = str(getattr(os, "status", "UNKNOWN") or "UNKNOWN").upper()
            filled = int(getattr(os, "filled", 0) or 0)
            remaining = int(getattr(os, "remaining", 0) or 0)
            avg = getattr(os, "avgFillPrice", 0) or 0
            avg_s = str(Decimal(str(avg)))
            order_id = getattr(o, "orderId", None)
            perm_id = getattr(o, "permId", None)

            order_state = {"status": st, "filled_qty": filled, "remaining_qty": remaining, "avg_fill_price": avg_s}
            fill = {"fill_qty": 0, "fill_price": "0", "commission": "0", "currency": "USD"}
            raw = {"event_time_utc": observed_at, "trade": str(t)}
            write_record(
                event_type="ORDER_STATUS",
                order_id=order_id if isinstance(order_id, int) else None,
                perm_id=perm_id if isinstance(perm_id, int) else None,
                order_state=order_state,
                fill=fill,
                raw=raw,
            )
    except Exception as e:
        _write_failure(
            day=day,
            produced_utc=produced_utc,
            code="EXEC_STREAM_TRADE_PARSE_OR_ATTRIBUTION_FAILED",
            message="Trade parse/attribution failed (day purity violation or malformed trade)",
            details={"error": str(e)},
            input_manifest=input_manifest,
            attempted_outputs=attempted_outputs,
        )
        print(f"FAIL: {e}")
        return 2

    # Executions -> EXEC_DETAILS (fills)
    try:
        for ex in executions:
            exec_obj = getattr(ex, "execution", None) or ex
            order_id = getattr(exec_obj, "orderId", None)
            perm_id = getattr(exec_obj, "permId", None)
            shares = getattr(exec_obj, "shares", 0) or 0
            price = getattr(exec_obj, "price", 0) or 0
            time_s = getattr(exec_obj, "time", None)
            event_time = str(time_s) if time_s is not None else observed_at

            fill = {"fill_qty": int(shares), "fill_price": str(Decimal(str(price))), "commission": "0", "currency": "USD"}
            order_state = {"status": "UNKNOWN", "filled_qty": int(shares), "remaining_qty": 0, "avg_fill_price": str(Decimal(str(price)))}
            raw = {"event_time_utc": event_time, "execution": str(exec_obj)}
            write_record(
                event_type="EXEC_DETAILS",
                order_id=order_id if isinstance(order_id, int) else None,
                perm_id=perm_id if isinstance(perm_id, int) else None,
                order_state=order_state,
                fill=fill,
                raw=raw,
            )
    except Exception as e:
        _write_failure(
            day=day,
            produced_utc=produced_utc,
            code="EXEC_STREAM_EXECUTION_PARSE_OR_ATTRIBUTION_FAILED",
            message="Execution parse/attribution failed (day purity violation or malformed execution)",
            details={"error": str(e)},
            input_manifest=input_manifest,
            attempted_outputs=attempted_outputs,
        )
        print(f"FAIL: {e}")
        return 2

    print(f"OK: EXECUTION_STREAM_SNAPSHOT_WRITTEN day={day} wrote={wrote} out_dir={str(out_day)}")
    return 0


if __name__ == "__main__":
    import sys
    raise SystemExit(main())
