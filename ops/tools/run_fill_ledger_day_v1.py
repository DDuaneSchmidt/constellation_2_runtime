#!/usr/bin/env python3
"""
run_fill_ledger_day_v1.py

Bundle 1: Fill Ledger Spine v1 (deterministic aggregation).

Reads:
  constellation_2/runtime/truth/execution_stream_v1/<DAY>/*.execution_event_stream_record.v1.json
  constellation_2/runtime/truth/execution_evidence_v1/submissions/<DAY>/<submission_id>/
    - broker_submission_record.v2.json
    - equity_order_plan.v1.json (for order_qty)
    - execution_event_record.v1.json (lineage if needed)

Writes (immutable):
  constellation_2/runtime/truth/fill_ledger_v1/<DAY>/<submission_id>.fill_ledger.v1.json

Fail-closed:
- missing submissions day dir
- stream records fail schema parse
- order_qty missing
- overfill (filled_qty > order_qty)
- missing lineage
- schema validation failure
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

STREAM_ROOT = (TRUTH / "execution_stream_v1").resolve()
SUB_ROOT = (TRUTH / "execution_evidence_v1/submissions").resolve()
OUT_ROOT = (TRUTH / "fill_ledger_v1").resolve()

SCHEMA_LEDGER = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/fill_ledger.v1.schema.json"


def _git_sha() -> str:
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception:
        return "UNKNOWN"


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _read_json_obj(p: Path) -> Dict[str, Any]:
    o = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(o, dict):
        raise RuntimeError(f"TOP_LEVEL_NOT_OBJECT: {p}")
    return o


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


def _list_stream_files(day: str) -> List[Path]:
    d = (STREAM_ROOT / day).resolve()
    if not d.exists() or not d.is_dir():
        return []
    return sorted([p for p in d.iterdir() if p.is_file() and p.name.endswith(".execution_event_stream_record.v1.json")])


def _list_submission_dirs(day: str) -> List[Path]:
    d = (SUB_ROOT / day).resolve()
    if not d.exists() or not d.is_dir():
        raise RuntimeError(f"MISSING_SUBMISSIONS_DAY_DIR: {d}")
    return sorted([p for p in d.iterdir() if p.is_dir()])


def _order_qty_from_submission(subdir: Path) -> int:
    # Equity only in our current bundle slice: qty_shares from equity_order_plan.v1.json
    p = (subdir / "equity_order_plan.v1.json").resolve()
    if not p.exists():
        raise RuntimeError(f"MISSING_EQUITY_ORDER_PLAN_V1_FOR_ORDER_QTY: {p}")
    o = _read_json_obj(p)
    qty = o.get("qty_shares")
    if not isinstance(qty, int) or qty <= 0:
        raise RuntimeError("EQUITY_ORDER_PLAN_QTY_INVALID")
    return int(qty)


def _lineage_from_submission(subdir: Path) -> Tuple[str, str, str, str]:
    """
    Returns (engine_id, source_intent_id, intent_sha256, binding_hash)
    """
    bsr_p = (subdir / "broker_submission_record.v2.json").resolve()
    if not bsr_p.exists():
        raise RuntimeError(f"MISSING_BROKER_SUBMISSION_RECORD: {bsr_p}")
    bsr = _read_json_obj(bsr_p)
    binding_hash = str(bsr.get("binding_hash") or "").strip()
    if not binding_hash:
        raise RuntimeError("BINDING_HASH_MISSING")

    engine_id = ""
    source_intent_id = ""
    intent_sha256 = ""

    evt_p = (subdir / "execution_event_record.v1.json").resolve()
    if evt_p.exists():
        evt = _read_json_obj(evt_p)
        engine_id = str(evt.get("engine_id") or "").strip()
        source_intent_id = str(evt.get("source_intent_id") or "").strip()
        intent_sha256 = str(evt.get("intent_sha256") or "").strip()

    if not engine_id or not source_intent_id or not intent_sha256:
        plan_p = (subdir / "equity_order_plan.v1.json").resolve()
        if plan_p.exists():
            plan = _read_json_obj(plan_p)
            engine_id = engine_id or str(plan.get("engine_id") or "").strip()
            source_intent_id = source_intent_id or str(plan.get("source_intent_id") or "").strip()
            intent_sha256 = intent_sha256 or str(plan.get("intent_sha256") or "").strip()

    if not (engine_id and source_intent_id and intent_sha256):
        raise RuntimeError("LINEAGE_MISSING_IN_SUBMISSION_DIR")

    return engine_id, source_intent_id, intent_sha256, binding_hash


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_fill_ledger_day_v1")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    produced_utc = f"{day}T00:00:00Z"

    # Load stream records once, group by submission_id
    stream_files = _list_stream_files(day)
    by_sub: Dict[str, List[Dict[str, Any]]] = {}
    for p in stream_files:
        o = _read_json_obj(p)
        subid = str(o.get("submission_id") or "").strip()
        if not subid:
            raise RuntimeError(f"STREAM_RECORD_MISSING_SUBMISSION_ID: {p}")
        by_sub.setdefault(subid, []).append(o)

    wrote = 0
    for subdir in _list_submission_dirs(day):
        submission_id = subdir.name

        # Skip if no stream data; still write a ledger with UNKNOWN state (audit-friendly)
        events = by_sub.get(submission_id, [])
        order_qty = _order_qty_from_submission(subdir)
        engine_id, source_intent_id, intent_sha256, binding_hash = _lineage_from_submission(subdir)

        # Deterministic sort by (event_time_utc, canonical_json_hash)
        def key(ev: Dict[str, Any]) -> Tuple[str, str]:
            return (str(ev.get("event_time_utc") or ""), str(ev.get("canonical_json_hash") or ""))

        events_sorted = sorted(events, key=key)

        filled_qty = 0
        # Weighted average price = sum(q*p)/sum(q), deterministic via Decimal
        num = Decimal("0")
        den = Decimal("0")
        status_latest = "UNKNOWN"

        event_hashes: List[str] = []
        for ev in events_sorted:
            event_hashes.append(str(ev.get("canonical_json_hash") or "").strip() or "0"*64)
            os = ev.get("order_state") if isinstance(ev.get("order_state"), dict) else {}
            st = str(os.get("status") or "UNKNOWN").strip().upper()
            if st:
                status_latest = st
            fill = ev.get("fill") if isinstance(ev.get("fill"), dict) else {}
            q = fill.get("fill_qty", 0)
            pr = fill.get("fill_price", "0")
            if isinstance(q, int) and q > 0:
                filled_qty += int(q)
                den += Decimal(str(q))
                num += (Decimal(str(q)) * Decimal(str(pr)))

        if filled_qty > order_qty:
            raise RuntimeError(f"OVERFILL_FAIL_CLOSED: submission_id={submission_id} filled_qty={filled_qty} order_qty={order_qty}")

        remaining = order_qty - filled_qty
        avg = "0"
        if den > 0:
            avg = str((num / den).quantize(Decimal("0.0001")))

        lifecycle = "UNKNOWN"
        st = status_latest.upper()
        if st in ("CANCELLED", "REJECTED", "INACTIVE"):
            lifecycle = st
        elif filled_qty == 0:
            lifecycle = "OPEN"
        elif filled_qty < order_qty:
            lifecycle = "PARTIALLY_FILLED"
        else:
            lifecycle = "FILLED"

        ledger: Dict[str, Any] = {
            "schema_id": "C2_FILL_LEDGER_V1",
            "schema_version": 1,
            "produced_utc": produced_utc,
            "day_utc": day,
            "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_fill_ledger_day_v1.py"},
            "status": "OK",
            "reason_codes": [],
            "submission_id": submission_id,
            "binding_hash": binding_hash,
            "engine_id": engine_id,
            "source_intent_id": source_intent_id,
            "intent_sha256": intent_sha256,
            "order_qty": int(order_qty),
            "filled_qty": int(filled_qty),
            "remaining_qty": int(remaining),
            "avg_fill_price_weighted": avg,
            "lifecycle_status": lifecycle,
            "event_hashes": event_hashes,
            "canonical_json_hash": "",
        }
        ledger["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(ledger)

        validate_against_repo_schema_v1(ledger, REPO_ROOT, SCHEMA_LEDGER)

        out_path = (OUT_ROOT / day / f"{submission_id}.fill_ledger.v1.json").resolve()
        payload = canonical_json_bytes_v1(ledger) + b"\n"
        _write_immutable(out_path, payload)
        wrote += 1

    print(f"OK: FILL_LEDGER_WRITTEN day={day} wrote={wrote} stream_records={len(stream_files)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
